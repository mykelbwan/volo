from __future__ import annotations

import asyncio
import inspect
import logging
from datetime import datetime, timezone
from typing import Awaitable, Optional, Protocol, cast

import httpx

from config.chains import get_chain_by_name
from config.solana_chains import get_solana_chain
from core.database.mongodb_async import AsyncMongoDB
from core.volume.pricing import VolumeWatcherAccess, resolve_prices
from core.volume.redis_keys import (
    VOLUME_SCAN_PATTERN,
    bucket_to_datetime,
    derive_sibling_keys,
    parse_volume_key,
)

logger = logging.getLogger(__name__)

_COLLECTION = "volume_aggregates"
_DEFAULT_FLUSH_INTERVAL_SECONDS = 5 * 60  # 5 minutes
_PRICE_HTTP_TIMEOUT = 10.0


class _RedisPipelineLike(Protocol):
    def get(self, key: str) -> object: ...
    def delete(self, key: str) -> object: ...
    def exec(self) -> Awaitable[object]: ...


def _restore_chain_name(value: str) -> str:
    return str(value or "").strip().replace("_", " ")


def _is_testnet_chain(chain: str) -> bool:
    restored = _restore_chain_name(chain).lower()
    if not restored:
        return False

    try:
        return bool(get_chain_by_name(restored).is_testnet)
    except KeyError:
        pass

    solana_candidates = {restored, restored.replace(" ", "-")}
    for candidate in solana_candidates:
        try:
            return bool(get_solana_chain(candidate).is_testnet)
        except KeyError:
            continue
    return False


async def _ensure_indexes() -> None:
    from pymongo import ASCENDING, IndexModel

    col = AsyncMongoDB.get_collection(_COLLECTION)
    try:
        await col.create_indexes(
            [
                # Primary upsert filter — must be unique.
                IndexModel(
                    [
                        ("execution_type", ASCENDING),
                        ("chain", ASCENDING),
                        ("token", ASCENDING),
                        ("hour_bucket", ASCENDING),
                    ],
                    unique=True,
                    name="idx_vol_bucket",
                ),
                # Time-range queries for dashboards and rollups.
                IndexModel(
                    [("timestamp", ASCENDING)],
                    name="idx_vol_ts",
                ),
                # Per-type analytics (swap volume over time, bridge volume, etc.).
                IndexModel(
                    [("execution_type", ASCENDING), ("timestamp", ASCENDING)],
                    name="idx_vol_type_ts",
                ),
                # Per-chain analytics.
                IndexModel(
                    [("chain", ASCENDING), ("timestamp", ASCENDING)],
                    name="idx_vol_chain_ts",
                ),
            ]
        )
        logger.debug("[VOLUME FLUSH] MongoDB indexes ensured on %r.", _COLLECTION)
    except Exception as exc:
        # Non-fatal — stale indexes do not break reads or writes.
        logger.warning("[VOLUME FLUSH] Index creation warning: %s", exc)


async def _scan_primary_keys(redis: object) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    cursor: int = 0
    while True:
        cursor, batch = await redis.scan(cursor, match=VOLUME_SCAN_PATTERN, count=200)  # type: ignore[attr-defined]
        if batch:
            for key in batch:
                if key in seen:
                    continue
                seen.add(key)
                keys.append(key)
        if cursor == 0:
            break
    return keys


async def _atomic_getdel(redis: object, key: str) -> object | None:
    getdel = getattr(redis, "getdel", None)
    if callable(getdel):
        getdel_result = getdel(key)
        if inspect.isawaitable(getdel_result):
            return await cast(Awaitable[object | None], getdel_result)
        return cast(object | None, getdel_result)

    pipe_factory = getattr(redis, "multi", None) or getattr(redis, "pipeline", None)
    if not callable(pipe_factory):
        raise RuntimeError("Redis client does not support atomic get+delete operations.")

    pipe = cast(_RedisPipelineLike, pipe_factory())
    pipe.get(key)
    pipe.delete(key)
    result = await pipe.exec()
    if isinstance(result, list) and result:
        return result[0]
    return None


async def _resolve_prices(
    tokens: set[str],
    watcher: VolumeWatcherAccess,
    http_client: httpx.AsyncClient,
) -> dict[str, float]:
    return await resolve_prices(
        tokens,
        price_cache=watcher.price_cache,
        http_client=http_client,
    )


async def _flush_once(redis: object, watcher: VolumeWatcherAccess) -> int:
    primary_keys = await _scan_primary_keys(redis)
    if not primary_keys:
        return 0

    # ── Parse keys, skip invalid ones ────────────────────────────────────────
    valid: list[tuple[str, dict[str, str], bool]] = []
    for key in primary_keys:
        meta = parse_volume_key(key)
        if meta:
            valid.append((key, meta, _is_testnet_chain(meta["chain"])))

    if not valid:
        return 0

    #  Register all tokens for ongoing price polling
    mainnet_tokens = {
        meta["token"].upper() for _, meta, is_testnet in valid if not is_testnet
    }
    if mainnet_tokens:
        watcher.register_volume_symbols(list(mainnet_tokens))

    #  Resolve prices in one batch pass
    prices: dict[str, float] = {}
    if mainnet_tokens:
        async with httpx.AsyncClient(timeout=_PRICE_HTTP_TIMEOUT) as http_client:
            prices = await _resolve_prices(mainnet_tokens, watcher, http_client)

    #  Per-key: GETDEL → MongoDB upsert
    col = AsyncMongoDB.get_collection(_COLLECTION)
    now = datetime.now(timezone.utc)
    written = 0

    for primary_key, meta, is_testnet in valid:
        try:
            _usd_key, count_key = derive_sibling_keys(primary_key)

            # Atomically read-and-delete the primary volume key.
            # Any INCR that races in after this point creates a fresh key.
            try:
                volume_raw = await _atomic_getdel(redis, primary_key)
            except Exception:
                logger.warning(
                    "[VOLUME FLUSH] Atomic read-delete unavailable for %s",
                    primary_key,
                )
                continue

            if volume_raw is None:
                # Already flushed by a concurrent instance or key expired.
                continue

            try:
                normalized_volume = float(
                    cast(float | int | str | bytes | bytearray, volume_raw)
                )
            except (ValueError, TypeError):
                logger.warning(
                    "[VOLUME FLUSH] Unparseable volume value for %s: %r",
                    primary_key,
                    volume_raw,
                )
                continue

            if normalized_volume <= 0:
                # Clean up the count sibling and skip.
                try:
                    await redis.delete(count_key)  # type: ignore[attr-defined]
                except Exception:
                    pass
                continue

            # Read-and-delete the execution count sibling.
            try:
                count_raw = await _atomic_getdel(redis, count_key)
            except Exception:
                count_raw = None

            try:
                execution_count = (
                    int(cast(int | str | bytes | bytearray, count_raw))
                    if count_raw
                    else 0
                )
            except (ValueError, TypeError):
                execution_count = 0

            # Compute USD volume if we have a price.
            token_upper = meta["token"].upper()
            price: Optional[float] = None if is_testnet else prices.get(token_upper)
            usd_volume: Optional[float] = (
                round(normalized_volume * price, 8) if price is not None else None
            )

            # Parse the hour bucket into a UTC timestamp.
            try:
                bucket_ts = bucket_to_datetime(meta["hour_bucket"])
            except ValueError:
                logger.warning(
                    "[VOLUME FLUSH] Invalid hour_bucket in key %s", primary_key
                )
                continue

            # Build the MongoDB update.
            # $inc    — accumulate volume/count across multiple flush cycles
            #           within the same hour bucket (flusher runs every 5 min).
            # $set    — keep last_flushed_at and timestamp current.
            # $setOnInsert — write immutable fields only on document creation.
            inc_fields: dict[str, float | int] = {
                "normalized_volume": normalized_volume,
                "execution_count": execution_count,
            }
            if usd_volume is not None:
                inc_fields["usd_volume"] = usd_volume

            await col.update_one(
                {
                    "execution_type": meta["exec_type"],
                    "chain": meta["chain"],
                    "token": token_upper,
                    "hour_bucket": meta["hour_bucket"],
                },
                {
                    "$inc": inc_fields,
                    "$set": {
                        "last_flushed_at": now,
                        "timestamp": bucket_ts,
                    },
                    "$setOnInsert": {
                        "execution_type": meta["exec_type"],
                        "chain": meta["chain"],
                        "token": token_upper,
                        "hour_bucket": meta["hour_bucket"],
                    },
                },
                upsert=True,
            )
            written += 1

        except Exception as exc:
            logger.error(
                "[VOLUME FLUSH] Failed to flush key %s: %s",
                primary_key,
                exc,
                exc_info=False,
            )

    return written


async def run_volume_flusher(
    watcher: VolumeWatcherAccess,
    interval_seconds: int = _DEFAULT_FLUSH_INTERVAL_SECONDS,
) -> None:
    from core.utils.upstash_client import get_async_redis

    redis = await get_async_redis()
    if redis is None:
        logger.warning(
            "[VOLUME FLUSH] Redis not configured "
            "(UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN missing) "
            "— volume flusher disabled."
        )
        return

    try:
        await _ensure_indexes()
    except Exception as exc:
        logger.warning("[VOLUME FLUSH] Could not ensure indexes: %s", exc)

    logger.info(
        "[VOLUME FLUSH] Volume flusher started (interval=%ds).",
        interval_seconds,
    )

    while True:
        try:
            await asyncio.sleep(interval_seconds)
            written = await _flush_once(redis, watcher)
            if written:
                logger.info(
                    "[VOLUME FLUSH] Flushed %d volume bucket(s) to MongoDB.",
                    written,
                )
            else:
                logger.debug("[VOLUME FLUSH] Flush cycle: no volume keys found.")

        except asyncio.CancelledError:
            logger.info("[VOLUME FLUSH] Volume flusher cancelled — shutting down.")
            break

        except Exception as exc:
            # Never let a flush error kill the task — log and continue.
            logger.error(
                "[VOLUME FLUSH] Unexpected error in flush cycle: %s",
                exc,
                exc_info=True,
            )


def start_volume_flusher(
    watcher: VolumeWatcherAccess,
    interval_seconds: int = _DEFAULT_FLUSH_INTERVAL_SECONDS,
) -> asyncio.Task:
    return asyncio.create_task(
        run_volume_flusher(watcher=watcher, interval_seconds=interval_seconds),
        name="volume_flusher",
    )
