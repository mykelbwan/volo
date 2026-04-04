from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from pymongo import ASCENDING, IndexModel
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from core.database.mongodb import MongoDB
from core.token_security.models import ResolvedToken, _cache_key

logger = logging.getLogger(__name__)

_COLLECTION_NAME = "token_security_cache"

# Documents are fresh for 6 days; between 6 and 7 days they are stale (returned
# but a background refresh is triggered).  After 7 days MongoDB TTL deletes them.
_FRESH_TTL_DAYS: int = 120
_HARD_TTL_DAYS: int = 121

# Type alias for the refresh callback injected by TokenSecurityManager.
RefreshCallback = Callable[[str, int], None]


class TokenSecurityCache:
    def __init__(
        self,
        refresh_callback: Optional[RefreshCallback] = None,
    ) -> None:
        self._col: Collection = MongoDB.get_collection(_COLLECTION_NAME)
        self._refresh_callback = refresh_callback
        self._refresh_lock = threading.Lock()
        self._refresh_inflight: set[str] = set()
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self._col.create_indexes(
                [
                    IndexModel(
                        [("_cache_key", ASCENDING)],
                        unique=True,
                        name="idx_cache_key",
                    ),
                    IndexModel(
                        [("expires_at", ASCENDING)],
                        expireAfterSeconds=0,
                        name="idx_ttl",
                    ),
                    # Secondary index for chain-level cache sweeps (e.g. invalidate all
                    # tokens on a chain after a mass exploit)
                    IndexModel(
                        [("chain_id", ASCENDING), ("symbol", ASCENDING)],
                        name="idx_chain_symbol",
                    ),
                ]
            )
        except Exception as exc:
            # Non-fatal — log and continue; stale indexes won't break reads/writes.
            logger.warning("TokenSecurityCache: index creation warning: %s", exc)

    # ── Public API ─────────────────────────────────────────────────────────────

    def get(self, symbol: str, chain_id: int) -> Optional[ResolvedToken]:
        key = _cache_key(symbol, chain_id)
        doc = self._col.find_one({"_cache_key": key})

        if doc is None:
            logger.debug("Cache MISS: %s", key)
            return None

        try:
            token = ResolvedToken.from_cache_doc(doc)
        except Exception as exc:
            # Corrupted document — treat as a miss and clean it up.
            logger.warning(
                "TokenSecurityCache: failed to deserialise doc for %s: %s. "
                "Deleting corrupted entry.",
                key,
                exc,
            )
            self._col.delete_one({"_cache_key": key})
            return None

        if not token.is_safe or token.has_critical_flags:
            logger.warning(
                "TokenSecurityCache: refusing unsafe cached token for %s. "
                "Deleting entry and forcing live resolution.",
                key,
            )
            self._col.delete_one({"_cache_key": key})
            return None

        # ── Freshness check ────────────────────────────────────────────────
        age = _age_days(token.last_checked)

        if age <= _FRESH_TTL_DAYS:
            logger.debug("Cache FRESH HIT: %s (age=%.1f days)", key, age)
            return token

        if age <= _HARD_TTL_DAYS:
            # Stale but still in the collection (MongoDB TTL hasn't fired yet).
            logger.info(
                "Cache STALE HIT: %s (age=%.1f days). "
                "Returning cached result and scheduling background refresh.",
                key,
                age,
            )
            self._schedule_refresh(symbol, chain_id)
            return token

        # Document is past the hard TTL but MongoDB hasn't deleted it yet
        # (TTL background job runs ~every 60 seconds).  Treat as a miss and
        # delete immediately to avoid serving very stale data.
        logger.debug(
            "Cache EXPIRED (not yet deleted by MongoDB TTL): %s (age=%.1f days). "
            "Deleting now.",
            key,
            age,
        )
        self._col.delete_one({"_cache_key": key})
        return None

    def set(self, token: ResolvedToken) -> None:
        key = _cache_key(token.symbol, token.chain_id)
        doc = token.to_cache_doc()

        try:
            self._col.replace_one(
                {"_cache_key": key},
                doc,
                upsert=True,
            )
            logger.debug(
                "Cache SET: %s (tier=%s, safe=%s)",
                key,
                token.security_tier.value,
                token.is_safe,
            )
        except DuplicateKeyError:
            # Race condition between two concurrent resolvers — harmless.
            logger.debug("Cache SET DuplicateKeyError (race condition) for %s", key)
        except Exception as exc:
            # Cache writes are best-effort: a failure here must never break
            # the resolution pipeline.
            logger.warning("TokenSecurityCache: failed to write %s: %s", key, exc)

    def invalidate(self, symbol: str, chain_id: int) -> bool:
        key = _cache_key(symbol, chain_id)
        result = self._col.delete_one({"_cache_key": key})
        deleted = result.deleted_count > 0
        if deleted:
            logger.info("Cache INVALIDATED: %s", key)
        return deleted

    def invalidate_chain(self, chain_id: int) -> int:
        result = self._col.delete_many({"chain_id": chain_id})
        count = result.deleted_count
        logger.info(
            "Cache INVALIDATE CHAIN: chain_id=%d, removed %d document(s).",
            chain_id,
            count,
        )
        return count

    def get_stats(self) -> dict:
        total = self._col.count_documents({})

        tier_pipeline = [{"$group": {"_id": "$security_tier", "count": {"$sum": 1}}}]
        by_tier = {
            doc["_id"]: doc["count"] for doc in self._col.aggregate(tier_pipeline)
        }

        chain_pipeline = [{"$group": {"_id": "$chain_id", "count": {"$sum": 1}}}]
        by_chain = {
            str(doc["_id"]): doc["count"] for doc in self._col.aggregate(chain_pipeline)
        }

        return {"total": total, "by_tier": by_tier, "by_chain_id": by_chain}

    # ── Background refresh ─────────────────────────────────────────────────────

    def _schedule_refresh(self, symbol: str, chain_id: int) -> None:
        if self._refresh_callback is None:
            return

        callback = self._refresh_callback
        key = _cache_key(symbol, chain_id)

        with self._refresh_lock:
            if key in self._refresh_inflight:
                logger.debug(
                    "Cache background refresh already in flight for %s; skipping.",
                    key,
                )
                return
            self._refresh_inflight.add(key)

        def _target() -> None:
            try:
                logger.info("Cache background refresh started for %s.", key)
                callback(symbol, chain_id)
                logger.info("Cache background refresh completed for %s.", key)
            except Exception as exc:
                logger.warning("Cache background refresh failed for %s: %s", key, exc)
            finally:
                with self._refresh_lock:
                    self._refresh_inflight.discard(key)

        thread = threading.Thread(
            target=_target, name=f"cache-refresh-{key}", daemon=True
        )
        thread.start()


def _age_days(last_checked: datetime) -> float:
    now = datetime.now(tz=timezone.utc)
    if last_checked.tzinfo is None:
        last_checked = last_checked.replace(tzinfo=timezone.utc)
    delta: timedelta = now - last_checked
    return delta.total_seconds() / 86_400.0
