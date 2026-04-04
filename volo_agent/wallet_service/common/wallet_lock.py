from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import Any
from uuid import uuid4

from core.utils.upstash_client import get_async_redis, upstash_configured

logger = logging.getLogger("volo.wallet_lock")

# Constants
_DEFAULT_LOCK_TTL_MS = 30_000
_DEFAULT_ACQUIRE_TIMEOUT_MS = 45_000
_DEFAULT_REFRESH_INTERVAL_MS = 10_000
_DEFAULT_BACKOFF_MIN_MS = 50
_DEFAULT_BACKOFF_MAX_MS = 1_000
_DEFAULT_REDIS_RETRY_ATTEMPTS = 3

# Lua Scripts for Atomic Operations (kept compatible with test classifiers)
_RELEASE_LOCK_SCRIPT = """
local key = KEYS[1]
local owner = ARGV[1]
if redis.call("GET", key) == owner then
    return redis.call("DEL", key)
end
return 0
"""

_REFRESH_LOCK_SCRIPT = """
local key = KEYS[1]
local owner = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
if redis.call("GET", key) == owner then
    return redis.call("PEXPIRE", key, ttl_ms)
end
return 0
"""


def _get_int_env(key: str, default: int) -> int:
    try:
        val = int(os.getenv(key, str(default)))
        return val if val > 0 else default
    except (ValueError, TypeError):
        return default


class WalletLock:
    """
    Redis-backed per-wallet execution lock.

    This lock is required for correctness in multi-step EVM flows. It does not
    rely on process-local state, and it fails closed if Redis is unavailable.
    Uses exponential backoff with full jitter for acquisition.
    """

    def __init__(
        self,
        *,
        sender: str,
        chain_id: int,
        ttl_ms: int | None = None,
        acquire_timeout_ms: int | None = None,
    ) -> None:
        sender_normalized = str(sender).strip().lower()
        self._chain_id = int(chain_id)
        self._sender = sender_normalized
        self._key = f"lock:{self._chain_id}:{sender_normalized}"
        self._owner = uuid4().hex

        self._ttl_ms = ttl_ms or _get_int_env(
            "WALLET_LOCK_TTL_MS", _DEFAULT_LOCK_TTL_MS
        )
        self._acquire_timeout_ms = acquire_timeout_ms or _get_int_env(
            "WALLET_LOCK_ACQUIRE_TIMEOUT_MS", _DEFAULT_ACQUIRE_TIMEOUT_MS
        )

        # Refresh interval: default to 1/3 of TTL or 10s max
        refresh_default = min(
            max(self._ttl_ms // 3, 1_000), _DEFAULT_REFRESH_INTERVAL_MS
        )
        configured_refresh_ms = _get_int_env(
            "WALLET_LOCK_REFRESH_INTERVAL_MS", refresh_default
        )
        # Ensure refresh happens before TTL expires
        max_safe_refresh_ms = max(min(self._ttl_ms // 3, self._ttl_ms - 10), 1)
        self._refresh_interval_seconds = (
            min(configured_refresh_ms, max_safe_refresh_ms) / 1000.0
        )

        self._backoff_min_seconds = (
            _get_int_env("WALLET_LOCK_BACKOFF_MIN_MS", _DEFAULT_BACKOFF_MIN_MS) / 1000.0
        )
        self._backoff_max_seconds = (
            _get_int_env("WALLET_LOCK_BACKOFF_MAX_MS", _DEFAULT_BACKOFF_MAX_MS) / 1000.0
        )
        self._redis_retry_attempts = _get_int_env(
            "WALLET_LOCK_REDIS_RETRY_ATTEMPTS",
            _DEFAULT_REDIS_RETRY_ATTEMPTS,
        )

        self._client: Any | None = None
        self._refresh_task: asyncio.Task[None] | None = None
        self._stop_refresh = asyncio.Event()
        self._lost_lock_reason: str | None = None

    async def __aenter__(self) -> WalletLock:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.release()

    async def acquire(self) -> None:
        if not upstash_configured():
            raise RuntimeError(
                "Redis-backed wallet locking is required for safe wallet execution."
            )

        client = await get_async_redis()
        if client is None:
            raise RuntimeError(
                "Redis-backed wallet locking is unavailable. Restore Redis before retrying."
            )
        self._client = client

        loop = asyncio.get_running_loop()
        deadline = loop.time() + (self._acquire_timeout_ms / 1000.0)
        attempt = 0
        self._lost_lock_reason = None

        while True:
            attempt += 1
            try:
                # Primary acquisition attempt
                acquired = await client.set(
                    self._key,
                    self._owner,
                    nx=True,
                    px=self._ttl_ms,
                )
                if acquired:
                    self._mark_acquired(attempt=attempt, recovered=False)
                    return

                # Secondary check: Did we already own it (e.g. retry after network timeout)?
                if await self._is_current_owner():
                    self._mark_acquired(attempt=attempt, recovered=True)
                    return

            except Exception as exc:
                logger.warning(
                    "wallet_lock_acquire_error key=%s owner=%s attempt=%s error=%s",
                    self._key,
                    self._owner,
                    attempt,
                    exc,
                )
                # On error, still check if we managed to set it despite the exception
                try:
                    if await self._is_current_owner():
                        self._mark_acquired(attempt=attempt, recovered=True)
                        return
                except Exception:
                    pass

            if loop.time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for wallet execution lock {self._key} after {attempt} attempts."
                )

            await asyncio.sleep(self._compute_backoff(attempt))

    def _mark_acquired(self, *, attempt: int, recovered: bool) -> None:
        logger.info(
            "wallet_lock_acquired key=%s owner=%s ttl_ms=%s attempt=%s recovered=%s",
            self._key,
            self._owner,
            self._ttl_ms,
            attempt,
            recovered,
        )
        self._lost_lock_reason = None
        self._stop_refresh.clear()
        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def _is_current_owner(self) -> bool:
        if self._client is None:
            return False
        try:
            current = await self._get_value_with_retry()
            if isinstance(current, bytes):
                current = current.decode("utf-8")
            return current == self._owner
        except Exception:
            return False

    async def _get_value_with_retry(self) -> Any:
        client = self._client
        if client is None:
            raise RuntimeError("Wallet lock Redis client is not initialized.")

        last_error: Exception | None = None
        for attempt in range(1, self._redis_retry_attempts + 1):
            try:
                return await client.get(self._key)
            except Exception as exc:
                last_error = exc
                if attempt >= self._redis_retry_attempts:
                    break
                await asyncio.sleep(self._compute_backoff(attempt))
        raise last_error or RuntimeError("Retry loop failed unexpectedly")

    async def release(self) -> None:
        # 1. Stop background refreshing first
        refresh_task = self._refresh_task
        self._refresh_task = None
        self._stop_refresh.set()
        if refresh_task is not None:
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass

        # 2. Atomic release in Redis
        client = self._client
        if client is None:
            return

        released = 0
        try:
            released = await self._eval_with_retry(
                _RELEASE_LOCK_SCRIPT,
                args=[self._owner],
            )
        except Exception as exc:
            logger.warning(
                "wallet_lock_release_error key=%s owner=%s error=%s",
                self._key,
                self._owner,
                exc,
            )
        finally:
            self._client = None

        logger.info(
            "wallet_lock_released key=%s owner=%s released=%s",
            self._key,
            self._owner,
            int(released or 0),
        )

    async def ensure_held(self) -> None:
        if self._lost_lock_reason:
            raise RuntimeError(self._lost_lock_reason)

    async def _refresh_loop(self) -> None:
        try:
            while not self._stop_refresh.is_set():
                try:
                    await asyncio.wait_for(
                        self._stop_refresh.wait(),
                        timeout=self._refresh_interval_seconds,
                    )
                    return  # Stop requested
                except asyncio.TimeoutError:
                    pass

                refreshed = await self._eval_with_retry(
                    _REFRESH_LOCK_SCRIPT,
                    args=[self._owner, str(self._ttl_ms)],
                )
                if int(refreshed or 0) != 1:
                    self._lost_lock_reason = f"Wallet execution lock {self._key} was lost before flow completion."
                    logger.warning(
                        "wallet_lock_lost key=%s owner=%s",
                        self._key,
                        self._owner,
                    )
                    return
                logger.info(
                    "wallet_lock_refreshed key=%s owner=%s ttl_ms=%s",
                    self._key,
                    self._owner,
                    self._ttl_ms,
                )
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._lost_lock_reason = (
                f"Wallet execution lock refresh failed for {self._key}: {exc}"
            )
            logger.warning(
                "wallet_lock_refresh_error key=%s owner=%s error=%s",
                self._key,
                self._owner,
                exc,
            )

    async def _eval_with_retry(self, script: str, *, args: list[str]) -> Any:
        client = self._client
        if client is None:
            raise RuntimeError("Wallet lock Redis client is not initialized.")

        last_error: Exception | None = None
        for attempt in range(1, self._redis_retry_attempts + 1):
            try:
                return await client.eval(script, keys=[self._key], args=args)
            except Exception as exc:
                last_error = exc
                if attempt >= self._redis_retry_attempts:
                    break
                await asyncio.sleep(self._compute_backoff(attempt))
        raise last_error or RuntimeError("Retry loop failed unexpectedly")

    def _compute_backoff(self, attempt: int) -> float:
        # Full Jitter: random.uniform(0, min(cap, base * 2^attempt))
        cap = self._backoff_max_seconds
        base = self._backoff_min_seconds
        # attempt starts at 1, so (2 ** (attempt - 1)) starts at 1
        return random.uniform(0, min(cap, base * (2 ** (attempt - 1))))


def wallet_lock(
    sender: str,
    chain_id: int,
    *,
    ttl_ms: int | None = None,
    acquire_timeout_ms: int | None = None,
) -> WalletLock:
    return WalletLock(
        sender=sender,
        chain_id=chain_id,
        ttl_ms=ttl_ms,
        acquire_timeout_ms=acquire_timeout_ms,
    )
