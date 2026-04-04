from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from typing import Any, Tuple
from uuid import uuid4

from core.utils.upstash_client import get_async_redis, upstash_configured

logger = logging.getLogger("volo.evm.nonce")

_NONCE_KEY_TTL_SECONDS = 20 * 60
_OP_IDEMPOTENCY_TTL_SECONDS = 60
_REDIS_RETRY_ATTEMPTS = 3
_REDIS_RETRY_BASE_SECONDS = 0.05
_NONCE_RECONCILE_SECONDS = 30


async def _fetch_pending_async(w3: Any, sender: str) -> int:
    checksum_sender = w3.to_checksum_address(sender)
    result = w3.eth.get_transaction_count(checksum_sender, "pending")
    if inspect.isawaitable(result):
        result = await result
    return int(result)


def _as_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        if isinstance(value, bytes):
            return int(value.decode("utf-8"))
        return int(value)  # type: ignore[arg-type]
    except (ValueError, TypeError, UnicodeDecodeError):
        return None


def _get_int_env(key: str, default: int) -> int:
    raw = str(os.getenv(key, "")).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


class AsyncUpstashNonceManager:
    def __init__(self, client: Any, prefix: str = "nonce") -> None:
        self._upstash = client
        self._prefix = prefix.strip() or "nonce"
        self._nonce_ttl_seconds = _get_int_env(
            "NONCE_MANAGER_TTL_SECONDS", _NONCE_KEY_TTL_SECONDS
        )
        self._reconcile_interval_seconds = _get_int_env(
            "NONCE_MANAGER_RECONCILE_SECONDS", _NONCE_RECONCILE_SECONDS
        )
        self._redis_retry_attempts = _get_int_env(
            "NONCE_MANAGER_REDIS_RETRY_ATTEMPTS", _REDIS_RETRY_ATTEMPTS
        )
        self._op_ttl_seconds = _get_int_env(
            "NONCE_MANAGER_OP_TTL_SECONDS", _OP_IDEMPOTENCY_TTL_SECONDS
        )
        self._fast_allocate_script = """
        local key, sync_key, op_key = KEYS[1], KEYS[2], KEYS[3]
        local now, ttl, op_ttl, reconcile_after = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
        local prior = redis.call("GET", op_key)
        if prior then
            local prior_num = tonumber(prior)
            if prior_num ~= nil then
                redis.call("EXPIRE", op_key, op_ttl)
                return prior_num
            end
        end
        local current = redis.call("GET", key)
        if not current then
            return -1
        end
        local current_num = tonumber(current)
        if current_num == nil then
            return -1
        end
        local last_sync = tonumber(redis.call("GET", sync_key) or "0")
        if last_sync == 0 or ((now - last_sync) >= reconcile_after) then
            return -1
        end
        local next_nonce = current_num
        redis.call("SET", key, next_nonce + 1, "EX", ttl)
        redis.call("SET", sync_key, now, "EX", ttl)
        redis.call("SET", op_key, next_nonce, "EX", op_ttl)
        return next_nonce
        """

        self._reconcile_allocate_script = """
        local key, sync_key, op_key = KEYS[1], KEYS[2], KEYS[3]
        local rpc_pending, now, ttl, op_ttl = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
        local prior = redis.call("GET", op_key)
        if prior then
            local prior_num = tonumber(prior)
            if prior_num ~= nil then
                redis.call("EXPIRE", op_key, op_ttl)
                return prior_num
            end
        end
        local current = redis.call("GET", key)
        local next_nonce
        if not current then
            next_nonce = rpc_pending
        else
            local current_num = tonumber(current)
            if current_num == nil then
                current_num = rpc_pending
            end
            next_nonce = math.max(current_num, rpc_pending)
        end
        redis.call("SET", key, next_nonce + 1, "EX", ttl)
        redis.call("SET", sync_key, now, "EX", ttl)
        redis.call("SET", op_key, next_nonce, "EX", op_ttl)
        return next_nonce
        """

        self._fast_peek_script = """
        local key, sync_key, op_key = KEYS[1], KEYS[2], KEYS[3]
        local now, ttl, op_ttl, reconcile_after = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
        local prior = redis.call("GET", op_key)
        if prior then
            local prior_num = tonumber(prior)
            if prior_num ~= nil then
                redis.call("EXPIRE", op_key, op_ttl)
                return prior_num
            end
        end
        local current = redis.call("GET", key)
        if not current then
            return -1
        end
        local current_num = tonumber(current)
        if current_num == nil then
            return -1
        end
        local last_sync = tonumber(redis.call("GET", sync_key) or "0")
        if last_sync == 0 or ((now - last_sync) >= reconcile_after) then
            return -1
        end
        redis.call("SET", key, current_num, "EX", ttl)
        redis.call("SET", sync_key, now, "EX", ttl)
        redis.call("SET", op_key, current_num, "EX", op_ttl)
        return current_num
        """

        self._reconcile_peek_script = """
        local key, sync_key, op_key = KEYS[1], KEYS[2], KEYS[3]
        local rpc_pending, now, ttl, op_ttl = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
        local prior = redis.call("GET", op_key)
        if prior then
            local prior_num = tonumber(prior)
            if prior_num ~= nil then
                redis.call("EXPIRE", op_key, op_ttl)
                return prior_num
            end
        end
        local current = redis.call("GET", key)
        local next_nonce = rpc_pending
        if current then
            local current_num = tonumber(current)
            if current_num ~= nil and current_num > next_nonce then
                next_nonce = current_num
            end
        end
        redis.call("SET", key, next_nonce, "EX", ttl)
        redis.call("SET", sync_key, now, "EX", ttl)
        redis.call("SET", op_key, next_nonce, "EX", op_ttl)
        return next_nonce
        """

        self._reset_script = """
        local key, sync_key, op_key = KEYS[1], KEYS[2], KEYS[3]
        local pending, now, ttl, op_ttl = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
        local prior = redis.call("GET", op_key)
        if prior then
            local prior_num = tonumber(prior)
            if prior_num ~= nil then
                redis.call("EXPIRE", op_key, op_ttl)
                return prior_num
            end
        end
        redis.call("SET", key, pending, "EX", ttl)
        redis.call("SET", sync_key, now, "EX", ttl)
        redis.call("SET", op_key, pending, "EX", op_ttl)
        return pending
        """

        self._rollback_script = """
        local key, sync_key, op_key = KEYS[1], KEYS[2], KEYS[3]
        local failed_nonce, rpc_pending, now, ttl, op_ttl = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
        local prior = redis.call("GET", op_key)
        if prior then
            local prior_num = tonumber(prior)
            if prior_num ~= nil then
                redis.call("EXPIRE", op_key, op_ttl)
                return prior_num
            end
        end
        local current = redis.call("GET", key)
        if not current then
            redis.call("SET", key, rpc_pending, "EX", ttl)
            redis.call("SET", sync_key, now, "EX", ttl)
            redis.call("SET", op_key, rpc_pending, "EX", op_ttl)
            return rpc_pending
        end
        local current_num = tonumber(current)
        if current_num == nil then
            current_num = rpc_pending
        end
        if current_num == (failed_nonce + 1) and failed_nonce >= rpc_pending then
            redis.call("SET", key, failed_nonce, "EX", ttl)
            redis.call("SET", sync_key, now, "EX", ttl)
            redis.call("SET", op_key, failed_nonce, "EX", op_ttl)
            return failed_nonce
        end
        local floor_nonce = math.max(current_num, rpc_pending)
        redis.call("SET", key, floor_nonce, "EX", ttl)
        redis.call("SET", sync_key, now, "EX", ttl)
        redis.call("SET", op_key, floor_nonce, "EX", op_ttl)
        return floor_nonce
        """

    def _cache_key(self, sender: str, chain_id: int) -> Tuple[str, int]:
        return (sender.strip().lower(), int(chain_id))

    def _key(self, sender: str, chain_id: int) -> str:
        sender_norm, chain = self._cache_key(sender, chain_id)
        return f"{self._prefix}:{sender_norm}:{chain}"

    def _operation_key(self, key: str, operation_id: str) -> str:
        return f"{key}:op:{operation_id}"

    def _sync_key(self, key: str) -> str:
        return f"{key}:sync"

    def _now_seconds(self) -> int:
        return int(time.time())

    async def _eval_nonce(self, script: str, *, key: str, args: list[int]) -> int:
        operation_id = uuid4().hex
        sync_key = self._sync_key(key)
        op_key = self._operation_key(key, operation_id)
        last_error: Exception | None = None
        for attempt in range(1, self._redis_retry_attempts + 1):
            try:
                result = await self._upstash.eval(
                    script,
                    keys=[key, sync_key, op_key],
                    args=[str(int(arg)) for arg in args],
                )
                parsed = _as_int(result)
                if parsed is None:
                    raise RuntimeError(
                        "Redis nonce script returned a non-integer result."
                    )
                return parsed
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "nonce_redis_error key=%s op_key=%s attempt=%s error=%s",
                    key,
                    op_key,
                    attempt,
                    exc,
                )
                if attempt >= self._redis_retry_attempts:
                    break
                await asyncio.sleep(_REDIS_RETRY_BASE_SECONDS * (2 ** (attempt - 1)))
        assert last_error is not None
        raise RuntimeError(
            "Redis nonce allocation is unavailable. Restore Redis before retrying."
        ) from last_error

    async def peek(self, sender: str, chain_id: int, w3: Any) -> int:
        key = self._key(sender, chain_id)
        now = self._now_seconds()
        nonce = await self._eval_nonce(
            self._fast_peek_script,
            key=key,
            args=[
                now,
                self._nonce_ttl_seconds,
                self._op_ttl_seconds,
                self._reconcile_interval_seconds,
            ],
        )
        pending = None
        if nonce < 0:
            pending = await _fetch_pending_async(w3, sender)
            nonce = await self._eval_nonce(
                self._reconcile_peek_script,
                key=key,
                args=[pending, now, self._nonce_ttl_seconds, self._op_ttl_seconds],
            )
        logger.info(
            "nonce_peek sender=%s chain_id=%s key=%s rpc_pending=%s next_nonce=%s ttl_seconds=%s",
            sender.strip().lower(),
            int(chain_id),
            key,
            pending,
            nonce,
            self._nonce_ttl_seconds,
        )
        return nonce

    async def pending(self, sender: str, chain_id: int, w3: Any) -> int:
        return await _fetch_pending_async(w3, sender)

    async def allocate(self, sender: str, chain_id: int, w3: Any) -> int:
        key = self._key(sender, chain_id)
        now = self._now_seconds()
        nonce = await self._eval_nonce(
            self._fast_allocate_script,
            key=key,
            args=[
                now,
                self._nonce_ttl_seconds,
                self._op_ttl_seconds,
                self._reconcile_interval_seconds,
            ],
        )
        pending = None
        if nonce < 0:
            pending = await _fetch_pending_async(w3, sender)
            nonce = await self._eval_nonce(
                self._reconcile_allocate_script,
                key=key,
                args=[pending, now, self._nonce_ttl_seconds, self._op_ttl_seconds],
            )
        logger.info(
            "nonce_allocate sender=%s chain_id=%s key=%s rpc_pending=%s allocated_nonce=%s ttl_seconds=%s",
            sender.strip().lower(),
            int(chain_id),
            key,
            pending,
            nonce,
            self._nonce_ttl_seconds,
        )
        return nonce

    async def allocate_safe(self, sender: str, chain_id: int, w3: Any) -> int:
        return await self.allocate(sender, chain_id, w3)

    async def reset(self, sender: str, chain_id: int, w3: Any) -> int:
        pending = await _fetch_pending_async(w3, sender)
        key = self._key(sender, chain_id)
        nonce = await self._eval_nonce(
            self._reset_script,
            key=key,
            args=[
                pending,
                self._now_seconds(),
                self._nonce_ttl_seconds,
                self._op_ttl_seconds,
            ],
        )
        logger.info(
            "nonce_reset sender=%s chain_id=%s key=%s rpc_pending=%s reset_nonce=%s ttl_seconds=%s",
            sender.strip().lower(),
            int(chain_id),
            key,
            pending,
            nonce,
            self._nonce_ttl_seconds,
        )
        return nonce

    async def rollback(self, sender: str, chain_id: int, nonce: int, w3: Any) -> int:
        pending = await _fetch_pending_async(w3, sender)
        key = self._key(sender, chain_id)
        next_nonce = await self._eval_nonce(
            self._rollback_script,
            key=key,
            args=[
                int(nonce),
                pending,
                self._now_seconds(),
                self._nonce_ttl_seconds,
                self._op_ttl_seconds,
            ],
        )
        logger.info(
            "nonce_rollback sender=%s chain_id=%s key=%s failed_nonce=%s rpc_pending=%s next_nonce=%s ttl_seconds=%s",
            sender.strip().lower(),
            int(chain_id),
            key,
            int(nonce),
            pending,
            next_nonce,
            self._nonce_ttl_seconds,
        )
        return next_nonce


_async_nonce_manager: AsyncUpstashNonceManager | None = None

_NONCE_ERROR_HINTS = (
    "nonce too low",
    "nonce too high",
    "nonce has already been used",
    "already known",
    "known transaction",
    "replacement transaction underpriced",
    "transaction underpriced",
    "nonce is too low",
    "nonce is too high",
    "nonces are too low",
    "invalid nonce",
)


async def get_async_nonce_manager() -> AsyncUpstashNonceManager:
    global _async_nonce_manager
    if _async_nonce_manager is not None:
        return _async_nonce_manager

    if upstash_configured():
        try:
            client = await get_async_redis()
            if client is not None:
                _async_nonce_manager = AsyncUpstashNonceManager(client)
                return _async_nonce_manager
        except Exception as exc:
            logger.warning("nonce_manager_init_failed error=%s", exc)
            _async_nonce_manager = None

    raise RuntimeError(
        "Redis-backed nonce management is required for safe transaction execution."
    )


def is_nonce_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(hint in msg for hint in _NONCE_ERROR_HINTS)


async def reset_on_error_async(
    exc: Exception,
    sender: str,
    chain_id: int,
    w3: Any,
) -> bool:
    if not is_nonce_error(exc):
        return False
    try:
        mgr = await get_async_nonce_manager()
        await mgr.reset(sender, chain_id, w3)
        return True
    except Exception:
        return False


async def rollback_after_signing_error_async(
    sender: str,
    chain_id: int,
    nonce: int,
    w3: Any,
) -> bool:
    """
    Release a reserved nonce when signing fails before a raw tx exists.
    """
    try:
        mgr = await get_async_nonce_manager()
        await mgr.rollback(sender, chain_id, nonce, w3)
        return True
    except Exception:
        return False
