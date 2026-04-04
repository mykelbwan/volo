from __future__ import annotations

import asyncio
import os
from typing import Any, Optional

try:
    from upstash_redis import Redis as UpstashRedis

    _UpstashRedisType = UpstashRedis
except Exception:  # pragma: no cover - optional dependency
    UpstashRedis = None  # type: ignore[assignment, misc]
    _UpstashRedisType = None  # type: ignore[assignment]

_UPSTASH_CLIENT: Optional[Any] = None

# Lazily initialised inside a running event loop so the underlying
# httpx.AsyncClient is always created with a live loop available.
# Double-checked locking prevents duplicate construction under concurrency.

_ASYNC_CLIENT: Optional[Any] = None
_ASYNC_LOCK: Optional[asyncio.Lock] = None


def upstash_configured() -> bool:
    if UpstashRedis is None:
        return False
    url = os.getenv("UPSTASH_REDIS_REST_URL", "").strip()
    token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()
    return bool(url and token)


def get_upstash_client() -> Optional[Any]:
    global _UPSTASH_CLIENT
    if _UPSTASH_CLIENT is not None:
        return _UPSTASH_CLIENT
    if not upstash_configured():
        return None
    if UpstashRedis is None:
        return None
    try:
        url = os.getenv("UPSTASH_REDIS_REST_URL", "").strip()
        token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()
        _UPSTASH_CLIENT = UpstashRedis(url=url, token=token)
        return _UPSTASH_CLIENT
    except Exception:
        _UPSTASH_CLIENT = None
        return None


async def get_async_redis() -> Optional[Any]:
    global _ASYNC_CLIENT, _ASYNC_LOCK

    # Fast path — already initialised.
    if _ASYNC_CLIENT is not None:
        return _ASYNC_CLIENT

    # Lazy-create the lock; safe because asyncio is single-threaded and this
    # assignment is atomic within one event loop.
    if _ASYNC_LOCK is None:
        _ASYNC_LOCK = asyncio.Lock()

    async with _ASYNC_LOCK:
        # Re-check inside the lock — another coroutine may have finished
        # initialisation while we were waiting to acquire it.
        if _ASYNC_CLIENT is not None:
            return _ASYNC_CLIENT

        if not upstash_configured():
            return None

        try:
            from upstash_redis.asyncio import Redis as AsyncRedis  # noqa: PLC0415

            url = os.getenv("UPSTASH_REDIS_REST_URL", "").strip()
            token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()
            _ASYNC_CLIENT = AsyncRedis(url=url, token=token)
            return _ASYNC_CLIENT

        except ImportError:
            return None

        except Exception:
            return None
