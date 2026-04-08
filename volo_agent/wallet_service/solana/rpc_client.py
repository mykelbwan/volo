from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

_SHARED_SOLANA_CLIENTS: dict[tuple[int, str], Any] = {}
_SHARED_SOLANA_CLIENTS_LOCK = threading.Lock()
_BLOCKHASH_TTL_SECONDS = 5.0


@dataclass(frozen=True)
class _BlockhashCacheEntry:
    blockhash: Any
    fetched_at: float


_BLOCKHASH_CACHE: dict[str, _BlockhashCacheEntry] = {}
_BLOCKHASH_CACHE_LOCK = threading.Lock()
_BLOCKHASH_IN_FLIGHT: dict[str, Future[Any]] = {}


async def get_shared_solana_client(rpc_url: str) -> Any:
    from solana.rpc.async_api import AsyncClient as SolanaClient

    normalized_rpc_url = str(rpc_url).strip()
    cache_key = (id(asyncio.get_running_loop()), normalized_rpc_url)
    with _SHARED_SOLANA_CLIENTS_LOCK:
        client = _SHARED_SOLANA_CLIENTS.get(cache_key)
        if client is None or getattr(client, "_closed", False):
            client = SolanaClient(normalized_rpc_url)
            _SHARED_SOLANA_CLIENTS[cache_key] = client
        return client


async def close_shared_solana_clients() -> None:
    with _SHARED_SOLANA_CLIENTS_LOCK:
        clients = list(_SHARED_SOLANA_CLIENTS.values())
        _SHARED_SOLANA_CLIENTS.clear()
    if not clients:
        return
    await asyncio.gather(
        *(client.close() for client in clients if hasattr(client, "close")),
        return_exceptions=True,
    )


def invalidate_cached_blockhash(rpc_url: str) -> None:
    with _BLOCKHASH_CACHE_LOCK:
        _BLOCKHASH_CACHE.pop(str(rpc_url).strip(), None)


async def get_cached_latest_blockhash(rpc_url: str) -> Any:
    normalized_rpc_url = str(rpc_url).strip()
    with _BLOCKHASH_CACHE_LOCK:
        cached = _BLOCKHASH_CACHE.get(normalized_rpc_url)
        now = time.monotonic()
        if cached is not None and (now - cached.fetched_at) < _BLOCKHASH_TTL_SECONDS:
            return cached.blockhash
        in_flight = _BLOCKHASH_IN_FLIGHT.get(normalized_rpc_url)
        if in_flight is None:
            # Share one refresh per RPC URL so concurrent builders do not all
            # stampede the same node when the TTL window expires together.
            in_flight = Future()
            _BLOCKHASH_IN_FLIGHT[normalized_rpc_url] = in_flight
            is_owner = True
        else:
            is_owner = False

    if not is_owner:
        return await asyncio.shield(asyncio.wrap_future(in_flight))

    try:
        client = await get_shared_solana_client(normalized_rpc_url)
        latest_blockhash = await client.get_latest_blockhash()
        blockhash_value = getattr(latest_blockhash, "value", None)
        recent_blockhash = getattr(blockhash_value, "blockhash", None)
        if recent_blockhash is not None:
            with _BLOCKHASH_CACHE_LOCK:
                _BLOCKHASH_CACHE[normalized_rpc_url] = _BlockhashCacheEntry(
                    blockhash=recent_blockhash,
                    fetched_at=time.monotonic(),
                )
        if not in_flight.done():
            in_flight.set_result(recent_blockhash)
        return recent_blockhash
    except BaseException as exc:
        if not in_flight.done():
            in_flight.set_exception(exc)
        raise
    finally:
        with _BLOCKHASH_CACHE_LOCK:
            if _BLOCKHASH_IN_FLIGHT.get(normalized_rpc_url) is in_flight:
                _BLOCKHASH_IN_FLIGHT.pop(normalized_rpc_url, None)
