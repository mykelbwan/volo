from __future__ import annotations

import asyncio
import inspect
import threading
from typing import Any, cast

from web3.exceptions import TimeExhausted

from config.abi import ERC20_ABI
from core.utils.async_resources import get_current_async_resource_scope
from core.utils.errors import NonRetryableError

_SHARED_AIOHTTP_SESSIONS: dict[int, Any] = {}
_SHARED_AIOHTTP_SESSIONS_LOCK = threading.Lock()


async def _get_shared_aiohttp_session() -> Any | None:
    """Return one shared aiohttp session per event loop for AsyncWeb3 providers."""
    try:
        import aiohttp  # type: ignore
    except Exception:
        return None

    loop_id = id(asyncio.get_running_loop())
    with _SHARED_AIOHTTP_SESSIONS_LOCK:
        session = _SHARED_AIOHTTP_SESSIONS.get(loop_id)
        if session is None or getattr(session, "closed", False):
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(
                limit=256,
                limit_per_host=64,
                ttl_dns_cache=300,
                keepalive_timeout=30,
                enable_cleanup_closed=True,
            )
            session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                raise_for_status=False,
            )
            _SHARED_AIOHTTP_SESSIONS[loop_id] = session
        return session


async def close_shared_async_web3_sessions() -> None:
    """Close pooled aiohttp sessions used by AsyncWeb3 providers."""
    with _SHARED_AIOHTTP_SESSIONS_LOCK:
        sessions = list(_SHARED_AIOHTTP_SESSIONS.values())
        _SHARED_AIOHTTP_SESSIONS.clear()
    if not sessions:
        return
    await asyncio.gather(
        *(session.close() for session in sessions if hasattr(session, "close")),
        return_exceptions=True,
    )


def _build_async_web3(rpc_url: str) -> Any:
    """Construct an AsyncWeb3 instance without blocking on network I/O."""
    try:
        from web3 import AsyncWeb3  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Async Web3 is not available. "
        ) from exc

    try:
        from web3.providers.async_rpc import AsyncHTTPProvider  # type: ignore
    except Exception:
        try:
            from web3 import AsyncHTTPProvider  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "Async HTTP provider is not available in web3. "
            ) from exc

    provider = AsyncHTTPProvider(rpc_url)
    scope = get_current_async_resource_scope()
    if scope is not None:
        scope.register_web3_provider(provider)
    return AsyncWeb3(provider)


async def get_shared_async_web3(rpc_url: str) -> Any:
    """Build AsyncWeb3 with a shared aiohttp session for connection reuse."""
    w3 = _build_async_web3(rpc_url)
    cache_async_session = getattr(w3.provider, "cache_async_session", None)
    if callable(cache_async_session):
        try:
            session = await _get_shared_aiohttp_session()
            if session is not None:
                # Reusing one ClientSession per loop preserves keep-alive and avoids
                # allocating a fresh TCP connector for each AsyncWeb3 instance.
                await cache_async_session(session)
        except Exception:
            # Session injection is a best-effort optimization; the provider can
            # still fall back to its native transport if this hook is unavailable.
            pass
    return w3


def make_async_web3(rpc_url: str):
    """Return AsyncWeb3 immediately, upgrading to the shared session when possible."""
    w3 = _build_async_web3(rpc_url)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return w3

    cache_async_session = getattr(w3.provider, "cache_async_session", None)
    if callable(cache_async_session):
        async def _attach_shared_session() -> None:
            try:
                session = await _get_shared_aiohttp_session()
                if session is not None:
                    await cache_async_session(session)
            except Exception:
                pass

        # Best-effort attach keeps the sync factory backward compatible while
        # avoiding blocking the caller just to prepare pooled transport state.
        loop.create_task(_attach_shared_session())
    return w3


async def async_get_allowance(
    w3: Any, token_address: str, owner: str, spender: str
) -> int:
    contract = w3.eth.contract(
        address=w3.to_checksum_address(token_address),
        abi=ERC20_ABI,
    )
    result = await contract.functions.allowance(
        w3.to_checksum_address(owner),
        w3.to_checksum_address(spender),
    ).call()
    try:
        return int(cast(Any, result))
    except Exception:
        return 0


async def async_broadcast_evm(w3: Any, signed_hex: str) -> str:
    raw = bytes.fromhex(signed_hex.removeprefix("0x"))
    tx_hash = await w3.eth.send_raw_transaction(raw)
    return tx_hash.hex()


async def async_await_evm_receipt(w3: Any, tx_hash: str, timeout: int = 300) -> None:
    try:
        receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
    except TimeExhausted:
        raise NonRetryableError(
            "The transaction is still pending and taking longer than expected. "
            "It may still go through — please wait a bit and try again."
        )
    if getattr(receipt, "status", None) == 0:
        raise NonRetryableError(
            "The transaction was rejected on-chain. Please try again in a moment."
        )


async def async_get_gas_price(w3: Any, chain_id: int | None = None) -> int:
    if chain_id is not None:
        try:
            from wallet_service.evm.gas_price import gas_price_cache

            return await gas_price_cache.get_wei(chain_id=chain_id)
        except Exception:
            # Fall back to direct RPC on cache failure.
            pass

    result = w3.eth.gas_price
    if callable(result):
        result = result()
    if inspect.isawaitable(result):
        result = await result
    return int(cast(Any, result))
