from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass
from typing import Optional, cast

from config.abi import ERC20_ABI
from config.chains import get_chain_by_id
from config.solana_chains import (
    fetch_solana_token_decimals,
    get_solana_chain_by_id,
    is_solana_chain_id,
)
from core.utils.evm_async import get_shared_async_web3

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 60.0
_LOOKUP_CACHE: dict[tuple[str, str, int], tuple[float, Optional[int]]] = {}
_LOOKUP_CACHE_LOCK = threading.Lock()
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
_NATIVE_SENTINEL = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

try:
    from core.token_security.token_db import (
        TokenRegistryEntry,
        get_async_token_registry,
        get_token_registry,
    )
except Exception as exc:  # pragma: no cover - env not configured in some contexts
    logger.debug("registry_lookup: token registry unavailable: %s", exc)
    TokenRegistryEntry = None  # type: ignore[assignment]
    get_async_token_registry = None  # type: ignore[assignment]
    get_token_registry = None  # type: ignore[assignment]


@dataclass(frozen=True)
class _OnchainTokenMetadata:
    address: str
    decimals: int
    symbol: Optional[str]


def _cache_get(kind: str, value: str, chain_id: int) -> Optional[int] | object:
    key = (kind, value, chain_id)
    now = time.monotonic()
    with _LOOKUP_CACHE_LOCK:
        cached = _LOOKUP_CACHE.get(key)
        if cached is None:
            return _CACHE_MISS
        expires_at, result = cached
        if expires_at <= now:
            _LOOKUP_CACHE.pop(key, None)
            return _CACHE_MISS
        return result


def _cache_set(kind: str, value: str, chain_id: int, result: Optional[int]) -> None:
    key = (kind, value, chain_id)
    expires_at = time.monotonic() + _CACHE_TTL_SECONDS
    with _LOOKUP_CACHE_LOCK:
        _LOOKUP_CACHE[key] = (expires_at, result)


def _address_cache_key(address: str) -> str:
    return address.strip().lower()


def get_native_decimals(chain_id: int) -> int:
    """Return the decimals for the native token on the given chain."""
    if is_solana_chain_id(chain_id):
        return 9
    # All supported EVM chains use 18 decimals for native.
    return 18


def _symbol_cache_key(symbol: str) -> str:
    return symbol.strip().upper()


def _trigger_background_onchain_lookup(address: str, chain_id: int) -> None:
    """Fire a non-blocking background task to populate the registry for a missing token."""
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            # Fire and forget; use force_onchain=True to bypass the cached None miss.
            loop.create_task(
                get_registry_decimals_by_address_async(
                    address, chain_id, force_onchain=True
                )
            )
    except RuntimeError:
        # No running event loop (e.g. sync CLI script).
        pass


_CACHE_MISS = object()


def _looks_like_native_alias(address: str) -> bool:
    lowered = address.strip().lower()
    return lowered in {_ZERO_ADDRESS, _NATIVE_SENTINEL}


def _normalize_symbol(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.split(b"\x00", 1)[0].decode("utf-8", errors="ignore")
    text = str(value).replace("\x00", "").strip().upper()
    if not text:
        return None
    return text


def _registry_lookup_by_address(address: str, chain_id: int) -> Optional[int]:
    if get_token_registry is None:
        return None
    try:
        entry = get_token_registry().get_by_address(address, chain_id)
        return entry.decimals if entry is not None else None
    except Exception as exc:
        logger.debug(
            "registry_lookup: failed address lookup %s on chain %s: %s",
            address,
            chain_id,
            exc,
        )
        return None


async def _registry_lookup_by_address_async(
    address: str, chain_id: int
) -> Optional[int]:
    if get_async_token_registry is None:
        return None
    try:
        entry = await get_async_token_registry().get_by_address(address, chain_id)
        return entry.decimals if entry is not None else None
    except Exception as exc:
        logger.debug(
            "registry_lookup: failed async address lookup %s on chain %s: %s",
            address,
            chain_id,
            exc,
        )
        return None


async def _fetch_onchain_token_metadata_async(
    address: str, chain_id: int
) -> Optional[_OnchainTokenMetadata]:
    if _looks_like_native_alias(address):
        return None

    # Handle Solana-family chains
    if is_solana_chain_id(chain_id):
        try:
            chain = get_solana_chain_by_id(chain_id)
            decimals = await fetch_solana_token_decimals(address, chain.rpc_url)
            # Placeholder symbol since fetch_solana_token_decimals doesn't fetch metadata yet
            return _OnchainTokenMetadata(
                address=address,
                decimals=decimals,
                symbol=None,
            )
        except Exception as exc:
            logger.debug(
                "registry_lookup: on-chain Solana decimals lookup failed for %s: %s",
                address,
                exc,
            )
            return None

    # Handle EVM-family chains
    try:
        chain = get_chain_by_id(chain_id)
    except Exception as exc:
        logger.debug(
            "registry_lookup: no EVM chain RPC for %s (chain=%s): %s",
            address,
            chain_id,
            exc,
        )
        return None

    rpc_url = str(chain.rpc_url or "").strip()
    if not rpc_url:
        return None

    try:
        w3 = await get_shared_async_web3(rpc_url)
        checksum_address = w3.to_checksum_address(address)
        contract = w3.eth.contract(address=checksum_address, abi=ERC20_ABI)
        decimals_raw = await contract.functions.decimals().call()
        decimals = int(decimals_raw)
        if decimals < 0 or decimals > 255:
            return None

        symbol: Optional[str] = None
        try:
            symbol_raw = await contract.functions.symbol().call()
            symbol = _normalize_symbol(symbol_raw)
        except Exception:
            symbol = None

        return _OnchainTokenMetadata(
            address=checksum_address,
            decimals=decimals,
            symbol=symbol,
        )
    except Exception as exc:
        logger.debug(
            "registry_lookup: on-chain EVM decimals lookup failed for %s on chain %s: %s",
            address,
            chain_id,
            exc,
        )
        return None


async def _upsert_onchain_metadata_async(
    metadata: _OnchainTokenMetadata,
    chain_id: int,
) -> None:
    if get_async_token_registry is None or TokenRegistryEntry is None:
        return

    # If symbol is missing, use a unique address-based placeholder.
    # symbol is part of the unique _reg_key index.
    symbol = metadata.symbol or f"T-{metadata.address[-6:].upper()}"

    try:
        # Check chain family for name resolution
        if is_solana_chain_id(chain_id):
            chain_obj = get_solana_chain_by_id(chain_id)
            chain_name = chain_obj.name
        else:
            chain_obj = get_chain_by_id(chain_id)
            chain_name = chain_obj.name

        registry = get_async_token_registry()

        existing = await registry.get(symbol, chain_id)
        if existing is not None:
            existing_addr = str(existing.address or "").strip().lower()
            if existing_addr and existing_addr != metadata.address.strip().lower():
                logger.warning(
                    "registry_lookup: symbol collision while upserting fallback "
                    "token metadata symbol=%s chain_id=%s existing=%s new=%s",
                    symbol,
                    chain_id,
                    existing.address,
                    metadata.address,
                )
                return

        entry = TokenRegistryEntry(
            symbol=symbol,
            chain_name=chain_name,
            chain_id=chain_id,
            address=metadata.address,
            decimals=int(metadata.decimals),
            aliases=[symbol.lower()],
            source="onchain_fallback",
        )
        await registry.upsert(entry)
    except Exception as exc:
        logger.debug(
            "registry_lookup: failed to upsert fallback metadata for %s on chain %s: %s",
            metadata.address,
            chain_id,
            exc,
        )


def get_registry_decimals_by_address(address: str, chain_id: int) -> Optional[int]:
    key = _address_cache_key(address)
    cached = _cache_get("address", key, chain_id)
    if cached is not _CACHE_MISS:
        return cast(Optional[int], cached)

    result = _registry_lookup_by_address(address, chain_id)
    if result is None:
        # Trigger on-chain fetch in the background to avoid blocking.
        _trigger_background_onchain_lookup(address, chain_id)
        # We DO NOT cache the None result here yet; we let the background task
        # complete and populate the cache with the real on-chain value.
    else:
        _cache_set("address", key, chain_id, result)

    return result


def get_registry_decimals_by_symbol(symbol: str, chain_id: int) -> Optional[int]:
    if get_token_registry is None:
        return None
    key = _symbol_cache_key(symbol)
    cached = _cache_get("symbol", key, chain_id)
    if cached is not _CACHE_MISS:
        return cast(Optional[int], cached)
    try:
        registry = get_token_registry()
        entry = registry.get(symbol, chain_id) or registry.get_by_alias(
            symbol, chain_id
        )
        result = entry.decimals if entry is not None else None
        _cache_set("symbol", key, chain_id, result)
        return result
    except Exception as exc:
        logger.debug(
            "registry_lookup: failed symbol lookup %s on chain %s: %s",
            symbol,
            chain_id,
            exc,
        )
        return None


async def get_registry_decimals_by_address_async(
    address: str, chain_id: int, force_onchain: bool = False
) -> Optional[int]:
    key = _address_cache_key(address)
    if not force_onchain:
        cached = _cache_get("address", key, chain_id)
        if cached is not _CACHE_MISS:
            return cast(Optional[int], cached)

    result = await _registry_lookup_by_address_async(address, chain_id)
    if result is None:
        metadata = await _fetch_onchain_token_metadata_async(address, chain_id)
        if metadata is not None:
            result = int(metadata.decimals)
            await _upsert_onchain_metadata_async(metadata, chain_id)

    _cache_set("address", key, chain_id, result)
    return result


async def get_registry_decimals_by_symbol_async(
    symbol: str, chain_id: int
) -> Optional[int]:
    if get_async_token_registry is None:
        return None
    key = _symbol_cache_key(symbol)
    cached = _cache_get("symbol", key, chain_id)
    if cached is not _CACHE_MISS:
        return cast(Optional[int], cached)
    try:
        registry = get_async_token_registry()
        entry = await registry.get(symbol, chain_id)
        if entry is None:
            entry = await registry.get_by_alias(symbol, chain_id)
        result = entry.decimals if entry is not None else None
        _cache_set("symbol", key, chain_id, result)
        return result
    except Exception as exc:
        logger.debug(
            "registry_lookup: failed async symbol lookup %s on chain %s: %s",
            symbol,
            chain_id,
            exc,
        )
        return None
