from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from config.chains import get_chain_by_name
from config.solana_chains import (
    get_solana_chain,
    is_solana_chain_id,
    is_solana_network,
)
from core.token_security.dexscreener import DexscreenerError, get_candidates
from core.token_security.registry_lookup import get_registry_decimals_by_address_async
from core.token_security.token_db import get_async_token_registry
from core.utils.async_tools import run_blocking
from intent_hub.registry.token_service import (
    get_address_for_chain,
    get_token_data_async,
)

_LOGGER = logging.getLogger("volo.routing.bridge.token_resolver")


@dataclass(frozen=True)
class ResolvedBridgeToken:
    address: str
    decimals: int
    is_native: bool


async def resolve_bridge_token(
    symbol: str,
    *,
    chain_id: int,
    chain_name: str,
) -> Optional[ResolvedBridgeToken]:
    """
    Resolve a token for a given chain using the existing registry/cache
    and Dexscreener fallback. Returns None on any failure so callers can
    gracefully skip unsupported routes.
    """
    symbol_upper = symbol.strip().upper()
    chain_name_norm = chain_name.strip().lower()

    if is_solana_chain_id(chain_id) or is_solana_network(chain_name_norm):
        return await _resolve_solana_token(symbol_upper, chain_name_norm)

    return await _resolve_evm_token(symbol_upper, chain_name_norm)


async def _resolve_evm_token(
    symbol_upper: str,
    chain_name_norm: str,
) -> Optional[ResolvedBridgeToken]:
    try:
        token_data = await get_token_data_async(symbol_upper, chain_name_norm)
    except Exception as exc:
        _LOGGER.warning(
            "[token_resolver] evm lookup failed for %s on %s: %s",
            symbol_upper,
            chain_name_norm,
            exc,
        )
        return None

    address = get_address_for_chain(token_data, chain_name_norm)
    if not address:
        _LOGGER.info(
            "[token_resolver] no address resolved for %s on %s",
            symbol_upper,
            chain_name_norm,
        )
        return None

    try:
        chain_cfg = get_chain_by_name(chain_name_norm)
    except Exception:
        _LOGGER.warning(
            "[token_resolver] unknown chain %s",
            chain_name_norm,
        )
        return None

    decimals = token_data.get("decimals")
    if decimals is None:
        decimals = await get_registry_decimals_by_address_async(
            address, chain_cfg.chain_id
        )

    if decimals is None:
        _LOGGER.warning(
            "[token_resolver] could not resolve decimals for %s on %s",
            symbol_upper,
            chain_name_norm,
        )
        return None

    is_native = symbol_upper == chain_cfg.native_symbol.upper()

    return ResolvedBridgeToken(
        address=address,
        decimals=int(decimals),
        is_native=is_native,
    )


async def _resolve_solana_token(
    symbol_upper: str,
    chain_name_norm: str,
) -> Optional[ResolvedBridgeToken]:
    try:
        chain_cfg = get_solana_chain(chain_name_norm)
    except KeyError as exc:
        _LOGGER.warning(
            "[token_resolver] unknown Solana network %s: %s",
            chain_name_norm,
            exc,
        )
        return None

    if chain_cfg.is_testnet:
        _LOGGER.info(
            "[token_resolver] skipping Solana testnet token resolution for %s",
            symbol_upper,
        )
        return None

    try:
        registry = get_async_token_registry()
        entry = await registry.get(symbol_upper, chain_cfg.chain_id)
        if entry is None:
            entry = await registry.get_by_alias(
                symbol_upper.lower(), chain_cfg.chain_id
            )
        if entry is not None:
            return ResolvedBridgeToken(
                address=entry.address,
                decimals=int(entry.decimals),
                is_native=symbol_upper == chain_cfg.native_symbol.upper(),
            )
    except Exception as exc:
        _LOGGER.warning(
            "[token_resolver] Solana registry lookup failed for %s: %s",
            symbol_upper,
            exc,
        )

    def _search() -> list:
        return get_candidates(
            symbol=symbol_upper,
            chain_id=chain_cfg.chain_id,
            chain_name=chain_cfg.name.lower(),
            max_candidates=3,
        )

    try:
        candidates = await run_blocking(_search)
    except DexscreenerError as exc:
        _LOGGER.warning(
            "[token_resolver] Dexscreener error for %s on Solana: %s",
            symbol_upper,
            exc,
        )
        return None
    except Exception as exc:
        _LOGGER.warning(
            "[token_resolver] Dexscreener lookup failed for %s on Solana: %s",
            symbol_upper,
            exc,
        )
        return None

    if not candidates:
        _LOGGER.info(
            "[token_resolver] no Dexscreener candidates for %s on Solana",
            symbol_upper,
        )
        return None

    mint = candidates[0].address
    decimals = await get_registry_decimals_by_address_async(mint, chain_cfg.chain_id)

    if decimals is None:
        _LOGGER.warning(
            "[token_resolver] could not resolve decimals for %s on Solana",
            mint,
        )
        return None

    is_native = symbol_upper == chain_cfg.native_symbol.upper()

    return ResolvedBridgeToken(
        address=mint,
        decimals=int(decimals),
        is_native=is_native,
    )
