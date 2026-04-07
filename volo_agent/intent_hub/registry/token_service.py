from __future__ import annotations

import asyncio
import copy
import logging
import os
from functools import lru_cache, partial
from typing import Any, Dict, Optional

from config.chains import get_chain_by_name
from config.solana_chains import SOL_DECIMALS, get_solana_chain
from core.token_security.registry_lookup import get_native_decimals
from core.utils.async_tools import run_blocking
from intent_hub.utils.messages import format_with_recovery, require_non_empty_str

logger = logging.getLogger(__name__)
_NATIVE_ADDRESS = "0x0000000000000000000000000000000000000000"
_SYMBOL_ALIASES = {
    "UDC": "USDC",
    "USC": "USDC",
}


def _native_token_entry(symbol_upper: str, chain_lower: str) -> Dict[str, Any] | None:
    try:
        chain_cfg = get_chain_by_name(chain_lower)
        if symbol_upper == chain_cfg.native_symbol.upper():
            native_addr = chain_cfg.wrapped_native or _NATIVE_ADDRESS
            return {
                "symbol": symbol_upper,
                "decimals": get_native_decimals(chain_cfg.chain_id),
                "chains": {chain_lower: {"address": native_addr}},
            }
    except KeyError:
        pass

    try:
        solana_chain = get_solana_chain(chain_lower)
        if symbol_upper == solana_chain.native_symbol.upper():
            return {
                "symbol": symbol_upper,
                "decimals": SOL_DECIMALS,
                "chains": {chain_lower: {"address": solana_chain.native_mint}},
            }
    except KeyError:
        pass

    return None


def _env_flag(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _skip_registry() -> bool:
    return _env_flag("SKIP_MONGODB_REGISTRY")


@lru_cache(maxsize=2048)
def _get_token_data_cached(
    symbol_upper: str,
    chain_lower: str,
    *,
    skip_registry: bool,
) -> Dict[str, Any]:
    # Native token shortcut
    native_entry = _native_token_entry(symbol_upper, chain_lower)
    if native_entry is not None:
        return native_entry

    if not skip_registry:
        # Registry (curated / whitelist)
        try:
            chain_id = _resolve_chain_id(chain_lower)
            if chain_id is not None:
                entry = _registry_lookup(symbol_upper, chain_lower, chain_id)
                if entry is not None:
                    return entry
        except Exception as exc:
            logger.warning(
                "get_token_data: registry lookup failed for %s/%s: %s",
                symbol_upper,
                chain_lower,
                exc,
            )

    # Live resolution via TokenSecurityManager
    # Always attempt live resolution after registry miss, even when
    # SKIP_MONGODB_REGISTRY is enabled. That flag should only bypass DB lookup.
    try:
        from core.token_security.resolver import get_token_security_manager

        mgr = get_token_security_manager()
        resolved = mgr.resolve(symbol_upper, chain_lower)
        return {
            "symbol": resolved.symbol,
            "decimals": resolved.decimals,
            "chains": {resolved.chain_name: {"address": resolved.address}},
        }
    except Exception as exc:
        logger.warning(
            "get_token_data: live resolution failed for %s/%s: %s",
            symbol_upper,
            chain_lower,
            exc,
        )

    # ── Fallback: minimal stub so callers can handle the miss gracefully ──────
    logger.error(
        "get_token_data: all resolution paths failed for %s on %s. "
        "Returning empty chains dict.",
        symbol_upper,
        chain_lower,
    )
    return {
        "symbol": symbol_upper,
        "decimals": 18,
        "chains": {},
    }


def get_token_data(symbol: str, chain: str) -> Dict[str, Any]:
    symbol_upper = require_non_empty_str(symbol, field="symbol").strip().upper()
    symbol_upper = _SYMBOL_ALIASES.get(symbol_upper, symbol_upper)
    chain_lower = require_non_empty_str(chain, field="chain").strip().lower()
    data = _get_token_data_cached(
        symbol_upper,
        chain_lower,
        skip_registry=_skip_registry(),
    )
    # Return a defensive copy to prevent accidental mutation of cached entries.
    return copy.deepcopy(data)


async def get_token_data_async(symbol: str, chain: str) -> Dict[str, Any]:
    timeout_seconds = _token_service_async_timeout_seconds()
    symbol_upper = require_non_empty_str(symbol, field="symbol").strip().upper()
    symbol_upper = _SYMBOL_ALIASES.get(symbol_upper, symbol_upper)
    chain_lower = require_non_empty_str(chain, field="chain").strip().lower()
    skip_registry = _skip_registry()

    if not skip_registry:
        try:
            chain_id = _resolve_chain_id(chain_lower)
            if chain_id is not None:
                entry = await _registry_lookup_async(
                    symbol_upper, chain_lower, chain_id
                )
                if entry is not None:
                    return copy.deepcopy(entry)
        except Exception as exc:
            logger.warning(
                "get_token_data_async: registry lookup failed for %s/%s: %s",
                symbol_upper,
                chain_lower,
                exc,
            )
    try:
        return await run_blocking(
            partial(
                _get_token_data_cached,
                symbol_upper,
                chain_lower,
                skip_registry=skip_registry,
            ),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError as exc:
        timeout_text = (
            f"{timeout_seconds:.1f}s" if timeout_seconds is not None else "N/A"
        )
        raise TimeoutError(
            format_with_recovery(
                f"Token metadata lookup timed out after {timeout_text}",
                "retry shortly; if this repeats, check token registry and network connectivity",
            )
        ) from exc


def get_address_for_chain(token_data: Dict[str, Any], chain_name: str) -> Optional[str]:
    chain = require_non_empty_str(chain_name, field="chain_name")
    if not isinstance(token_data, dict):
        return None
    chains_data: Dict[str, Any] = token_data.get("chains", {})
    if not chains_data:
        return None

    def _norm(value: str) -> str:
        return " ".join(value.lower().replace("-", " ").split())

    target = _norm(chain.strip())

    # Exact match first (avoids unnecessary iteration for the common case)
    value: Any = chains_data.get(target)

    # Case-insensitive scan on miss
    if value is None:
        for k, v in chains_data.items():
            if _norm(k) == target:
                value = v
                break

    if value is None:
        return None

    if isinstance(value, str):
        return value or None

    if isinstance(value, dict):
        addr = value.get("address")
        return addr if addr else None

    return None


async def get_address_for_chain_async(
    token_data: Dict[str, Any], chain_name: str
) -> Optional[str]:
    return get_address_for_chain(token_data, chain_name)


def _token_service_async_timeout_seconds() -> float | None:
    raw = os.getenv("INTENT_TOKEN_SERVICE_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


@lru_cache(maxsize=256)
def _resolve_chain_id(chain_lower: str) -> Optional[int]:
    try:
        chain_cfg = get_chain_by_name(chain_lower)
        return chain_cfg.chain_id
    except KeyError:
        try:
            return get_solana_chain(chain_lower).chain_id
        except KeyError:
            logger.debug(
                "_resolve_chain_id: chain '%s' not found in config/chains.py or config/solana_chains.py.",
                chain_lower,
            )
            return None


def _registry_lookup(
    symbol: str, chain_name: str, chain_id: int
) -> Optional[Dict[str, Any]]:
    from core.token_security.token_db import get_token_registry

    registry = get_token_registry()

    # 1. Exact symbol match
    entry = registry.get(symbol, chain_id)

    # 2. Alias match
    if entry is None:
        entry = registry.get_by_alias(symbol.lower(), chain_id)

    if entry is None:
        return None

    return {
        "symbol": entry.symbol,
        "decimals": entry.decimals,
        "chains": {chain_name: {"address": entry.address}},
    }


async def _registry_lookup_async(
    symbol: str, chain_name: str, chain_id: int
) -> Optional[Dict[str, Any]]:
    from core.token_security.token_db import get_async_token_registry

    registry = get_async_token_registry()

    entry = await registry.get(symbol, chain_id)
    if entry is None:
        entry = await registry.get_by_alias(symbol.lower(), chain_id)

    if entry is None:
        return None

    return {
        "symbol": entry.symbol,
        "decimals": entry.decimals,
        "chains": {chain_name: {"address": entry.address}},
    }
