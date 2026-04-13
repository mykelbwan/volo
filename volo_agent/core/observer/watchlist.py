from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from core.observer.price_keys import normalize_chain_name
from core.observer.price_observer import DexTokenRef

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WatchlistRefreshDeps:
    get_pending_price_triggers: Callable[[], Awaitable[list[dict[str, Any]]]]
    set_symbols: Callable[[list[str]], Awaitable[None]]
    set_dex_tokens: Callable[[list[DexTokenRef]], Awaitable[None]]
    dex_address_cache: dict[tuple[str, str], str]
    static_symbols: list[str]
    volume_symbols: set[str]


async def refresh_price_watchlist(deps: WatchlistRefreshDeps) -> None:
    try:
        pending = await deps.get_pending_price_triggers()
    except Exception as exc:
        logger.debug("ObserverWatcher: could not refresh watchlist: %s", exc)
        return

    dex_tokens: list[DexTokenRef] = []
    seen: set[str] = set()
    dynamic_symbols: set[str] = set()

    for doc in pending:
        cond = doc.get("trigger_condition", {}) or {}
        chain = cond.get("chain")
        address = cond.get("token_address")
        symbol = (cond.get("asset") or "").strip()
        if symbol:
            dynamic_symbols.add(symbol.upper())

        if not chain:
            continue

        if not address and symbol:
            address = _resolve_token_address(
                symbol=symbol,
                chain=chain,
                dex_address_cache=deps.dex_address_cache,
            )

        if not address:
            continue

        if symbol:
            chain_norm = normalize_chain_name(chain)
            cache_key = (
                chain_norm or chain.strip().lower(),
                symbol.strip().upper(),
            )
            deps.dex_address_cache.setdefault(cache_key, address)

        ref = DexTokenRef.from_chain_address(symbol=symbol, chain=chain, address=address)
        if not ref or ref.price_key in seen:
            continue
        seen.add(ref.price_key)
        dex_tokens.append(ref)

    merged_symbols = sorted(
        set(deps.static_symbols) | dynamic_symbols | deps.volume_symbols
    )
    await deps.set_symbols(merged_symbols)
    await deps.set_dex_tokens(dex_tokens)


def _resolve_token_address(
    *,
    symbol: str,
    chain: str,
    dex_address_cache: dict[tuple[str, str], str],
) -> str | None:
    chain_norm = normalize_chain_name(chain)
    cache_key = (
        chain_norm or chain.strip().lower(),
        symbol.strip().upper(),
    )
    address = dex_address_cache.get(cache_key)
    if address:
        return address
    try:
        from intent_hub.registry.token_service import (
            get_address_for_chain,
            get_token_data,
        )

        token_data = get_token_data(symbol, chain)
        address = get_address_for_chain(token_data, chain)
        if address:
            dex_address_cache[cache_key] = address
        return address
    except Exception as exc:
        logger.debug(
            "ObserverWatcher: could not resolve address for %s on %s: %s",
            symbol,
            chain,
            exc,
        )
        return None
