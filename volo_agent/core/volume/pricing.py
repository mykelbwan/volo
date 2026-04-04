from __future__ import annotations

import logging
from typing import Protocol

import httpx

from core.observer.price_observer import (
    COINGECKO_ID_MAP,
    PriceCache,
    fetch_prices_batch_coingecko,
    fetch_prices_dexscreener,
)

logger = logging.getLogger(__name__)


class VolumeWatcherAccess(Protocol):
    price_cache: PriceCache

    def register_volume_symbols(self, symbols: list[str]) -> None: ...


async def resolve_prices(
    tokens: set[str],
    *,
    price_cache: PriceCache,
    http_client: httpx.AsyncClient,
) -> dict[str, float]:
    if not tokens:
        return {}

    prices: dict[str, float] = {}
    cache_misses: set[str] = set()
    for token in tokens:
        cached = price_cache.get_sync(token)
        if cached is not None:
            prices[token] = cached
        else:
            cache_misses.add(token)

    if not cache_misses:
        return prices

    cg_tokens = [t for t in cache_misses if t in COINGECKO_ID_MAP]
    remaining: set[str] = set(cache_misses)

    if cg_tokens:
        try:
            cg_prices = await fetch_prices_batch_coingecko(cg_tokens, http_client)
            prices.update(cg_prices)
            remaining -= set(cg_prices.keys())
        except Exception as exc:
            logger.warning("[VOLUME FLUSH] CoinGecko batch error: %s", exc)

    if remaining:
        try:
            dex_prices = await fetch_prices_dexscreener(list(remaining), http_client)
            prices.update(dex_prices)
        except Exception as exc:
            logger.warning("[VOLUME FLUSH] Dexscreener batch error: %s", exc)

    for symbol, price in prices.items():
        if symbol in cache_misses:
            try:
                await price_cache.set(symbol, price)
            except Exception:
                pass

    return prices
