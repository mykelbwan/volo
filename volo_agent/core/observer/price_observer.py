from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

from core.observer.price_keys import (
    key_for_chain_address,
    key_for_symbol,
    normalize_chain_name,
)
from core.token_security.dexscreener import get_dexscreener_slug_for_chain_name
from core.utils.http import async_request_json

logger = logging.getLogger(__name__)

# Maps uppercase asset symbol → CoinGecko coin ID.
# Add entries here when you want to support additional assets.
COINGECKO_ID_MAP: dict[str, str] = {
    "ETH": "ethereum",
    "BTC": "bitcoin",
    "BNB": "binancecoin",
    "SOL": "solana",
    "MATIC": "matic-network",
    "POL": "matic-network",
    "AVAX": "avalanche-2",
    "ARB": "arbitrum",
    "OP": "optimism",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "AAVE": "aave",
    "PEPE": "pepe",
}

# Inverse map: CoinGecko ID → canonical asset symbol
_ID_TO_ASSET: dict[str, str] = {}
for _asset, _id in COINGECKO_ID_MAP.items():
    if _id not in _ID_TO_ASSET:
        _ID_TO_ASSET[_id] = _asset

_COINGECKO_REST_BASE = "https://api.coingecko.com/api/v3"
_COINGECKO_SIMPLE_PRICE_ENDPOINT = "/simple/price"

# Dexscreener REST (search-based price lookup for non-CoinGecko tokens)
_DEXSCREENER_REST_BASE = "https://api.dexscreener.com"
_DEXSCREENER_SEARCH_ENDPOINT = "/latest/dex/search"
_DEXSCREENER_TOKEN_PAIRS_ENDPOINT = "/token-pairs/v1/{chain}/{token}"

# Retry / backoff constants
_REST_TIMEOUT_SECONDS = 10
_DEXSCREENER_MAX_CONCURRENCY = 6


@dataclass(frozen=True)
class DexTokenRef:
    symbol: str
    chain: str
    address: str
    chain_slug: str
    price_key: str

    @classmethod
    def from_chain_address(
        cls, *, symbol: str, chain: str, address: str
    ) -> Optional["DexTokenRef"]:
        chain_norm, chain_slug = _normalize_chain_and_slug(chain)
        if not chain_norm or not chain_slug:
            return None
        addr_lower = address.strip().lower()
        if not addr_lower:
            return None
        price_key = key_for_chain_address(chain_norm, addr_lower)
        if not price_key:
            return None
        return cls(
            symbol=symbol.upper() if symbol else addr_lower[:10],
            chain=chain_norm,
            address=addr_lower,
            chain_slug=chain_slug,
            price_key=price_key,
        )


def _safe_float(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float, str)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None


def _chunked(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]

class PriceCache:
    def __init__(
        self, on_update: Optional[Callable[[str, float], None]] = None
    ) -> None:
        self._prices: dict[str, float] = {}
        self._timestamps: dict[str, float] = {}  # UNIX timestamps
        self._lock = asyncio.Lock()
        self.on_update = on_update

    def _normalize_key(self, asset: str) -> str:
        key = asset.strip()
        # Chain-aware token keys are stored as "chain:address" (lowercase).
        if ":" in key:
            return key.lower()
        return key_for_symbol(key) or key.upper()

    async def set(self, asset: str, price: float) -> None:
        key = self._normalize_key(asset)
        async with self._lock:
            self._prices[key] = price
            self._timestamps[key] = time.time()

        if self.on_update:
            try:
                self.on_update(key, price)
            except Exception as exc:
                logger.warning("PriceCache on_update callback raised: %s", exc)

    async def get(self, asset: str) -> Optional[float]:
        key = self._normalize_key(asset)
        async with self._lock:
            return self._prices.get(key)

    def get_sync(self, asset: str) -> Optional[float]:
        key = self._normalize_key(asset)
        return self._prices.get(key)

    async def snapshot(self) -> dict[str, float]:
        async with self._lock:
            return dict(self._prices)

    def age_seconds(self, asset: str) -> Optional[float]:
        key = self._normalize_key(asset)
        ts = self._timestamps.get(key)
        return (time.time() - ts) if ts is not None else None

    def is_stale(self, asset: str, max_age_seconds: float = 300) -> bool:
        age = self.age_seconds(asset)
        return age is None or age > max_age_seconds

    def all_assets(self) -> list[str]:
        return sorted(self._prices.keys())


async def fetch_price_coingecko(
    asset: str, _client: object | None = None
) -> Optional[float]:
    coin_id = COINGECKO_ID_MAP.get(asset.upper())
    if not coin_id:
        logger.debug("fetch_price_coingecko: no CoinGecko id for asset %r", asset)
        return None

    url = f"{_COINGECKO_REST_BASE}{_COINGECKO_SIMPLE_PRICE_ENDPOINT}"
    try:
        resp = await async_request_json(
            "GET",
            url,
            params={"ids": coin_id, "vs_currencies": "usd"},
            timeout=_REST_TIMEOUT_SECONDS,
            service="coingecko_rest",
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            logger.warning(
                "CoinGecko REST HTTP error for %s: %s %s",
                coin_id,
                resp.status_code,
                resp.text[:200],
            )
            return None
        price = _safe_float((resp.json().get(coin_id) or {}).get("usd"))
        if price is None:
            raise ValueError("Missing or invalid price in CoinGecko response.")
        return price
    except Exception as exc:
        logger.warning("CoinGecko REST request error for %s: %s", coin_id, exc)
    return None


def _normalize_chain_and_slug(chain: str) -> tuple[Optional[str], Optional[str]]:
    try:
        chain_norm = normalize_chain_name(chain)
        if not chain_norm:
            return None, None
        chain_slug = get_dexscreener_slug_for_chain_name(chain_norm)
        if not chain_slug:
            return None, None
        return chain_norm, chain_slug
    except Exception:
        return None, None


def _select_best_dexscreener_pair(pairs: list[dict], symbol: str) -> Optional[dict]:
    symbol_upper = symbol.upper()
    best: Optional[dict] = None
    best_liq = 0.0

    for pair in pairs:
        base = pair.get("baseToken") or {}
        base_symbol = (base.get("symbol") or "").upper()
        if base_symbol != symbol_upper:
            continue

        liquidity = pair.get("liquidity") or {}
        liquidity_usd = _safe_float(liquidity.get("usd")) or 0.0

        if liquidity_usd <= best_liq:
            continue

        best = pair
        best_liq = liquidity_usd

    return best


def _select_best_pair_by_liquidity(
    pairs: list[dict], token_address: str
) -> Optional[dict]:
    addr_lower = token_address.lower()
    best: Optional[dict] = None
    best_liq = 0.0

    for pair in pairs:
        base = pair.get("baseToken") or {}
        base_addr = (base.get("address") or "").lower()
        if base_addr and base_addr != addr_lower:
            continue

        liquidity = pair.get("liquidity") or {}
        liquidity_usd = _safe_float(liquidity.get("usd")) or 0.0

        if liquidity_usd <= best_liq:
            continue

        best = pair
        best_liq = liquidity_usd

    return best


async def fetch_price_dexscreener(
    asset: str, _client: object | None = None
) -> Optional[float]:
    symbol = asset.upper()
    url = f"{_DEXSCREENER_REST_BASE}{_DEXSCREENER_SEARCH_ENDPOINT}"
    try:
        resp = await async_request_json(
            "GET",
            url,
            params={"q": symbol},
            timeout=_REST_TIMEOUT_SECONDS,
            service="dexscreener",
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            logger.warning(
                "Dexscreener HTTP error for %s: %s %s",
                symbol,
                resp.status_code,
                resp.text[:200],
            )
            return None
        data = resp.json()
    except Exception as exc:
        logger.warning("Dexscreener request error for %s: %s", symbol, exc)
        return None

    pairs = data.get("pairs") or []
    if not isinstance(pairs, list) or not pairs:
        return None

    best = _select_best_dexscreener_pair(pairs, symbol)
    if not best:
        return None

    price_raw = best.get("priceUsd")
    return _safe_float(price_raw)


async def fetch_prices_dexscreener(
    assets: list[str], client: object | None = None
) -> dict[str, float]:
    unique_assets = sorted({a.upper() for a in assets if a})
    if not unique_assets:
        return {}

    results: dict[str, float] = {}
    sem = asyncio.Semaphore(_DEXSCREENER_MAX_CONCURRENCY)

    async def _fetch(asset: str) -> None:
        async with sem:
            price = await fetch_price_dexscreener(asset, client)
            if price is not None:
                results[asset.upper()] = price

    await asyncio.gather(*(_fetch(a) for a in unique_assets))
    return results


async def fetch_price_dexscreener_token(
    token: DexTokenRef, _client: object | None = None
) -> Optional[float]:
    url = f"{_DEXSCREENER_REST_BASE}{_DEXSCREENER_TOKEN_PAIRS_ENDPOINT.format(chain=token.chain_slug, token=token.address)}"
    try:
        resp = await async_request_json(
            "GET",
            url,
            timeout=_REST_TIMEOUT_SECONDS,
            service="dexscreener",
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            logger.warning(
                "Dexscreener HTTP error for %s on %s: %s %s",
                token.symbol,
                token.chain,
                resp.status_code,
                resp.text[:200],
            )
            return None
        data = resp.json()
    except Exception as exc:
        logger.warning(
            "Dexscreener request error for %s on %s: %s",
            token.symbol,
            token.chain,
            exc,
        )
        return None

    pairs = data.get("pairs") if isinstance(data, dict) else data
    if not isinstance(pairs, list) or not pairs:
        logger.warning(
            "Dexscreener token lookup returned no pairs for %s on %s.",
            token.symbol,
            token.chain,
        )
        return None

    best = _select_best_pair_by_liquidity(pairs, token.address)
    if not best:
        return None

    price_raw = best.get("priceUsd")
    return _safe_float(price_raw)


async def fetch_prices_dexscreener_tokens(
    tokens: list[DexTokenRef], client: object | None = None
) -> dict[str, float]:
    if not tokens:
        return {}

    results: dict[str, float] = {}
    sem = asyncio.Semaphore(_DEXSCREENER_MAX_CONCURRENCY)

    async def _fetch(token: DexTokenRef) -> None:
        async with sem:
            price = await fetch_price_dexscreener_token(token, client)
            if price is not None:
                results[token.price_key] = price

    await asyncio.gather(*(_fetch(t) for t in tokens))
    return results


async def fetch_prices_batch_coingecko(
    assets: list[str],
    _client: object | None = None,
) -> dict[str, float]:
    ids: list[str] = []
    for asset in assets:
        coin_id = COINGECKO_ID_MAP.get(asset.upper())
        if coin_id:
            ids.append(coin_id)

    unique_ids = sorted(set(ids))
    if not unique_ids:
        return {}

    url = f"{_COINGECKO_REST_BASE}{_COINGECKO_SIMPLE_PRICE_ENDPOINT}"
    results: dict[str, float] = {}

    for batch in _chunked(unique_ids, 200):
        try:
            resp = await async_request_json(
                "GET",
                url,
                params={"ids": ",".join(batch), "vs_currencies": "usd"},
                timeout=_REST_TIMEOUT_SECONDS,
                service="coingecko_rest",
            )
            if resp.status_code < 200 or resp.status_code >= 300:
                logger.warning(
                    "CoinGecko batch HTTP error: %s %s",
                    resp.status_code,
                    resp.text[:200],
                )
                continue
            data = resp.json()
        except Exception as exc:
            logger.warning("CoinGecko batch request error: %s", exc)
            continue

        if not isinstance(data, dict):
            logger.warning("CoinGecko batch response is not a dict.")
            continue

        for coin_id, payload in data.items():
            asset = _ID_TO_ASSET.get(coin_id)
            if not asset:
                continue
            price = _safe_float((payload or {}).get("usd"))
            if price is not None:
                results[asset] = price

    return results

class PriceObserver:
    def __init__(
        self,
        cache: PriceCache,
        symbols: Optional[list[str]] = None,
        dex_tokens: Optional[list[DexTokenRef]] = None,
    ) -> None:
        self.cache = cache
        self._symbol_lock = asyncio.Lock()
        # Start with an empty watchlist — symbols are discovered dynamically
        # from pending triggers and volume data. Callers who need a static
        # seed can pass an explicit list; passing None no longer defaults to
        # the full COINGECKO_ID_MAP so we never poll tokens nobody has used.
        self.symbols = [s.upper() for s in (symbols or []) if s]
        # Resolve which CoinGecko assets we actually need
        self._coingecko_symbols: list[str] = [
            s for s in self.symbols if s in COINGECKO_ID_MAP
        ]
        # Non-CoinGecko symbols will be priced via Dexscreener REST polling.
        self._dex_symbols: list[str] = [
            s for s in self.symbols if s not in COINGECKO_ID_MAP
        ]
        # Address-scoped Dexscreener tokens (chain-aware).
        self._dex_tokens: dict[str, DexTokenRef] = {
            t.price_key: t for t in (dex_tokens or []) if t
        }
        self._dex_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._logged_empty_watchlist = False

    def stop(self) -> None:
        self._stop_event.set()

    async def set_dex_tokens(self, tokens: list[DexTokenRef]) -> None:
        async with self._dex_lock:
            self._dex_tokens = {t.price_key: t for t in tokens if t}

    async def set_symbols(self, symbols: list[str]) -> None:
        normalized = [s.upper() for s in symbols if s]
        async with self._symbol_lock:
            self.symbols = normalized
            self._coingecko_symbols = [s for s in normalized if s in COINGECKO_ID_MAP]
            self._dex_symbols = [s for s in normalized if s not in COINGECKO_ID_MAP]
            self._logged_empty_watchlist = False

    async def add_symbols(self, symbols: list[str]) -> None:
        normalized = {s.strip().upper() for s in symbols if s}
        if not normalized:
            return
        async with self._symbol_lock:
            existing = set(self.symbols)
            added = normalized - existing
            if not added:
                return
            merged = sorted(existing | added)
            self.symbols = merged
            self._coingecko_symbols = [s for s in merged if s in COINGECKO_ID_MAP]
            self._dex_symbols = [s for s in merged if s not in COINGECKO_ID_MAP]
            self._logged_empty_watchlist = False
            logger.debug(
                "PriceObserver watchlist expanded: added %s (total=%d)",
                sorted(added),
                len(merged),
            )

    async def run_rest_polling(self, poll_interval: float = 60.0) -> None:
        logger.info(
            "PriceObserver REST polling started (interval=%.0fs, symbols=%s)",
            poll_interval,
            self.symbols,
        )
        while not self._stop_event.is_set():
            try:
                prices: dict[str, float] = {}

                async with self._symbol_lock:
                    coingecko_symbols = list(self._coingecko_symbols)
                    dex_symbols = list(self._dex_symbols)

                async with self._dex_lock:
                    dex_tokens = list(self._dex_tokens.values())

                if not coingecko_symbols and not dex_symbols and not dex_tokens:
                    if not self._logged_empty_watchlist:
                        logger.info("REST polling idle: no watched assets in DB.")
                        self._logged_empty_watchlist = True
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(), timeout=poll_interval
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue

                self._logged_empty_watchlist = False

                # CoinGecko batch for supported assets
                if coingecko_symbols:
                    coingecko_prices = await fetch_prices_batch_coingecko(
                        coingecko_symbols
                    )
                    prices.update(coingecko_prices)

                # Dexscreener search for non-CoinGecko assets
                if dex_symbols:
                    dex_prices = await fetch_prices_dexscreener(dex_symbols)
                    prices.update(dex_prices)

                # Dexscreener token address pricing (chain-aware)
                if dex_tokens:
                    dex_token_prices = await fetch_prices_dexscreener_tokens(dex_tokens)
                    prices.update(dex_token_prices)

                for asset, price in prices.items():
                    await self.cache.set(asset, price)
                    logger.debug("REST price update: %s = $%.4f", asset, price)

                if prices:
                    logger.info(
                        "REST tick: updated %d asset(s). Sample: %s",
                        len(prices),
                        ", ".join(f"{a}=${p:.2f}" for a, p in list(prices.items())[:4]),
                    )
                else:
                    logger.warning(
                        "REST tick: received empty price map from CoinGecko/Dexscreener"
                    )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("REST polling unexpected error: %s", exc, exc_info=True)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=poll_interval)
            except asyncio.TimeoutError:
                pass  # Normal — timeout just means it's time for the next tick

        logger.info("PriceObserver REST polling stopped.")

price_cache = PriceCache()
