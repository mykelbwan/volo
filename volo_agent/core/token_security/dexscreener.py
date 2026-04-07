from __future__ import annotations

import logging
from typing import Any, Optional

from requests import HTTPError, RequestException

from config.chains import CHAINS, find_chain_by_id, find_chain_by_name
from config.solana_chains import (
    SOLANA_CHAINS,
    get_solana_chain,
    get_solana_chain_by_id,
    is_solana_chain_id,
    is_solana_network,
)
from core.token_security.models import DexscreenerCandidate
from core.utils.http import request_json

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.dexscreener.com"
_SEARCH_ENDPOINT = "/latest/dex/search"

_DEFAULT_TIMEOUT_SECONDS: float = 10.0

_DEFAULT_MAX_CANDIDATES: int = 3

# Minimum USD liquidity for a candidate pair to be considered at all.
# This pre-filters completely illiquid/dead pairs before security scanning.
_MIN_PAIR_LIQUIDITY_USD: float = 1_000.0


def _normalize_dexscreener_slug(slug: Optional[str]) -> Optional[str]:
    value = (slug or "").strip().lower()
    return value or None


def _build_chain_id_to_dex_slug() -> dict[int, str]:
    mapping: dict[int, str] = {}

    for chain in CHAINS.values():
        slug = _normalize_dexscreener_slug(chain.dexscreener_slug)
        if slug:
            mapping[chain.chain_id] = slug

    for chain in SOLANA_CHAINS.values():
        slug = _normalize_dexscreener_slug(chain.dexscreener_slug)
        if slug:
            mapping[chain.chain_id] = slug

    return mapping


# Built from registered chain config so Dexscreener support lives in one place.
CHAIN_ID_TO_DEX_SLUG: dict[int, str] = _build_chain_id_to_dex_slug()


def get_dexscreener_slug_for_chain_id(chain_id: int) -> Optional[str]:
    return CHAIN_ID_TO_DEX_SLUG.get(chain_id)


def get_dexscreener_slug_for_chain_name(chain_name: str) -> Optional[str]:
    key = (chain_name or "").strip().lower()
    if not key:
        return None

    try:
        if is_solana_network(key):
            chain = get_solana_chain(key)
            if chain.is_testnet:
                return None
            return _normalize_dexscreener_slug(chain.dexscreener_slug)
    except Exception:
        pass

    try:
        return _normalize_dexscreener_slug(find_chain_by_name(key).dexscreener_slug)
    except Exception:
        return None


class DexscreenerError(Exception):
    """Base class for Dexscreener client errors."""


class DexscreenerRateLimitError(DexscreenerError):
    """Raised after all retries are exhausted due to rate limiting (429)."""


class DexscreenerUnavailableError(DexscreenerError):
    """Raised after all retries are exhausted due to server errors (5xx)."""


def _is_testnet_chain(chain_id: int, chain_name: str) -> bool:
    try:
        if is_solana_chain_id(chain_id):
            return get_solana_chain_by_id(chain_id).is_testnet
        return find_chain_by_id(chain_id).is_testnet
    except Exception:
        pass

    try:
        if is_solana_network(chain_name):
            return get_solana_chain(chain_name).is_testnet
    except Exception:
        pass

    return False


def _get_with_retry(
    url: str,
    params: dict,
    timeout: float = _DEFAULT_TIMEOUT_SECONDS,
) -> dict:
    try:
        resp = request_json(
            "GET",
            url,
            params=params,
            timeout=timeout,
            service="dexscreener",
        )
    except RequestException as exc:
        raise DexscreenerError(
            f"Dexscreener request failed due to network error: {exc}"
        ) from exc

    if resp.status_code == 200:
        try:
            return resp.json()
        except ValueError as exc:
            raise DexscreenerError(
                f"Dexscreener returned non-JSON response: {resp.text[:200]}"
            ) from exc

    # request_json already retries transient 429/5xx statuses.
    if resp.status_code == 429:
        raise DexscreenerRateLimitError(
            "Dexscreener rate limit hit after shared retry attempts."
        )
    if resp.status_code >= 500:
        raise DexscreenerUnavailableError(
            f"Dexscreener unavailable (HTTP {resp.status_code}) after shared retry attempts."
        )

    try:
        resp.raise_for_status()
    except HTTPError as exc:
        raise DexscreenerError(
            f"Dexscreener non-retryable HTTP {resp.status_code}: {resp.text[:200]}"
        ) from exc

    raise DexscreenerError("Dexscreener request failed unexpectedly.")


def _parse_pairs(
    raw: dict,
    symbol: str,
    target_chain_id: int,
) -> list[dict]:
    pairs: list[dict] = raw.get("pairs") or []
    if not pairs:
        return []

    target_slug = get_dexscreener_slug_for_chain_id(target_chain_id)
    if target_slug is None:
        logger.debug(
            "_parse_pairs: chain_id=%d not in CHAIN_ID_TO_DEX_SLUG — 0 results.",
            target_chain_id,
        )
        return []

    symbol_upper = symbol.upper()
    filtered: list[dict] = []

    for pair in pairs:
        # Chain filter
        pair_chain = (pair.get("chainId") or "").lower()
        if pair_chain != target_slug:
            continue

        # Exact base-token symbol match
        base_token = pair.get("baseToken") or {}
        base_symbol = (base_token.get("symbol") or "").upper()
        if base_symbol != symbol_upper:
            continue

        # Minimum liquidity gate
        liquidity = pair.get("liquidity") or {}
        liquidity_usd = _safe_float(liquidity.get("usd")) or 0.0
        if liquidity_usd < _MIN_PAIR_LIQUIDITY_USD:
            continue

        filtered.append(pair)

    # Sort by USD liquidity descending
    filtered.sort(
        key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0),
        reverse=True,
    )

    return filtered


def _aggregate_candidates(
    pairs: list[dict],
    symbol: str,
    chain_id: int,
    chain_name: str,
    max_candidates: int,
) -> list[DexscreenerCandidate]:
    # address (lowercase) → accumulated data
    aggregated: dict[str, dict] = {}

    for pair in pairs:
        base_token = pair.get("baseToken") or {}
        raw_address: str = (base_token.get("address") or "").strip()
        if not raw_address:
            continue

        # Normalise to lowercase for deduplication; we'll checksum later.
        addr_lower = raw_address.lower()
        liquidity_usd = _safe_float((pair.get("liquidity") or {}).get("usd")) or 0.0

        if addr_lower not in aggregated:
            aggregated[addr_lower] = {
                "address": raw_address,  # keep original casing from first occurrence
                "name": base_token.get("name"),
                "total_liquidity_usd": 0.0,
                "best_pair_address": pair.get("pairAddress"),
                "dex_id": pair.get("dexId"),
                "price_usd": _safe_float(pair.get("priceUsd")),
                "volume_h24_usd": _safe_float((pair.get("volume") or {}).get("h24")),
            }

        aggregated[addr_lower]["total_liquidity_usd"] += liquidity_usd

    # Sort by total liquidity descending
    sorted_entries = sorted(
        aggregated.values(),
        key=lambda e: e["total_liquidity_usd"],
        reverse=True,
    )

    candidates: list[DexscreenerCandidate] = []
    for entry in sorted_entries[:max_candidates]:
        try:
            candidate = DexscreenerCandidate(
                symbol=symbol.upper(),
                name=entry["name"],
                address=entry["address"],
                chain_id=chain_id,
                chain_name=chain_name,
                liquidity_usd=entry["total_liquidity_usd"],
                pair_address=entry["best_pair_address"],
                dex_id=entry["dex_id"],
                price_usd=entry["price_usd"],
                volume_h24_usd=entry["volume_h24_usd"],
            )
            candidates.append(candidate)
        except Exception as exc:
            logger.warning(
                "Dexscreener: failed to build candidate for address %s: %s",
                entry.get("address"),
                exc,
            )

    return candidates


def get_candidates(
    symbol: str,
    chain_id: int,
    chain_name: str,
    max_candidates: int = _DEFAULT_MAX_CANDIDATES,
    timeout: float = _DEFAULT_TIMEOUT_SECONDS,
) -> list[DexscreenerCandidate]:
    if _is_testnet_chain(chain_id, chain_name):
        logger.info(
            "Dexscreener: skipping testnet chain %s (id=%d) for %s.",
            chain_name,
            chain_id,
            symbol,
        )
        return []

    # Fast path: if the chain has no Dexscreener slug, don't bother calling the API.
    target_slug = get_dexscreener_slug_for_chain_id(chain_id)
    if target_slug is None:
        logger.debug(
            "Dexscreener: chain_id=%d has no slug mapping — skipping API call.",
            chain_id,
        )
        return []

    symbol_upper = symbol.upper()
    url = f"{_BASE_URL}{_SEARCH_ENDPOINT}"
    params = {"q": symbol_upper}

    logger.info(
        "Dexscreener: searching for %s on chain_id=%d (%s).",
        symbol_upper,
        chain_id,
        chain_name,
    )

    try:
        raw = _get_with_retry(url, params=params, timeout=timeout)
    except DexscreenerError:
        # Re-raise so the resolver can decide how to handle it
        raise
    except Exception as exc:
        raise DexscreenerError(
            f"Unexpected error querying Dexscreener for {symbol_upper}: {exc}"
        ) from exc

    pairs = _parse_pairs(raw, symbol=symbol_upper, target_chain_id=chain_id)

    if not pairs:
        logger.info(
            "Dexscreener: no valid pairs found for %s on chain_id=%d.",
            symbol_upper,
            chain_id,
        )
        return []

    candidates = _aggregate_candidates(
        pairs=pairs,
        symbol=symbol_upper,
        chain_id=chain_id,
        chain_name=chain_name,
        max_candidates=max_candidates,
    )

    logger.info(
        "Dexscreener: found %d unique candidate(s) for %s on chain_id=%d. "
        "Top liquidity: $%.0f.",
        len(candidates),
        symbol_upper,
        chain_id,
        candidates[0].liquidity_usd if candidates else 0,
    )

    return candidates


def _safe_float(value: Any) -> Optional[float]:
    """
    Convert *value* to float, returning ``None`` on any error.

    Dexscreener returns some numeric fields as strings e.g. priceUsd,
    while others are native JSON numbers.  This helper handles both.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
