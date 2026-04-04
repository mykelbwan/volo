from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from config.chains import get_chain_by_id, get_chain_by_name
from core.token_security.cache import TokenSecurityCache
from core.token_security.dexscreener import (
    DexscreenerError,
    get_candidates,
)
from core.token_security.goplus_scanner import (
    GOPLUS_SUPPORTED_CHAIN_IDS,
    GoplusError,
    GoplusScanner,
)
from core.token_security.models import (
    DexscreenerCandidate,
    ResolvedToken,
    SecurityFlag,
    SecurityTier,
    TokenNotFoundError,
    TokenUnsafeError,
)
from core.token_security.simulation_fallback import SimulationFallback
from core.token_security.token_db import (
    TokenRegistryDB,
    TokenRegistryEntry,
    get_token_registry,
)

logger = logging.getLogger(__name__)

_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _registry_entry_to_resolved(
    entry: TokenRegistryEntry,
    chain_name: str,
    chain_id: int,
    liquidity_usd: Optional[float] = None,
) -> ResolvedToken:
    return ResolvedToken(
        symbol=entry.symbol,
        name=entry.name,
        chain_name=chain_name.lower(),
        chain_id=chain_id,
        address=entry.address,
        decimals=entry.decimals,
        security_tier=SecurityTier.WHITELIST,
        is_safe=True,
        flags=[],
        liquidity_usd=liquidity_usd,
        source="registry",
        last_checked=datetime.now(tz=timezone.utc),
    )


def _goplus_to_resolved(
    candidate: DexscreenerCandidate,
    report: Any,  # GoplusTokenReport
    chain_name: str,
) -> ResolvedToken:
    return ResolvedToken(
        symbol=candidate.symbol,
        name=report.token_name or candidate.name,
        chain_name=chain_name.lower(),
        chain_id=candidate.chain_id,
        address=candidate.address,
        decimals=report.decimals,
        security_tier=report.security_tier,
        is_safe=report.is_safe,
        flags=list(report.flags),
        liquidity_usd=candidate.liquidity_usd,
        buy_tax=report.buy_tax_pct if report.raw_buy_tax > 0 else None,
        sell_tax=report.sell_tax_pct if report.raw_sell_tax > 0 else None,
        total_supply=report.total_supply,
        is_mintable=report.is_mintable,
        is_proxy=report.is_proxy,
        source="dexscreener+goplus",
        last_checked=datetime.now(tz=timezone.utc),
    )


def _simulation_to_resolved(
    candidate: DexscreenerCandidate,
    report: Any,  # SimulationReport
    chain_name: str,
) -> ResolvedToken:
    return ResolvedToken(
        symbol=report.symbol or candidate.symbol,
        name=candidate.name,
        chain_name=chain_name.lower(),
        chain_id=candidate.chain_id,
        address=report.address,
        decimals=report.decimals,
        security_tier=report.security_tier,
        is_safe=report.is_safe,
        flags=list(report.flags),
        liquidity_usd=candidate.liquidity_usd,
        total_supply=(
            str(report.total_supply_raw)
            if report.total_supply_raw is not None
            else None
        ),
        source="dexscreener+simulation",
        last_checked=datetime.now(tz=timezone.utc),
    )


class TokenSecurityManager:
    def __init__(
        self,
        cache: Optional[TokenSecurityCache] = None,
        goplus: Optional[GoplusScanner] = None,
        simulation: Optional[SimulationFallback] = None,
        registry: Optional[TokenRegistryDB] = None,
    ) -> None:
        # Inject the background-refresh callback so the cache can schedule
        # re-verification of stale entries without knowing about the resolver.
        self._cache = cache or TokenSecurityCache(
            refresh_callback=self._background_refresh
        )
        self._goplus = goplus or GoplusScanner()
        self._simulation = simulation or SimulationFallback()
        self._registry = registry or get_token_registry()

    def resolve(self, symbol: str, chain_name: str) -> ResolvedToken:
        symbol_upper = symbol.strip().upper()
        chain_name_lower = chain_name.strip().lower()

        try:
            chain = get_chain_by_name(chain_name_lower)
        except KeyError:
            raise ValueError(
                f"Chain '{chain_name}' is not registered. "
                f"Check config/chains.py for valid chain names."
            )

        chain_id = chain.chain_id

        logger.info(
            "TokenSecurityManager.resolve: symbol=%s chain=%s (id=%d)",
            symbol_upper,
            chain_name_lower,
            chain_id,
        )

        # Native token
        native_result = self._try_native_token(
            symbol_upper, chain_name_lower, chain_id, chain
        )
        if native_result is not None:
            logger.info(
                "Resolved %s on %s as NATIVE (Tier 1, zero address).",
                symbol_upper,
                chain_name_lower,
            )
            return native_result

        # DB registry (curated/whitelisted tokens)
        registry_result = self._try_registry(symbol_upper, chain_name_lower, chain_id)
        if registry_result is not None:
            logger.info(
                "Resolved %s on %s from REGISTRY (Tier 1).",
                symbol_upper,
                chain_name_lower,
            )
            return registry_result

        # MongoDB cache
        cached = self._cache.get(symbol_upper, chain_id)
        if cached is not None:
            logger.info(
                "Resolved %s on %s from CACHE (Tier 2, tier=%s).",
                symbol_upper,
                chain_name_lower,
                cached.security_tier.value,
            )
            return cached

        # Live resolution
        logger.info(
            "Cache MISS for %s on %s — starting live resolution (Tier 3).",
            symbol_upper,
            chain_name_lower,
        )

        result = self._resolve_live(
            symbol=symbol_upper,
            chain_name=chain_name_lower,
            chain_id=chain_id,
            rpc_url=chain.rpc_url,
        )

        # Cache the result for future lookups
        self._cache.set(result)

        logger.info(
            "Resolved %s on %s LIVE → tier=%s, safe=%s, address=%s",
            symbol_upper,
            chain_name_lower,
            result.security_tier.value,
            result.is_safe,
            result.address,
        )

        return result

    def resolve_address_only(self, symbol: str, chain_name: str) -> str:
        return self.resolve(symbol, chain_name).address

    def get_security_metadata(self, symbol: str, chain_name: str) -> dict[str, Any]:
        try:
            token = self.resolve(symbol, chain_name)
            return {
                "resolved_token": token.address,
                "security_source": token.source,
                "security_tier": token.security_tier.value,
                "is_safe": token.is_safe,
                "flags": token.human_flags,
                "summary": token.security_summary,
                "liquidity_usd": token.liquidity_usd,
                "buy_tax": token.buy_tax,
                "sell_tax": token.sell_tax,
            }
        except TokenNotFoundError as exc:
            return {
                "resolved_token": None,
                "security_source": "not_found",
                "security_tier": SecurityTier.UNVERIFIED.value,
                "is_safe": False,
                "flags": [],
                "summary": str(exc),
                "error": str(exc),
            }
        except TokenUnsafeError as exc:
            return {
                "resolved_token": None,
                "security_source": exc.source or "unknown",
                "security_tier": SecurityTier.UNSAFE.value,
                "is_safe": False,
                "flags": [f.value for f in exc.flags],
                "summary": str(exc),
                "error": str(exc),
            }
        except Exception as exc:
            logger.error(
                "get_security_metadata: unexpected error for %s/%s: %s",
                symbol,
                chain_name,
                exc,
                exc_info=True,
            )
            return {
                "resolved_token": None,
                "security_source": "error",
                "security_tier": SecurityTier.UNVERIFIED.value,
                "is_safe": False,
                "flags": [],
                "summary": f"Resolution error: {exc}",
                "error": str(exc),
            }

    def _try_native_token(
        self,
        symbol: str,
        chain_name: str,
        chain_id: int,
        chain: Any,
    ) -> Optional[ResolvedToken]:
        native_symbol = (getattr(chain, "native_symbol", None) or "").upper()
        if symbol != native_symbol:
            return None

        return ResolvedToken(
            symbol=symbol,
            name=f"{symbol} (native)",
            chain_name=chain_name,
            chain_id=chain_id,
            address=_ZERO_ADDRESS,
            decimals=18,
            security_tier=SecurityTier.WHITELIST,
            is_safe=True,
            flags=[],
            source="native_token",
            last_checked=datetime.now(tz=timezone.utc),
        )

    def _try_registry(
        self,
        symbol: str,
        chain_name: str,
        chain_id: int,
    ) -> Optional[ResolvedToken]:
        # Exact symbol lookup — hits idx_reg_key, O(1)
        entry = self._registry.get(symbol, chain_id)

        # Alias lookup on miss — hits idx_aliases, O(log n)
        if entry is None:
            entry = self._registry.get_by_alias(symbol.lower(), chain_id)

        if entry is None:
            logger.debug(
                "_try_registry: no active entry for %s on chain_id=%d.",
                symbol,
                chain_id,
            )
            return None

        logger.debug(
            "_try_registry: HIT for %s on chain_id=%d (address=%s).",
            symbol,
            chain_id,
            entry.address,
        )
        return _registry_entry_to_resolved(entry, chain_name, chain_id)

    def _resolve_live(
        self,
        symbol: str,
        chain_name: str,
        chain_id: int,
        rpc_url: str,
    ) -> ResolvedToken:
        # Discover candidates via Dexscreener
        try:
            candidates = get_candidates(
                symbol=symbol,
                chain_id=chain_id,
                chain_name=chain_name,
                max_candidates=3,
            )
        except DexscreenerError as exc:
            logger.warning(
                "Dexscreener discovery failed for %s/%s: %s. "
                "Attempting simulation fallback with empty candidates.",
                symbol,
                chain_name,
                exc,
            )
            candidates = []

        if not candidates:
            # For dark chains with no Dexscreener data, we have no address to
            # simulate against — nothing we can do.
            raise TokenNotFoundError(symbol=symbol, chain_name=chain_name)

        logger.info(
            "Live resolution: %d candidate(s) for %s on %s. "
            "Top address: %s (liquidity $%.0f).",
            len(candidates),
            symbol,
            chain_name,
            candidates[0].address,
            candidates[0].liquidity_usd,
        )

        # Security fork
        if chain_id in GOPLUS_SUPPORTED_CHAIN_IDS:
            return self._resolve_via_goplus(candidates, chain_name)
        else:
            return self._resolve_via_simulation(candidates, chain_name, rpc_url)

    def _resolve_via_goplus(
        self,
        candidates: list[DexscreenerCandidate],
        chain_name: str,
    ) -> ResolvedToken:
        chain_id = candidates[0].chain_id
        addresses = [c.address for c in candidates]

        # Build a liquidity map for the LOW_LIQUIDITY check inside GoPlus
        liquidity_map = {c.address.lower(): c.liquidity_usd for c in candidates}

        try:
            reports = self._goplus.scan_batch(
                addresses=addresses,
                chain_id=chain_id,
                liquidity_map=liquidity_map,
            )
        except GoplusError as exc:
            # GoPlus failed — downgrade to simulation fallback across candidates.
            logger.warning(
                "GoPlus scan failed for %s on chain %d: %s. "
                "Falling back to simulation across candidates.",
                [a[:10] for a in addresses],
                chain_id,
                exc,
            )
            try:
                chain = get_chain_by_name(chain_name)
                return self._resolve_via_simulation(candidates, chain_name, chain.rpc_url)
            except Exception:
                raise TokenNotFoundError(
                    symbol=candidates[0].symbol, chain_name=chain_name
                ) from exc

        if not reports:
            logger.warning(
                "GoPlus returned empty results for %s on chain_id=%d. "
                "Falling back to simulation across candidates.",
                [a[:10] for a in addresses],
                chain_id,
            )
            try:
                chain = get_chain_by_name(chain_name)
                return self._resolve_via_simulation(candidates, chain_name, chain.rpc_url)
            except Exception:
                raise TokenNotFoundError(
                    symbol=candidates[0].symbol, chain_name=chain_name
                )

        # Iterate through candidates in liquidity order, pick the first safe one
        unsafe_tokens: list[tuple[str, list[SecurityFlag]]] = []

        for candidate in candidates:
            addr_lower = candidate.address.lower()
            report = reports.get(addr_lower)

            if report is None:
                # GoPlus has no data for this address (not indexed yet)
                logger.debug(
                    "GoPlus: no report for %s — skipping candidate.",
                    candidate.address[:10],
                )
                continue

            if report.is_safe:
                resolved = _goplus_to_resolved(candidate, report, chain_name)
                logger.info(
                    "GoPlus verification PASSED for %s on %s. %s",
                    candidate.address[:10],
                    chain_name,
                    report.short_summary(),
                )
                return resolved

            else:
                unsafe_tokens.append((candidate.address, list(report.flags)))
                logger.warning(
                    "GoPlus verification FAILED for %s on %s: %s",
                    candidate.address[:10],
                    chain_name,
                    report.short_summary(),
                )

        # All candidates either failed or were unindexed
        if unsafe_tokens:
            # Report the first (highest-liquidity) unsafe token's flags
            worst_address, worst_flags = unsafe_tokens[0]
            raise TokenUnsafeError(
                address=worst_address,
                flags=worst_flags,
                symbol=candidates[0].symbol,
                chain_name=chain_name,
                source="goplus",
            )

        # All candidates were unindexed by GoPlus — fallback to simulation
        logger.info(
            "GoPlus had no data for any candidate of %s on %s — "
            "falling back to simulation.",
            candidates[0].symbol,
            chain_name,
        )
        try:
            chain = get_chain_by_name(chain_name)
            return self._resolve_via_simulation(candidates, chain_name, chain.rpc_url)
        except Exception:
            raise TokenNotFoundError(symbol=candidates[0].symbol, chain_name=chain_name)

    def _resolve_via_simulation(
        self,
        candidates: list[DexscreenerCandidate],
        chain_name: str,
        rpc_url: str,
    ) -> ResolvedToken:
        if not candidates:
            raise TokenNotFoundError(symbol="unknown", chain_name=chain_name)

        unsafe_tokens: list[tuple[DexscreenerCandidate, list[SecurityFlag]]] = []

        for candidate in candidates:
            logger.info(
                "Simulation fallback: scanning %s (%s) on %s",
                candidate.symbol,
                candidate.address[:10],
                chain_name,
            )

            try:
                report = self._simulation.scan(candidate, rpc_url=rpc_url)
            except Exception as exc:
                logger.error(
                    "SimulationFallback.scan raised unexpectedly for %s: %s",
                    candidate.address[:10],
                    exc,
                )
                continue

            resolved = _simulation_to_resolved(candidate, report, chain_name)

            critical_flags = [
                f for f in report.flags if f == SecurityFlag.SIMULATION_REVERTED
            ]
            if critical_flags:
                unsafe_tokens.append((candidate, critical_flags))
                logger.warning(
                    "Simulation REJECTED %s on %s: flags=%s",
                    candidate.address[:10],
                    chain_name,
                    [f.value for f in critical_flags],
                )
                continue

            logger.info(
                "Simulation verification PASSED for %s on %s. %s",
                candidate.address[:10],
                chain_name,
                report.short_summary(),
            )
            return resolved

        if unsafe_tokens:
            candidate, critical_flags = unsafe_tokens[0]
            raise TokenUnsafeError(
                address=candidate.address,
                flags=critical_flags,
                symbol=candidate.symbol,
                chain_name=chain_name,
                source="simulation",
            )

        raise TokenNotFoundError(symbol=candidates[0].symbol, chain_name=chain_name)

    def _background_refresh(self, symbol: str, chain_id: int) -> None:
        try:
            chain = get_chain_by_id(chain_id)
            result = self._resolve_live(
                symbol=symbol,
                chain_name=chain.name.lower(),
                chain_id=chain_id,
                rpc_url=chain.rpc_url,
            )
            self._cache.set(result)
            logger.info(
                "Background refresh complete: %s on chain_id=%d → tier=%s",
                symbol,
                chain_id,
                result.security_tier.value,
            )
        except TokenUnsafeError as exc:
            logger.warning(
                "Background refresh invalidated unsafe token %s on chain_id=%d: %s",
                symbol,
                chain_id,
                exc,
            )
            self._cache.invalidate(symbol, chain_id)
        except Exception as exc:
            logger.warning(
                "Background refresh failed for %s on chain_id=%d: %s",
                symbol,
                chain_id,
                exc,
            )


_MANAGER: Optional[TokenSecurityManager] = None


def get_token_security_manager() -> TokenSecurityManager:
    global _MANAGER
    if _MANAGER is None:
        _MANAGER = TokenSecurityManager()
    return _MANAGER
