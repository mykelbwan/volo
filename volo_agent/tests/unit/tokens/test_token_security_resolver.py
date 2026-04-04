"""
Unit-test suite for the Token Security Resolver subsystem.
All external I/O (MongoDB, HTTP, Web3) is replaced with unittest.mock.

Run with:
    pytest tests/unit/test_token_security_resolver.py -v
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Shared object factories
# ─────────────────────────────────────────────────────────────────────────────


def _utc(days_ago: float = 0.0) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(days=days_ago)


def _make_chain(
    chain_id: int = 8453,
    name: str = "Base",
    rpc_url: str = "https://mainnet.base.org",
    native_symbol: str = "ETH",
    wrapped_native: str = "0x4200000000000000000000000000000000000006",
):
    from config.chains import ChainConfig

    return ChainConfig(
        chain_id=chain_id,
        name=name,
        rpc_url=rpc_url,
        native_symbol=native_symbol,
        wrapped_native=wrapped_native,
    )


def _make_candidate(
    address: str = "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa",
    symbol: str = "USDC",
    chain_id: int = 8453,
    chain_name: str = "base",
    liquidity_usd: float = 500_000.0,
):
    from core.token_security.models import DexscreenerCandidate

    return DexscreenerCandidate(
        symbol=symbol,
        address=address,
        chain_id=chain_id,
        chain_name=chain_name,
        liquidity_usd=liquidity_usd,
    )


def _make_resolved(
    symbol: str = "USDC",
    chain_name: str = "base",
    chain_id: int = 8453,
    address: str = "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa",
    security_tier=None,
    is_safe: bool = True,
    flags=None,
    liquidity_usd: float = 500_000.0,
    days_old: float = 0.0,
):
    from core.token_security.models import ResolvedToken, SecurityTier

    tier = security_tier if security_tier is not None else SecurityTier.GOPLUS_VERIFIED
    return ResolvedToken(
        symbol=symbol,
        chain_name=chain_name,
        chain_id=chain_id,
        address=address,
        decimals=6,
        security_tier=tier,
        is_safe=is_safe,
        flags=flags or [],
        liquidity_usd=liquidity_usd,
        source="dexscreener+goplus",
        last_checked=_utc(days_old),
    )


def _make_goplus_report(
    address: str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    chain_id: int = 8453,
    is_safe: bool = True,
    flags=None,
):
    from core.token_security.goplus_scanner import GoplusTokenReport
    from core.token_security.models import SecurityTier

    tier = SecurityTier.GOPLUS_VERIFIED if is_safe else SecurityTier.UNSAFE
    return GoplusTokenReport(
        address=address,
        chain_id=chain_id,
        token_name="USD Coin",
        token_symbol="USDC",
        decimals=6,
        flags=flags or [],
        security_tier=tier,
    )


def _make_sim_report(
    address: str = "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa",
    chain_id: int = 50312,
    is_safe: bool = True,
    flags=None,
):
    from core.token_security.models import SecurityFlag, SecurityTier
    from core.token_security.simulation_fallback import SimulationReport

    tier = SecurityTier.FALLBACK_HEURISTIC if is_safe else SecurityTier.UNVERIFIED
    f = list(flags) if flags is not None else []
    if not is_safe and SecurityFlag.SIMULATION_REVERTED not in f:
        f.append(SecurityFlag.SIMULATION_REVERTED)
    return SimulationReport(
        address=address,
        chain_id=chain_id,
        rpc_url="https://dream-rpc.somnia.network",
        symbol="MYTOKEN",
        decimals=18,
        total_supply_raw=10**27,
        transfer_gas_estimate=45_000 if is_safe else None,
        transfer_reverted=not is_safe,
        liquidity_usd=10_000.0,
        flags=f,
        security_tier=tier,
    )


def _mock_col(find_one_return=None):
    """MagicMock shaped like a pymongo Collection."""
    col = MagicMock()
    col.find_one.return_value = find_one_return
    col.replace_one.return_value = MagicMock(upserted_id=None)
    col.delete_one.return_value = MagicMock(deleted_count=0)
    col.delete_many.return_value = MagicMock(deleted_count=0)
    col.count_documents.return_value = 0
    col.aggregate.return_value = iter([])
    col.create_indexes.return_value = None
    return col


def _build_manager(
    cache_get=None, goplus_return=None, sim_return=None, registry_get=None
):
    """
    Return (mgr, mock_cache, mock_goplus, mock_sim) — no real I/O.

    The mock registry is injected into the manager and accessible via
    ``mgr._registry``.  Pass ``registry_get`` to control what
    ``mgr._registry.get()`` returns (simulates a registry hit).
    ``mgr._registry.get_by_alias()`` defaults to ``None`` (no alias hit);
    override it with ``mgr._registry.get_by_alias.return_value = entry``
    inside the test body when needed.
    """
    from core.token_security.cache import TokenSecurityCache
    from core.token_security.goplus_scanner import GoplusScanner
    from core.token_security.resolver import TokenSecurityManager
    from core.token_security.simulation_fallback import SimulationFallback
    from core.token_security.token_db import TokenRegistryDB

    mock_cache = MagicMock(spec=TokenSecurityCache)
    mock_cache.get.return_value = cache_get

    mock_goplus = MagicMock(spec=GoplusScanner)
    if goplus_return is not None:
        mock_goplus.scan_batch.return_value = goplus_return

    mock_sim = MagicMock(spec=SimulationFallback)
    if sim_return is not None:
        mock_sim.scan.return_value = sim_return

    mock_registry = MagicMock(spec=TokenRegistryDB)
    mock_registry.get.return_value = registry_get
    mock_registry.get_by_alias.return_value = None  # default: no alias hit

    mgr = TokenSecurityManager(
        cache=mock_cache,
        goplus=mock_goplus,
        simulation=mock_sim,
        registry=mock_registry,
    )
    return mgr, mock_cache, mock_goplus, mock_sim


# Canonical test addresses
_ADDR = "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"
_ADDR2 = "0xBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBb"
_ADDR_L = _ADDR.lower()
_ADDR2_L = _ADDR2.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Layer 1 – Pure helpers
# ─────────────────────────────────────────────────────────────────────────────


class TestCacheKey:
    """_cache_key produces a stable compound lookup key."""

    def test_format(self):
        from core.token_security.models import _cache_key

        assert _cache_key("usdc", 8453) == "USDC:8453"

    def test_uppercases_symbol(self):
        from core.token_security.models import _cache_key

        assert _cache_key("weth", 1) == "WETH:1"

    def test_different_chains_produce_different_keys(self):
        from core.token_security.models import _cache_key

        assert _cache_key("USDC", 1) != _cache_key("USDC", 137)


# ─────────────────────────────────────────────────────────────────────────────
# Layer 2 – ResolvedToken model
# ─────────────────────────────────────────────────────────────────────────────


class TestResolvedTokenModel:
    def test_to_cache_doc_has_required_keys(self):
        doc = _make_resolved().to_cache_doc()
        for k in ("_cache_key", "expires_at", "last_checked", "security_tier", "flags"):
            assert k in doc, f"Missing key: {k}"

    def test_expires_at_is_7_days_after_last_checked(self):
        doc = _make_resolved().to_cache_doc()
        delta = doc["expires_at"] - doc["last_checked"]
        assert abs(delta.total_seconds() - 7 * 86_400) < 2

    def test_cache_key_format_in_doc(self):
        doc = _make_resolved(symbol="WETH", chain_id=1).to_cache_doc()
        assert doc["_cache_key"] == "WETH:1"

    def test_flags_serialised_as_plain_strings(self):
        from core.token_security.models import SecurityFlag

        doc = _make_resolved(
            flags=[SecurityFlag.HONEYPOT, SecurityFlag.MINTABLE]
        ).to_cache_doc()
        assert doc["flags"] == ["honeypot", "mintable"]

    def test_security_tier_serialised_as_string(self):
        doc = _make_resolved().to_cache_doc()
        assert isinstance(doc["security_tier"], str)
        assert doc["security_tier"] == "goplus_verified"

    def test_round_trip_preserves_all_fields(self):
        from core.token_security.models import ResolvedToken

        orig = _make_resolved(symbol="WETH", chain_id=1, chain_name="ethereum")
        rest = ResolvedToken.from_cache_doc(orig.to_cache_doc())
        assert rest.symbol == orig.symbol
        assert rest.chain_id == orig.chain_id
        assert rest.address == orig.address
        assert rest.security_tier == orig.security_tier
        assert rest.is_safe == orig.is_safe

    def test_from_cache_doc_makes_naive_datetime_tz_aware(self):
        from core.token_security.models import ResolvedToken

        doc = _make_resolved().to_cache_doc()
        doc["last_checked"] = doc["last_checked"].replace(tzinfo=None)
        restored = ResolvedToken.from_cache_doc(doc)
        assert restored.last_checked.tzinfo is not None

    def test_security_summary_contains_symbol_and_tier_label(self):
        s = _make_resolved(symbol="USDC").security_summary
        assert "USDC" in s and "GoPlus" in s

    def test_security_summary_safe_token_has_checkmark(self):
        assert "✅" in _make_resolved(is_safe=True, flags=[]).security_summary

    def test_security_summary_flagged_token_has_warning(self):
        from core.token_security.models import SecurityFlag

        t = _make_resolved(flags=[SecurityFlag.HIGH_BUY_TAX])
        assert "⚠️" in t.security_summary

    def test_security_summary_fallback_has_microscope_icon(self):
        from core.token_security.models import SecurityTier

        t = _make_resolved(security_tier=SecurityTier.FALLBACK_HEURISTIC)
        assert "🔬" in t.security_summary

    def test_has_critical_flags_true_for_honeypot(self):
        from core.token_security.models import SecurityFlag

        assert _make_resolved(flags=[SecurityFlag.HONEYPOT]).has_critical_flags is True

    def test_has_critical_flags_true_for_transfer_paused(self):
        from core.token_security.models import SecurityFlag

        assert (
            _make_resolved(flags=[SecurityFlag.TRANSFER_PAUSED]).has_critical_flags
            is True
        )

    def test_has_critical_flags_true_for_cannot_sell(self):
        from core.token_security.models import SecurityFlag

        assert (
            _make_resolved(flags=[SecurityFlag.CANNOT_SELL]).has_critical_flags is True
        )

    def test_has_critical_flags_false_for_non_critical_flags(self):
        from core.token_security.models import SecurityFlag

        t = _make_resolved(flags=[SecurityFlag.HIGH_BUY_TAX, SecurityFlag.MINTABLE])
        assert t.has_critical_flags is False

    def test_is_native_true_for_zero_address(self):
        t = _make_resolved(address="0x0000000000000000000000000000000000000000")
        assert t.is_native is True

    def test_is_native_false_for_contract_address(self):
        t = _make_resolved(address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")
        assert t.is_native is False

    def test_human_flags_returns_plain_strings(self):
        from core.token_security.models import SecurityFlag

        t = _make_resolved(flags=[SecurityFlag.MINTABLE, SecurityFlag.PROXY_CONTRACT])
        assert t.human_flags == ["mintable", "proxy_contract"]

    def test_human_flags_empty_when_no_flags(self):
        assert _make_resolved(flags=[]).human_flags == []


# ─────────────────────────────────────────────────────────────────────────────
# Layer 3 – TokenSecurityCache  (MongoDB fully mocked)
# ─────────────────────────────────────────────────────────────────────────────


class TestTokenSecurityCache:
    def _build(self, find_one_return=None, refresh_callback=None):
        col = _mock_col(find_one_return=find_one_return)
        with patch(
            "core.token_security.cache.MongoDB.get_collection", return_value=col
        ):
            from core.token_security.cache import TokenSecurityCache

            cache = TokenSecurityCache(refresh_callback=refresh_callback)
        cache._col = col  # swap in the mock after construction
        return cache, col

    # ── get() ──────────────────────────────────────────────────────────────

    def test_get_miss_returns_none(self):
        cache, _ = self._build()
        assert cache.get("USDC", 8453) is None

    def test_get_fresh_hit_returns_correct_token(self):
        doc = _make_resolved(days_old=1).to_cache_doc()
        doc["last_checked"] = _utc(days_ago=1)
        cache, _ = self._build(find_one_return=doc)
        result = cache.get("USDC", 8453)
        assert result is not None and result.symbol == "USDC"

    def test_get_fresh_hit_does_not_trigger_refresh(self):
        cb = MagicMock()
        doc = _make_resolved(days_old=1).to_cache_doc()
        doc["last_checked"] = _utc(days_ago=1)
        cache, _ = self._build(find_one_return=doc, refresh_callback=cb)
        cache.get("USDC", 8453)
        time.sleep(0.05)
        cb.assert_not_called()

    def test_get_stale_hit_returns_token_immediately(self):
        doc = _make_resolved(days_old=120.5).to_cache_doc()
        doc["last_checked"] = _utc(days_ago=120.5)
        cache, _ = self._build(find_one_return=doc)
        result = cache.get("USDC", 8453)
        assert result is not None and result.symbol == "USDC"

    def test_get_stale_hit_schedules_background_refresh(self):
        cb = MagicMock()
        doc = _make_resolved(days_old=120.5).to_cache_doc()
        doc["last_checked"] = _utc(days_ago=120.5)
        cache, _ = self._build(find_one_return=doc, refresh_callback=cb)
        cache.get("USDC", 8453)
        time.sleep(0.15)  # give the daemon thread time to fire
        cb.assert_called_once_with("USDC", 8453)

    def test_get_stale_without_callback_does_not_crash(self):
        doc = _make_resolved(days_old=120.5).to_cache_doc()
        doc["last_checked"] = _utc(days_ago=120.5)
        cache, _ = self._build(find_one_return=doc, refresh_callback=None)
        assert cache.get("USDC", 8453) is not None  # must not raise

    def test_get_expired_returns_none(self):
        doc = _make_resolved(days_old=122).to_cache_doc()
        doc["last_checked"] = _utc(days_ago=122)
        cache, _ = self._build(find_one_return=doc)
        assert cache.get("USDC", 8453) is None

    def test_get_expired_deletes_document_immediately(self):
        doc = _make_resolved(days_old=122).to_cache_doc()
        doc["last_checked"] = _utc(days_ago=122)
        cache, col = self._build(find_one_return=doc)
        cache.get("USDC", 8453)
        col.delete_one.assert_called_once()

    def test_get_corrupted_doc_returns_none(self):
        bad = {"_cache_key": "USDC:8453", "garbage": True}
        cache, _ = self._build(find_one_return=bad)
        assert cache.get("USDC", 8453) is None

    def test_get_corrupted_doc_deletes_document(self):
        bad = {"_cache_key": "USDC:8453", "garbage": True}
        cache, col = self._build(find_one_return=bad)
        cache.get("USDC", 8453)
        col.delete_one.assert_called_once()

    # ── set() ──────────────────────────────────────────────────────────────

    def test_set_calls_replace_one_with_upsert_true(self):
        cache, col = self._build()
        cache.set(_make_resolved())
        _, kwargs = col.replace_one.call_args
        assert kwargs.get("upsert") is True

    def test_set_uses_correct_cache_key_as_filter(self):
        cache, col = self._build()
        cache.set(_make_resolved(symbol="WETH", chain_id=1))
        filter_doc, _ = col.replace_one.call_args[0]
        assert filter_doc == {"_cache_key": "WETH:1"}

    def test_set_survives_duplicate_key_error(self):
        from pymongo.errors import DuplicateKeyError

        cache, col = self._build()
        col.replace_one.side_effect = DuplicateKeyError("dup")
        cache.set(_make_resolved())  # must not raise

    def test_set_survives_generic_db_error(self):
        cache, col = self._build()
        col.replace_one.side_effect = RuntimeError("connection refused")
        cache.set(_make_resolved())  # must not raise

    # ── invalidate() ───────────────────────────────────────────────────────

    def test_invalidate_returns_true_when_document_deleted(self):
        cache, col = self._build()
        col.delete_one.return_value = MagicMock(deleted_count=1)
        assert cache.invalidate("USDC", 8453) is True

    def test_invalidate_returns_false_when_document_absent(self):
        cache, col = self._build()
        col.delete_one.return_value = MagicMock(deleted_count=0)
        assert cache.invalidate("USDC", 8453) is False

    def test_invalidate_passes_correct_filter(self):
        cache, col = self._build()
        col.delete_one.return_value = MagicMock(deleted_count=1)
        cache.invalidate("USDC", 8453)
        col.delete_one.assert_called_once_with({"_cache_key": "USDC:8453"})

    # ── invalidate_chain() ─────────────────────────────────────────────────

    def test_invalidate_chain_returns_deleted_count(self):
        cache, col = self._build()
        col.delete_many.return_value = MagicMock(deleted_count=7)
        assert cache.invalidate_chain(8453) == 7

    def test_invalidate_chain_filters_by_chain_id(self):
        cache, col = self._build()
        col.delete_many.return_value = MagicMock(deleted_count=0)
        cache.invalidate_chain(8453)
        col.delete_many.assert_called_once_with({"chain_id": 8453})

    # ── _ensure_indexes ────────────────────────────────────────────────────

    def test_ensure_indexes_called_on_construction(self):
        col = _mock_col()
        with patch(
            "core.token_security.cache.MongoDB.get_collection", return_value=col
        ):
            from core.token_security.cache import TokenSecurityCache

            TokenSecurityCache()
        col.create_indexes.assert_called_once()

    def test_ensure_indexes_does_not_raise_on_error(self):
        col = _mock_col()
        col.create_indexes.side_effect = RuntimeError("index conflict")
        with patch(
            "core.token_security.cache.MongoDB.get_collection", return_value=col
        ):
            from core.token_security.cache import TokenSecurityCache

            TokenSecurityCache()  # must not raise


# ─────────────────────────────────────────────────────────────────────────────
# Layer 4-a – _try_native_token
# ─────────────────────────────────────────────────────────────────────────────


class TestTryNativeToken:
    """_try_native_token: zero-address shortcut for native gas tokens."""

    def _mgr(self):
        return _build_manager()[0]

    def test_match_returns_token(self):
        chain = _make_chain(native_symbol="ETH")
        assert self._mgr()._try_native_token("ETH", "base", 8453, chain) is not None

    def test_match_zero_address(self):
        chain = _make_chain(native_symbol="ETH")
        r = self._mgr()._try_native_token("ETH", "base", 8453, chain)
        assert r is not None
        assert r.address == "0x0000000000000000000000000000000000000000"

    def test_match_whitelist_tier_and_safe(self):
        from core.token_security.models import SecurityTier

        chain = _make_chain(native_symbol="ETH")
        r = self._mgr()._try_native_token("ETH", "base", 8453, chain)
        assert r is not None
        assert r.security_tier == SecurityTier.WHITELIST
        assert r.is_safe is True

    def test_non_native_symbol_returns_none(self):
        chain = _make_chain(native_symbol="ETH")
        assert self._mgr()._try_native_token("USDC", "base", 8453, chain) is None

    def test_empty_native_symbol_returns_none(self):
        chain = _make_chain(native_symbol="")
        assert self._mgr()._try_native_token("ETH", "base", 8453, chain) is None

    def test_matic_on_polygon(self):
        chain = _make_chain(chain_id=137, name="Polygon", native_symbol="MATIC")
        r = self._mgr()._try_native_token("MATIC", "polygon", 137, chain)
        assert r is not None and r.symbol == "MATIC"

    def test_stt_on_somnia(self):
        chain = _make_chain(chain_id=50312, name="Somnia Testnet", native_symbol="STT")
        r = self._mgr()._try_native_token("STT", "somnia testnet", 50312, chain)
        assert r is not None and r.is_native is True


# ─────────────────────────────────────────────────────────────────────────────
# Layer 4-b – _try_whitelist
# ─────────────────────────────────────────────────────────────────────────────


class TestTryRegistry:
    """_try_registry: MongoDB-backed token registry lookup."""

    @staticmethod
    def _entry(
        symbol="USDC",
        chain_name="base",
        chain_id=8453,
        address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        decimals=6,
        aliases=None,
        name="USD Coin",
    ):
        from core.token_security.token_db import TokenRegistryEntry

        return TokenRegistryEntry(
            symbol=symbol,
            chain_name=chain_name,
            chain_id=chain_id,
            address=address,
            decimals=decimals,
            aliases=aliases or [],
            name=name,
        )

    def test_direct_symbol_hit_returns_correct_address(self):
        entry = self._entry()
        mgr, _, _, _ = _build_manager(registry_get=entry)
        r = mgr._try_registry("USDC", "base", 8453)
        assert r is not None
        assert r.address == "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

    def test_result_has_whitelist_tier(self):
        from core.token_security.models import SecurityTier

        entry = self._entry()
        mgr, _, _, _ = _build_manager(registry_get=entry)
        r = mgr._try_registry("USDC", "base", 8453)
        assert r is not None
        assert r.security_tier == SecurityTier.WHITELIST

    def test_result_is_always_safe(self):
        entry = self._entry()
        mgr, _, _, _ = _build_manager(registry_get=entry)
        r = mgr._try_registry("USDC", "base", 8453)
        assert r is not None
        assert r.is_safe is True

    def test_alias_hit_returns_token(self):
        weth_entry = self._entry(
            symbol="WETH",
            address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            chain_name="ethereum",
            chain_id=1,
            aliases=["eth", "ether", "wrapped ether"],
            name="Wrapped Ether",
        )
        # Direct miss → alias hit
        mgr, _, _, _ = _build_manager(registry_get=None)
        registry = cast(MagicMock, mgr._registry)
        registry.get_by_alias.return_value = weth_entry
        r = mgr._try_registry("ETH", "ethereum", 1)
        assert r is not None
        assert r.address == "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

    def test_unknown_symbol_returns_none(self):
        mgr, _, _, _ = _build_manager(registry_get=None)
        assert mgr._try_registry("PEPE", "ethereum", 1) is None

    def test_known_symbol_unknown_chain_returns_none(self):
        mgr, _, _, _ = _build_manager(registry_get=None)
        assert mgr._try_registry("USDC", "polygon", 137) is None

    def test_source_is_registry(self):
        entry = self._entry()
        mgr, _, _, _ = _build_manager(registry_get=entry)
        r = mgr._try_registry("USDC", "base", 8453)
        assert r is not None
        assert r.source == "registry"

    def test_registry_get_called_with_correct_args(self):
        mgr, _, _, _ = _build_manager(registry_get=None)
        mgr._try_registry("USDC", "base", 8453)
        registry = cast(MagicMock, mgr._registry)
        registry.get.assert_called_once_with("USDC", 8453)

    def test_alias_lookup_not_called_on_direct_hit(self):
        entry = self._entry()
        mgr, _, _, _ = _build_manager(registry_get=entry)
        mgr._try_registry("USDC", "base", 8453)
        registry = cast(MagicMock, mgr._registry)
        registry.get_by_alias.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Layer 4-c – resolve() full pipeline
# ─────────────────────────────────────────────────────────────────────────────


class TestResolveFullPipeline:
    """resolve() exercises every tier with fully mocked I/O."""

    def _base_chain(self):
        return _make_chain(chain_id=8453, name="Base", native_symbol="ETH")

    def _somnia_chain(self):
        return _make_chain(
            chain_id=50312,
            name="Somnia Testnet",
            native_symbol="STT",
            rpc_url="https://dream-rpc.somnia.network",
        )

    # ── Tier 1a: native token ───────────────────────────────────────────────

    def test_tier1a_native_returns_zero_address(self):
        mgr, _, _, _ = _build_manager()
        with patch(
            "core.token_security.resolver.get_chain_by_name",
            return_value=_make_chain(chain_id=8453, name="Base", native_symbol="ETH"),
        ):
            result = mgr.resolve("ETH", "base")
        assert result.address == "0x0000000000000000000000000000000000000000"

    def test_tier1a_native_no_network_calls(self):
        mgr, mock_cache, mock_goplus, mock_sim = _build_manager()
        with patch(
            "core.token_security.resolver.get_chain_by_name",
            return_value=_make_chain(chain_id=8453, name="Base", native_symbol="ETH"),
        ):
            mgr.resolve("ETH", "base")
        mock_cache.get.assert_not_called()
        mock_goplus.scan_batch.assert_not_called()
        mock_sim.scan.assert_not_called()

    def test_tier1a_native_whitelist_tier(self):
        from core.token_security.models import SecurityTier

        mgr, _, _, _ = _build_manager()
        with patch(
            "core.token_security.resolver.get_chain_by_name",
            return_value=_make_chain(chain_id=8453, name="Base", native_symbol="ETH"),
        ):
            result = mgr.resolve("ETH", "base")
        assert result.security_tier == SecurityTier.WHITELIST

    # ── Tier 1b: DB registry ────────────────────────────────────────────────

    def test_tier1b_registry_returns_correct_address(self):
        from core.token_security.token_db import TokenRegistryEntry

        entry = TokenRegistryEntry(
            symbol="USDC",
            chain_name="base",
            chain_id=8453,
            address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            decimals=6,
        )
        mgr, _, _, _ = _build_manager(registry_get=entry)
        with patch(
            "core.token_security.resolver.get_chain_by_name",
            return_value=_make_chain(chain_id=8453, name="Base", native_symbol="ETH"),
        ):
            result = mgr.resolve("USDC", "base")
        assert result.address == "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

    def test_tier1b_registry_skips_cache_and_live(self):
        from core.token_security.token_db import TokenRegistryEntry

        entry = TokenRegistryEntry(
            symbol="USDC",
            chain_name="base",
            chain_id=8453,
            address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            decimals=6,
        )
        mgr, mock_cache, mock_goplus, mock_sim = _build_manager(registry_get=entry)
        with patch(
            "core.token_security.resolver.get_chain_by_name",
            return_value=_make_chain(chain_id=8453, name="Base", native_symbol="ETH"),
        ):
            mgr.resolve("USDC", "base")
        mock_cache.get.assert_not_called()
        mock_goplus.scan_batch.assert_not_called()

    # ── Tier 2: cache hit ───────────────────────────────────────────────────

    def test_tier2_cache_hit_returns_cached_token(self):
        cached = _make_resolved(address=_ADDR)
        mgr, _, mock_goplus, _ = _build_manager(cache_get=cached)
        with patch(
            "core.token_security.resolver.get_chain_by_name",
            return_value=_make_chain(chain_id=8453, name="Base", native_symbol="ETH"),
        ):
            result = mgr.resolve("USDC", "base")
        assert result.address == _ADDR
        mock_goplus.scan_batch.assert_not_called()

    def test_tier2_cache_hit_does_not_write_cache(self):
        cached = _make_resolved(address=_ADDR)
        mgr, mock_cache, _, _ = _build_manager(cache_get=cached)
        with patch(
            "core.token_security.resolver.get_chain_by_name",
            return_value=_make_chain(chain_id=8453, name="Base", native_symbol="ETH"),
        ):
            mgr.resolve("USDC", "base")
        mock_cache.set.assert_not_called()

    # ── Tier 3: GoPlus path ─────────────────────────────────────────────────

    def test_tier3_goplus_single_safe_candidate(self):
        candidate = _make_candidate(address=_ADDR, chain_id=8453)
        report = _make_goplus_report(address=_ADDR_L, chain_id=8453, is_safe=True)
        mgr, mock_cache, _, _ = _build_manager(goplus_return={_ADDR_L: report})
        with (
            patch(
                "core.token_security.resolver.get_chain_by_name",
                return_value=_make_chain(
                    chain_id=8453, name="Base", native_symbol="ETH"
                ),
            ),
            patch(
                "core.token_security.resolver.get_candidates", return_value=[candidate]
            ),
        ):
            result = mgr.resolve("USDC", "base")
        assert result.address == _ADDR
        mock_cache.set.assert_called_once()

    def test_tier3_goplus_skips_unsafe_picks_second_safe(self):
        candidate1 = _make_candidate(
            address=_ADDR, chain_id=8453, liquidity_usd=900_000
        )
        candidate2 = _make_candidate(
            address=_ADDR2, chain_id=8453, liquidity_usd=400_000
        )
        from core.token_security.models import SecurityFlag

        unsafe_report = _make_goplus_report(
            address=_ADDR_L, is_safe=False, flags=[SecurityFlag.HONEYPOT]
        )
        safe_report = _make_goplus_report(address=_ADDR2_L, is_safe=True)
        mgr, _, _, _ = _build_manager(
            goplus_return={_ADDR_L: unsafe_report, _ADDR2_L: safe_report}
        )
        with (
            patch(
                "core.token_security.resolver.get_chain_by_name",
                return_value=_make_chain(
                    chain_id=8453, name="Base", native_symbol="ETH"
                ),
            ),
            patch(
                "core.token_security.resolver.get_candidates",
                return_value=[candidate1, candidate2],
            ),
        ):
            result = mgr.resolve("USDC", "base")
        assert result.address == _ADDR2

    def test_tier3_goplus_all_unsafe_raises_token_unsafe_error(self):
        from core.token_security.models import SecurityFlag, TokenUnsafeError

        candidate = _make_candidate(address=_ADDR, chain_id=8453)
        unsafe_report = _make_goplus_report(
            address=_ADDR_L, is_safe=False, flags=[SecurityFlag.HONEYPOT]
        )
        mgr, _, _, _ = _build_manager(goplus_return={_ADDR_L: unsafe_report})
        with (
            patch(
                "core.token_security.resolver.get_chain_by_name",
                return_value=_make_chain(
                    chain_id=8453, name="Base", native_symbol="ETH"
                ),
            ),
            patch(
                "core.token_security.resolver.get_candidates", return_value=[candidate]
            ),
        ):
            with pytest.raises(TokenUnsafeError):
                mgr.resolve("USDC", "base")

    def test_tier3_goplus_all_unindexed_falls_back_to_simulation(self):
        # GoPlus returns empty dict → no reports for any address → simulation
        candidate = _make_candidate(address=_ADDR, chain_id=8453)
        sim_report = _make_sim_report(address=_ADDR, chain_id=8453, is_safe=True)
        mgr, _, _, mock_sim = _build_manager(
            goplus_return={},  # no reports
            sim_return=sim_report,
        )
        chain = _make_chain(chain_id=8453, name="Base", native_symbol="ETH")
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch("core.token_security.resolver.get_chain_by_id", return_value=chain),
            patch(
                "core.token_security.resolver.get_candidates", return_value=[candidate]
            ),
        ):
            result = mgr.resolve("USDC", "base")
        mock_sim.scan.assert_called_once()
        assert result is not None

    def test_tier3_goplus_all_unindexed_falls_back_across_candidates(self):
        candidate1 = _make_candidate(
            address=_ADDR, chain_id=8453, liquidity_usd=900_000
        )
        candidate2 = _make_candidate(
            address=_ADDR2, chain_id=8453, liquidity_usd=400_000
        )
        mgr, _, _, mock_sim = _build_manager(goplus_return={})
        mock_sim.scan.side_effect = [
            _make_sim_report(address=_ADDR, chain_id=8453, is_safe=False),
            _make_sim_report(address=_ADDR2, chain_id=8453, is_safe=True),
        ]
        chain = _make_chain(chain_id=8453, name="Base", native_symbol="ETH")
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch("core.token_security.resolver.get_chain_by_id", return_value=chain),
            patch(
                "core.token_security.resolver.get_candidates",
                return_value=[candidate1, candidate2],
            ),
        ):
            result = mgr.resolve("USDC", "base")
        assert result.address == _ADDR2
        assert mock_sim.scan.call_count == 2

    def test_tier3_goplus_api_failure_falls_back_to_simulation(self):
        from core.token_security.goplus_scanner import GoplusError

        candidate = _make_candidate(address=_ADDR, chain_id=8453)
        sim_report = _make_sim_report(address=_ADDR, chain_id=8453, is_safe=True)
        mgr, _, mock_goplus, mock_sim = _build_manager(sim_return=sim_report)
        mock_goplus.scan_batch.side_effect = GoplusError("rate limited")
        chain = _make_chain(chain_id=8453, name="Base", native_symbol="ETH")
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch("core.token_security.resolver.get_chain_by_id", return_value=chain),
            patch(
                "core.token_security.resolver.get_candidates", return_value=[candidate]
            ),
        ):
            result = mgr.resolve("USDC", "base")
        mock_sim.scan.assert_called_once()
        assert result is not None

    def test_tier3_goplus_api_failure_falls_back_across_candidates(self):
        from core.token_security.goplus_scanner import GoplusError

        candidate1 = _make_candidate(
            address=_ADDR, chain_id=8453, liquidity_usd=900_000
        )
        candidate2 = _make_candidate(
            address=_ADDR2, chain_id=8453, liquidity_usd=400_000
        )
        mgr, _, mock_goplus, mock_sim = _build_manager()
        mock_goplus.scan_batch.side_effect = GoplusError("rate limited")
        mock_sim.scan.side_effect = [
            _make_sim_report(address=_ADDR, chain_id=8453, is_safe=False),
            _make_sim_report(address=_ADDR2, chain_id=8453, is_safe=True),
        ]
        chain = _make_chain(chain_id=8453, name="Base", native_symbol="ETH")
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch("core.token_security.resolver.get_chain_by_id", return_value=chain),
            patch(
                "core.token_security.resolver.get_candidates",
                return_value=[candidate1, candidate2],
            ),
        ):
            result = mgr.resolve("USDC", "base")
        assert result.address == _ADDR2
        assert mock_sim.scan.call_count == 2

    # ── Tier 3: simulation path ─────────────────────────────────────────────

    def test_tier3_simulation_safe_returns_fallback_heuristic(self):
        from core.token_security.models import SecurityTier

        candidate = _make_candidate(
            address=_ADDR, chain_id=50312, chain_name="somnia testnet"
        )
        sim_report = _make_sim_report(address=_ADDR, chain_id=50312, is_safe=True)
        mgr, _, _, _ = _build_manager(sim_return=sim_report)
        chain = _make_chain(
            chain_id=50312,
            name="Somnia Testnet",
            native_symbol="STT",
            rpc_url="https://dream-rpc.somnia.network",
        )
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch(
                "core.token_security.resolver.get_candidates", return_value=[candidate]
            ),
        ):
            result = mgr.resolve("MYTOKEN", "somnia testnet")
        assert result.security_tier == SecurityTier.FALLBACK_HEURISTIC

    def test_tier3_simulation_reverted_raises_token_unsafe_error(self):
        from core.token_security.models import TokenUnsafeError

        candidate = _make_candidate(
            address=_ADDR, chain_id=50312, chain_name="somnia testnet"
        )
        sim_report = _make_sim_report(address=_ADDR, chain_id=50312, is_safe=False)
        mgr, _, _, _ = _build_manager(sim_return=sim_report)
        chain = _make_chain(
            chain_id=50312,
            name="Somnia Testnet",
            native_symbol="STT",
            rpc_url="https://dream-rpc.somnia.network",
        )
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch(
                "core.token_security.resolver.get_candidates", return_value=[candidate]
            ),
        ):
            with pytest.raises(TokenUnsafeError):
                mgr.resolve("MYTOKEN", "somnia testnet")

    def test_tier3_simulation_result_cached(self):
        candidate = _make_candidate(
            address=_ADDR, chain_id=50312, chain_name="somnia testnet"
        )
        sim_report = _make_sim_report(address=_ADDR, chain_id=50312, is_safe=True)
        mgr, mock_cache, _, _ = _build_manager(sim_return=sim_report)
        chain = _make_chain(
            chain_id=50312,
            name="Somnia Testnet",
            native_symbol="STT",
            rpc_url="https://dream-rpc.somnia.network",
        )
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch(
                "core.token_security.resolver.get_candidates", return_value=[candidate]
            ),
        ):
            mgr.resolve("MYTOKEN", "somnia testnet")
        mock_cache.set.assert_called_once()

    # ── Error paths ─────────────────────────────────────────────────────────

    def test_no_candidates_raises_token_not_found(self):
        from core.token_security.models import TokenNotFoundError

        mgr, _, _, _ = _build_manager()
        chain = _make_chain(chain_id=50312, name="Somnia Testnet", native_symbol="STT")
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch("core.token_security.resolver.get_candidates", return_value=[]),
        ):
            with pytest.raises(TokenNotFoundError):
                mgr.resolve("GHOST", "somnia testnet")

    def test_unknown_chain_raises_value_error(self):
        mgr, _, _, _ = _build_manager()
        with pytest.raises(ValueError, match="not registered"):
            mgr.resolve("USDC", "nonexistent chain xyz")

    def test_dexscreener_error_raises_token_not_found(self):
        from core.token_security.dexscreener import DexscreenerError
        from core.token_security.models import TokenNotFoundError

        mgr, _, _, _ = _build_manager()
        chain = _make_chain(chain_id=50312, name="Somnia Testnet", native_symbol="STT")
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch(
                "core.token_security.resolver.get_candidates",
                side_effect=DexscreenerError("timeout"),
            ),
        ):
            with pytest.raises(TokenNotFoundError):
                mgr.resolve("GHOST", "somnia testnet")


# ─────────────────────────────────────────────────────────────────────────────
# Layer 4-d – resolve_address_only  &  get_security_metadata
# ─────────────────────────────────────────────────────────────────────────────


class TestResolveAddressOnly:
    def test_returns_address_string(self):
        cached = _make_resolved(address=_ADDR)
        mgr, _, _, _ = _build_manager(cache_get=cached)
        with patch(
            "core.token_security.resolver.get_chain_by_name",
            return_value=_make_chain(chain_id=8453, name="Base", native_symbol="ETH"),
        ):
            addr = mgr.resolve_address_only("USDC", "base")
        assert addr == _ADDR

    def test_propagates_token_not_found(self):
        from core.token_security.models import TokenNotFoundError

        mgr, _, _, _ = _build_manager()
        chain = _make_chain(chain_id=50312, name="Somnia Testnet", native_symbol="STT")
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch("core.token_security.resolver.get_candidates", return_value=[]),
        ):
            with pytest.raises(TokenNotFoundError):
                mgr.resolve_address_only("GHOST", "somnia testnet")


class TestGetSecurityMetadata:
    def test_safe_token_returns_correct_shape(self):
        cached = _make_resolved(address=_ADDR)
        mgr, _, _, _ = _build_manager(cache_get=cached)
        with patch(
            "core.token_security.resolver.get_chain_by_name",
            return_value=_make_chain(chain_id=8453, name="Base", native_symbol="ETH"),
        ):
            meta = mgr.get_security_metadata("USDC", "base")
        assert meta["resolved_token"] == _ADDR
        assert meta["is_safe"] is True
        assert "error" not in meta

    def test_not_found_returns_error_key(self):

        mgr, _, _, _ = _build_manager()
        chain = _make_chain(chain_id=50312, name="Somnia Testnet", native_symbol="STT")
        with (
            patch("core.token_security.resolver.get_chain_by_name", return_value=chain),
            patch("core.token_security.resolver.get_candidates", return_value=[]),
        ):
            meta = mgr.get_security_metadata("GHOST", "somnia testnet")
        assert meta["resolved_token"] is None
        assert meta["is_safe"] is False
        assert "error" in meta

    def test_unsafe_token_returns_error_key(self):
        from core.token_security.models import SecurityFlag, TokenUnsafeError

        mgr, _, _, _ = _build_manager()
        with (
            patch(
                "core.token_security.resolver.get_chain_by_name",
                return_value=_make_chain(
                    chain_id=8453, name="Base", native_symbol="ETH"
                ),
            ),
            patch.object(
                mgr,
                "resolve",
                side_effect=TokenUnsafeError(
                    address=_ADDR,
                    flags=[SecurityFlag.HONEYPOT],
                    symbol="SCAM",
                    chain_name="base",
                ),
            ),
        ):
            meta = mgr.get_security_metadata("SCAM", "base")
        assert meta["is_safe"] is False
        assert "honeypot" in meta["flags"]
        assert "error" in meta

    def test_generic_exception_returns_error_key(self):
        mgr, _, _, _ = _build_manager()
        with (
            patch(
                "core.token_security.resolver.get_chain_by_name",
                return_value=_make_chain(
                    chain_id=8453, name="Base", native_symbol="ETH"
                ),
            ),
            patch.object(mgr, "resolve", side_effect=RuntimeError("unexpected")),
        ):
            meta = mgr.get_security_metadata("USDC", "base")
        assert meta["resolved_token"] is None
        assert "error" in meta

    def test_metadata_contains_all_expected_keys(self):
        cached = _make_resolved(address=_ADDR)
        mgr, _, _, _ = _build_manager(cache_get=cached)
        with patch(
            "core.token_security.resolver.get_chain_by_name",
            return_value=_make_chain(chain_id=8453, name="Base", native_symbol="ETH"),
        ):
            meta = mgr.get_security_metadata("USDC", "base")
        for key in (
            "resolved_token",
            "security_source",
            "security_tier",
            "is_safe",
            "flags",
            "summary",
            "liquidity_usd",
        ):
            assert key in meta, f"Missing key: {key}"


# ─────────────────────────────────────────────────────────────────────────────
# Layer 5 – _background_refresh
# ─────────────────────────────────────────────────────────────────────────────


class TestBackgroundRefresh:
    def test_success_calls_resolve_live_and_sets_cache(self):
        resolved = _make_resolved(address=_ADDR, chain_id=8453)
        mgr, mock_cache, _, _ = _build_manager()
        chain = _make_chain(chain_id=8453, name="Base", native_symbol="ETH")
        with (
            patch("core.token_security.resolver.get_chain_by_id", return_value=chain),
            patch.object(mgr, "_resolve_live", return_value=resolved) as mock_live,
        ):
            mgr._background_refresh("USDC", 8453)
        mock_live.assert_called_once_with(
            symbol="USDC",
            chain_name="base",
            chain_id=8453,
            rpc_url=chain.rpc_url,
        )
        mock_cache.set.assert_called_once_with(resolved)

    def test_exception_is_swallowed_not_raised(self):
        mgr, _, _, _ = _build_manager()
        with patch(
            "core.token_security.resolver.get_chain_by_id",
            side_effect=KeyError("unknown chain"),
        ):
            # Must not raise — exceptions must be swallowed
            mgr._background_refresh("USDC", 99999)

    def test_resolve_live_failure_is_swallowed(self):
        mgr, mock_cache, _, _ = _build_manager()
        chain = _make_chain(chain_id=8453, name="Base", native_symbol="ETH")
        with (
            patch("core.token_security.resolver.get_chain_by_id", return_value=chain),
            patch.object(mgr, "_resolve_live", side_effect=RuntimeError("rpc down")),
        ):
            mgr._background_refresh("USDC", 8453)
        mock_cache.set.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Layer 6 – get_token_security_manager singleton
# ─────────────────────────────────────────────────────────────────────────────


class TestGetTokenSecurityManager:
    def teardown_method(self):
        import core.token_security.resolver as mod

        mod._MANAGER = None

    def test_returns_token_security_manager_instance(self):
        from core.token_security.resolver import (
            TokenSecurityManager,
            get_token_security_manager,
        )

        col = _mock_col()
        with (
            patch("core.token_security.cache.MongoDB.get_collection", return_value=col),
            patch(
                "core.token_security.token_db.MongoDB.get_collection", return_value=col
            ),
        ):
            mgr = get_token_security_manager()
        assert isinstance(mgr, TokenSecurityManager)

    def test_repeated_calls_return_same_instance(self):
        from core.token_security.resolver import get_token_security_manager

        col = _mock_col()
        with (
            patch("core.token_security.cache.MongoDB.get_collection", return_value=col),
            patch(
                "core.token_security.token_db.MongoDB.get_collection", return_value=col
            ),
        ):
            first = get_token_security_manager()
            second = get_token_security_manager()
        assert first is second

    def test_re_creates_after_manual_reset(self):
        import core.token_security.resolver as mod
        from core.token_security.resolver import get_token_security_manager

        col = _mock_col()
        with (
            patch("core.token_security.cache.MongoDB.get_collection", return_value=col),
            patch(
                "core.token_security.token_db.MongoDB.get_collection", return_value=col
            ),
        ):
            first = get_token_security_manager()
            mod._MANAGER = None
            second = get_token_security_manager()
        assert first is not second
