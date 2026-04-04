from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from core.token_security.goplus_scanner import GoplusScanner, GoplusTokenReport
from core.token_security.models import (
    ResolvedToken,
    SecurityFlag,
    SecurityTier,
    TokenUnsafeError,
)


def _utc(days_ago: float = 0.0) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(days=days_ago)


def _make_chain(
    chain_id: int = 8453,
    name: str = "Base",
    rpc_url: str = "https://mainnet.base.org",
    native_symbol: str = "ETH",
):
    from config.chains import ChainConfig

    return ChainConfig(
        chain_id=chain_id,
        name=name,
        rpc_url=rpc_url,
        native_symbol=native_symbol,
        wrapped_native="0x4200000000000000000000000000000000000006",
    )


def _make_candidate(
    address: str,
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


def _make_sim_report(
    address: str,
    chain_id: int,
    is_safe: bool,
):
    from core.token_security.simulation_fallback import SimulationReport

    flags = [] if is_safe else [SecurityFlag.SIMULATION_REVERTED]
    return SimulationReport(
        address=address,
        chain_id=chain_id,
        rpc_url="https://rpc.example",
        symbol="USDC",
        decimals=6,
        total_supply_raw=10**12,
        transfer_gas_estimate=45_000 if is_safe else None,
        transfer_reverted=not is_safe,
        liquidity_usd=50_000.0,
        flags=flags,
        security_tier=(
            SecurityTier.FALLBACK_HEURISTIC if is_safe else SecurityTier.UNVERIFIED
        ),
    )


def _mock_col(find_one_return=None):
    col = MagicMock()
    col.find_one.return_value = find_one_return
    col.replace_one.return_value = MagicMock(upserted_id=None)
    col.delete_one.return_value = MagicMock(deleted_count=0)
    col.delete_many.return_value = MagicMock(deleted_count=0)
    col.count_documents.return_value = 0
    col.aggregate.return_value = iter([])
    col.create_indexes.return_value = None
    return col


def test_cache_rejects_unsafe_cached_documents():
    from core.token_security.cache import TokenSecurityCache

    unsafe_doc = ResolvedToken(
        symbol="SCAM",
        chain_name="base",
        chain_id=8453,
        address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        decimals=18,
        security_tier=SecurityTier.UNSAFE,
        is_safe=False,
        flags=[SecurityFlag.HONEYPOT],
        source="dexscreener+goplus",
        last_checked=_utc(1),
    ).to_cache_doc()
    col = _mock_col(find_one_return=unsafe_doc)

    with patch("core.token_security.cache.MongoDB.get_collection", return_value=col):
        cache = TokenSecurityCache()

    assert cache.get("SCAM", 8453) is None
    col.delete_one.assert_called_once_with({"_cache_key": "SCAM:8453"})


def test_cache_stale_refresh_is_deduplicated_while_in_flight():
    from core.token_security.cache import TokenSecurityCache

    stale_doc = ResolvedToken(
        symbol="USDC",
        chain_name="base",
        chain_id=8453,
        address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        decimals=6,
        security_tier=SecurityTier.GOPLUS_VERIFIED,
        is_safe=True,
        flags=[],
        source="dexscreener+goplus",
        last_checked=_utc(120.5),
    ).to_cache_doc()
    col = _mock_col(find_one_return=stale_doc)
    calls: list[tuple[str, int]] = []

    def refresh(symbol: str, chain_id: int) -> None:
        calls.append((symbol, chain_id))
        time.sleep(0.1)

    with patch("core.token_security.cache.MongoDB.get_collection", return_value=col):
        cache = TokenSecurityCache(refresh_callback=refresh)

    assert cache.get("USDC", 8453) is not None
    assert cache.get("USDC", 8453) is not None
    time.sleep(0.2)

    assert calls == [("USDC", 8453)]


def test_background_refresh_invalidates_cache_when_token_turns_unsafe():
    from core.token_security.resolver import TokenSecurityManager

    mock_cache = MagicMock()
    manager = TokenSecurityManager(
        cache=mock_cache,
        goplus=MagicMock(),
        simulation=MagicMock(),
        registry=MagicMock(),
    )

    with (
        patch("core.token_security.resolver.get_chain_by_id", return_value=_make_chain()),
        patch.object(
            manager,
            "_resolve_live",
            side_effect=TokenUnsafeError(
                address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                flags=[SecurityFlag.HONEYPOT],
                symbol="SCAM",
                chain_name="base",
                source="goplus",
            ),
        ),
    ):
        manager._background_refresh("SCAM", 8453)

    mock_cache.invalidate.assert_called_once_with("SCAM", 8453)
    mock_cache.set.assert_not_called()


def test_simulation_fallback_skips_unsafe_top_candidate_and_returns_next_safe():
    from core.token_security.resolver import TokenSecurityManager

    candidate_one = _make_candidate(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        liquidity_usd=900_000.0,
    )
    candidate_two = _make_candidate(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        liquidity_usd=400_000.0,
    )
    simulation = MagicMock()
    simulation.scan.side_effect = [
        _make_sim_report(candidate_one.address, 8453, is_safe=False),
        _make_sim_report(candidate_two.address, 8453, is_safe=True),
    ]
    manager = TokenSecurityManager(
        cache=MagicMock(),
        goplus=MagicMock(),
        simulation=simulation,
        registry=MagicMock(),
    )

    resolved = manager._resolve_via_simulation(
        [candidate_one, candidate_two],
        chain_name="base",
        rpc_url="https://mainnet.base.org",
    )

    assert resolved.address == candidate_two.address
    assert simulation.scan.call_count == 2


def test_get_security_metadata_preserves_simulation_source_for_unsafe_results():
    from core.token_security.resolver import TokenSecurityManager

    manager = TokenSecurityManager(
        cache=MagicMock(),
        goplus=MagicMock(),
        simulation=MagicMock(),
        registry=MagicMock(),
    )

    with patch.object(
        manager,
        "resolve",
        side_effect=TokenUnsafeError(
            address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            flags=[SecurityFlag.SIMULATION_REVERTED],
            symbol="SCAM",
            chain_name="base",
            source="simulation",
        ),
    ):
        metadata = manager.get_security_metadata("SCAM", "base")

    assert metadata["security_source"] == "simulation"


def test_goplus_scan_single_normalizes_lookup_key():
    report = GoplusTokenReport(
        address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        chain_id=8453,
        token_symbol="USDC",
    )
    scanner = GoplusScanner()
    scanner.scan_batch = MagicMock(
        return_value={"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": report}
    )

    result = scanner.scan_single("  0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa  ", 8453)

    assert result is report


def test_dexscreener_parse_pairs_ignores_malformed_liquidity_values():
    from core.token_security.dexscreener import _parse_pairs

    raw = {
        "pairs": [
            {
                "chainId": "base",
                "baseToken": {
                    "symbol": "USDC",
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
                "liquidity": {"usd": "not-a-number"},
            }
        ]
    }

    assert _parse_pairs(raw, symbol="USDC", target_chain_id=8453) == []


def test_token_registry_get_by_address_escapes_regex_fallback():
    from core.token_security.token_db import TokenRegistryDB

    doc = {
        "symbol": "SCAM",
        "chain_name": "base",
        "chain_id": 8453,
        "address": "0xabc.*",
        "decimals": 18,
        "aliases": [],
        "is_active": True,
    }
    col = _mock_col()
    col.find_one.side_effect = [None, doc]
    db = TokenRegistryDB(collection=col)

    entry = db.get_by_address("0xabc.*", 8453)

    assert entry is not None
    first_query = col.find_one.call_args_list[0].args[0]
    second_query = col.find_one.call_args_list[1].args[0]
    assert first_query["address_lower"] == "0xabc.*"
    assert second_query["address"]["$regex"] == r"^0xabc\.\*$"
