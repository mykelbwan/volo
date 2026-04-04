from decimal import Decimal

import pytest
from core.planning.fee_table import (
    GLOBAL_DEFAULT_PERCENT,
    FeeTable,
)
from core.planning.vws import (
    ARRIVAL_BUFFER,
    BRIDGE_FEE_TABLE,
    NATIVE_ADDRESS,
    VirtualWalletState,
)


def test_fee_table_percent_plus_flat_and_percent_rules():
    # Build fee rules via the raw dict helper (mirrors config rows)
    raw_cbridge = {
        "protocol_id": "cbridge_v2",
        "src_chain": "ethereum",
        "dst_chain": "base",
        "token": "USDC",
        "fee_type": "percent_plus_flat",
        "percent": 0.0012,
        "flat": 0.5,
        "min_fee": 0.1,
        "max_fee": 50.0,
        "last_updated": "2026-03-01T12:00:00Z",
    }

    raw_hop = {
        "protocol_id": "hop",
        "src_chain": "ethereum",
        "dst_chain": "polygon",
        "token": None,
        "fee_type": "percent",
        "percent": 0.002,
        "flat": None,
        "last_updated": "2026-02-21T08:30:00Z",
    }

    # Use the internal loader to parse raw rows into FeeRule objects then build table
    rule_c = FeeTable._rule_from_raw(raw_cbridge, 0)
    rule_h = FeeTable._rule_from_raw(raw_hop, 1)
    tbl = FeeTable([rule_c, rule_h])

    # cbridge: percent 0.0012 * 1000 = 1.2 + flat 0.5 => 1.7
    fee_usdc, used_rule = tbl.estimate_fee_for_amount(
        Decimal("1000"), "ethereum", "base", token="USDC", protocol="cbridge_v2"
    )
    assert used_rule is not None
    assert used_rule.protocol_id == "cbridge_v2"
    assert fee_usdc == Decimal("1.70000000")

    # hop: percent 0.002 * 100 = 0.2
    fee_hop, used_rule2 = tbl.estimate_fee_for_amount(
        Decimal("100"), "ethereum", "polygon", token="ETH", protocol="hop"
    )
    assert used_rule2 is not None
    assert used_rule2.protocol_id == "hop"
    assert fee_hop == Decimal("0.20000000")


def test_fee_table_fallback_for_unknown_pair():
    tbl = FeeTable([])  # empty table
    amt = Decimal("50")
    fee, used = tbl.estimate_fee_for_amount(amt, "unknown_a", "unknown_b", token="X")
    # Should use global fallback percent
    expected = (amt * GLOBAL_DEFAULT_PERCENT).quantize(Decimal("0.00000001"))
    assert used is None
    assert fee == expected


def test_vws_estimate_bridge_arrival_matches_expected_formula():
    # Validate the same formula implemented in VWS for a known protocol key.
    amount = Decimal("100")
    protocol = "across"
    # fee_rate from BRIDGE_FEE_TABLE
    fee_rate = BRIDGE_FEE_TABLE.get(protocol)
    assert fee_rate is not None
    expected = amount * (Decimal("1") - fee_rate) * ARRIVAL_BUFFER
    arrival = VirtualWalletState.estimate_bridge_arrival(amount, protocol)
    assert arrival == expected


def test_vws_bridge_then_swap_fails_when_destination_lacks_native_gas():
    """
    Simulate a 'bridge_first' candidate:
      - Source: ethereum
      - Dest: base
    The source wallet has enough native to pay for the bridge and enough token
    to bridge. The destination chain has zero native funds, so attempting a
    subsequent swap on the destination should fail due to missing native gas.
    """
    sender = "0xabc"
    # Prepare snapshot:
    # - source native (ETH) sufficient to pay gas
    # - source token (0xtoken) has 1.0 available to bridge
    # - destination native on 'base' is absent (implicitly zero)
    snapshot = {
        f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.1",  # plenty for gas (conservative)
        f"{sender}|ethereum|0xtoken": "1.0",
        # Note: no entry for base native => 0 balance for gas on destination
    }

    vws = VirtualWalletState.from_balance_snapshot(snapshot, sender=sender)

    # Run bridge step: should succeed and credit dest token balance
    bridge_res = vws.simulate_bridge(
        source_chain="ethereum",
        source_chain_id=1,  # non-None so gas is estimated (fallback used)
        dest_chain="base",
        token_address="0xtoken",
        dest_token_address="0xdest",
        amount=Decimal("1.0"),
        protocol="across",
        native_address=NATIVE_ADDRESS,
    )
    assert bridge_res.success is True

    # Destination token balance should equal conservative arrival estimate
    expected_arrival = VirtualWalletState.estimate_bridge_arrival(
        Decimal("1.0"), "across"
    )
    dest_balance = vws.get_balance("base", "0xdest")
    assert dest_balance == expected_arrival

    # Now attempt to swap on the destination chain (requires native gas there)
    swap_res = vws.simulate_swap(
        chain="base",
        chain_id=8453,  # arbitrary non-None chain id to trigger gas check
        token_in_address="0xdest",
        amount_in=Decimal("0.1"),
        native_address=NATIVE_ADDRESS,
    )
    assert swap_res.success is False
    # Should fail because destination chain has no native token to cover gas
    assert "not enough gas on base" in swap_res.rejection_reason


def test_vws_swap_fails_when_source_lacks_native_gas_or_token():
    sender = "0xdead"
    # No native balance and no token balance => both checks would fail.
    snapshot = {
        f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.0",
        f"{sender}|ethereum|0xtoken": "0.0",
    }
    vws = VirtualWalletState.from_balance_snapshot(snapshot, sender=sender)

    # Attempt swap: should fail on gas check first (no native)
    res = vws.simulate_swap(
        chain="ethereum",
        chain_id=1,
        token_in_address="0xtoken",
        amount_in=Decimal("0.5"),
        native_address=NATIVE_ADDRESS,
    )
    assert res.success is False
    assert "not enough gas on ethereum" in res.rejection_reason

    # If we provide minimal native but insufficient token, it should fail on token check
    snapshot2 = {
        f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.1",
        f"{sender}|ethereum|0xtoken": "0.1",  # need 0.5
    }
    vws2 = VirtualWalletState.from_balance_snapshot(snapshot2, sender=sender)
    res2 = vws2.simulate_swap(
        chain="ethereum",
        chain_id=1,
        token_in_address="0xtoken",
        amount_in=Decimal("0.5"),
        native_address=NATIVE_ADDRESS,
    )
    assert res2.success is False
    assert "not enough 0xtoken" or "not enough 0xtoken" in res2.rejection_reason
