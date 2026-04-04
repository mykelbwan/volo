import asyncio
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from tool_nodes.bridge.executors.across_executor import _build_approve_tx
from tool_nodes.bridge.simulators.across_simulator import AcrossBridgeQuote


class _DummyContract:
    def encode_abi(self, fn_name, args):
        assert fn_name == "approve"
        assert len(args) == 2
        return "0xdata"


class _DummyEth:
    def contract(self, address, abi):
        return _DummyContract()


class _DummyW3:
    eth = _DummyEth()

    def to_checksum_address(self, addr):
        return addr


def test_build_approve_tx_shapes_fields():
    w3 = _DummyW3()
    tx = _build_approve_tx(
        w3=w3,
        token_address="0xtoken",
        spender="0xspender",
        nonce=1,
        max_fee_per_gas=123,
        max_priority_fee_per_gas=123,
        chain_id=1,
    )

    assert tx["to"] == "0xtoken"
    assert tx["data"] == "0xdata"
    assert tx["nonce"] == 1
    assert tx["gas"] == hex(100_000)
    assert tx["maxFeePerGas"] == hex(123)
    assert tx["maxPriorityFeePerGas"] == hex(123)
    assert tx["chainId"] == 1


def _quote_with_timestamp(ts: int, fill_deadline: int) -> AcrossBridgeQuote:
    return AcrossBridgeQuote(
        protocol="across",
        token_symbol="USDC",
        input_token="0xinput",
        output_token="0xoutput",
        source_chain_id=1,
        dest_chain_id=8453,
        source_chain_name="Ethereum",
        dest_chain_name="Base",
        input_amount=Decimal("100"),
        output_amount=Decimal("99"),
        total_fee=Decimal("1"),
        total_fee_pct=Decimal("1.0"),
        lp_fee=Decimal("0.5"),
        relayer_fee=Decimal("0.4"),
        gas_fee=Decimal("0.1"),
        input_decimals=6,
        output_decimals=6,
        quote_timestamp=ts,
        fill_deadline=fill_deadline,
        exclusivity_deadline=0,
        exclusive_relayer="0xrelayer",
        spoke_pool="0xpool",
        is_native_input=False,
        avg_fill_time_seconds=120,
    )


def test_execute_across_bridge_rejects_stale_quote():
    from tool_nodes.bridge.executors.across_executor import execute_across_bridge

    now = 1_700_000_000
    quote = _quote_with_timestamp(ts=now - 10_000, fill_deadline=now + 300)

    with patch(
        "tool_nodes.bridge.executors.across_executor.time.time",
        return_value=now,
    ):
        with pytest.raises(ValueError, match="stale"):
            asyncio.run(
                execute_across_bridge(quote, sub_org_id="sub", sender="0xsender")
            )


def test_execute_across_bridge_rejects_future_quote_timestamp():
    from tool_nodes.bridge.executors.across_executor import execute_across_bridge

    now = 1_700_000_000
    quote = _quote_with_timestamp(ts=now + 120, fill_deadline=now + 300)

    with patch(
        "tool_nodes.bridge.executors.across_executor.time.time",
        return_value=now,
    ):
        with pytest.raises(ValueError, match="timestamp"):
            asyncio.run(
                execute_across_bridge(quote, sub_org_id="sub", sender="0xsender")
            )
