from decimal import Decimal

import pytest

from core.fees.fee_engine import DEFAULT_FLAT_FEE, FEE_TABLE, FeeEngine, _NATIVE
from core.fees.fee_reducer import FeeContext
from core.planning.execution_plan import ExecutionPlan, PlanNode


def _make_node(**args):
    return PlanNode(
        id="step_0",
        tool="swap",
        args=args,
        depends_on=[],
        approval_required=True,
    )


def test_fee_engine_disabled_returns_none(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("FEE_TREASURY_ADDRESS", raising=False)
    monkeypatch.delenv("FEE_TREASURY_EVM_ADDRESS", raising=False)
    monkeypatch.delenv("FEE_TREASURY_SOLANA_ADDRESS", raising=False)
    engine = FeeEngine()

    node = _make_node(chain="Ethereum", token_in_address=_NATIVE, amount_in="1")
    context = FeeContext(sender="0xabc")

    assert engine.quote_node(node, context) is None
    plan = ExecutionPlan(goal="g", nodes={"step_0": node})
    assert engine.quote_plan(plan, context) == []


def test_fee_engine_native_percentage_fee(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FEE_TREASURY_ADDRESS", "0xfee")
    engine = FeeEngine()

    node = _make_node(chain="Ethereum", token_in_address=_NATIVE, amount_in="2")
    context = FeeContext(sender="0xabc")

    quote = engine.quote_node(node, context)
    assert quote is not None
    assert quote.is_native_tx is True
    assert quote.base_fee_bps == FEE_TABLE["swap"]
    assert quote.fee_recipient == "0xfee"
    assert quote.fee_amount_native == Decimal("0.004000")


def test_fee_engine_erc20_flat_fee_discount(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FEE_TREASURY_ADDRESS", "0xfee")
    engine = FeeEngine()
    engine._reducer.compute_discount = lambda _ctx: (10, ["test"])

    node = _make_node(chain="Ethereum", token_in_address="0xabc", amount_in="10")
    context = FeeContext(sender="0xabc")

    quote = engine.quote_node(node, context)
    assert quote is not None
    assert quote.is_native_tx is False
    assert quote.fee_amount_native <= DEFAULT_FLAT_FEE
    assert quote.fee_amount_native == Decimal("0.000250")


def test_fee_engine_skips_balance_checks(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FEE_TREASURY_ADDRESS", "0xfee")
    engine = FeeEngine()

    node = PlanNode(
        id="step_0",
        tool="check_balance",
        args={"chain": "Ethereum"},
        depends_on=[],
        approval_required=False,
    )
    context = FeeContext(sender="0xabc")

    assert engine.quote_node(node, context) is None


def test_fee_engine_skips_unwrap(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FEE_TREASURY_ADDRESS", "0xfee")
    engine = FeeEngine()

    node = PlanNode(
        id="step_0",
        tool="unwrap",
        args={"chain": "Ethereum", "token_address": "0xwrapped", "token_symbol": "ETH"},
        depends_on=[],
        approval_required=False,
    )
    context = FeeContext(sender="0xabc")

    assert engine.quote_node(node, context) is None


def test_fee_engine_quotes_solana_swap_with_family_treasury(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("FEE_TREASURY_ADDRESS", raising=False)
    monkeypatch.setenv("FEE_TREASURY_SOLANA_ADDRESS", "11111111111111111111111111111111")
    engine = FeeEngine()

    node = PlanNode(
        id="step_0",
        tool="solana_swap",
        args={
            "network": "solana",
            "token_in_mint": "So11111111111111111111111111111111111111112",
            "amount_in": "2",
        },
        depends_on=[],
        approval_required=True,
    )
    context = FeeContext(sender="So1sender111111111111111111111111111111111")

    quote = engine.quote_node(node, context)
    assert quote is not None
    assert quote.chain_family == "solana"
    assert quote.chain_network == "solana"
    assert quote.native_symbol == "SOL"
    assert quote.fee_recipient == "11111111111111111111111111111111"
    assert quote.base_fee_bps == FEE_TABLE["swap"]
    assert quote.is_native_tx is True
    assert quote.fee_amount_native == Decimal("0.004000")
