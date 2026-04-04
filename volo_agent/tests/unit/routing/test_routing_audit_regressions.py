from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from core.execution.runtime import _clear_route_fast_path, _route_meta_matches_node
from core.routing.bridge.socket import SocketAggregator
from core.routing.bridge.token_resolver import ResolvedBridgeToken
from core.routing.models import BridgeRouteQuote, SwapRouteQuote
from core.routing.router import RoutePlanner
from core.routing.scorer import score_bridge_quote
from tool_nodes.bridge.executors.socket_executor import execute_socket_bridge
from core.utils.errors import NonRetryableError


class _LedgerStub:
    def get_stats(self, key: str):  # noqa: ARG002
        return None


def test_score_bridge_quote_ignores_non_comparable_gas_units():
    ledger = _LedgerStub()
    quote_a = BridgeRouteQuote(
        aggregator="lifi",
        token_symbol="USDC",
        source_chain_id=1,
        dest_chain_id=8453,
        source_chain_name="Ethereum",
        dest_chain_name="Base",
        input_amount=Decimal("10"),
        output_amount=Decimal("9.8"),
        total_fee=Decimal("0.2"),
        total_fee_pct=Decimal("2"),
        estimated_fill_time_seconds=120,
        gas_cost_source=Decimal("0.5"),
    )
    quote_b = BridgeRouteQuote(
        aggregator="lifi",
        token_symbol="USDC",
        source_chain_id=1,
        dest_chain_id=8453,
        source_chain_name="Ethereum",
        dest_chain_name="Base",
        input_amount=Decimal("10"),
        output_amount=Decimal("9.8"),
        total_fee=Decimal("0.2"),
        total_fee_pct=Decimal("2"),
        estimated_fill_time_seconds=120,
        gas_cost_source=Decimal("500"),
    )

    assert score_bridge_quote(quote_a, ledger) == score_bridge_quote(quote_b, ledger)


def test_route_meta_guard_rejects_mismatched_bridge_identity():
    route_meta = {
        "aggregator": "socket",
        "source_chain_id": 1,
        "dest_chain_id": 8453,
        "token_symbol": "USDC",
        "calldata": "0xabc",
        "tool_data": {"route": {}},
    }
    resolved_args = {
        "source_chain": "Base",
        "target_chain": "Ethereum",
        "token_symbol": "USDC",
    }

    assert not _route_meta_matches_node(
        tool="bridge",
        route_meta=route_meta,
        resolved_args=resolved_args,
    )
    assert "calldata" not in _clear_route_fast_path(route_meta)
    assert "tool_data" not in _clear_route_fast_path(route_meta)


def test_route_planner_uses_each_aggregator_timeout(monkeypatch):
    class _Agg:
        name = "slow-but-valid"
        TIMEOUT_SECONDS = 9.5

        async def get_quote(self, **kwargs):  # noqa: ANN003
            return SwapRouteQuote(
                aggregator=self.name,
                chain_id=1,
                token_in=kwargs["token_in"],
                token_out=kwargs["token_out"],
                amount_in=kwargs["amount_in"],
                amount_out=Decimal("100"),
                amount_out_min=Decimal("99"),
                gas_estimate=120000,
                gas_cost_usd=Decimal("2"),
                price_impact_pct=Decimal("0.1"),
            )

    captured: list[float] = []

    async def _fake_timed_call(coro, source_name: str, timeout: float):  # noqa: ARG001
        captured.append(timeout)
        return await coro

    monkeypatch.setattr(
        "core.routing.router.get_chain_by_name",
        lambda name: SimpleNamespace(
            chain_id=1,
            name="Ethereum",
            v3_quoter=None,
            v3_router=None,
            v2_router=None,
            v2_factory=None,
        ),
    )
    monkeypatch.setattr("core.routing.router._timed_aggregator_call", _fake_timed_call)

    planner = RoutePlanner(swap_aggregators=[_Agg()], bridge_aggregators=[], solana_aggregators=[])
    decision = asyncio.run(
        planner.get_best_swap_route(
            node_args={
                "token_in_address": "0x1111111111111111111111111111111111111111",
                "token_out_address": "0x2222222222222222222222222222222222222222",
                "amount_in": "1.5",
                "chain": "ethereum",
                "slippage": 0.5,
            },
            sender="0x3333333333333333333333333333333333333333",
            ledger=_LedgerStub(),
        )
    )

    assert decision is not None
    assert captured == [9.5]


def test_route_planner_skips_solana_routing_when_decimals_are_unverified(monkeypatch):
    class _Agg:
        name = "jupiter"
        TIMEOUT_SECONDS = 8.0

        def __init__(self) -> None:
            self.called = False

        async def get_quote(self, **kwargs):  # noqa: ANN003
            self.called = True
            return None

    aggregator = _Agg()

    async def _boom(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("rpc unavailable")

    monkeypatch.setattr(
        "core.routing.router.get_solana_chain",
        lambda network: SimpleNamespace(network="solana", rpc_url="https://rpc"),
    )
    monkeypatch.setattr(
        "core.routing.router.fetch_solana_token_decimals",
        _boom,
    )

    planner = RoutePlanner(swap_aggregators=[], bridge_aggregators=[], solana_aggregators=[aggregator])
    decision = asyncio.run(
        planner.get_best_solana_swap_route(
            node_args={
                "token_in_mint": "So11111111111111111111111111111111111111112",
                "token_out_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "amount_in": "1",
                "network": "solana",
                "slippage": 0.5,
            },
            sender="wallet111111111111111111111111111111111111",
            ledger=_LedgerStub(),
        )
    )

    assert decision is None
    assert aggregator.called is False


def test_socket_aggregator_keeps_metadata_in_sync_with_fallback_route(monkeypatch):
    aggregator = SocketAggregator()
    route_a = {
        "toAmount": "9000000",
        "serviceTime": "120",
        "usedBridgeNames": ["bridge-a"],
    }
    route_b = {
        "toAmount": "8000000",
        "serviceTime": "45",
        "usedBridgeNames": ["bridge-b"],
    }
    quote_payload = {
        "success": True,
        "result": {
            "routes": [route_a, route_b],
        },
    }

    async def _resolve_bridge_token(*args, **kwargs):  # noqa: ANN002, ANN003
        return ResolvedBridgeToken(
            address="0x1111111111111111111111111111111111111111",
            decimals=6,
            is_native=False,
        )

    responses = [
        quote_payload,
        {"success": False, "message": "first build failed"},
        {
            "success": True,
            "result": {
                "txData": "0xfeed",
                "txTarget": "0x2222222222222222222222222222222222222222",
                "chainId": 1,
            },
        },
    ]

    async def _fake_run_blocking(func, *args, **kwargs):  # noqa: ANN002, ANN003
        assert callable(func)
        return responses.pop(0)

    monkeypatch.setattr("core.routing.bridge.socket._api_key", lambda: "key")
    monkeypatch.setattr("core.routing.bridge.socket.resolve_bridge_token", _resolve_bridge_token)
    monkeypatch.setattr("core.routing.bridge.socket.run_blocking", _fake_run_blocking)

    quote = asyncio.run(
        aggregator.get_quote(
            token_symbol="USDC",
            source_chain_id=1,
            dest_chain_id=8453,
            source_chain_name="Ethereum",
            dest_chain_name="Base",
            amount=Decimal("10"),
            sender="0x3333333333333333333333333333333333333333",
            recipient="0x4444444444444444444444444444444444444444",
        )
    )

    assert quote is not None
    assert quote.output_amount == Decimal("8")
    assert quote.tool_data["route"] is route_b
    assert quote.tool_data["usedBridgeNames"] == ["bridge-b"]


def test_execute_socket_bridge_rejects_chain_mismatch_before_execution():
    with pytest.raises(NonRetryableError, match="requested source chain"):
        asyncio.run(
            execute_socket_bridge(
                route_meta={
                    "tool_data": {
                        "route": {"fromChainId": 10, "toChainId": 8453},
                        "buildTxResult": {
                            "txTarget": "0x2222222222222222222222222222222222222222",
                            "txData": "0xfeed",
                            "chainId": 10,
                        },
                    }
                },
                token_symbol="USDC",
                source_chain_id=1,
                dest_chain_id=8453,
                source_chain_name="Ethereum",
                dest_chain_name="Base",
                input_amount=Decimal("10"),
                output_amount=Decimal("9"),
                sub_org_id="sub-org",
                sender="0x3333333333333333333333333333333333333333",
                recipient="0x4444444444444444444444444444444444444444",
            )
        )
