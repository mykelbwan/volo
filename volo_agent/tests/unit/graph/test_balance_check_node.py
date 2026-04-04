import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from core.planning.execution_plan import ExecutionPlan, PlanNode
from graph.agent_state import AgentState
from graph.nodes.balance_check_node import balance_check_node


class _DummyChain:
    def __init__(self):
        self.rpc_url = "http://example"
        self.native_symbol = "ETH"
        self.chain_id = 1
        self.name = "Ethereum"
        self.is_testnet = False
        self.wrapped_native = "0xwrap"
        self.supports_native_swaps = True
        self.v3_router = None
        self.v3_quoter = None
        self.v2_router = None
        self.v2_factory = None


def _make_state(plan_history) -> AgentState:
    return {
        "user_id": "user-1",
        "provider": "discord",
        "username": "alice",
        "user_info": {"volo_user_id": "user-1"},
        "intents": [],
        "plans": [],
        "goal_parameters": {},
        "plan_history": plan_history,
        "execution_state": None,
        "artifacts": {},
        "context": {},
        "route_decision": None,
        "confirmation_status": None,
        "pending_transactions": [],
        "reasoning_logs": [],
        "messages": [],
        "fee_quotes": [],
        "trigger_id": None,
        "is_triggered_execution": None,
    }


def _make_plan(args):
    node = PlanNode(
        id="step_0",
        tool="swap",
        args=args,
        depends_on=[],
        approval_required=True,
    )
    return ExecutionPlan(goal="g", nodes={"step_0": node})


def _make_transfer_plan(args):
    node = PlanNode(
        id="step_0",
        tool="transfer",
        args=args,
        depends_on=[],
        approval_required=True,
    )
    return ExecutionPlan(goal="g", nodes={"step_0": node})


def _make_balance_plan(args):
    node = PlanNode(
        id="step_0",
        tool="check_balance",
        args=args,
        depends_on=[],
        approval_required=False,
    )
    return ExecutionPlan(goal="g", nodes={"step_0": node})


def _native_balance_key(
    sender: str,
    chain_name: str,
    token_ref: str = "0x0000000000000000000000000000000000000000",
) -> str:
    return f"{sender.strip().lower()}|{chain_name.strip().lower()}|{token_ref.strip().lower()}"


def test_balance_check_no_plan_history_returns_confirm():
    state = _make_state(plan_history=[])
    result = asyncio.run(balance_check_node(state))
    assert result["route_decision"] == "confirm"


def test_balance_check_read_only_routes_execute():
    plan = _make_balance_plan(
        {"sender": "{{SENDER_ADDRESS}}", "chain": "Somnia Testnet"}
    )
    state = _make_state(plan_history=[plan])

    result = asyncio.run(balance_check_node(state))

    assert result["route_decision"] == "execute"
    assert result["fee_quotes"] == []


def test_balance_check_uses_vws_payload_as_source_of_truth():
    plan = _make_plan(
        {
            "sender": "0xabc",
            "chain": "Ethereum",
            "token_in_address": "0xusdc",
            "amount_in": "1",
        }
    )
    state = _make_state(plan_history=[plan])
    vws_payload = {
        "plan_history": [],
        "reasoning_logs": ["[VWS] Simulated 1 node(s) with 2 projected delta(s)."],
        "balance_snapshot": {"0xabc|ethereum|0xusdc": "10"},
        "resource_snapshots": {},
        "native_requirements": {"step_0": "0.011"},
        "reservation_requirements": {"step_0": []},
        "projected_deltas": {"0xabc|ethereum|0xusdc": "-1"},
        "preflight_estimates": {"step_0": {"fee_estimate_native": "0.001"}},
        "vws_simulation": {"step_0": {"status": "success"}},
        "vws_failure": None,
        "fee_quotes": [{"node_id": "step_0", "fee_amount_native": "0.001"}],
    }

    with patch(
        "graph.nodes.balance_check_node.run_vws_preflight",
        new=AsyncMock(return_value=vws_payload),
    ):
        result = asyncio.run(balance_check_node(state))

    assert result["route_decision"] == "confirm"
    assert result["native_requirements"] == {"step_0": "0.011"}
    assert result["projected_deltas"] == {"0xabc|ethereum|0xusdc": "-1"}
    assert result["fee_quotes"] == [{"node_id": "step_0", "fee_amount_native": "0.001"}]
    assert result["reasoning_logs"][-1] == "[BALANCE_CHECK] Using VWS outputs as source of truth."


def test_balance_check_propagates_vws_failure_without_fallback():
    plan = _make_plan(
        {
            "sender": "0xabc",
            "chain": "Ethereum",
            "token_in_address": "0xusdc",
            "amount_in": "1",
        }
    )
    state = _make_state(plan_history=[plan])
    vws_failure_payload = {
        "route_decision": "end",
        "reasoning_logs": ["[VWS] Rejected step_0: insufficient_funds"],
        "messages": [MagicMock(content="Insufficient balance for step_0.")],
        "balance_snapshot": {},
        "resource_snapshots": {},
        "native_requirements": {},
        "reservation_requirements": {},
        "projected_deltas": {},
        "preflight_estimates": {},
        "vws_simulation": {},
        "vws_failure": {
            "node_id": "step_0",
            "tool": "swap",
            "category": "insufficient_funds",
            "reason": "not enough balance",
        },
        "fee_quotes": [],
    }

    with patch(
        "graph.nodes.balance_check_node.run_vws_preflight",
        new=AsyncMock(return_value=vws_failure_payload),
    ):
        result = asyncio.run(balance_check_node(state))

    assert result["route_decision"] == "end"
    assert result["vws_failure"]["category"] == "insufficient_funds"
    assert result["messages"][0].content == "Insufficient balance for step_0."


def test_balance_check_emits_structured_reservation_metadata_from_vws():
    plan = _make_plan(
        {
            "sender": "0xabc",
            "sub_org_id": "sub-1",
            "chain": "Ethereum",
            "token_in_address": "0xusdc",
            "token_in_symbol": "USDC",
            "amount_in": "1",
        }
    )
    state = _make_state(plan_history=[plan])

    async def _fake_token_balance_for_chain(**kwargs):
        balance_snapshot = kwargs["balance_snapshot"]
        resource_snapshots = kwargs["resource_snapshots"]
        key = _native_balance_key("0xabc", "ethereum", "0xusdc")
        balance_snapshot[key] = "10"
        resource_snapshots[key] = {
            "resource_key": key,
            "wallet_scope": "suborg:sub-1",
            "sender": "0xabc",
            "chain": "ethereum",
            "token_ref": "0xusdc",
            "symbol": "USDC",
            "decimals": 6,
            "available": "10",
            "available_base_units": "10000000",
            "observed_at": "1",
            "chain_family": "evm",
        }
        return key, "USDC", Decimal("10")

    async def _fake_native_balance_for_chain(**kwargs):
        balance_snapshot = kwargs["balance_snapshot"]
        resource_snapshots = kwargs["resource_snapshots"]
        key = _native_balance_key("0xabc", "ethereum")
        balance_snapshot[key] = "1"
        resource_snapshots[key] = {
            "resource_key": key,
            "wallet_scope": "suborg:sub-1",
            "sender": "0xabc",
            "chain": "ethereum",
            "token_ref": "0x0000000000000000000000000000000000000000",
            "symbol": "ETH",
            "decimals": 18,
            "available": "1",
            "available_base_units": "1000000000000000000",
            "observed_at": "1",
            "chain_family": "evm",
        }
        return key, Decimal("1")

    reservation_service = MagicMock()
    reservation_service.get_reserved_totals = AsyncMock(
        side_effect=lambda *, wallet_scope, resource_keys: {
            resource_key: 0 for resource_key in resource_keys
        }
    )

    with patch("graph.nodes.balance_check_node.get_chain_by_name", return_value=_DummyChain()):
        with patch(
            "graph.nodes.balance_check_node.get_ledger",
            return_value=MagicMock(get_total_lifetime_txs=lambda: 0),
        ):
            with patch(
                "graph.nodes.balance_check_node.FeeEngine",
                return_value=MagicMock(quote_plan=lambda *_args, **_kwargs: []),
            ):
                with patch(
                    "graph.nodes.balance_check_node._get_token_balance_for_chain",
                    new=AsyncMock(side_effect=_fake_token_balance_for_chain),
                ):
                    with patch(
                        "graph.nodes.balance_check_node.get_reservation_service",
                        new=AsyncMock(return_value=reservation_service),
                    ):
                        with patch(
                            "graph.nodes.balance_check_node._get_native_balance_for_chain",
                            new=AsyncMock(side_effect=_fake_native_balance_for_chain),
                        ):
                            result = asyncio.run(balance_check_node(state))

    assert result["route_decision"] == "confirm"
    assert result["resource_snapshots"]
    assert result["reservation_requirements"]["step_0"]


def test_balance_check_transfer_supports_asset_ref_alias_for_token_preflight():
    plan = _make_transfer_plan(
        {
            "sender": "0xabc",
            "sub_org_id": "sub-1",
            "chain": "Ethereum",
            "asset_symbol": "USDC",
            "asset_ref": "0xusdc",
            "recipient": "0xdef",
            "amount": "1",
        }
    )
    state = _make_state(plan_history=[plan])

    async def _fake_token_balance_for_chain(**kwargs):
        balance_snapshot = kwargs["balance_snapshot"]
        resource_snapshots = kwargs["resource_snapshots"]
        key = _native_balance_key("0xabc", "ethereum", "0xusdc")
        balance_snapshot[key] = "10"
        resource_snapshots[key] = {
            "resource_key": key,
            "wallet_scope": "suborg:sub-1",
            "sender": "0xabc",
            "chain": "ethereum",
            "token_ref": "0xusdc",
            "symbol": "USDC",
            "decimals": 6,
            "available": "10",
            "available_base_units": "10000000",
            "observed_at": "1",
            "chain_family": "evm",
        }
        return key, "USDC", Decimal("10")

    async def _fake_native_balance_for_chain(**kwargs):
        balance_snapshot = kwargs["balance_snapshot"]
        resource_snapshots = kwargs["resource_snapshots"]
        key = _native_balance_key("0xabc", "ethereum")
        balance_snapshot[key] = "1"
        resource_snapshots[key] = {
            "resource_key": key,
            "wallet_scope": "suborg:sub-1",
            "sender": "0xabc",
            "chain": "ethereum",
            "token_ref": "0x0000000000000000000000000000000000000000",
            "symbol": "ETH",
            "decimals": 18,
            "available": "1",
            "available_base_units": "1000000000000000000",
            "observed_at": "1",
            "chain_family": "evm",
        }
        return key, Decimal("1")

    reservation_service = MagicMock()
    reservation_service.get_reserved_totals = AsyncMock(
        side_effect=lambda *, wallet_scope, resource_keys: {
            resource_key: 0 for resource_key in resource_keys
        }
    )

    with patch("graph.nodes.balance_check_node.get_chain_by_name", return_value=_DummyChain()):
        with patch(
            "graph.nodes.balance_check_node.get_ledger",
            return_value=MagicMock(get_total_lifetime_txs=lambda: 0),
        ):
            with patch(
                "graph.nodes.balance_check_node.FeeEngine",
                return_value=MagicMock(quote_plan=lambda *_args, **_kwargs: []),
            ):
                with patch(
                    "graph.nodes.balance_check_node.get_reservation_service",
                    new=AsyncMock(return_value=reservation_service),
                ):
                    with patch(
                        "graph.nodes.balance_check_node._get_token_balance_for_chain",
                        new=AsyncMock(side_effect=_fake_token_balance_for_chain),
                    ) as token_fetch:
                        with patch(
                            "graph.nodes.balance_check_node._get_native_balance_for_chain",
                            new=AsyncMock(side_effect=_fake_native_balance_for_chain),
                        ) as native_fetch:
                            result = asyncio.run(balance_check_node(state))

    assert result["route_decision"] == "confirm"
    assert token_fetch.await_count == 1
    assert native_fetch.await_count == 1
    assert result["reservation_requirements"]["step_0"]


def test_balance_check_uses_net_available_balance_after_reservations():
    plan = _make_plan(
        {
            "sender": "0xabc",
            "sub_org_id": "sub-1",
            "chain": "Ethereum",
            "token_in_address": "0xusdc",
            "token_in_symbol": "USDC",
            "amount_in": "8",
        }
    )
    state = _make_state(plan_history=[plan])

    async def _fake_token_balance_for_chain(**kwargs):
        balance_snapshot = kwargs["balance_snapshot"]
        resource_snapshots = kwargs["resource_snapshots"]
        key = _native_balance_key("0xabc", "ethereum", "0xusdc")
        balance_snapshot[key] = "10"
        resource_snapshots[key] = {
            "resource_key": key,
            "wallet_scope": "suborg:sub-1",
            "sender": "0xabc",
            "chain": "ethereum",
            "token_ref": "0xusdc",
            "symbol": "USDC",
            "decimals": 6,
            "available": "10",
            "available_base_units": "10000000",
            "observed_at": "1",
            "chain_family": "evm",
        }
        return key, "USDC", Decimal("10")

    async def _fake_native_balance_for_chain(**kwargs):
        balance_snapshot = kwargs["balance_snapshot"]
        resource_snapshots = kwargs["resource_snapshots"]
        key = _native_balance_key("0xabc", "ethereum")
        balance_snapshot[key] = "1"
        resource_snapshots[key] = {
            "resource_key": key,
            "wallet_scope": "suborg:sub-1",
            "sender": "0xabc",
            "chain": "ethereum",
            "token_ref": "0x0000000000000000000000000000000000000000",
            "symbol": "ETH",
            "decimals": 18,
            "available": "1",
            "available_base_units": "1000000000000000000",
            "observed_at": "1",
            "chain_family": "evm",
        }
        return key, Decimal("1")

    reservation_service = MagicMock()
    reservation_service.get_reserved_totals = AsyncMock(
        side_effect=lambda *, wallet_scope, resource_keys: {
            resource_key: (
                3_000_000
                if resource_key.endswith("|0xusdc")
                else 0
            )
            for resource_key in resource_keys
        }
    )

    with patch("graph.nodes.balance_check_node.get_chain_by_name", return_value=_DummyChain()):
        with patch(
            "graph.nodes.balance_check_node.get_ledger",
            return_value=MagicMock(get_total_lifetime_txs=lambda: 0),
        ):
            with patch(
                "graph.nodes.balance_check_node.FeeEngine",
                return_value=MagicMock(quote_plan=lambda *_args, **_kwargs: []),
            ):
                with patch(
                    "graph.nodes.balance_check_node.get_reservation_service",
                    new=AsyncMock(return_value=reservation_service),
                ):
                    with patch(
                        "graph.nodes.balance_check_node._get_token_balance_for_chain",
                        new=AsyncMock(side_effect=_fake_token_balance_for_chain),
                    ):
                        with patch(
                            "graph.nodes.balance_check_node._get_native_balance_for_chain",
                            new=AsyncMock(side_effect=_fake_native_balance_for_chain),
                        ):
                            result = asyncio.run(balance_check_node(state))

    token_key = _native_balance_key("0xabc", "ethereum", "0xusdc")
    assert result["route_decision"] == "end"
    assert result["balance_snapshot"][token_key] == "7"
    assert result["resource_snapshots"][token_key]["available"] == "10"
    assert result["resource_snapshots"][token_key]["reserved"] == "3"
    assert result["resource_snapshots"][token_key]["net_available"] == "7"


def test_balance_check_solana_swap_fails_when_vws_detects_native_input_shortfall():
    plan = ExecutionPlan(
        goal="g",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="solana_swap",
                args={
                    "sender": "SoLUser1111111111111111111111111111111111111",
                    "network": "solana",
                    "token_in_mint": "So11111111111111111111111111111111111111112",
                    "token_out_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                    "token_in_symbol": "SOL",
                    "token_out_symbol": "USDC",
                    "amount_in": "1",
                },
                depends_on=[],
                approval_required=True,
            )
        },
    )
    state = _make_state(plan_history=[plan])

    with patch(
        "graph.nodes.balance_check_node.get_ledger",
        return_value=MagicMock(get_total_lifetime_txs=lambda: 0),
    ), patch(
        "graph.nodes.balance_check_node.FeeEngine",
        return_value=MagicMock(quote_plan=lambda *_args, **_kwargs: []),
    ), patch(
        "graph.nodes.balance_check_node.get_reservation_service",
        new=AsyncMock(
            return_value=MagicMock(
                get_reserved_totals=AsyncMock(
                    side_effect=lambda *, wallet_scope, resource_keys: {
                        resource_key: 0 for resource_key in resource_keys
                    }
                )
            )
        ),
    ), patch(
        "graph.nodes.balance_check_node.get_native_balance_solana_async",
        new=AsyncMock(return_value=Decimal("0.5")),
    ):
        result = asyncio.run(balance_check_node(state))

    assert result["route_decision"] == "end"
    assert "not enough" in result["messages"][0].content.lower()
    assert "solana" in result["messages"][0].content.lower()


def test_balance_check_solana_swap_fails_on_platform_fee_shortfall_inside_vws():
    plan = ExecutionPlan(
        goal="g",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="solana_swap",
                args={
                    "sender": "SoLUser1111111111111111111111111111111111111",
                    "network": "solana",
                    "token_in_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                    "token_out_mint": "So11111111111111111111111111111111111111112",
                    "token_in_symbol": "USDC",
                    "token_out_symbol": "SOL",
                    "amount_in": "25",
                },
                depends_on=[],
                approval_required=True,
            )
        },
    )
    state = _make_state(plan_history=[plan])

    fake_quote = MagicMock()
    fake_quote.node_id = "step_0"
    fake_quote.fee_amount_native = Decimal("0.005")
    fake_quote.to_dict.return_value = {
        "node_id": "step_0",
        "fee_amount_native": "0.005",
        "chain_family": "solana",
    }

    async def _fake_token_balance_for_chain(**kwargs):
        balance_snapshot = kwargs["balance_snapshot"]
        key = _native_balance_key(
            "SoLUser1111111111111111111111111111111111111",
            "solana",
            "epjfwdd5aufqssqem2qn1xzybapc8g4weggkzwytdt1v",
        )
        balance_snapshot[key] = "25"
        return key, "USDC", Decimal("25")

    async def _fake_native_balance_for_chain(**kwargs):
        balance_snapshot = kwargs["balance_snapshot"]
        key = _native_balance_key(
            "SoLUser1111111111111111111111111111111111111",
            "solana",
            "So11111111111111111111111111111111111111112",
        )
        balance_snapshot[key] = "0.005005"
        return key, Decimal("0.005005")

    with patch(
        "graph.nodes.balance_check_node.get_ledger",
        return_value=MagicMock(get_total_lifetime_txs=lambda: 0),
    ), patch(
        "graph.nodes.balance_check_node.FeeEngine",
        return_value=MagicMock(quote_plan=lambda *_args, **_kwargs: [fake_quote]),
    ), patch(
        "graph.nodes.balance_check_node.get_reservation_service",
        new=AsyncMock(
            return_value=MagicMock(
                get_reserved_totals=AsyncMock(
                    side_effect=lambda *, wallet_scope, resource_keys: {
                        resource_key: 0 for resource_key in resource_keys
                    }
                )
            )
        ),
    ), patch(
        "graph.nodes.balance_check_node._get_token_balance_for_chain",
        new=AsyncMock(side_effect=_fake_token_balance_for_chain),
    ), patch(
        "graph.nodes.balance_check_node._prefetch_solana_wallet_balances",
        new=AsyncMock(return_value={}),
    ) as prefetch_wallet_balances, patch(
        "graph.nodes.balance_check_node._get_native_balance_for_chain",
        new=AsyncMock(side_effect=_fake_native_balance_for_chain),
    ):
        result = asyncio.run(balance_check_node(state))

    prefetch_wallet_balances.assert_awaited_once()
    assert result["route_decision"] == "end"
    assert "platform fee" in result["vws_failure"]["reason"]


def test_balance_check_fails_closed_when_reservation_service_is_unavailable():
    plan = _make_plan(
        {
            "sender": "0xabc",
            "chain": "Ethereum",
            "token_in_address": "0xusdc",
            "token_in_symbol": "USDC",
            "amount_in": "1",
        }
    )
    state = _make_state(plan_history=[plan])

    async def _fake_token_balance_for_chain(**kwargs):
        balance_snapshot = kwargs["balance_snapshot"]
        resource_snapshots = kwargs["resource_snapshots"]
        key = _native_balance_key("0xabc", "ethereum", "0xusdc")
        balance_snapshot[key] = "10"
        resource_snapshots[key] = {
            "resource_key": key,
            "wallet_scope": "sender:0xabc",
            "sender": "0xabc",
            "chain": "ethereum",
            "token_ref": "0xusdc",
            "symbol": "USDC",
            "decimals": 6,
            "available": "10",
            "available_base_units": "10000000",
            "observed_at": "1",
            "chain_family": "evm",
        }
        return key, "USDC", Decimal("10")

    async def _fake_native_balance_for_chain(**kwargs):
        balance_snapshot = kwargs["balance_snapshot"]
        resource_snapshots = kwargs["resource_snapshots"]
        key = _native_balance_key("0xabc", "ethereum")
        balance_snapshot[key] = "1"
        resource_snapshots[key] = {
            "resource_key": key,
            "wallet_scope": "sender:0xabc",
            "sender": "0xabc",
            "chain": "ethereum",
            "token_ref": "0x0000000000000000000000000000000000000000",
            "symbol": "ETH",
            "decimals": 18,
            "available": "1",
            "available_base_units": "1000000000000000000",
            "observed_at": "1",
            "chain_family": "evm",
        }
        return key, Decimal("1")

    with patch("graph.nodes.balance_check_node.get_chain_by_name", return_value=_DummyChain()):
        with patch(
            "graph.nodes.balance_check_node.get_ledger",
            return_value=MagicMock(get_total_lifetime_txs=lambda: 0),
        ):
            with patch(
                "graph.nodes.balance_check_node.FeeEngine",
                return_value=MagicMock(quote_plan=lambda *_args, **_kwargs: []),
            ):
                with patch(
                    "graph.nodes.balance_check_node._get_token_balance_for_chain",
                    new=AsyncMock(side_effect=_fake_token_balance_for_chain),
                ):
                    with patch(
                        "graph.nodes.balance_check_node._get_native_balance_for_chain",
                        new=AsyncMock(side_effect=_fake_native_balance_for_chain),
                    ):
                        with patch(
                            "graph.nodes.balance_check_node.get_reservation_service",
                            new=AsyncMock(side_effect=RuntimeError("reservation store down")),
                        ):
                            result = asyncio.run(balance_check_node(state))

    assert result["route_decision"] == "end"
    assert "reservation service unavailable" in result["vws_failure"]["reason"].lower(), (
        "spend preflight must fail closed when reservation totals cannot be loaded"
    )
