import asyncio
from unittest.mock import AsyncMock, patch

from core.planning.execution_plan import ExecutionPlan, PlanNode
from graph.nodes.balance_check_node import balance_check_node


def test_balance_check_uses_existing_vws_payload_without_reprojecting():
    plan = ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": "0xabc",
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "amount_in": "1",
                },
                depends_on=[],
                approval_required=True,
            )
        },
    )
    native_key = "0xabc|ethereum|0x0000000000000000000000000000000000000000"
    state = {
        "user_id": "user-1",
        "provider": "discord",
        "username": "alice",
        "user_info": {"volo_user_id": "user-1"},
        "intents": [],
        "plans": [],
        "goal_parameters": {},
        "plan_history": [plan],
        "execution_state": None,
        "artifacts": {},
        "context": {},
        "route_decision": None,
        "confirmation_status": None,
        "pending_transactions": [],
        "reasoning_logs": [],
        "messages": [],
        "fee_quotes": [{"node_id": "step_0", "fee_amount_native": "0.001"}],
        "balance_snapshot": {native_key: "0.1"},
        "resource_snapshots": {
            native_key: {
                "resource_key": native_key,
                "wallet_scope": "sender:0xabc",
                "sender": "0xabc",
                "chain": "ethereum",
                "token_ref": "0x0000000000000000000000000000000000000000",
                "symbol": "ETH",
                "decimals": 18,
                "available": "0.1",
                "available_base_units": "100000000000000000",
                "observed_at": "1",
                "chain_family": "evm",
            }
        },
        "native_requirements": {"step_0": "0.011"},
        "reservation_requirements": {
            "step_0": [
                {
                    "resource_key": native_key,
                    "wallet_scope": "sender:0xabc",
                    "sender": "0xabc",
                    "chain": "ethereum",
                    "token_ref": "0x0000000000000000000000000000000000000000",
                    "symbol": "ETH",
                    "decimals": 18,
                    "required": "0.011",
                    "required_base_units": "11000000000000000",
                    "kind": "native_reserve",
                }
            ]
        },
        "projected_deltas": {native_key: "-0.011"},
        "preflight_estimates": {"step_0": {"fee_estimate_native": "0.001"}},
        "vws_simulation": {
            "step_0": {
                "resolved_args": {
                    "sender": "0xabc",
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "amount_in": "1",
                },
                "ending_balances": {native_key: "0.089"},
                "platform_fee_native": "0.001",
                "native_requirement_total": "0.011",
            }
        },
        "vws_failure": None,
        "trigger_id": None,
        "is_triggered_execution": None,
    }

    with patch(
        "graph.nodes.balance_check_node.run_vws_preflight",
        new=AsyncMock(side_effect=AssertionError("VWS should not be recomputed")),
    ):
        result = asyncio.run(balance_check_node(state))

    assert result["route_decision"] == "confirm"
    assert result["native_requirements"]["step_0"] == "0.011"
    assert result["projected_deltas"][native_key] == "-0.011"
    assert result["fee_quotes"] == [{"node_id": "step_0", "fee_amount_native": "0.001"}]
    assert result["vws_simulation"]["step_0"]["ending_balances"] == {native_key: "0.089"}
