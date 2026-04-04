from decimal import Decimal
from unittest.mock import patch

from core.planning.execution_plan import ExecutionPlan, PlanNode
from core.planning.vws import FALLBACK_GAS_PRICE_WEI, NATIVE_ADDRESS
from core.planning.vws_execution import (
    simulate_execution_plan,
    simulate_many_execution_plans,
)

_SOLANA_NATIVE_REF = "So11111111111111111111111111111111111111112"
_SOLANA_USDC_DEVNET = "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU"


def _single_step_transfer_plan(args):
    return ExecutionPlan(
        goal="transfer",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="transfer",
                args=args,
                depends_on=[],
                approval_required=True,
            )
        },
    )


def _expected_evm_transfer_gas(units: int) -> Decimal:
    return (
        Decimal(units) * Decimal(FALLBACK_GAS_PRICE_WEI) * Decimal("1.5")
    ) / Decimal(10**18)


def test_simulate_execution_plan_bridge_then_swap_resolves_dynamic_output():
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="bridge then swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="bridge",
                args={
                    "sender": sender,
                    "source_chain": "Ethereum",
                    "target_chain": "Base",
                    "source_address": "0xusdc",
                    "target_address": "0xbaseusdc",
                    "amount": "1.5",
                },
                metadata={
                    "route": {
                        "aggregator": "across",
                        "token_symbol": "USDC",
                        "amount_in": "1.5",
                        "source_chain_id": 1,
                        "dest_chain_id": 8453,
                        "source_chain": "Ethereum",
                        "target_chain": "Base",
                        "output_amount": "1.25",
                        "tool_data": {"planned_quote": {"protocol": "across"}},
                    }
                },
                depends_on=[],
                approval_required=True,
            ),
            "step_1": PlanNode(
                id="step_1",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Base",
                    "token_in_address": "0xbaseusdc",
                    "token_out_address": "0xeth",
                    "amount_in": "{{OUTPUT_OF:step_0}}",
                },
                metadata={
                    "route": {
                        "aggregator": "uniswap_v3",
                        "chain_id": 8453,
                        "token_in": "0xbaseusdc",
                        "token_out": "0xeth",
                        "amount_in": "1.25",
                        "amount_out": "0.5",
                        "amount_out_min": "0.49",
                        "gas_estimate": 200000,
                        "execution": {
                            "protocol": "v3",
                            "path": ["0xbaseusdc", "0xeth"],
                            "fee_tiers": [3000],
                        },
                    }
                },
                depends_on=["step_0"],
                approval_required=True,
            ),
        },
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.1",
            f"{sender}|ethereum|0xusdc": "10",
            f"{sender}|base|{NATIVE_ADDRESS}": "0.1",
            f"{sender}|base|0xbaseusdc": "0",
        },
        execution_state=None,
        context={},
        preflight_estimates={
            "step_0": {"protocol": "across", "output_amount": "1.25"},
            "step_1": {"amount_out": "0.5", "gas_estimate": 200000},
        },
    )

    assert result.valid is True
    assert result.failure is None
    assert result.node_metadata["step_1"]["resolved_args"]["amount_in"] == 1.25
    assert (
        Decimal(result.node_metadata["step_1"]["balance_deltas"][f"{sender}|base|0xbaseusdc"])
        == Decimal("-1.25")
    )
    assert (
        Decimal(result.node_metadata["step_1"]["balance_deltas"][f"{sender}|base|0xeth"])
        == Decimal("0.5")
    )
    assert "step_0" in result.native_requirements
    assert "step_1" in result.native_requirements


def test_simulate_execution_plan_uses_route_meta_output_for_dynamic_dependencies():
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="swap then swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xweth",
                    "amount_in": "1",
                },
                metadata={
                        "route": {
                            "aggregator": "uniswap_v3",
                            "chain_id": 1,
                            "token_in": "0xusdc",
                            "token_out": "0xweth",
                            "amount_in": "1",
                            "amount_out": "0.5",
                            "amount_out_min": "0.49",
                            "gas_estimate": 200000,
                            "execution": {
                                "protocol": "v3",
                                "path": ["0xusdc", "0xweth"],
                            "fee_tiers": [3000],
                        },
                    }
                },
                depends_on=[],
                approval_required=True,
            ),
            "step_1": PlanNode(
                id="step_1",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xweth",
                    "token_out_address": "0xdai",
                    "amount_in": "{{OUTPUT_OF:step_0}}",
                },
                metadata={
                    "route": {
                        "aggregator": "uniswap_v3",
                        "chain_id": 1,
                        "token_in": "0xweth",
                        "token_out": "0xdai",
                        "amount_in": "0.5",
                        "amount_out": "1.0",
                        "amount_out_min": "0.99",
                        "gas_estimate": 200000,
                        "execution": {
                            "protocol": "v3",
                            "path": ["0xweth", "0xdai"],
                            "fee_tiers": [3000],
                        },
                    }
                },
                depends_on=["step_0"],
                approval_required=True,
            ),
        },
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.2",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    assert result.valid is True
    assert result.failure is None
    assert result.node_metadata["step_1"]["resolved_args"]["amount_in"] == 0.5


def test_simulate_execution_plan_requires_route_meta_for_routed_nodes():
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xweth",
                    "amount_in": "1",
                },
                depends_on=[],
                approval_required=True,
            )
        },
        metadata={"route_planner": {"applied": True}},
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.2",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    assert result.valid is False
    assert result.failure is not None
    assert result.failure.category == "route_meta_validation_failure"
    assert result.failure.node_id == "step_0"


def test_simulate_execution_plan_allows_unrouted_nodes_when_route_planner_is_partial():
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xweth",
                    "amount_in": "1",
                },
                depends_on=[],
                approval_required=True,
            )
        },
        metadata={
            "route_planner": {
                "applied": True,
                "routable_nodes": 1,
                "routed_nodes": 0,
                "unrouted_nodes": 1,
            }
        },
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.2",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    assert result.valid is True
    assert result.failure is None


def test_simulate_execution_plan_uses_route_meta_when_preflight_cache_missing():
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xweth",
                    "amount_in": "1",
                },
                depends_on=[],
                approval_required=True,
                metadata={
                    "route": {
                        "aggregator": "uniswap_v3",
                        "chain_id": 1,
                        "token_in": "0xusdc",
                        "token_out": "0xweth",
                        "amount_in": "1",
                        "amount_out": "0.5",
                        "amount_out_min": "0.49",
                        "gas_estimate": 200000,
                        "execution": {
                            "protocol": "v3",
                            "path": ["0xusdc", "0xweth"],
                            "fee_tiers": [3000],
                        },
                    }
                },
            )
        },
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.2",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    assert result.valid is True
    assert result.failure is None
    assert result.node_metadata["step_0"]["route_meta_used"] is True
    assert (
        Decimal(result.node_metadata["step_0"]["balance_deltas"][f"{sender}|ethereum|0xweth"])
        == Decimal("0.5")
    )


def test_simulate_execution_plan_prefers_route_meta_over_conflicting_preflight() -> None:
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xweth",
                    "amount_in": "1",
                },
                depends_on=[],
                approval_required=True,
                metadata={
                    "route": {
                        "aggregator": "uniswap_v3",
                        "provider": "uniswap_v3",
                        "chain_id": 1,
                        "token_in": "0xusdc",
                        "token_out": "0xweth",
                        "amount_in": "1",
                        "amount_out": "0.5",
                        "amount_out_min": "0.49",
                        "gas_estimate": 200000,
                        "execution": {
                            "protocol": "v3",
                            "path": ["0xusdc", "0xweth"],
                            "fee_tiers": [3000],
                        },
                    }
                },
            )
        },
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.2",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={},
        preflight_estimates={
            "step_0": {
                "amount_out": "999",
                "amount_out_min": "998",
                "gas_estimate": 1,
            }
        },
    )

    assert result.valid is True
    assert result.failure is None
    assert result.node_metadata["step_0"]["route_meta_used"] is True
    assert (
        Decimal(result.node_metadata["step_0"]["balance_deltas"][f"{sender}|ethereum|0xweth"])
        == Decimal("0.5")
    )


def test_simulate_execution_plan_rejects_expired_route_meta():
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xweth",
                    "amount_in": "1",
                },
                depends_on=[],
                approval_required=True,
                metadata={
                    "route": {
                        "aggregator": "uniswap_v3",
                        "provider": "uniswap_v3",
                        "chain_id": 1,
                        "token_in": "0xusdc",
                        "token_out": "0xweth",
                        "amount_in": "1",
                        "amount_out": "0.5",
                        "amount_out_min": "0.49",
                        "gas_estimate": 200000,
                        "expiry_timestamp": 10,
                        "execution": {
                            "protocol": "v3",
                            "path": ["0xusdc", "0xweth"],
                            "fee_tiers": [3000],
                        },
                    }
                },
            )
        },
        metadata={"route_planner": {"applied": True}},
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.2",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={"route_meta_now": 11},
        preflight_estimates={},
    )

    assert result.valid is False
    assert result.failure is not None
    assert result.failure.category == "route_expired"


def test_simulate_execution_plan_rejects_invalid_route_meta_marker() -> None:
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xweth",
                    "amount_in": "1",
                },
                depends_on=[],
                approval_required=True,
                metadata={
                    "route": {
                        "aggregator": "uniswap_v3",
                        "invalid": True,
                        "invalid_reason": "planner rejected route",
                    }
                },
            )
        },
        metadata={"route_planner": {"applied": True}},
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.2",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    assert result.valid is False
    assert result.failure is not None
    assert result.failure.category == "route_meta_validation_failure"
    assert result.failure.reason == "planner rejected route"


def test_simulate_execution_plan_rejects_simulated_output_below_min_output() -> None:
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xweth",
                    "amount_in": "1",
                },
                depends_on=[],
                approval_required=True,
                metadata={
                    "route": {
                        "aggregator": "uniswap_v3",
                        "provider": "uniswap_v3",
                        "chain_id": 1,
                        "token_in": "0xusdc",
                        "token_out": "0xweth",
                        "amount_in": "1",
                        "amount_out": "0.5",
                        "amount_out_min": "0.49",
                        "gas_estimate": 200000,
                        "execution": {
                            "protocol": "v3",
                            "path": ["0xusdc", "0xweth"],
                            "fee_tiers": [3000],
                        },
                    }
                },
            )
        },
    )

    with patch(
        "core.planning.vws_execution.preflight_from_route_meta",
        return_value={"amount_out": "0.48", "gas_estimate": 200000},
    ):
        result = simulate_execution_plan(
            plan=plan,
            balance_snapshot={
                f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.2",
                f"{sender}|ethereum|0xusdc": "10",
            },
            execution_state=None,
            context={},
            preflight_estimates={},
        )

    assert result.valid is False
    assert result.failure is not None
    assert result.failure.category == "slippage_exceeded"
    assert result.failure.reason == "simulated output 0.48 is below minimum output 0.49"


def test_simulate_execution_plan_rejects_invalid_minimum_output_contract():
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xweth",
                    "amount_in": "1",
                },
                depends_on=[],
                approval_required=True,
                metadata={
                    "route": {
                        "aggregator": "uniswap_v3",
                        "provider": "uniswap_v3",
                        "chain_id": 1,
                        "token_in": "0xusdc",
                        "token_out": "0xweth",
                        "amount_in": "1",
                        "amount_out": "0.5",
                        "amount_out_min": "0.6",
                        "min_output": "0.6",
                        "gas_estimate": 200000,
                        "execution": {
                            "protocol": "v3",
                            "path": ["0xusdc", "0xweth"],
                            "fee_tiers": [3000],
                        },
                    }
                },
            )
        },
        metadata={"route_planner": {"applied": True}},
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.2",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    assert result.valid is False
    assert result.failure is not None
    assert result.failure.category == "route_meta_validation_failure"


def test_simulate_execution_plan_applies_platform_fee_inside_vws():
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xweth",
                    "amount_in": "1",
                },
                metadata={
                    "route": {
                        "aggregator": "uniswap_v3",
                        "chain_id": 1,
                        "token_in": "0xusdc",
                        "token_out": "0xweth",
                        "amount_in": "1",
                        "amount_out": "0.5",
                        "amount_out_min": "0.49",
                        "gas_estimate": 200000,
                        "execution": {
                            "protocol": "v3",
                            "path": ["0xusdc", "0xweth"],
                            "fee_tiers": [3000],
                        },
                    }
                },
                depends_on=[],
                approval_required=True,
            )
        },
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.1",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
        platform_fee_native_by_node={"step_0": Decimal("0.001")},
    )

    native_key = f"{sender}|ethereum|{NATIVE_ADDRESS}"
    assert result.valid is True
    assert result.native_requirements["step_0"] == Decimal("0.01")
    assert result.projected_deltas[native_key] == Decimal("-0.01")
    assert result.node_metadata["step_0"]["platform_fee_native"] == "0.001"
    assert Decimal(result.node_metadata["step_0"]["native_requirement_total"]) == Decimal(
        "0.01"
    )


def test_simulate_execution_plan_evm_native_transfer_uses_native_profile():
    sender = "0xabc"
    expected_gas = _expected_evm_transfer_gas(21_000)
    plan = _single_step_transfer_plan(
        {
            "sender": sender,
            "recipient": "0xdef",
            "network": "Ethereum",
            "asset_symbol": "ETH",
            "asset_ref": NATIVE_ADDRESS,
            "amount": "1",
        }
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={f"{sender}|ethereum|{NATIVE_ADDRESS}": "2"},
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    native_key = f"{sender}|ethereum|{NATIVE_ADDRESS}"
    assert result.valid is True
    assert result.failure is None
    assert result.native_requirements["step_0"] == expected_gas
    assert result.projected_deltas[native_key] == -(Decimal("1") + expected_gas)
    assert [req.kind for req in result.reservation_requirements["step_0"]] == [
        "token_spend",
        "native_reserve",
    ]


def test_simulate_execution_plan_evm_token_transfer_uses_token_profile():
    sender = "0xabc"
    expected_gas = _expected_evm_transfer_gas(65_000)
    plan = _single_step_transfer_plan(
        {
            "sender": sender,
            "recipient": "0xdef",
            "network": "Ethereum",
            "asset_symbol": "USDC",
            "asset_ref": "0xusdc",
            "amount": "5",
        }
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "1",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    assert result.valid is True
    assert result.failure is None
    assert result.native_requirements["step_0"] == expected_gas
    assert result.projected_deltas[f"{sender}|ethereum|0xusdc"] == Decimal("-5")
    assert result.projected_deltas[f"{sender}|ethereum|{NATIVE_ADDRESS}"] == -expected_gas


def test_simulate_execution_plan_unwrap_without_amount_uses_full_wrapped_balance():
    sender = "0xabc"
    wrapped = "0xc02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
    plan = ExecutionPlan(
        goal="unwrap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="unwrap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_symbol": "ETH",
                    "token_address": wrapped,
                },
                depends_on=[],
                approval_required=False,
            )
        },
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.5",
            f"{sender}|ethereum|{wrapped}": "1.25",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    native_key = f"{sender}|ethereum|{NATIVE_ADDRESS}"
    wrapped_key = f"{sender}|ethereum|{wrapped}"
    assert result.valid is True
    assert result.failure is None
    assert result.native_requirements["step_0"] == Decimal("0.0045")
    assert result.projected_deltas[wrapped_key] == Decimal("-1.25")
    assert result.projected_deltas[native_key] == Decimal("1.2455")


def test_simulate_execution_plan_unwrap_rejects_amount_above_wrapped_balance():
    sender = "0xabc"
    wrapped = "0xc02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
    plan = ExecutionPlan(
        goal="unwrap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="unwrap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_symbol": "ETH",
                    "token_address": wrapped,
                    "amount": "2",
                },
                depends_on=[],
                approval_required=False,
            )
        },
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.5",
            f"{sender}|ethereum|{wrapped}": "1.25",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    assert result.valid is False
    assert result.failure is not None
    assert result.failure.category == "insufficient_funds"
    assert "have 1.250000" in result.failure.reason


def test_simulate_execution_plan_solana_native_transfer_reserves_native_only():
    sender = "9xQeWvG816bUx9EPjHmaT23yvVMcFVN8Y6GsB7mQ7X7G"
    sender_key = sender.lower()
    expected_gas = Decimal("0.00001")
    plan = _single_step_transfer_plan(
        {
            "sender": sender,
            "recipient": "H3qWmD5MZx7Sg1H4C4r4Y6xWm4kD1a2o3p4q5r6s7t8",
            "network": "solana-devnet",
            "asset_symbol": "SOL",
            "asset_ref": _SOLANA_NATIVE_REF,
            "amount": "0.5",
        }
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={f"{sender_key}|solana-devnet|{_SOLANA_NATIVE_REF.lower()}": "1"},
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    native_key = f"{sender_key}|solana-devnet|{_SOLANA_NATIVE_REF.lower()}"
    assert result.valid is True
    assert result.failure is None
    assert result.native_requirements["step_0"] == expected_gas
    assert result.projected_deltas[native_key] == -(Decimal("0.5") + expected_gas)


def test_simulate_execution_plan_solana_spl_transfer_avoids_evm_gas_assumptions():
    sender = "9xQeWvG816bUx9EPjHmaT23yvVMcFVN8Y6GsB7mQ7X7G"
    sender_key = sender.lower()
    expected_gas = Decimal("0.00001")
    plan = _single_step_transfer_plan(
        {
            "sender": sender,
            "recipient": "H3qWmD5MZx7Sg1H4C4r4Y6xWm4kD1a2o3p4q5r6s7t8",
            "network": "solana-devnet",
            "asset_symbol": "USDC",
            "asset_ref": _SOLANA_USDC_DEVNET,
            "amount": "2",
        }
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={
            f"{sender_key}|solana-devnet|{_SOLANA_NATIVE_REF.lower()}": "1",
            f"{sender_key}|solana-devnet|{_SOLANA_USDC_DEVNET.lower()}": "10",
        },
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    assert result.valid is True
    assert result.failure is None
    assert result.native_requirements["step_0"] == expected_gas
    assert result.projected_deltas[f"{sender_key}|solana-devnet|{_SOLANA_USDC_DEVNET.lower()}"] == Decimal("-2")
    assert result.projected_deltas[f"{sender_key}|solana-devnet|{_SOLANA_NATIVE_REF.lower()}"] == -expected_gas


def test_simulate_execution_plan_rejects_ambiguous_transfer_asset_identity():
    sender = "0xabc"
    plan = _single_step_transfer_plan(
        {
            "sender": sender,
            "recipient": "0xdef",
            "network": "Ethereum",
            "asset_symbol": "USDC",
            "amount": "1",
        }
    )

    result = simulate_execution_plan(
        plan=plan,
        balance_snapshot={f"{sender}|ethereum|{NATIVE_ADDRESS}": "2"},
        execution_state=None,
        context={},
        preflight_estimates={},
    )

    assert result.valid is False
    assert result.failure is not None
    assert result.failure.category == "dependency_resolution_failure"
    assert "explicit asset reference" in result.failure.reason


def test_simulate_many_execution_plans_reuses_identical_prefixes():
    sender = "0xabc"
    shared_step = PlanNode(
        id="step_0",
        tool="swap",
        args={
            "sender": sender,
            "chain": "Ethereum",
            "token_in_address": "0xusdc",
            "token_out_address": "0xweth",
            "amount_in": "1",
        },
        metadata={
                "route": {
                    "aggregator": "uniswap_v3",
                    "chain_id": 1,
                    "token_in": "0xusdc",
                    "token_out": "0xweth",
                    "amount_in": "1",
                    "amount_out": "0.5",
                    "amount_out_min": "0.49",
                    "gas_estimate": 200000,
                    "execution": {
                        "protocol": "v3",
                        "path": ["0xusdc", "0xweth"],
                        "fee_tiers": [3000],
                    },
                }
            },
        depends_on=[],
        approval_required=True,
    )
    plan_a = ExecutionPlan(
        goal="candidate-a",
        nodes={
            "step_0": shared_step,
            "step_1": PlanNode(
                id="step_1",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Ethereum",
                    "token_in_address": "0xweth",
                    "token_out_address": "0xdai",
                    "amount_in": "{{OUTPUT_OF:step_0}}",
                },
                metadata={
                    "route": {
                        "aggregator": "uniswap_v2",
                        "chain_id": 1,
                        "token_in": "0xweth",
                        "token_out": "0xdai",
                        "amount_in": "0.5",
                        "amount_out": "900",
                        "amount_out_min": "890",
                        "gas_estimate": 200000,
                        "execution": {
                            "protocol": "v2",
                            "path": ["0xweth", "0xdai"],
                        },
                    }
                },
                depends_on=["step_0"],
                approval_required=True,
            ),
        },
        metadata={
            "vws_label": "candidate_a",
            "candidate_preflight_estimates": {
                "step_0": {"amount_out": "0.5", "gas_estimate": 200000},
                "step_1": {"amount_out": "900", "gas_estimate": 200000},
            },
        },
    )
    plan_b = plan_a.model_copy(deep=True)
    plan_b.metadata["vws_label"] = "candidate_b"
    plan_b.nodes["step_1"].metadata["route"]["aggregator"] = "paraswap"
    plan_b.nodes["step_1"].metadata["route"]["amount_out"] = "905"
    plan_b.nodes["step_1"].metadata["route"]["amount_out_min"] = "895"
    plan_b.metadata["candidate_preflight_estimates"]["step_1"] = {
        "amount_out": "905",
        "gas_estimate": 200000,
    }

    results = simulate_many_execution_plans(
        plans=[plan_a, plan_b],
        balance_snapshot={
            f"{sender}|ethereum|{NATIVE_ADDRESS}": "0.2",
            f"{sender}|ethereum|0xusdc": "10",
        },
        execution_state=None,
        context={},
    )

    assert len(results) == 2
    assert results[0].valid is True
    assert results[1].valid is True
    assert results[1].shared_prefix_hits >= 1


def test_simulate_many_execution_plans_projects_when_balance_snapshot_missing():
    sender = "0xabc"
    plan = ExecutionPlan(
        goal="bridge then swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="bridge",
                args={
                    "sender": sender,
                    "source_chain": "Ethereum",
                    "target_chain": "Base",
                    "source_address": "0xusdc",
                    "target_address": "0xbaseusdc",
                    "amount": "1.5",
                },
                depends_on=[],
                approval_required=True,
            ),
            "step_1": PlanNode(
                id="step_1",
                tool="swap",
                args={
                    "sender": sender,
                    "chain": "Base",
                    "token_in_address": "0xbaseusdc",
                    "token_out_address": "0xeth",
                    "amount_in": "{{OUTPUT_OF:step_0}}",
                },
                depends_on=["step_0"],
                approval_required=True,
            ),
        },
        metadata={
            "vws_label": "projected_candidate",
            "candidate_preflight_estimates": {
                "step_0": {"protocol": "across", "output_amount": "1.25"},
                "step_1": {"amount_out": "0.5", "gas_estimate": 200000},
            },
        },
    )

    result = simulate_many_execution_plans(
        plans=[plan],
        balance_snapshot={},
        execution_state=None,
        context={},
    )[0]

    assert result.valid is True
    assert result.balance_validation_skipped is True
    assert (
        result.per_step_state_transitions["step_1"]["resolved_args"]["amount_in"] == 1.25
    )
