import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from core.fees.models import FeeQuote
from core.planning.execution_plan import ExecutionPlan, PlanNode
from graph.nodes.confirmation_node import confirmation_node


def test_confirmation_node_uses_short_clear_swap_copy():
    plan = ExecutionPlan(
        goal="Swap 0.2 STT for NIA",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "amount_in": 0.2,
                    "token_in_symbol": "STT",
                    "token_out_symbol": "NIA",
                    "chain": "somnia testnet",
                },
                depends_on=[],
                approval_required=True,
            )
        },
    )
    fee_quote = FeeQuote(
        node_id="step_0",
        tool="swap",
        chain="Somnia Testnet",
        native_symbol="STT",
        base_fee_bps=20,
        discount_bps=0,
        final_fee_bps=20,
        fee_amount_native=Decimal("0.000400"),
        fee_recipient="0xfee",
        expires_at=int(time.time()) + 60,
    )
    state = {
        "plan_history": [plan],
        "execution_state": None,
        "fee_quotes": [fee_quote.to_dict()],
        "preflight_estimates": {
            "step_0": {
                "swap_quote": {
                    "amount_out": "3.75",
                },
                "gas_estimate_native": "0.003600",
                "native_symbol": "STT",
                "chain": "Somnia Testnet",
            }
        },
    }

    result = asyncio.run(confirmation_node(state))
    message = result["messages"][0].content

    assert message.startswith("Review this request.")
    assert "Swap 0.2 STT to NIA on somnia testnet." in message
    assert "You should receive about 3.75 NIA." in message
    assert "Network fee: about 0.003600 STT." in message
    assert "Platform fee: 0.000400 STT." in message
    assert "Reply 'confirm' to continue or 'cancel' to stop." in message
    assert "Transaction Receipt" not in message


def test_confirmation_node_records_waiting_confirmation_task():
    plan = ExecutionPlan(
        goal="Swap 0.2 STT for NIA",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "amount_in": 0.2,
                    "token_in_symbol": "STT",
                    "token_out_symbol": "NIA",
                    "chain": "somnia testnet",
                },
                depends_on=[],
                approval_required=True,
            )
        },
    )
    state = {
        "user_id": "user-1",
        "provider": "discord",
        "user_info": {"volo_user_id": "user-1"},
        "context": {"thread_id": "thread-1"},
        "execution_id": "exec-1",
        "plan_history": [plan],
        "execution_state": None,
        "fee_quotes": [],
        "preflight_estimates": {},
    }

    with patch(
        "graph.nodes.confirmation_node.upsert_task_from_state",
        new=AsyncMock(),
    ) as upsert_task:
        asyncio.run(confirmation_node(state))

    upsert_task.assert_awaited_once()
    assert upsert_task.await_args.kwargs["status"] == "WAITING_CONFIRMATION"


def test_confirmation_node_does_not_show_fee_refresh_copy_when_quote_expired():
    plan = ExecutionPlan(
        goal="Swap 0.2 STT for NIA",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "amount_in": 0.2,
                    "token_in_symbol": "STT",
                    "token_out_symbol": "NIA",
                    "chain": "somnia testnet",
                },
                depends_on=[],
                approval_required=True,
            )
        },
    )
    expired_quote = FeeQuote(
        node_id="step_0",
        tool="swap",
        chain="Somnia Testnet",
        native_symbol="STT",
        base_fee_bps=20,
        discount_bps=0,
        final_fee_bps=20,
        fee_amount_native=Decimal("0.000400"),
        fee_recipient="0xfee",
        expires_at=int(time.time()) - 5,
    )
    state = {
        "plan_history": [plan],
        "execution_state": None,
        "fee_quotes": [expired_quote.to_dict()],
        "preflight_estimates": {},
    }

    result = asyncio.run(confirmation_node(state))
    message = result["messages"][0].content

    assert "Platform fee: 0.000400 STT." in message
    assert "The fee will be refreshed before send." not in message


def test_confirmation_node_shows_minimum_receive_when_exact_output_missing():
    plan = ExecutionPlan(
        goal="Swap 70.22505483 NIA for STT",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "amount_in": 70.22505483,
                    "token_in_symbol": "NIA",
                    "token_out_symbol": "STT",
                    "chain": "somnia testnet",
                },
                depends_on=[],
                approval_required=True,
            )
        },
    )
    state = {
        "plan_history": [plan],
        "execution_state": None,
        "fee_quotes": [],
        "preflight_estimates": {
            "step_0": {
                "swap_quote": {
                    "amount_out_minimum": "0.244100",
                },
            }
        },
    }

    result = asyncio.run(confirmation_node(state))
    message = result["messages"][0].content

    assert "Swap 70.22505483 NIA to STT on somnia testnet." in message
    assert "You should receive at least 0.244100 STT." in message


def test_confirmation_node_omits_platform_fee_line_for_unwrap_without_quote():
    plan = ExecutionPlan(
        goal="Unwrap ETH",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="unwrap",
                args={
                    "token_symbol": "ETH",
                    "wrapped_token_symbol": "WETH",
                    "chain": "base sepolia",
                },
                depends_on=[],
                approval_required=False,
            )
        },
    )
    state = {
        "plan_history": [plan],
        "execution_state": None,
        "fee_quotes": [],
        "preflight_estimates": {},
    }

    result = asyncio.run(confirmation_node(state))
    message = result["messages"][0].content

    assert "Unwrap all available WETH on base sepolia." in message
    assert "Platform fee: unavailable." not in message
