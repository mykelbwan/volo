import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import AIMessage

from core.planning.execution_plan import (
    ExecutionPlan,
    ExecutionState,
    NodeState,
    PlanNode,
    StepStatus,
)
from graph.nodes import planner_node as planner_mod
from core.memory.ledger import ErrorCategory


def test_planner_stops_on_non_retryable_failure(monkeypatch):
    class DummyLLM:
        async def ainvoke(self, *_args, **_kwargs):
            raise AssertionError("Planner should not call LLM for non-retryable errors")

    monkeypatch.setattr(planner_mod, "planning_llm", DummyLLM())

    plan = ExecutionPlan(
        goal="Swap STT to NIA",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={"amount": 0.5, "from_token": "STT", "to_token": "NIA"},
                depends_on=[],
                approval_required=True,
            )
        },
    )

    execution_state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.FAILED,
                error="Swap reverted on-chain",
                error_category=ErrorCategory.NON_RETRYABLE.value,
            )
        }
    )

    state = {"plan_history": [plan], "execution_state": execution_state}

    result = asyncio.run(planner_mod.planner_node(state))
    assert result.get("route_decision") == "FAILED"
    assert result.get("messages")


def test_planner_returns_balance_message_on_completed_balance_plan():
    plan = ExecutionPlan(
        goal="Check balances",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="check_balance",
                args={"chain": "solana"},
                depends_on=[],
                approval_required=False,
            )
        },
    )
    execution_state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.SUCCESS,
                result={
                    "message": "Balances on Solana for abc...1234:\n  - 1 SOL (Solana)"
                },
            )
        },
        completed=True,
    )
    state = {"plan_history": [plan], "execution_state": execution_state}

    result = asyncio.run(planner_mod.planner_node(state))
    assert result["route_decision"] == "FINISHED"
    assert "Balance request completed" in result["reasoning_logs"][0]
    assert "Balances on Solana" in result["messages"][0].content


def test_planner_skips_duplicate_balance_message_when_already_present_in_state():
    balance_text = "Balances on Solana for abc...1234:\n  - 1 SOL (Solana)"
    plan = ExecutionPlan(
        goal="Check balances",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="check_balance",
                args={"chain": "solana"},
                depends_on=[],
                approval_required=False,
            )
        },
    )
    execution_state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.SUCCESS,
                result={"message": balance_text},
            )
        },
        completed=True,
    )
    state = {
        "plan_history": [plan],
        "execution_state": execution_state,
        "messages": [AIMessage(content=balance_text)],
    }

    result = asyncio.run(planner_mod.planner_node(state))
    assert result["route_decision"] == "FINISHED"
    assert "Balance request completed" in result["reasoning_logs"][0]
    assert "messages" not in result


def test_planner_returns_short_completion_message_for_finished_action_plan():
    plan = ExecutionPlan(
        goal="Swap STT to NIA",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={"amount_in": 0.2, "token_in_symbol": "STT", "token_out_symbol": "NIA"},
                depends_on=[],
                approval_required=True,
            )
        },
    )
    execution_state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.SUCCESS,
                result={"message": "Swap complete."},
            )
        },
        completed=True,
    )
    state = {"plan_history": [plan], "execution_state": execution_state}

    result = asyncio.run(planner_mod.planner_node(state))

    assert result["route_decision"] == "FINISHED"
    assert result["messages"][0].content == "Done. Your request is complete."


def test_planner_updates_task_on_completion():
    plan = ExecutionPlan(
        goal="Swap STT to NIA",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={"amount_in": 0.2, "token_in_symbol": "STT", "token_out_symbol": "NIA"},
                depends_on=[],
                approval_required=True,
            )
        },
    )
    execution_state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.SUCCESS,
                result={"message": "Swap complete."},
            )
        },
        completed=True,
    )
    state = {
        "user_id": "user-1",
        "provider": "discord",
        "user_info": {"volo_user_id": "user-1"},
        "context": {"thread_id": "thread-1"},
        "execution_id": "exec-1",
        "plan_history": [plan],
        "execution_state": execution_state,
    }

    with patch(
        "graph.nodes.planner_node.upsert_task_from_state",
        new=AsyncMock(),
    ) as upsert_task:
        result = asyncio.run(planner_mod.planner_node(state))

    assert result["route_decision"] == "FINISHED"
    upsert_task.assert_awaited_once()
    assert upsert_task.await_args.kwargs["status"] == "COMPLETED"


def test_planner_updates_task_on_non_retryable_failure(monkeypatch):
    class DummyLLM:
        async def ainvoke(self, *_args, **_kwargs):
            raise AssertionError("Planner should not call LLM for non-retryable errors")

    monkeypatch.setattr(planner_mod, "planning_llm", DummyLLM())

    plan = ExecutionPlan(
        goal="Swap STT to NIA",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={"amount": 0.5, "from_token": "STT", "to_token": "NIA"},
                depends_on=[],
                approval_required=True,
            )
        },
    )

    execution_state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.FAILED,
                error="Swap reverted on-chain",
                error_category=ErrorCategory.NON_RETRYABLE.value,
            )
        }
    )

    state = {
        "user_id": "user-1",
        "provider": "discord",
        "user_info": {"volo_user_id": "user-1"},
        "context": {"thread_id": "thread-1"},
        "execution_id": "exec-1",
        "plan_history": [plan],
        "execution_state": execution_state,
    }

    with patch(
        "graph.nodes.planner_node.upsert_task_from_state",
        new=AsyncMock(),
    ) as upsert_task:
        result = asyncio.run(planner_mod.planner_node(state))

    assert result.get("route_decision") == "FAILED"
    upsert_task.assert_awaited_once()
    assert upsert_task.await_args.kwargs["status"] == "FAILED"


def test_planner_does_not_finish_while_step_is_still_running(monkeypatch):
    class DummyLLM:
        async def ainvoke(self, *_args, **_kwargs):
            return type("Resp", (), {"content": '{"status":"FINISHED","reasoning":"Done."}'})()

    monkeypatch.setattr(planner_mod, "planning_llm", DummyLLM())

    plan = ExecutionPlan(
        goal="Send SOL",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="transfer",
                args={"amount": 0.01, "asset_symbol": "SOL", "network": "solana-devnet"},
                depends_on=[],
                approval_required=True,
            )
        },
    )
    execution_state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.RUNNING,
            )
        }
    )
    state = {"plan_history": [plan], "execution_state": execution_state}

    result = asyncio.run(planner_mod.planner_node(state))

    assert result["route_decision"] == "WAITING"
    message = result["messages"][0].content.lower()
    assert "still running" in message or "still pending" in message
