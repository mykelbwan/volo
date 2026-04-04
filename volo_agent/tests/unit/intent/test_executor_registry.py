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
from graph.agent_state import AgentState
from graph.nodes.executor_node import execution_engine_node


@pytest.fixture(autouse=True)
def _stub_noncritical_side_effects(monkeypatch: pytest.MonkeyPatch):
    class _NoopLedger:
        def record_execution(self, *args, **kwargs):
            return None

        def record_fee(self, *args, **kwargs):
            return None

    monkeypatch.setattr("core.execution.runtime.get_ledger", lambda: _NoopLedger())
    monkeypatch.setattr(
        "core.execution.runtime.track_execution_volume",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "core.utils.event_stream.publish_event", lambda *args, **kwargs: False
    )
    async def _noop_publish_async(*args, **kwargs):
        return False

    monkeypatch.setattr(
        "graph.nodes.executor_node.publish_event", lambda *args, **kwargs: False
    )
    monkeypatch.setattr(
        "graph.nodes.executor_node.publish_event_async", _noop_publish_async
    )
    class _NoopTaskHistoryRegistry:
        async def record_event(self, *args, **kwargs):
            return "task-id"

    monkeypatch.setattr(
        "core.history.task_history.TaskHistoryRegistry.record_event", _NoopTaskHistoryRegistry.record_event
    )
    monkeypatch.setattr(
        "graph.nodes.executor_node.ConversationTaskRegistry", lambda: AsyncMock()
    )


def _make_agent_state(plan: ExecutionPlan, state: ExecutionState) -> AgentState:
    return {
        "user_id": "user-1",
        "provider": "discord",
        "username": "alice",
        "user_info": {},
        "intents": [],
        "plans": [],
        "goal_parameters": {},
        "plan_history": [plan],
        "execution_state": state,
        "artifacts": {
            "sub_org_id": "mock-sub-org-id",
            "sender_address": "0xSenderAddress",
        },
        "context": {},
        "route_decision": "CONFIRMED",
        "confirmation_status": "WAITING",
        "pending_transactions": [],
        "reasoning_logs": [],
        "messages": [],
        "fee_quotes": [],
        "trigger_id": None,
        "is_triggered_execution": None,
    }


def test_executor_node_success():
    # Setup state with a plan
    node_id = "step_0"
    node = PlanNode(
        id=node_id,
        tool="swap",
        args={
            "token_in_symbol": "USDC",
            "token_out_symbol": "WETH",
            "amount_in": 100.0,
            "chain": "somnia testnet",
            "sub_org_id": "mock-sub-org-id",
            "sender": "0xSenderAddress",
        },
        depends_on=[],
        approval_required=False,  # Already approved
    )

    plan = ExecutionPlan(goal="Swap USDC for WETH", nodes={node_id: node})

    state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})

    agent_state = _make_agent_state(plan, state)

    # Execute node
    mock_tool_result = {"message": "Swap executed", "tx_hash": "0xabc123"}
    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(mock_tool_result, 0.05)),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    # Assertions
    assert result["confirmation_status"] == "EXECUTED"
    assert len(result["pending_transactions"]) == 1
    assert result["pending_transactions"][0]["type"] == "swap"
    assert result["execution_state"].node_states[node_id].status == StepStatus.SUCCESS
    assert result["execution_state"].completed is True
    assert isinstance(result["messages"][0], AIMessage)


def test_executor_node_invalid_args():
    # Setup state with a plan that has MISSING required arguments
    node_id = "step_0"
    node = PlanNode(
        id=node_id,
        tool="swap",
        args={
            "token_in_symbol": "USDC",
            # missing token_out_symbol
            "amount_in": 100.0,
            "chain": "somnia testnet",
            "sub_org_id": "mock-sub-org-id",
            "sender": "0xSenderAddress",
        },
        depends_on=[],
        approval_required=False,
    )

    plan = ExecutionPlan(goal="Invalid Swap", nodes={node_id: node})

    state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})

    agent_state = _make_agent_state(plan, state)

    # Execute node
    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch("graph.nodes.executor_node._run_with_timing") as mock_run:
        result = asyncio.run(execution_engine_node(agent_state))

    # Assertions: Should fail validation
    assert result["execution_state"].node_states[node_id].status == StepStatus.FAILED
    assert (
        "validationerror"
        in result["execution_state"].node_states[node_id].error.lower()
    )
    mock_run.assert_not_called()


if __name__ == "__main__":
    test_executor_node_success()
    test_executor_node_invalid_args()
    print("All executor registry tests passed!")
