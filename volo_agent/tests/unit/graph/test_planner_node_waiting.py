import asyncio

from core.planning.execution_plan import ExecutionPlan, ExecutionState, NodeState, StepStatus
from graph.nodes.planner_node import planner_node


def test_planner_node_waiting_when_in_flight_and_no_ready_nodes():
    plan = ExecutionPlan(
        goal="test",
        nodes={
            "step_0": {
                "id": "step_0",
                "tool": "bridge",
                "args": {},
                "depends_on": [],
                "approval_required": False,
            },
            "step_1": {
                "id": "step_1",
                "tool": "swap",
                "args": {},
                "depends_on": ["step_0"],
                "approval_required": False,
            },
        },
    )

    execution_state = ExecutionState(
        node_states={
            "step_0": NodeState(node_id="step_0", status=StepStatus.RUNNING),
            "step_1": NodeState(node_id="step_1", status=StepStatus.PENDING),
        }
    )

    state = {
        "plan_history": [plan],
        "execution_state": execution_state,
    }

    result = asyncio.run(planner_node(state))
    assert result["route_decision"] == "WAITING"
    assert "messages" in result


def test_planner_node_waiting_for_funds_short_circuits_before_llm():
    plan = ExecutionPlan(
        goal="test",
        nodes={
            "step_0": {
                "id": "step_0",
                "tool": "swap",
                "args": {},
                "depends_on": [],
                "approval_required": False,
            },
        },
    )
    execution_state = ExecutionState(
        node_states={
            "step_0": NodeState(node_id="step_0", status=StepStatus.PENDING),
        }
    )
    state = {
        "plan_history": [plan],
        "execution_state": execution_state,
        "waiting_for_funds": {
            "wait_id": "wait-1",
            "node_id": "step_0",
        },
    }

    result = asyncio.run(planner_node(state))
    assert result["route_decision"] == "WAITING_FUNDS"
