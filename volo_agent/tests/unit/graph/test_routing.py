from core.planning.execution_plan import ExecutionPlan, PlanNode
from graph.nodes.routing import route_balance_check, route_planner


def _plan_with_tool(tool: str) -> ExecutionPlan:
    return ExecutionPlan(
        goal="g",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool=tool,
                args={},
                depends_on=[],
                approval_required=True,
            )
        },
    )


def test_route_balance_check_bypasses_confirmation_for_unwrap():
    state = {
        "route_decision": "confirm",
        "plan_history": [_plan_with_tool("unwrap")],
    }

    assert route_balance_check(state) == "execute"


def test_route_balance_check_preserves_end_for_unwrap_failures():
    state = {
        "route_decision": "end",
        "plan_history": [_plan_with_tool("unwrap")],
    }

    assert route_balance_check(state) == "end"


def test_route_planner_bypasses_approval_for_unwrap_plan():
    state = {
        "route_decision": "REQUIRE_APPROVAL",
        "plan_history": [_plan_with_tool("unwrap")],
    }

    assert route_planner(state) == "continue"


def test_route_planner_keeps_approval_for_non_unwrap_plan():
    state = {
        "route_decision": "REQUIRE_APPROVAL",
        "plan_history": [_plan_with_tool("swap")],
    }

    assert route_planner(state) == "approval"
