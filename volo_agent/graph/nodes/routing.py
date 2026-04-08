from graph.agent_state import AgentState
from intent_hub.ontology.intent import IntentStatus
from intent_hub.resolver.templates import can_apply_templates


def _plan_contains_unwrap(state: AgentState) -> bool:
    plan_history = state.get("plan_history") or []
    if not plan_history:
        return False
    plan = plan_history[-1]
    nodes = getattr(plan, "nodes", None)
    if nodes is None and isinstance(plan, dict):
        nodes = plan.get("nodes")
    if not isinstance(nodes, dict):
        return False
    for node in nodes.values():
        if isinstance(node, dict):
            tool = node.get("tool")
        else:
            tool = getattr(node, "tool", None)
        if str(tool or "").strip().lower() == "unwrap":
            return True
    return False


def route_onboarding(state: AgentState) -> str:
    if state.get("route_decision") == "end":
        return "end"
    return "router"


def route_main(state: AgentState) -> str:
    decision = state.get("route_decision")
    if decision in ("CONVERSATION", "STATUS", "CANCELLED"):
        return "end"
    if decision == "CONFIRMED":
        return "execute"
    return "parse"


def route_post_parse(state: AgentState) -> str:
    intents = state.get("intents", [])
    if not intents:
        return "end"

    # block on any incomplete intent regardless of conditions.
    for data in intents:
        if data.get("status") == IntentStatus.INCOMPLETE:
            # Allow deterministic template resolution for common flows.
            if can_apply_templates(intents):
                break
            return "end"

    # any complete intent with a trigger condition → park the graph.
    for data in intents:
        condition = data.get("condition")
        if condition and isinstance(condition, dict) and condition.get("type"):
            return "wait_trigger"

    # all complete, no conditions → immediate execution path.
    return "resolve"


def route_after_trigger(state: AgentState) -> str:
    decision = state.get("route_decision")
    if decision == "resolve":
        return "resolve"
    return "end"


def route_after_resolver(state: AgentState) -> str:
    decision = state.get("route_decision")
    if decision == "resolve":
        return "resolve"
    return "end"


def route_balance_check(state: AgentState) -> str:
    decision = state.get("route_decision")
    if decision == "execute":
        return "execute"
    if decision != "confirm":
        return "end"
    if _plan_contains_unwrap(state):
        return "execute"

    # If this execution was triggered by the Observer, bypass the receipt
    # and go directly to the executor — the user already committed to the
    # action when they set the limit order.
    if state.get("is_triggered_execution"):
        return "execute"
    if state.get("auto_resume_execution"):
        return "execute"

    return "confirm"


def route_after_plan_optimizer(state: AgentState) -> str:
    decision = state.get("route_decision")
    if decision == "PLAN_RETRY":
        return "planner"
    return "balance_check"


def route_after_wait_for_funds(state: AgentState) -> str:
    decision = state.get("route_decision")
    if decision == "resume":
        return "resume"
    return "end"


def route_planner(state: AgentState) -> str:
    decision = state.get("route_decision")
    if decision == "CONTINUE":
        return "continue"
    if decision == "REQUIRE_APPROVAL":
        if _plan_contains_unwrap(state):
            return "continue"
        return "approval"
    if decision == "WAITING_FUNDS":
        return "wait_funds"
    if decision == "WAITING":
        return "end"
    return "end"
