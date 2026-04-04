import asyncio
from unittest.mock import patch

from langchain_core.messages import AIMessage, HumanMessage

from core.planning.execution_plan import (
    ExecutionPlan,
    ExecutionState,
    NodeState,
    PlanNode,
    StepStatus,
)
from graph.nodes.parser_node import intent_parser_node
from graph.nodes.router_node import conversational_router_node
from intent_hub.ontology.intent import Intent, IntentStatus


def _make_state(messages, **overrides):
    state = {
        "user_id": "user-1",
        "provider": "discord",
        "username": "alice",
        "user_info": {"volo_user_id": "user-1"},
        "intents": [],
        "plans": [],
        "goal_parameters": {},
        "plan_history": [],
        "execution_state": None,
        "artifacts": {},
        "context": {},
        "route_decision": None,
        "confirmation_status": None,
        "pending_transactions": [],
        "reasoning_logs": [],
        "messages": messages,
        "fee_quotes": [],
        "trigger_id": None,
        "is_triggered_execution": None,
        "pending_cancel": None,
    }
    state.update(overrides)
    return state


def _complete_balance_intent() -> Intent:
    return Intent(
        intent_type="balance",
        slots={"chain": "base"},
        missing_slots=[],
        constraints=None,
        confidence=0.97,
        status=IntentStatus.COMPLETE,
        raw_input="check my balance on base",
        clarification_prompt=None,
    )


def test_bridge_worker_completion_then_balance_does_not_replay_old_bridge():
    bridge_node = PlanNode(
        id="step_0",
        tool="bridge",
        args={
            "amount": 0.1,
            "token_symbol": "ETH",
            "source_chain": "ethereum",
            "target_chain": "base",
        },
        depends_on=[],
        approval_required=False,
    )
    plan = ExecutionPlan(goal="bridge", nodes={"step_0": bridge_node})
    # Simulate post-worker state: bridge node is terminal.
    execution_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS)},
        completed=True,
    )
    messages = [
        HumanMessage(content="bridge 0.1 eth from ethereum to base"),
        AIMessage(content="Bridge submitted."),
        AIMessage(content="Bridge ethereum -> base via across success."),
        HumanMessage(content="show my balance on base"),
    ]
    state = _make_state(
        messages=messages,
        plan_history=[plan],
        execution_state=execution_state,
    )

    with patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "ACTION", "response": None},
    ) as route_mock:
        routed = asyncio.run(conversational_router_node(state))

    # Balance requests should bypass router LLM and scope parsing to newest user turn.
    route_mock.assert_not_called()
    assert routed["route_decision"] == "ACTION"
    assert routed["parse_scope"] == "last_user"

    parser_state = _make_state(
        messages=messages,
        plan_history=[plan],
        execution_state=execution_state,
        parse_scope=routed["parse_scope"],
    )
    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[_complete_balance_intent()]
    ) as parse_mock:
        parsed = asyncio.run(intent_parser_node(parser_state))

    scoped_messages = parse_mock.call_args[0][0]
    assert len(scoped_messages) == 1
    assert isinstance(scoped_messages[0], HumanMessage)
    assert scoped_messages[0].content == "show my balance on base"
    assert parsed["intents"][0]["intent_type"] == "balance"
