import asyncio
from unittest.mock import AsyncMock, patch

from langchain_core.messages import AIMessage, HumanMessage

from graph.agent_state import AgentState
from graph.nodes.parser_node import intent_parser_node
from graph import replay_guard
from intent_hub.ontology.intent import Intent, IntentStatus


def _reset_replay_guard_counters() -> None:
    with replay_guard._LOCK:
        for key in list(replay_guard._COUNTERS.keys()):
            replay_guard._COUNTERS[key] = 0


def _snapshot_replay_guard_counters() -> dict[str, int]:
    with replay_guard._LOCK:
        return {key: int(value) for key, value in replay_guard._COUNTERS.items()}


def _make_state(messages) -> AgentState:
    return {
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
        "pending_edit": None,
        "pending_intent": None,
        "pending_intent_queue": None,
        "pending_clarification": None,
    }


def test_parser_node_sets_clarification_message():
    intent = Intent(
        intent_type="swap",
        slots={"token_in": {"symbol": "ETH"}},
        missing_slots=["token_out"],
        constraints=None,
        confidence=0.7,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap eth",
        clarification_prompt="Which token would you like to receive?",
    )

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[intent]
    ):
        state = _make_state(messages=[])
        result = asyncio.run(intent_parser_node(state))

    assert "intents" in result
    assert result["intents"][0]["intent_type"] == "swap"
    assert isinstance(result["messages"][0], AIMessage)
    assert "Which token" in result["messages"][0].content


def test_parser_node_records_waiting_input_task():
    intent = Intent(
        intent_type="swap",
        slots={"token_in": {"symbol": "ETH"}},
        missing_slots=["token_out"],
        constraints=None,
        confidence=0.7,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap eth",
        clarification_prompt="Which token would you like to receive?",
    )

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[intent]
    ), patch(
        "graph.nodes.parser_node.upsert_task_from_state",
        new=AsyncMock(),
    ) as upsert_task:
        state = _make_state(messages=[])
        result = asyncio.run(intent_parser_node(state))

    assert "intents" in result
    upsert_task.assert_awaited_once()
    assert upsert_task.await_args.kwargs["status"] == "WAITING_INPUT"


def test_parser_node_balance_missing_chain_prompts():
    intent = Intent(
        intent_type="balance",
        slots={"chain": None},
        missing_slots=["chain"],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.INCOMPLETE,
        raw_input="check my balance",
        clarification_prompt="Which network (e.g., Somnia, Ethereum, Base) would you like to check your balances on?",
    )

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[intent]
    ):
        state = _make_state(messages=[])
        result = asyncio.run(intent_parser_node(state))

    assert "intents" in result
    assert result["intents"][0]["intent_type"] == "balance"
    assert isinstance(result["messages"][0], AIMessage)
    assert "Which network" in result["messages"][0].content


def test_parser_node_balance_with_chain_no_prompt():
    intent = Intent(
        intent_type="balance",
        slots={"chain": "somnia testnet"},
        missing_slots=[],
        constraints=None,
        confidence=0.95,
        status=IntentStatus.COMPLETE,
        raw_input="check my balance on somnia testnet",
        clarification_prompt=None,
    )

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[intent]
    ):
        state = _make_state(messages=[])
        result = asyncio.run(intent_parser_node(state))

    assert "intents" in result
    assert result["intents"][0]["intent_type"] == "balance"
    assert "messages" not in result


def test_parser_node_parse_error_returns_fallback_message():
    with patch("graph.nodes.parser_node.parse_async", side_effect=RuntimeError("bad")):
        state = _make_state(messages=[])
        result = asyncio.run(intent_parser_node(state))

    assert result["intents"] == []
    assert isinstance(result["messages"][0], AIMessage)
    assert "couldn't understand" in result["messages"][0].content.lower()


def test_parser_node_empty_intents_returns_fallback_message():
    with patch("graph.nodes.parser_node.parse_async", return_value=[]):
        state = _make_state(messages=[])
        result = asyncio.run(intent_parser_node(state))

    assert result["intents"] == []
    assert isinstance(result["messages"][0], AIMessage)
    assert "couldn't understand" in result["messages"][0].content.lower()


def test_parser_node_parse_error_with_weak_action_evidence_hands_back_to_conversation():
    with patch(
        "graph.nodes.parser_node.parse_async", side_effect=RuntimeError("bad")
    ), patch(
        "graph.nodes.parser_node.respond_conversation",
        new=AsyncMock(return_value="I'm Volo."),
    ):
        state = _make_state(messages=[HumanMessage(content="what is your name?")])
        result = asyncio.run(intent_parser_node(state))

    assert result["intents"] == []
    assert result["route_decision"] == "CONVERSATION"
    assert result["messages"][0].content == "I'm Volo."


def test_parser_node_empty_intents_with_weak_action_evidence_hands_back_to_conversation():
    with patch("graph.nodes.parser_node.parse_async", return_value=[]), patch(
        "graph.nodes.parser_node.respond_conversation",
        new=AsyncMock(return_value="I can help with swaps, bridges, transfers, and balances."),
    ):
        state = _make_state(messages=[HumanMessage(content="what can you do?")])
        result = asyncio.run(intent_parser_node(state))

    assert result["intents"] == []
    assert result["route_decision"] == "CONVERSATION"
    assert "swaps" in result["messages"][0].content.lower()


def test_parser_node_missing_prompt_uses_fallback():
    intent = Intent(
        intent_type="swap",
        slots={"token_in": {"symbol": "ETH"}},
        missing_slots=["amount", "token_out"],
        constraints=None,
        confidence=0.6,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap eth",
        clarification_prompt=None,
    )

    with patch("graph.nodes.parser_node.parse_async", return_value=[intent]):
        state = _make_state(messages=[])
        result = asyncio.run(intent_parser_node(state))

    assert isinstance(result["messages"][0], AIMessage)
    assert (
        result["messages"][0].content
        == "Please share the output token, amount, and chain."
    )


def test_parser_node_persists_first_incomplete_intent():
    intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "STT"},
            "token_out": {"symbol": "NIA"},
            "amount": 0.2,
        },
        missing_slots=["chain"],
        constraints=None,
        confidence=0.8,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap 0.2 stt for nia",
        clarification_prompt="Which chain?",
    )

    with patch("graph.nodes.parser_node.parse_async", return_value=[intent]):
        state = _make_state(messages=[HumanMessage(content="swap 0.2 stt for nia")])
        result = asyncio.run(intent_parser_node(state))

    assert result["pending_intent"]["intent_type"] == "swap"
    assert result["pending_intent"]["missing_slots"] == ["chain"]
    assert result["pending_clarification"]["slot_name"] == "chain"
    assert result["pending_clarification"]["attempt_count"] == 0


def test_parser_node_persists_downstream_queue_for_dependent_sequence():
    bridge_intent = Intent(
        intent_type="bridge",
        slots={
            "token_in": {"symbol": "USDC"},
            "chain": "base sepolia",
            "target_chain": "sepolia",
        },
        missing_slots=["amount"],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.INCOMPLETE,
        raw_input="bridge usdc from base sepolia to sepolia and swap it for eth",
        clarification_prompt="How much USDC would you like to bridge?",
    )
    swap_intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "USDC"},
            "token_out": {"symbol": "ETH"},
            "chain": "sepolia",
            "_carry_amount_from_prev": True,
        },
        missing_slots=["amount"],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.INCOMPLETE,
        raw_input="bridge usdc from base sepolia to sepolia and swap it for eth",
        clarification_prompt="How much USDC would you like to swap?",
    )

    with patch("graph.nodes.parser_node.parse_async", return_value=[bridge_intent, swap_intent]):
        state = _make_state(
            messages=[
                HumanMessage(
                    content="bridge usdc from base sepolia to sepolia and swap it for eth"
                )
            ]
        )
        result = asyncio.run(intent_parser_node(state))

    assert result["pending_intent"]["intent_type"] == "bridge"
    assert isinstance(result["pending_intent_queue"], list)
    assert len(result["pending_intent_queue"]) == 1
    assert result["pending_intent_queue"][0]["intent_type"] == "swap"
    assert result["pending_intent_queue"][0]["missing_slots"] == ["amount"]


def test_parser_node_recent_turn_carries_bridge_amount_to_queued_swap():
    pending_intent = Intent(
        intent_type="bridge",
        slots={
            "token_in": {"symbol": "USDC"},
            "chain": "base sepolia",
            "target_chain": "sepolia",
        },
        missing_slots=["amount"],
        constraints=None,
        confidence=0.92,
        status=IntentStatus.INCOMPLETE,
        raw_input="bridge usdc from base sepolia to sepolia and swap it for eth",
        clarification_prompt="How much USDC would you like to bridge?",
    )
    queued_swap = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "USDC"},
            "token_out": {"symbol": "ETH"},
            "chain": "sepolia",
            "_carry_amount_from_prev": True,
            "_carry_token_from_prev": True,
        },
        missing_slots=["amount"],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.INCOMPLETE,
        raw_input="bridge usdc from base sepolia to sepolia and swap it for eth",
        clarification_prompt="How much USDC would you like to swap?",
    )

    state = _make_state(
        messages=[
            AIMessage(content="How much USDC would you like to bridge?"),
            HumanMessage(content="2"),
        ]
    )
    state["parse_scope"] = "recent_turn"
    state["pending_intent"] = pending_intent.model_dump()
    state["pending_intent_queue"] = [queued_swap.model_dump()]

    with patch(
        "graph.nodes.parser_node._get_token_registry_async",
        return_value={},
    ), patch(
        "graph.nodes.parser_node.parse_async",
        side_effect=RuntimeError("should not need full reparse"),
    ):
        result = asyncio.run(intent_parser_node(state))

    assert len(result["intents"]) == 2
    assert result["intents"][0]["intent_type"] == "bridge"
    assert result["intents"][0]["status"] == IntentStatus.COMPLETE
    assert result["intents"][0]["slots"]["amount"] == 2
    assert result["intents"][1]["intent_type"] == "swap"
    assert result["intents"][1]["status"] == IntentStatus.COMPLETE
    assert result["intents"][1]["slots"]["amount"] == 2
    assert result["pending_intent"] is None
    assert result["pending_intent_queue"] is None
    assert result["pending_clarification"] is None
    assert "messages" not in result


def test_parser_node_dependent_token_mismatch_requests_clarification():
    pending_intent = Intent(
        intent_type="bridge",
        slots={
            "token_in": {"symbol": "USDC"},
            "chain": "base sepolia",
            "target_chain": "sepolia",
        },
        missing_slots=["amount"],
        constraints=None,
        confidence=0.92,
        status=IntentStatus.INCOMPLETE,
        raw_input="bridge usdc from base sepolia to sepolia then swap it for eth",
        clarification_prompt="How much USDC would you like to bridge?",
    )
    queued_swap = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "DAI"},
            "token_out": {"symbol": "ETH"},
            "chain": "sepolia",
            "_carry_amount_from_prev": True,
            "_carry_token_from_prev": True,
        },
        missing_slots=["amount"],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap it for eth",
        clarification_prompt="How much DAI would you like to swap?",
    )

    state = _make_state(
        messages=[
            AIMessage(content="How much USDC would you like to bridge?"),
            HumanMessage(content="2"),
        ]
    )
    state["parse_scope"] = "recent_turn"
    state["pending_intent"] = pending_intent.model_dump()
    state["pending_intent_queue"] = [queued_swap.model_dump()]

    with patch(
        "graph.nodes.parser_node._get_token_registry_async",
        return_value={},
    ), patch(
        "graph.nodes.parser_node.parse_async",
        side_effect=RuntimeError("should not need full reparse"),
    ):
        result = asyncio.run(intent_parser_node(state))

    assert result["pending_intent"]["intent_type"] == "swap"
    assert result["pending_intent"]["missing_slots"] == ["token_in"]
    assert result["pending_intent_queue"] is None
    assert "which token would you like to use" in result["messages"][0].content.lower()


def test_parser_node_recent_turn_continues_pending_swap_from_stored_intent():
    pending_intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "STT"},
            "token_out": {"symbol": "NIA"},
            "amount": 0.2,
        },
        missing_slots=["chain"],
        constraints=None,
        confidence=0.92,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap 0.2 stt for nia",
        clarification_prompt="Which chain?",
    )
    messages = [
        AIMessage(content="Which chain?"),
        HumanMessage(content="somnia chain"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "recent_turn"
    state["pending_intent"] = pending_intent.model_dump()

    with patch(
        "graph.nodes.parser_node._get_token_registry_async",
        return_value={},
    ), patch(
        "graph.nodes.parser_node.parse_async",
        side_effect=RuntimeError("should not need reparse for chain slot"),
    ):
        result = asyncio.run(intent_parser_node(state))

    assert result["intents"][0]["intent_type"] == "swap"
    assert result["intents"][0]["status"] == IntentStatus.COMPLETE
    assert result["intents"][0]["slots"]["chain"] == "somnia testnet"
    assert result["pending_intent"] is None
    assert result["pending_clarification"] is None
    assert "messages" not in result


def test_parser_node_recent_turn_uncertain_reply_keeps_pending_intent_and_clarifies():
    pending_intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "STT"},
            "token_out": {"symbol": "NIA"},
            "amount": 0.2,
        },
        missing_slots=["chain"],
        constraints=None,
        confidence=0.92,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap 0.2 stt for nia",
        clarification_prompt="Which chain?",
    )
    messages = [
        AIMessage(content="Which chain?"),
        HumanMessage(content="not sure"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "recent_turn"
    state["pending_intent"] = pending_intent.model_dump()
    state["pending_clarification"] = {
        "slot_name": "chain",
        "question_type": "missing_slot",
        "attempt_count": 0,
        "last_resolution_error": None,
    }

    with patch(
        "graph.nodes.parser_node._get_token_registry_async",
        return_value={},
    ), patch(
        "graph.nodes.parser_node.parse_async",
        side_effect=RuntimeError("should not reparse uncertain follow-up"),
    ), patch(
        "graph.nodes.parser_node.upsert_task_from_state",
        new=AsyncMock(),
    ) as upsert_task:
        result = asyncio.run(intent_parser_node(state))

    assert result["intents"] == []
    assert result["pending_intent"]["intent_type"] == "swap"
    assert "still need the chain" in result["messages"][0].content.lower()
    assert result["pending_clarification"]["attempt_count"] == 1
    assert result["pending_clarification"]["last_resolution_error"] == "uncertain"
    upsert_task.assert_awaited_once()


def test_parser_node_recent_turn_accepts_maybe_base_as_chain_fill():
    pending_intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "STT"},
            "token_out": {"symbol": "NIA"},
            "amount": 0.2,
        },
        missing_slots=["chain"],
        constraints=None,
        confidence=0.92,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap 0.2 stt for nia",
        clarification_prompt="Which chain?",
    )
    messages = [
        AIMessage(content="Which chain?"),
        HumanMessage(content="maybe base"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "recent_turn"
    state["pending_intent"] = pending_intent.model_dump()
    state["pending_clarification"] = {
        "slot_name": "chain",
        "question_type": "missing_slot",
        "attempt_count": 1,
        "last_resolution_error": "uncertain",
    }

    with patch(
        "graph.nodes.parser_node._get_token_registry_async",
        return_value={},
    ), patch(
        "graph.nodes.parser_node.parse_async",
        side_effect=RuntimeError("should not need reparse for maybe base"),
    ):
        result = asyncio.run(intent_parser_node(state))

    assert result["intents"][0]["status"] == IntentStatus.COMPLETE
    assert result["intents"][0]["slots"]["chain"] == "base"
    assert result["pending_intent"] is None
    assert result["pending_clarification"] is None


def test_parser_node_reference_reply_requests_exact_slot_value():
    pending_intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "STT"},
            "amount": 0.2,
            "chain": "base",
        },
        missing_slots=["token_out"],
        constraints=None,
        confidence=0.92,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap 0.2 stt",
        clarification_prompt="Which token would you like to receive?",
    )
    messages = [
        AIMessage(content="Which token would you like to receive?"),
        HumanMessage(content="same token as before"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "recent_turn"
    state["pending_intent"] = pending_intent.model_dump()
    state["pending_clarification"] = {
        "slot_name": "token_out",
        "question_type": "missing_slot",
        "attempt_count": 0,
        "last_resolution_error": None,
    }

    with patch(
        "graph.nodes.parser_node._get_token_registry_async",
        return_value={},
    ), patch(
        "graph.nodes.parser_node.parse_async",
        side_effect=RuntimeError("should not reparse unresolved reference"),
    ):
        result = asyncio.run(intent_parser_node(state))

    content = result["messages"][0].content.lower()
    assert result["intents"] == []
    assert "can't safely infer" in content
    assert "output token" in content
    assert result["pending_clarification"]["last_resolution_error"] == "reference"


def test_parser_node_that_one_again_is_treated_as_reference_not_token():
    pending_intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "USDC"},
            "amount": 0.2,
            "chain": "base",
        },
        missing_slots=["token_out"],
        constraints=None,
        confidence=0.92,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap 0.2 usdc on base",
        clarification_prompt="Which token would you like to receive?",
    )
    messages = [
        AIMessage(content="Which token would you like to receive?"),
        HumanMessage(content="that one again"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "recent_turn"
    state["pending_intent"] = pending_intent.model_dump()
    state["pending_clarification"] = {
        "slot_name": "token_out",
        "question_type": "missing_slot",
        "attempt_count": 0,
        "last_resolution_error": None,
    }

    with patch(
        "graph.nodes.parser_node._get_token_registry_async",
        return_value={},
    ), patch(
        "graph.nodes.parser_node.parse_async",
        side_effect=RuntimeError("should not reparse unresolved reference"),
    ):
        result = asyncio.run(intent_parser_node(state))

    content = result["messages"][0].content.lower()
    assert result["intents"] == []
    assert "can't safely infer" in content
    assert "output token" in content
    assert result["pending_clarification"]["last_resolution_error"] == "reference"


def test_parser_node_second_failed_clarification_includes_known_slot_summary():
    pending_intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "STT"},
            "amount": 0.2,
            "chain": "base",
        },
        missing_slots=["token_out"],
        constraints=None,
        confidence=0.92,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap 0.2 stt on base",
        clarification_prompt="Which token would you like to receive?",
    )
    messages = [
        AIMessage(content="Which token would you like to receive?"),
        HumanMessage(content="not sure"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "recent_turn"
    state["pending_intent"] = pending_intent.model_dump()
    state["pending_clarification"] = {
        "slot_name": "token_out",
        "question_type": "missing_slot",
        "attempt_count": 1,
        "last_resolution_error": "unknown",
    }

    with patch(
        "graph.nodes.parser_node._get_token_registry_async",
        return_value={},
    ), patch(
        "graph.nodes.parser_node.parse_async",
        side_effect=RuntimeError("should not reparse uncertain follow-up"),
    ):
        result = asyncio.run(intent_parser_node(state))

    content = result["messages"][0].content.lower()
    assert "i already have input token stt" in content
    assert "chain base" in content
    assert result["pending_clarification"]["attempt_count"] == 2


def test_parser_node_last_user_scope_does_not_force_old_pending_intent_merge():
    pending_intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "STT"},
            "token_out": {"symbol": "NIA"},
            "amount": 0.2,
        },
        missing_slots=["chain"],
        constraints=None,
        confidence=0.92,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap 0.2 stt for nia",
        clarification_prompt="Which chain?",
    )
    balance_intent = Intent(
        intent_type="balance",
        slots={"chain": "base"},
        missing_slots=[],
        constraints=None,
        confidence=0.97,
        status=IntentStatus.COMPLETE,
        raw_input="show my balance on base",
        clarification_prompt=None,
    )
    messages = [HumanMessage(content="show my balance on base")]
    state = _make_state(messages=messages)
    state["parse_scope"] = "last_user"
    state["pending_intent"] = pending_intent.model_dump()

    with patch(
        "graph.nodes.parser_node.parse_async",
        return_value=[balance_intent],
    ):
        result = asyncio.run(intent_parser_node(state))

    assert result["intents"][0]["intent_type"] == "balance"
    assert result["pending_intent"] is None


def _complete_balance_intent() -> Intent:
    return Intent(
        intent_type="balance",
        slots={"chain": "base"},
        missing_slots=[],
        constraints=None,
        confidence=0.95,
        status=IntentStatus.COMPLETE,
        raw_input="check my balance on base",
        clarification_prompt=None,
    )


def test_parser_node_last_user_scope_only_passes_latest_user_message():
    messages = [
        HumanMessage(content="swap 1 eth to usdc"),
        AIMessage(content="Done."),
        HumanMessage(content="show my balance"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "last_user"

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[_complete_balance_intent()]
    ) as parse_mock:
        result = asyncio.run(intent_parser_node(state))

    scoped_messages = parse_mock.call_args[0][0]
    assert len(scoped_messages) == 1
    assert isinstance(scoped_messages[0], HumanMessage)
    assert scoped_messages[0].content == "show my balance"
    assert result["parse_scope"] is None


def test_parser_node_recent_turn_scope_passes_last_ai_and_last_user():
    messages = [
        HumanMessage(content="swap 1 eth to usdc"),
        AIMessage(content="That succeeded."),
        HumanMessage(content="base"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "recent_turn"

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[_complete_balance_intent()]
    ) as parse_mock:
        result = asyncio.run(intent_parser_node(state))

    scoped_messages = parse_mock.call_args[0][0]
    assert len(scoped_messages) == 2
    assert isinstance(scoped_messages[0], AIMessage)
    assert isinstance(scoped_messages[1], HumanMessage)
    assert scoped_messages[1].content == "base"
    assert result["parse_scope"] is None


def test_parser_node_recent_window_scope_limits_message_count():
    messages = [
        HumanMessage(content="msg-1"),
        AIMessage(content="msg-2"),
        HumanMessage(content="msg-3"),
        AIMessage(content="msg-4"),
        HumanMessage(content="msg-5"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "recent_window:2"

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[_complete_balance_intent()]
    ) as parse_mock:
        result = asyncio.run(intent_parser_node(state))

    scoped_messages = parse_mock.call_args[0][0]
    assert len(scoped_messages) == 2
    assert str(scoped_messages[0].content) == "msg-4"
    assert str(scoped_messages[1].content) == "msg-5"
    assert result["parse_scope"] is None


def test_parser_node_uses_parse_scope_default_env(monkeypatch):
    messages = [
        HumanMessage(content="swap 1 eth to usdc"),
        AIMessage(content="Done."),
        HumanMessage(content="show my balance"),
    ]
    state = _make_state(messages=messages)
    monkeypatch.setenv("VOLO_PARSE_SCOPE_DEFAULT", "last_user")

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[_complete_balance_intent()]
    ) as parse_mock:
        result = asyncio.run(intent_parser_node(state))

    scoped_messages = parse_mock.call_args[0][0]
    assert len(scoped_messages) == 1
    assert isinstance(scoped_messages[0], HumanMessage)
    assert scoped_messages[0].content == "show my balance"
    assert result["parse_scope"] is None


def test_parser_node_invalid_default_scope_falls_back_to_full(monkeypatch):
    messages = [
        HumanMessage(content="swap 1 eth to usdc"),
        AIMessage(content="Done."),
        HumanMessage(content="show my balance"),
    ]
    state = _make_state(messages=messages)
    monkeypatch.setenv("VOLO_PARSE_SCOPE_DEFAULT", "bad-scope")

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[_complete_balance_intent()]
    ) as parse_mock:
        asyncio.run(intent_parser_node(state))

    scoped_messages = parse_mock.call_args[0][0]
    assert len(scoped_messages) == 3


def test_parser_node_writes_rolling_summary_artifact():
    messages = [
        HumanMessage(content="bridge 0.1 eth from ethereum to base"),
        AIMessage(content="Bridge submitted."),
        HumanMessage(content="show my balance on base"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "last_user"

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[_complete_balance_intent()]
    ):
        result = asyncio.run(intent_parser_node(state))

    artifact = result["artifacts"]["rolling_message_summary"]
    assert artifact["scope"] == "last_user"
    assert artifact["summarized_message_count"] == 2
    assert artifact["highlights"]


def test_parser_node_records_scope_and_token_counters():
    _reset_replay_guard_counters()
    messages = [
        HumanMessage(content="bridge 0.1 eth from ethereum to base"),
        AIMessage(content="Bridge submitted."),
        HumanMessage(content="show my balance on base"),
    ]
    state = _make_state(messages=messages)
    state["parse_scope"] = "last_user"

    with patch(
        "graph.nodes.parser_node.parse_async", return_value=[_complete_balance_intent()]
    ):
        asyncio.run(intent_parser_node(state))

    counters = _snapshot_replay_guard_counters()
    assert counters["parse_scope_total"] == 1
    assert counters["parse_scope_last_user"] == 1
    assert counters["parse_messages_total"] == 3
    assert counters["parse_messages_selected_total"] == 1
    assert counters["parse_tokens_estimate_total"] >= counters[
        "parse_tokens_estimate_selected_total"
    ]
