import asyncio
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage

from intent_hub.ontology.trigger import TriggerCondition
from graph.nodes.wait_for_trigger_node import (
    _build_trigger_resume_token,
    wait_for_trigger_node,
)


def _make_state(intents):
    return {
        "user_id": "user-1",
        "provider": "discord",
        "username": "alice",
        "user_info": {"volo_user_id": "user-1"},
        "intents": intents,
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
        "messages": [],
        "fee_quotes": [],
        "trigger_id": None,
        "is_triggered_execution": None,
    }


def _conditional_intent():
    return {
        "intent_type": "swap",
        "condition": {"type": "price_below", "asset": "ETH", "target": 1000},
        "slots": {"amount": 1, "chain": "Base"},
    }


def _resolved_intents():
    return [{"intent_type": "swap", "slots": {"amount": 1, "chain": "Base"}}]


def _stored_condition_dict():
    condition = TriggerCondition(
        type="price_below",
        asset="ETH",
        target=1000,
    ).to_dict()
    condition["chain"] = "Base"
    return condition


def _trigger_doc(*, token: str):
    return {
        "trigger_id": "t-999",
        "status": "pending",
        "trigger_condition": _stored_condition_dict(),
        "payload": {
            "intents": _resolved_intents(),
            "resume_auth": {"resume_token": token},
        },
    }


@pytest.fixture(autouse=True)
def _set_trigger_secret(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("VOLO_TRIGGER_RESUME_SECRET", "test-trigger-secret")


def test_wait_for_trigger_no_conditional_intents(monkeypatch):
    state = _make_state(intents=[{"intent_type": "swap"}])
    registry = AsyncMock()
    monkeypatch.setattr(
        "graph.nodes.wait_for_trigger_node.TriggerRegistry",
        lambda: registry,
    )

    result = asyncio.run(wait_for_trigger_node(state, {"configurable": {"thread_id": "t"}}))

    assert result["route_decision"] == "resolve"


def test_wait_for_trigger_rejects_missing_resume_token(monkeypatch):
    state = _make_state(intents=[_conditional_intent()])
    registry = AsyncMock()
    registry.get_triggers_for_thread.return_value = [
        {
            "trigger_id": "t-123",
            "status": "pending",
            "trigger_condition": {"type": "price_below", "asset": "ETH", "target": 1000},
            "payload": {"intents": _resolved_intents()},
        }
    ]
    monkeypatch.setattr(
        "graph.nodes.wait_for_trigger_node.TriggerRegistry",
        lambda: registry,
    )

    result = asyncio.run(wait_for_trigger_node(state, {"configurable": {"thread_id": "t"}}))

    assert result["route_decision"] == "end"
    assert "secure resume authentication" in result["messages"][0].content.lower()


def test_wait_for_trigger_rejects_invalid_resume_token(monkeypatch):
    state = _make_state(intents=[_conditional_intent()])
    valid_token = _build_trigger_resume_token(
        thread_id="t",
        user_id="user-1",
        trigger_condition_dict={"type": "price_below", "asset": "ETH", "target": 1000},
        payload_intents=_resolved_intents(),
    )
    registry = AsyncMock()
    registry.get_triggers_for_thread.return_value = []
    registry.register_trigger.return_value = "t-999"
    registry.get_trigger.return_value = _trigger_doc(token=valid_token)

    def _fake_interrupt(_value):
        return {
            "condition_met": True,
            "trigger_id": "t-999",
            "matched_price": 900.0,
            "asset": "ETH",
            "trigger_type": "price_below",
            "resume_token": "invalid.token",
        }

    monkeypatch.setattr(
        "graph.nodes.wait_for_trigger_node.TriggerRegistry",
        lambda: registry,
    )
    monkeypatch.setattr("graph.nodes.wait_for_trigger_node.interrupt", _fake_interrupt)

    result = asyncio.run(wait_for_trigger_node(state, {"configurable": {"thread_id": "t"}}))

    assert result["route_decision"] == "end"
    assert "failed authentication" in result["messages"][0].content.lower(), (
        "a spoofed resume token must be rejected before any triggered execution continues"
    )


def test_wait_for_trigger_rejects_forged_token_signed_for_different_actions(monkeypatch):
    state = _make_state(intents=[_conditional_intent()])
    stored_token = _build_trigger_resume_token(
        thread_id="t",
        user_id="user-1",
        trigger_condition_dict={"type": "price_below", "asset": "ETH", "target": 1000},
        payload_intents=_resolved_intents(),
    )
    forged_token = _build_trigger_resume_token(
        thread_id="t",
        user_id="user-1",
        trigger_condition_dict={"type": "price_below", "asset": "ETH", "target": 1000},
        payload_intents=[{"intent_type": "transfer", "slots": {"amount": 999}}],
    )
    registry = AsyncMock()
    registry.get_triggers_for_thread.return_value = []
    registry.register_trigger.return_value = "t-999"
    registry.get_trigger.return_value = _trigger_doc(token=stored_token)

    def _fake_interrupt(_value):
        return {
            "condition_met": True,
            "trigger_id": "t-999",
            "matched_price": 900.0,
            "asset": "ETH",
            "trigger_type": "price_below",
            "resume_token": forged_token,
        }

    monkeypatch.setattr(
        "graph.nodes.wait_for_trigger_node.TriggerRegistry",
        lambda: registry,
    )
    monkeypatch.setattr("graph.nodes.wait_for_trigger_node.interrupt", _fake_interrupt)

    result = asyncio.run(wait_for_trigger_node(state, {"configurable": {"thread_id": "t"}}))

    assert result["route_decision"] == "end"
    assert "failed authentication" in result["messages"][0].content.lower(), (
        "a validly signed token for a different action set must still be rejected"
    )


def test_wait_for_trigger_accepts_only_valid_signed_resume_token(monkeypatch):
    state = _make_state(intents=[_conditional_intent()])
    valid_token = _build_trigger_resume_token(
        thread_id="t",
        user_id="user-1",
        trigger_condition_dict=_stored_condition_dict(),
        payload_intents=_resolved_intents(),
    )
    registry = AsyncMock()
    registry.get_triggers_for_thread.return_value = []
    registry.register_trigger.return_value = "t-999"
    registry.get_trigger.return_value = _trigger_doc(token=valid_token)

    def _fake_interrupt(_value):
        return {
            "condition_met": True,
            "trigger_id": "t-999",
            "matched_price": 900.0,
            "asset": "ETH",
            "trigger_type": "price_below",
            "resume_token": valid_token,
            "trigger_fire_id": "fire-1",
        }

    monkeypatch.setattr(
        "graph.nodes.wait_for_trigger_node.TriggerRegistry",
        lambda: registry,
    )
    monkeypatch.setattr("graph.nodes.wait_for_trigger_node.interrupt", _fake_interrupt)

    result = asyncio.run(wait_for_trigger_node(state, {"configurable": {"thread_id": "t"}}))

    assert result["route_decision"] == "resolve"
    assert result["is_triggered_execution"] is True
    assert result["trigger_id"] == "t-999"
    assert result["execution_id"] == "trigger:t-999:fire-1"
    assert result["intents"] == _resolved_intents()
    assert isinstance(result["messages"][0], AIMessage)


def test_wait_for_trigger_invalid_condition(monkeypatch):
    state = _make_state(
        intents=[
            {
                "intent_type": "swap",
                "condition": {"type": "not_a_real_type"},
            }
        ]
    )

    registry = AsyncMock()
    monkeypatch.setattr(
        "graph.nodes.wait_for_trigger_node.TriggerRegistry",
        lambda: registry,
    )

    result = asyncio.run(wait_for_trigger_node(state, {"configurable": {"thread_id": "t"}}))

    assert result["route_decision"] == "end"
    assert "malformed" in result["messages"][0].content
