import asyncio

from graph.nodes.wait_for_funds_node import wait_for_funds_node


def _make_state(waiting_for_funds):
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
        "messages": [],
        "fee_quotes": [],
        "trigger_id": None,
        "is_triggered_execution": None,
        "waiting_for_funds": waiting_for_funds,
    }


def test_wait_for_funds_node_interrupt_and_resume(monkeypatch):
    captured = {}

    def _fake_interrupt(value):
        captured["value"] = value
        return {"wait_id": value["wait_id"], "resume_token": value["resume_token"]}

    monkeypatch.setattr("graph.nodes.wait_for_funds_node.interrupt", _fake_interrupt)

    result = asyncio.run(
        wait_for_funds_node(
            _make_state(
                {
                    "wait_id": "wait-1",
                    "resume_token": "a" * 32,
                    "node_id": "step_0",
                    "message": "Queued behind Task 7",
                }
            ),
            {"configurable": {"thread_id": "thread-1"}},
        )
    )

    assert captured["value"]["status"] == "waiting_funds"
    assert captured["value"]["thread_id"] == "thread-1"
    assert result["route_decision"] == "resume"
    assert result["waiting_for_funds"] is None
    assert result["auto_resume_execution"] is True


def test_wait_for_funds_node_fails_cleanly_without_wait_id():
    result = asyncio.run(
        wait_for_funds_node(
            _make_state({"node_id": "step_0", "resume_token": "a" * 32}),
            {"configurable": {"thread_id": "thread-1"}},
        )
    )

    assert result["route_decision"] == "end"
    assert "missing" in result["messages"][0].content.lower()


def test_wait_for_funds_node_rejects_invalid_resume_token(monkeypatch):
    def _fake_interrupt(value):
        return {"wait_id": value["wait_id"], "resume_token": "wrong-token"}

    monkeypatch.setattr("graph.nodes.wait_for_funds_node.interrupt", _fake_interrupt)

    result = asyncio.run(
        wait_for_funds_node(
            _make_state(
                {
                    "wait_id": "wait-1",
                    "resume_token": "a" * 32,
                    "node_id": "step_0",
                }
            ),
            {"configurable": {"thread_id": "thread-1"}},
        )
    )

    assert result["route_decision"] == "end"
    assert "unexpected token" in result["messages"][0].content.lower()
