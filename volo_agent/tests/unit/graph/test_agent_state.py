from langchain_core.messages import AIMessage, HumanMessage

from graph.agent_state import add_messages_bounded, _max_message_history
from graph import replay_guard


def _reset_replay_guard_counters() -> None:
    with replay_guard._LOCK:
        for key in list(replay_guard._COUNTERS.keys()):
            replay_guard._COUNTERS[key] = 0


def _snapshot_replay_guard_counters() -> dict[str, int]:
    with replay_guard._LOCK:
        return {key: int(value) for key, value in replay_guard._COUNTERS.items()}


def test_add_messages_bounded_trims_to_env_limit(monkeypatch):
    monkeypatch.setenv("VOLO_MAX_MESSAGE_HISTORY", "3")
    _max_message_history.cache_clear()
    left = [
        HumanMessage(content="u1", id="u1"),
        AIMessage(content="a1", id="a1"),
    ]
    right = [
        HumanMessage(content="u2", id="u2"),
        AIMessage(content="a2", id="a2"),
    ]

    merged = add_messages_bounded(left, right)

    assert len(merged) == 3
    assert [m.id for m in merged] == ["a1", "u2", "a2"]


def test_add_messages_bounded_can_be_disabled(monkeypatch):
    monkeypatch.setenv("VOLO_MAX_MESSAGE_HISTORY", "0")
    _max_message_history.cache_clear()
    left = [
        HumanMessage(content="u1", id="u1"),
        AIMessage(content="a1", id="a1"),
    ]
    right = [HumanMessage(content="u2", id="u2")]

    merged = add_messages_bounded(left, right)

    assert len(merged) == 3
    assert [m.id for m in merged] == ["u1", "a1", "u2"]


def test_add_messages_bounded_preserves_replacement_semantics(monkeypatch):
    monkeypatch.setenv("VOLO_MAX_MESSAGE_HISTORY", "5")
    _max_message_history.cache_clear()
    left = [HumanMessage(content="old", id="same")]
    right = [HumanMessage(content="new", id="same")]

    merged = add_messages_bounded(left, right)

    assert len(merged) == 1
    assert merged[0].content == "new"


def test_add_messages_bounded_records_trim_metrics(monkeypatch):
    _reset_replay_guard_counters()
    monkeypatch.setenv("VOLO_MAX_MESSAGE_HISTORY", "2")
    _max_message_history.cache_clear()
    left = [
        HumanMessage(content="u1", id="u1"),
        AIMessage(content="a1", id="a1"),
    ]
    right = [
        HumanMessage(content="u2", id="u2"),
        AIMessage(content="a2", id="a2"),
    ]

    merged = add_messages_bounded(left, right)

    assert len(merged) == 2
    counters = _snapshot_replay_guard_counters()
    assert counters["history_trim_events_total"] == 1
    assert counters["history_messages_dropped_total"] == 2
