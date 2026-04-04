from langchain_core.messages import HumanMessage

from graph.runtime_io import build_thread_config, build_turn_input


def test_build_turn_input_includes_human_message_and_identity():
    payload = build_turn_input(
        user_message="swap 1 eth to usdc",
        user_id="u-1",
        provider="discord",
        username="alice",
    )

    assert payload["user_id"] == "u-1"
    assert payload["provider"] == "discord"
    assert payload["username"] == "alice"
    assert len(payload["messages"]) == 1
    assert isinstance(payload["messages"][0], HumanMessage)
    assert payload["messages"][0].content == "swap 1 eth to usdc"


def test_build_turn_input_merges_context_and_thread_id():
    payload = build_turn_input(
        user_message="status",
        user_id="u-1",
        provider="telegram",
        thread_id="t-123",
        context={"channel": "dm"},
    )

    assert payload["context"]["thread_id"] == "t-123"
    assert payload["context"]["channel"] == "dm"


def test_build_turn_input_includes_selected_task_number():
    payload = build_turn_input(
        user_message="task status",
        user_id="u-1",
        provider="telegram",
        selected_task_number=2,
    )

    assert payload["selected_task_number"] == 2


def test_build_thread_config_sets_configurable_fields():
    config = build_thread_config(thread_id="t-123", checkpoint_ns="ns-1")
    assert config == {"configurable": {"thread_id": "t-123", "checkpoint_ns": "ns-1"}}
