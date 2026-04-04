import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from graph.nodes.router_node import (
    _route_conversation_non_blocking,
    conversational_router_node,
)
from graph import replay_guard
from core.history.task_history import TaskHistoryRegistry
from core.planning.execution_plan import (
    ExecutionPlan,
    ExecutionState,
    NodeState,
    PlanNode,
    StepStatus,
)


def _reset_replay_guard_counters() -> None:
    with replay_guard._LOCK:
        for key in list(replay_guard._COUNTERS.keys()):
            replay_guard._COUNTERS[key] = 0


def _snapshot_replay_guard_counters() -> dict[str, int]:
    with replay_guard._LOCK:
        return {key: int(value) for key, value in replay_guard._COUNTERS.items()}


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
        "selected_task_number": None,
    }
    state.update(overrides)
    return state


@pytest.fixture(autouse=True)
def _stub_task_registry():
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = []
    with patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        yield


def test_route_conversation_non_blocking_awaits_async_router():
    router_mock = AsyncMock(return_value={"category": "CONVERSATION", "response": "hi"})
    with patch("graph.nodes.router_node.route_conversation", router_mock):
        result = asyncio.run(
            _route_conversation_non_blocking([HumanMessage(content="hello")])
        )

    assert result == {"category": "CONVERSATION", "response": "hi"}
    router_mock.assert_awaited_once()


def test_router_persists_client_message_id_and_nonce_for_execution_dedup():
    state = _make_state(
        messages=[
            HumanMessage(
                content="swap 10 usdc to eth",
                additional_kwargs={
                    "message_id": "msg-123",
                    "nonce": "nonce-123",
                },
            )
        ]
    )

    with patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "ACTION", "response": None},
    ):
        result = asyncio.run(conversational_router_node(state))

    assert result["context"]["client_message_id"] == "msg-123"
    assert result["context"]["client_nonce"] == "nonce-123"


def test_status_with_no_pending_tasks_returns_empty_message():
    registry = AsyncMock()
    history_registry = AsyncMock()
    history_registry.list_recent.return_value = []
    registry.get_triggers_for_user.return_value = []

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "STATUS", "response": None},
    ):
        state = _make_state(messages=[HumanMessage(content="status")])
        result = asyncio.run(conversational_router_node(state))

    assert "pending tasks" in result["messages"][0].content.lower()


def test_status_includes_pending_orders_and_txs():
    registry = AsyncMock()
    history_registry = AsyncMock()
    history_registry.list_recent.return_value = []
    registry.get_triggers_for_user.side_effect = [
        [
            {
                "trigger_id": "t-12345678",
                "trigger_condition": {
                    "type": "price_below",
                    "asset": "ETH",
                    "target": 1000,
                },
                "status": "pending",
            }
        ],
        [],
    ]

    pending_transactions = [
        {
            "type": "bridge",
            "status": "PENDING",
            "protocol": "across",
            "source_chain": "ethereum",
            "dest_chain": "base",
            "tx_hash": "0xabc",
        },
        {
            "type": "transfer",
            "status": "running",
            "network": "base",
        },
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "STATUS", "response": None},
    ):
        state = _make_state(
            messages=[HumanMessage(content="status")],
            pending_transactions=pending_transactions,
        )
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content
    assert "pending orders" in content.lower()
    assert "pending transactions" in content.lower()
    assert "transfer on base" in content.lower()
    assert "0x" not in content.lower()


def test_cancel_flow_sets_pending_cancel_and_confirms():
    registry = AsyncMock()
    history_registry = AsyncMock()
    history_registry.list_recent.return_value = []
    registry.get_triggers_for_user.return_value = [
        {
            "trigger_id": "t-abcdef123456",
            "trigger_condition": {"type": "price_below", "asset": "ETH", "target": 1000},
            "status": "pending",
        }
    ]
    registry.cancel_trigger.return_value = True

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ):
        state = _make_state(messages=[HumanMessage(content="cancel t-abcdef")])
        first = asyncio.run(conversational_router_node(state))

        assert first["pending_cancel"]["trigger_id"] == "t-abcdef123456"
        assert "confirm cancellation" in first["messages"][0].content.lower()

        state_next = _make_state(
            messages=[HumanMessage(content="confirm")],
            pending_cancel=first["pending_cancel"],
        )
        second = asyncio.run(conversational_router_node(state_next))

    assert second["pending_cancel"] is None
    assert "cancelled" in second["messages"][0].content.lower()


def test_status_shows_expired_orders():
    registry = AsyncMock()
    history_registry = AsyncMock()
    history_registry.list_recent.return_value = []
    registry.get_triggers_for_user.side_effect = [
        [],
        [
            {
                "trigger_id": "t-expired1234",
                "trigger_condition": {
                    "type": "price_below",
                    "asset": "ETH",
                    "target": 1000,
                },
                "status": "expired",
            }
        ],
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "STATUS", "response": None},
    ):
        state = _make_state(messages=[HumanMessage(content="status")])
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "expired orders" in content
    assert "create a new order" in content


def test_show_my_tasks_uses_conversation_task_numbers():
    registry = AsyncMock()
    history_registry = AsyncMock()
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 1,
            "title": "Bridge 100 USDC to Base",
            "status": "WAITING_EXTERNAL",
        },
        {
            "task_number": 2,
            "title": "Swap STT to NIA",
            "status": "FAILED",
        },
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        state = _make_state(messages=[HumanMessage(content="show my tasks")])
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "your recent tasks" in content
    assert "task 1" in content
    assert "task 2" in content
    assert "bridge 100 usdc to base" in content
    assert "in progress" in content
    assert "failed" in content


def test_show_task_number_returns_task_detail():
    registry = AsyncMock()
    history_registry = AsyncMock()
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 2,
            "title": "Swap STT to NIA",
            "status": "WAITING_CONFIRMATION",
            "latest_summary": "Waiting for your confirmation.",
        }
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        state = _make_state(messages=[HumanMessage(content="show task 2")])
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "task 2: swap stt to nia" in content
    assert "needs confirmation" in content
    assert "waiting for your confirmation" in content


def test_use_task_number_selects_task_for_detail_flows():
    registry = AsyncMock()
    history_registry = AsyncMock()
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 2,
            "title": "Swap STT to NIA",
            "status": "WAITING_CONFIRMATION",
            "latest_summary": "Waiting for your confirmation.",
        }
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        state = _make_state(messages=[HumanMessage(content="use task 2")])
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert result["selected_task_number"] == 2
    assert "task 2: swap stt to nia is selected" in content
    assert "execution still follows the current live task" in content


def test_generic_task_status_uses_selected_task():
    registry = AsyncMock()
    history_registry = AsyncMock()
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 2,
            "title": "Swap STT to NIA",
            "status": "WAITING_CONFIRMATION",
            "latest_summary": "Waiting for your confirmation.",
        }
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        state = _make_state(
            messages=[HumanMessage(content="task status")],
            selected_task_number=2,
        )
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "task 2: swap stt to nia" in content
    assert "needs confirmation" in content


def test_generic_task_status_without_selected_task_guides_user():
    registry = AsyncMock()
    history_registry = AsyncMock()

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ):
        state = _make_state(messages=[HumanMessage(content="task status")])
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "no task is selected right now" in content
    assert "use task <number>" in content


def test_clear_task_selection_clears_selected_task_number():
    registry = AsyncMock()
    history_registry = AsyncMock()

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ):
        state = _make_state(
            messages=[HumanMessage(content="clear selection")],
            selected_task_number=2,
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["selected_task_number"] is None
    content = result["messages"][0].content.lower()
    assert "task selection cleared" in content


def test_generic_task_status_with_stale_selected_task_clears_local_selection():
    registry = AsyncMock()
    history_registry = AsyncMock()
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = []

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        state = _make_state(
            messages=[HumanMessage(content="task status")],
            selected_task_number=2,
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["selected_task_number"] is None
    content = result["messages"][0].content.lower()
    assert "no task is selected right now" in content


def test_cancel_updates_task_status():
    registry = AsyncMock()
    history_registry = AsyncMock()

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "core.conversation.router_handlers.upsert_task_from_state",
        new=AsyncMock(),
    ) as upsert_task:
        plan = ExecutionPlan(
            goal="Swap STT to NIA",
            nodes={
                "step_0": PlanNode(
                    id="step_0",
                    tool="swap",
                    args={},
                    depends_on=[],
                    approval_required=False,
                )
            },
        )
        state = _make_state(
            messages=[HumanMessage(content="cancel")],
            plan_history=[plan],
            execution_state=ExecutionState(
                node_states={"step_0": NodeState(node_id="step_0")}
            ),
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "CANCELLED"
    upsert_task.assert_awaited_once()
    assert upsert_task.await_args.kwargs["status"] == "CANCELLED"


def test_cancel_task_number_cancels_current_task():
    registry = AsyncMock()
    history_registry = AsyncMock()
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 2,
            "title": "Swap STT to NIA",
            "status": "RUNNING",
            "thread_id": "thread-2",
            "execution_id": "exec-2",
        }
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ), patch(
        "core.conversation.router_handlers.upsert_task_from_state",
        new=AsyncMock(),
    ) as upsert_task:
        plan = ExecutionPlan(
            goal="Swap STT to NIA",
            nodes={
                "step_0": PlanNode(
                    id="step_0",
                    tool="swap",
                    args={},
                    depends_on=[],
                    approval_required=False,
                )
            },
        )
        state = _make_state(
            messages=[HumanMessage(content="cancel task 2")],
            context={"thread_id": "thread-2"},
            execution_id="exec-2",
            plan_history=[plan],
            execution_state=ExecutionState(
                node_states={"step_0": NodeState(node_id="step_0")}
            ),
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "CANCELLED"
    assert result["selected_task_number"] == 2
    assert "Cancelled this run locally." in result["messages"][0].content
    upsert_task.assert_awaited_once()


def test_retry_failed_step_resets_execution_state():
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.FAILED)}
    )

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "STATUS", "response": None},
    ):
        state = _make_state(
            messages=[HumanMessage(content="retry")],
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    merged = exec_state.merge(result["execution_state"])
    assert merged.node_states["step_0"].status == StepStatus.PENDING
    assert result["route_decision"] == "CONFIRMED"


def test_retry_with_no_failures_returns_message():
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS)}
    )

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "STATUS", "response": None},
    ):
        state = _make_state(
            messages=[HumanMessage(content="retry")],
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    assert "nothing to retry" in result["messages"][0].content.lower()


def test_retry_with_multiple_failed_tasks_guides_user_to_task_list():
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {"task_number": 1, "title": "Bridge 100 USDC to Base", "status": "FAILED"},
        {"task_number": 2, "title": "Swap STT to NIA", "status": "FAILED"},
    ]

    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS)}
    )

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        state = _make_state(
            messages=[HumanMessage(content="retry")],
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "more than one failed task" in content
    assert "show my tasks" in content
    assert "task 1: bridge 100 usdc to base" in content


def test_retry_task_number_outside_current_task_returns_clear_message():
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 1,
            "title": "Bridge 100 USDC to Base",
            "status": "FAILED",
            "thread_id": "thread-older",
            "execution_id": "exec-older",
        },
        {
            "task_number": 2,
            "title": "Swap STT to NIA",
            "status": "FAILED",
            "thread_id": "thread-1",
            "execution_id": "exec-1",
        },
    ]
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.FAILED)}
    )

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        state = _make_state(
            messages=[HumanMessage(content="retry task 1")],
            context={"thread_id": "thread-1"},
            execution_id="exec-1",
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "task 1: bridge 100 usdc to base" in content
    assert "not the current live task" in content
    assert "show task 1" in content


def test_retry_with_no_failures_and_pending_solana_returns_wallet_message():
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS)}
    )

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "STATUS", "response": None},
    ):
        state = _make_state(
            messages=[HumanMessage(content="retry")],
            execution_state=exec_state,
            user_info={
                "volo_user_id": "user-1",
                "sub_org_id": "sub-1",
                "sender_address": "0xabc",
                "solana_address": None,
                "metadata": {"solana_provision_last_error": "TimeoutError"},
            },
        )
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "solana wallet setup is still pending" in content
    assert "reply 'retry'" in content


def test_solana_address_request_returns_solana_wallet_address():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ):
        state = _make_state(
            messages=[HumanMessage(content="solana address")],
            user_info={
                "volo_user_id": "user-1",
                "sender_address": "0x2cfd095Ae9887f85d061c685C806B5ccbB38625F",
                "solana_address": "6f6dR2jt6Wn6kR5VduN2jVAhUrS2RABxg7MihhFYf9hE",
            },
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "STATUS"
    content = result["messages"][0].content
    assert "solana address" in content.lower()
    assert "6f6dr2jt6wn6kr5vdun2jvahurs2rabxg7mihhfyf9he" in content.lower()


def test_address_on_solana_request_routes_to_wallet_status():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ):
        state = _make_state(
            messages=[HumanMessage(content="my address on solana")],
            user_info={
                "volo_user_id": "user-1",
                "sender_address": "0x2cfd095Ae9887f85d061c685C806B5ccbB38625F",
                "solana_address": "6f6dR2jt6Wn6kR5VduN2jVAhUrS2RABxg7MihhFYf9hE",
            },
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "STATUS"
    assert "solana address" in result["messages"][0].content.lower()


def test_evm_address_request_returns_evm_wallet_address():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ):
        state = _make_state(
            messages=[HumanMessage(content="evm address")],
            user_info={
                "volo_user_id": "user-1",
                "sender_address": "0x2cfd095Ae9887f85d061c685C806B5ccbB38625F",
                "solana_address": "6f6dR2jt6Wn6kR5VduN2jVAhUrS2RABxg7MihhFYf9hE",
            },
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "STATUS"
    content = result["messages"][0].content.lower()
    assert "evm address" in content
    assert "0x2cfd095ae9887f85d061c685c806b5ccbb38625f" in content


def test_solana_address_typo_routes_to_wallet_status():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ):
        state = _make_state(
            messages=[HumanMessage(content="show my sol addess")],
            user_info={
                "volo_user_id": "user-1",
                "sender_address": "0x2cfd095Ae9887f85d061c685C806B5ccbB38625F",
                "solana_address": "6f6dR2jt6Wn6kR5VduN2jVAhUrS2RABxg7MihhFYf9hE",
            },
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "STATUS"
    assert "solana address" in result["messages"][0].content.lower()


def test_conversation_route_uses_conversation_responder():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "CONVERSATION", "response": None},
    ), patch(
        "graph.nodes.router_node.respond_conversation",
        AsyncMock(return_value="Hey there. I'm doing well."),
    ):
        state = _make_state(messages=[HumanMessage(content="hi")])
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "CONVERSATION"
    assert result["messages"][0].content == "Hey there. I'm doing well."


def test_conversation_route_uses_router_reply_without_conversation_model():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "CONVERSATION", "response": "Hey. How can I help?"},
    ), patch(
        "graph.nodes.router_node.respond_conversation",
        AsyncMock(side_effect=AssertionError("conversation model should not run")),
    ):
        state = _make_state(messages=[HumanMessage(content="hi")])
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "CONVERSATION"
    assert result["messages"][0].content == "Hey. How can I help?"


def test_route_conversation_error_falls_back_to_conversation_not_action():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.route_conversation",
        side_effect=RuntimeError("router blew up"),
    ), patch(
        "graph.nodes.router_node.respond_conversation",
        AsyncMock(return_value="I'm Volo."),
    ):
        state = _make_state(messages=[HumanMessage(content="what is your name?")])
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "CONVERSATION"
    assert result["messages"][0].content == "I'm Volo."


def test_pending_intent_follow_up_routes_to_action_recent_turn():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.route_conversation",
        AsyncMock(side_effect=AssertionError("router should not run")),
    ):
        state = _make_state(
            messages=[
                AIMessage(content="Which token would you like to receive?"),
                HumanMessage(content="same token as before"),
            ],
            pending_intent={
                "intent_type": "swap",
                "slots": {"token_in": {"symbol": "STT"}, "amount": 0.2, "chain": "base"},
                "missing_slots": ["token_out"],
                "status": "incomplete",
                "raw_input": "swap 0.2 stt on base",
            },
            pending_clarification={
                "slot_name": "token_out",
                "question_type": "missing_slot",
                "attempt_count": 0,
                "last_resolution_error": None,
            },
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "ACTION"
    assert result["parse_scope"] == "recent_turn"


def test_cancel_clears_pending_intent_follow_up_state():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ):
        state = _make_state(
            messages=[HumanMessage(content="cancel")],
            pending_intent={
                "intent_type": "transfer",
                "slots": {"token": {"symbol": "USDC"}, "amount": 5},
                "missing_slots": ["recipient", "chain"],
                "status": "incomplete",
                "raw_input": "send 5 usdc",
            },
            pending_clarification={
                "slot_name": "recipient",
                "question_type": "missing_slot",
                "attempt_count": 1,
                "last_resolution_error": "uncertain",
            },
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "STATUS"
    assert result["pending_intent"] is None
    assert result["pending_clarification"] is None
    assert "cancelled" in result["messages"][0].content.lower()


def test_running_execution_message_names_current_task():
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 3,
            "title": "Bridge 100 USDC to Base",
            "status": "WAITING_EXTERNAL",
            "thread_id": "thread-1",
            "execution_id": "exec-1",
            "latest_summary": "Waiting for the network.",
        }
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        state = _make_state(
            messages=[HumanMessage(content="what's happening")],
            context={"thread_id": "thread-1"},
            execution_id="exec-1",
            execution_state=ExecutionState(
                node_states={
                    "step_0": NodeState(node_id="step_0", status=StepStatus.RUNNING)
                }
            ),
        )
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "task 3: bridge 100 usdc to base" in content
    assert "still in progress" in content
    assert "latest update: waiting for the network." in content


def test_confirm_task_number_waiting_confirmation_confirms_referenced_task():
    registry = AsyncMock()
    history_registry = AsyncMock()
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 2,
            "title": "Swap STT to NIA",
            "status": "WAITING_CONFIRMATION",
            "thread_id": "thread-2",
            "execution_id": "exec-2",
        }
    ]
    plan = ExecutionPlan(
        goal="Swap STT to NIA",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={},
                depends_on=[],
                approval_required=True,
            )
        },
    )

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        state = _make_state(
            messages=[HumanMessage(content="confirm task 2")],
            context={"thread_id": "thread-2"},
            execution_id="exec-2",
            confirmation_status="WAITING",
            plan_history=[plan],
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "CONFIRMED"
    assert all(node.approval_required is False for node in result["plan_history"][-1].nodes.values())


def test_cancel_task_number_with_different_selected_task_cancels_referenced_task():
    registry = AsyncMock()
    history_registry = AsyncMock()
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 2,
            "title": "Swap STT to NIA",
            "status": "RUNNING",
            "thread_id": "thread-2",
            "execution_id": "exec-2",
        },
        {
            "task_number": 3,
            "title": "Bridge 100 USDC to Base",
            "status": "WAITING_EXTERNAL",
            "thread_id": "thread-3",
            "execution_id": "exec-3",
        },
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ), patch(
        "core.conversation.router_handlers.upsert_task_from_state",
        new=AsyncMock(),
    ) as upsert_task:
        plan = ExecutionPlan(
            goal="Swap STT to NIA",
            nodes={
                "step_0": PlanNode(
                    id="step_0",
                    tool="swap",
                    args={},
                    depends_on=[],
                    approval_required=False,
                )
            },
        )
        state = _make_state(
            messages=[HumanMessage(content="cancel task 2")],
            context={"thread_id": "thread-2"},
            execution_id="exec-2",
            selected_task_number=3,
            plan_history=[plan],
            execution_state=ExecutionState(
                node_states={"step_0": NodeState(node_id="step_0")}
            ),
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "CANCELLED"
    assert result["selected_task_number"] == 2
    assert "Cancelled this run locally." in result["messages"][0].content
    upsert_task.assert_awaited_once()


def test_confirm_task_number_does_not_confirm_different_live_task_when_referenced_task_is_terminal():
    registry = AsyncMock()
    history_registry = AsyncMock()
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 2,
            "title": "Swap STT to NIA",
            "status": "COMPLETED",
            "thread_id": "thread-2",
            "execution_id": "exec-2",
        },
        {
            "task_number": 3,
            "title": "Bridge 100 USDC to Base",
            "status": "WAITING_CONFIRMATION",
            "thread_id": "thread-3",
            "execution_id": "exec-3",
        },
    ]
    plan = ExecutionPlan(
        goal="Bridge 100 USDC to Base",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="bridge",
                args={},
                depends_on=[],
                approval_required=True,
            )
        },
    )

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ):
        state = _make_state(
            messages=[HumanMessage(content="confirm task 2")],
            context={"thread_id": "thread-3"},
            execution_id="exec-3",
            confirmation_status="WAITING",
            selected_task_number=3,
            plan_history=[plan],
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "STATUS"
    assert result["selected_task_number"] == 2
    assert "no longer waiting for confirmation" in result["messages"][0].content.lower()


def test_cancel_task_number_does_not_cancel_different_live_task_when_referenced_task_is_terminal():
    registry = AsyncMock()
    history_registry = AsyncMock()
    task_registry = AsyncMock()
    task_registry.list_recent.return_value = [
        {
            "task_number": 2,
            "title": "Swap STT to NIA",
            "status": "COMPLETED",
            "thread_id": "thread-2",
            "execution_id": "exec-2",
        },
        {
            "task_number": 3,
            "title": "Bridge 100 USDC to Base",
            "status": "WAITING_EXTERNAL",
            "thread_id": "thread-3",
            "execution_id": "exec-3",
        },
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.ConversationTaskRegistry", return_value=task_registry
    ), patch(
        "core.conversation.router_handlers.upsert_task_from_state",
        new=AsyncMock(),
    ) as upsert_task:
        plan = ExecutionPlan(
            goal="Bridge 100 USDC to Base",
            nodes={
                "step_0": PlanNode(
                    id="step_0",
                    tool="bridge",
                    args={},
                    depends_on=[],
                    approval_required=False,
                )
            },
        )
        state = _make_state(
            messages=[HumanMessage(content="cancel task 2")],
            context={"thread_id": "thread-3"},
            execution_id="exec-3",
            selected_task_number=3,
            plan_history=[plan],
            execution_state=ExecutionState(
                node_states={"step_0": NodeState(node_id="step_0")}
            ),
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "STATUS"
    assert result["selected_task_number"] == 2
    assert "is not running right now" in result["messages"][0].content.lower()
    upsert_task.assert_not_awaited()


def test_sup_routes_to_conversation_response():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ):
        state = _make_state(messages=[HumanMessage(content="sup")])
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "CONVERSATION"
    assert result["messages"][0].content == "Hey. How can I help?"


def test_conversation_responder_failure_returns_clear_recovery_message():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "CONVERSATION", "response": None},
    ), patch(
        "graph.nodes.router_node.respond_conversation",
        AsyncMock(side_effect=RuntimeError("chat failed")),
    ):
        state = _make_state(messages=[HumanMessage(content="what is your name?")])
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert result["route_decision"] == "CONVERSATION"
    assert "try again" in content
    assert "swap" not in content
    assert "reply" in content


def test_plain_cancel_keeps_recovery_path_explicit():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ):
        state = _make_state(messages=[HumanMessage(content="cancel")])
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "cancelled" in content
    assert "try again" in content


def test_cancel_running_execution_marks_step_skipped():
    plan = ExecutionPlan(
        goal="Swap STT for NIA",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={"chain": "somnia", "amount_in": "0.2"},
            )
        },
    )
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.RUNNING)}
    )

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ):
        state = _make_state(
            messages=[HumanMessage(content="cancel")],
            plan_history=[plan],
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "CANCELLED"
    merged = exec_state.merge(result["execution_state"])
    assert merged.node_states["step_0"].status == StepStatus.SKIPPED
    content = result["messages"][0].content.lower()
    assert "cancelled this run locally" in content
    assert "may still settle on-chain" in content


def test_link_status_marks_primary_identity():
    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ):
        state = _make_state(
            messages=[HumanMessage(content="linked accounts")],
            user_info={
                "volo_user_id": "user-1",
                "identities": [
                    {
                        "provider": "discord",
                        "provider_user_id": "user-1",
                        "username": "alice",
                        "is_primary": True,
                    },
                    {
                        "provider": "telegram",
                        "provider_user_id": "user-2",
                        "username": "alice_tg",
                    },
                ],
            },
        )
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "linked accounts:" in content
    assert "primary" in content
    assert "to unlink one" in content


def test_unlink_account_returns_remaining_accounts_message():
    service = AsyncMock()
    service.unlink_identity.return_value = {
        "identities": [
            {"provider": "discord", "provider_user_id": "user-1"},
            {"provider": "twitter", "provider_user_id": "user-3"},
        ]
    }
    with patch("graph.nodes.router_node.AsyncIdentityService", return_value=service):
        state = _make_state(
            messages=[HumanMessage(content="unlink telegram")],
            user_info={
                "volo_user_id": "user-1",
                "identities": [
                    {"provider": "discord", "provider_user_id": "user-1", "is_primary": True},
                    {"provider": "telegram", "provider_user_id": "user-2", "username": "alice_tg"},
                    {"provider": "twitter", "provider_user_id": "user-3"},
                ],
            },
        )
        result = asyncio.run(conversational_router_node(state))

    service.unlink_identity.assert_awaited_once_with("telegram", "user-2")
    assert "2 linked accounts remain" in result["messages"][0].content.lower()


def test_unlink_account_blocks_last_provider_before_service_call():
    service = AsyncMock()
    with patch("graph.nodes.router_node.AsyncIdentityService", return_value=service):
        state = _make_state(
            messages=[HumanMessage(content="unlink account")],
            user_info={
                "volo_user_id": "user-1",
                "identities": [
                    {"provider": "discord", "provider_user_id": "user-1", "is_primary": True}
                ],
            },
        )
        result = asyncio.run(conversational_router_node(state))

    service.unlink_identity.assert_not_called()
    assert "link another platform first" in result["messages"][0].content.lower()


def test_unlink_account_requires_explicit_target_when_multiple_accounts():
    service = AsyncMock()
    with patch("graph.nodes.router_node.AsyncIdentityService", return_value=service):
        state = _make_state(
            messages=[HumanMessage(content="unlink account")],
            user_info={
                "volo_user_id": "user-1",
                "identities": [
                    {"provider": "discord", "provider_user_id": "user-1", "is_primary": True},
                    {"provider": "telegram", "provider_user_id": "user-2", "username": "alice_tg"},
                ],
            },
        )
        result = asyncio.run(conversational_router_node(state))

    service.unlink_identity.assert_not_called()
    content = result["messages"][0].content.lower()
    assert "multiple linked accounts" in content
    assert "unlink @alice_tg" in content or "unlink telegram" in content


def test_unlink_account_unknown_target_returns_help():
    service = AsyncMock()
    with patch("graph.nodes.router_node.AsyncIdentityService", return_value=service):
        state = _make_state(
            messages=[HumanMessage(content="unlink farcaster")],
            user_info={
                "volo_user_id": "user-1",
                "identities": [
                    {"provider": "discord", "provider_user_id": "user-1", "is_primary": True},
                    {"provider": "telegram", "provider_user_id": "user-2", "username": "alice_tg"},
                ],
            },
        )
        result = asyncio.run(conversational_router_node(state))

    service.unlink_identity.assert_not_called()
    content = result["messages"][0].content.lower()
    assert "couldn't match 'farcaster'" in content
    assert "unlink" in content


def test_edit_flow_prompts_for_changes_and_sets_pending_edit():
    node = PlanNode(
        id="step_0",
        tool="swap",
        args={
            "amount_in": 1,
            "token_in_symbol": "ETH",
            "token_out_symbol": "USDC",
            "chain": "base",
        },
        depends_on=[],
        approval_required=False,
    )
    plan = ExecutionPlan(goal="swap", nodes={"step_0": node})
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.FAILED)}
    )

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "STATUS", "response": None},
    ):
        state = _make_state(
            messages=[HumanMessage(content="edit")],
            plan_history=[plan],
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["pending_edit"]["node_id"] == "step_0"
    assert "current failed step" in result["messages"][0].content.lower()


def test_edit_followup_routes_to_parse():
    pending_edit = {
        "node_id": "step_0",
        "tool": "swap",
        "args": {},
        "summary": "Swap 1 ETH to USDC on Base",
    }

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=AsyncMock()
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "STATUS", "response": None},
    ):
        state = _make_state(
            messages=[HumanMessage(content="swap 2 eth to usdc on base")],
            pending_edit=pending_edit,
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "ACTION"
    assert result["pending_edit"] is None


def test_status_includes_recent_history():
    registry = AsyncMock()
    registry.get_triggers_for_user.side_effect = [[], []]
    history_registry = AsyncMock()
    history_registry.list_recent.return_value = [
        {"summary": "Swap 1 ETH to USDC on base", "status": "SUCCESS"},
        {"summary": "Bridge 0.1 ETH from ethereum to base", "status": "FAILED"},
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ), patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "STATUS", "response": None},
    ):
        state = _make_state(messages=[HumanMessage(content="status")])
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "recent tasks" in content
    assert "swap 1 eth to usdc on base" in content


def test_history_command_shows_success_only():
    registry = AsyncMock()
    history_registry = AsyncMock()
    history_registry.list_recent.return_value = [
        {"summary": "Swap 1 ETH to USDC on base", "status": "SUCCESS"},
    ]

    with patch(
        "graph.nodes.router_node.TriggerRegistry", return_value=registry
    ), patch(
        "graph.nodes.router_node.TaskHistoryRegistry", return_value=history_registry
    ):
        state = _make_state(messages=[HumanMessage(content="history")])
        result = asyncio.run(conversational_router_node(state))

    content = result["messages"][0].content.lower()
    assert "recent successful tasks" in content
    assert "swap 1 eth to usdc on base" in content


def test_post_completion_guard_uses_last_user_for_explicit_action():
    node = PlanNode(
        id="step_0",
        tool="swap",
        args={"amount_in": 1, "token_in_symbol": "ETH", "token_out_symbol": "USDC"},
        depends_on=[],
        approval_required=False,
    )
    plan = ExecutionPlan(goal="swap", nodes={"step_0": node})
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS)},
        completed=False,
    )

    with patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "ACTION", "response": None},
    ):
        state = _make_state(
            messages=[HumanMessage(content="swap 2 eth for usdc on base")],
            plan_history=[plan],
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "ACTION"
    assert result["parse_scope"] == "last_user"


def test_post_completion_guard_uses_recent_turn_for_short_slot_reply():
    node = PlanNode(
        id="step_0",
        tool="swap",
        args={"amount_in": 1, "token_in_symbol": "ETH", "token_out_symbol": "USDC"},
        depends_on=[],
        approval_required=False,
    )
    plan = ExecutionPlan(goal="swap", nodes={"step_0": node})
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS)},
        completed=False,
    )

    with patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "ACTION", "response": None},
    ):
        state = _make_state(
            messages=[
                AIMessage(content="Which chain should I use?"),
                HumanMessage(content="base"),
            ],
            plan_history=[plan],
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "ACTION"
    assert result["parse_scope"] == "recent_turn"


def test_post_completion_guard_does_not_treat_conversational_sup_as_slot_fill():
    node = PlanNode(
        id="step_0",
        tool="swap",
        args={"amount_in": 1, "token_in_symbol": "ETH", "token_out_symbol": "USDC"},
        depends_on=[],
        approval_required=False,
    )
    plan = ExecutionPlan(goal="swap", nodes={"step_0": node})
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS)},
        completed=False,
    )

    with patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "CONVERSATION", "response": None},
    ), patch(
        "graph.nodes.router_node.respond_conversation",
        AsyncMock(return_value="All good. What's up?"),
    ):
        state = _make_state(
            messages=[
                AIMessage(content="Glad you enjoyed it!"),
                HumanMessage(content="sup"),
            ],
            plan_history=[plan],
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "CONVERSATION"
    assert "parse_scope" not in result
    assert result["messages"][0].content == "All good. What's up?"


def test_balance_phrase_how_much_routes_to_last_user_parse_scope():
    with patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "CONVERSATION", "response": "unused"},
    ):
        state = _make_state(messages=[HumanMessage(content="how much do i have on base")])
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "ACTION"
    assert result["parse_scope"] == "last_user"


def test_balance_typo_routes_to_last_user_parse_scope():
    with patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "CONVERSATION", "response": None},
    ):
        state = _make_state(messages=[HumanMessage(content="base balnce")])
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "ACTION"
    assert result["parse_scope"] == "last_user"


def test_bridge_swap_prompt_does_not_use_balance_fast_path():
    router_mock = AsyncMock(return_value={"category": "ACTION", "response": None})
    with patch("graph.nodes.router_node.route_conversation", router_mock):
        state = _make_state(
            messages=[
                HumanMessage(
                    content="bridge usdc from base sepolia to sepolia and swap it for eth"
                )
            ]
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "ACTION"
    assert "pending_intent" not in result
    router_mock.assert_not_awaited()


def test_post_completion_guard_can_be_disabled(monkeypatch):
    monkeypatch.setenv("VOLO_ENABLE_REPLAY_GUARD", "0")
    node = PlanNode(
        id="step_0",
        tool="swap",
        args={"amount_in": 1, "token_in_symbol": "ETH", "token_out_symbol": "USDC"},
        depends_on=[],
        approval_required=False,
    )
    plan = ExecutionPlan(goal="swap", nodes={"step_0": node})
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS)},
        completed=True,
    )

    with patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "ACTION", "response": None},
    ):
        state = _make_state(
            messages=[HumanMessage(content="swap 2 eth for usdc on base")],
            plan_history=[plan],
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    assert result["route_decision"] == "ACTION"
    assert "parse_scope" not in result


def test_post_completion_guard_records_replay_prevented_counter():
    _reset_replay_guard_counters()
    node = PlanNode(
        id="step_0",
        tool="swap",
        args={"amount_in": 1, "token_in_symbol": "ETH", "token_out_symbol": "USDC"},
        depends_on=[],
        approval_required=False,
    )
    plan = ExecutionPlan(goal="swap", nodes={"step_0": node})
    exec_state = ExecutionState(
        node_states={"step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS)},
        completed=True,
    )

    with patch(
        "graph.nodes.router_node.route_conversation",
        return_value={"category": "ACTION", "response": None},
    ):
        state = _make_state(
            messages=[HumanMessage(content="swap 2 eth for usdc on base")],
            plan_history=[plan],
            execution_state=exec_state,
        )
        result = asyncio.run(conversational_router_node(state))

    counters = _snapshot_replay_guard_counters()
    assert result["parse_scope"] == "last_user"
    assert counters["replay_prevented_total"] == 1
