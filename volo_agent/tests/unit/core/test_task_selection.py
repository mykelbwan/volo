import asyncio
from unittest.mock import AsyncMock, patch

from core.tasks.selection import ConversationTaskSelectionRegistry
from core.tasks.thread_resolver import (
    persist_selection_for_thread,
    persist_selected_task_number,
    resolve_turn_routing,
    resolve_selected_thread_id,
)


class _SelectionRegistry:
    stored: dict[str, int | None] = {}

    async def set_selected_task_number(
        self,
        *,
        conversation_id: str,
        task_number: int | None,
    ) -> None:
        self.stored[str(conversation_id)] = task_number

    async def get_selected_task_number(self, conversation_id: str) -> int | None:
        return self.stored.get(str(conversation_id))


class _TaskRegistry:
    def __init__(self) -> None:
        self.tasks = {
            ("discord:user-1", 2): {
                "task_number": 2,
                "thread_id": "thread-2",
                "status": "FAILED",
            }
        }

    async def get_task_by_number(
        self,
        conversation_id: str,
        *,
        task_number: int,
    ):
        return self.tasks.get((str(conversation_id), int(task_number)))

    async def get_latest_task_for_thread(
        self,
        conversation_id: str,
        *,
        thread_id: str,
    ):
        for (stored_conversation_id, _task_number), task in self.tasks.items():
            if stored_conversation_id == str(conversation_id) and task.get("thread_id") == str(thread_id):
                return task
        return None

    async def list_active(self, conversation_id: str, *, limit: int = 10):
        return [
            task
            for (stored_conversation_id, _task_number), task in self.tasks.items()
            if stored_conversation_id == str(conversation_id)
            and str(task.get("status") or "").upper()
            in {
                "FAILED",
                "RUNNING",
                "WAITING_EXTERNAL",
                "WAITING_INPUT",
                "WAITING_CONFIRMATION",
                "WAITING_FUNDS",
            }
        ][:limit]

    async def list_recent(self, conversation_id: str, *, limit: int = 10):
        return await self.list_active(conversation_id, limit=limit)


def test_persist_selected_task_number_uses_conversation_key():
    _SelectionRegistry.stored = {}

    conversation_id = asyncio.run(
        persist_selected_task_number(
            provider="discord",
            provider_user_id="user-1",
            selected_task_number=2,
            context=None,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert conversation_id == "discord:user-1"
    assert _SelectionRegistry.stored["discord:user-1"] == 2


def test_clear_selected_task_number_if_matches_uses_precise_filter():
    collection = AsyncMock()
    collection.delete_one.return_value = type("DeleteResult", (), {"deleted_count": 1})()

    with patch(
        "core.tasks.selection.AsyncMongoDB.get_collection",
        return_value=collection,
    ):
        registry = ConversationTaskSelectionRegistry()
        cleared = asyncio.run(
            registry.clear_selected_task_number_if_matches(
                conversation_id="discord:user-1",
                task_number=2,
            )
        )

    assert cleared is True
    assert collection.delete_one.await_args.args[0] == {
        "conversation_id": "discord:user-1",
        "selected_task_number": 2,
    }


def test_resolve_selected_thread_id_uses_selected_task_mapping():
    _SelectionRegistry.stored = {"discord:user-1": 2}

    thread_id, selected_task_number = asyncio.run(
        resolve_selected_thread_id(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="retry",
            context=None,
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert thread_id == "thread-2"
    assert selected_task_number == 2


def test_resolve_selected_thread_id_falls_back_when_task_is_missing():
    _SelectionRegistry.stored = {"discord:user-1": 7}

    thread_id, selected_task_number = asyncio.run(
        resolve_selected_thread_id(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="retry",
            context=None,
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert thread_id == "thread-default"
    assert selected_task_number is None


def test_resolve_selected_thread_id_does_not_hijack_new_explicit_actions():
    _SelectionRegistry.stored = {"discord:user-1": 2}

    thread_id, selected_task_number = asyncio.run(
        resolve_selected_thread_id(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="swap 1 eth to usdc",
            context=None,
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert thread_id == "thread-default"
    assert selected_task_number == 2


def test_resolve_selected_thread_id_does_not_hijack_balance_actions():
    _SelectionRegistry.stored = {"discord:user-1": 2}

    thread_id, selected_task_number = asyncio.run(
        resolve_selected_thread_id(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="sepolia balance",
            context=None,
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert thread_id == "thread-default"
    assert selected_task_number == 2


def test_resolve_selected_thread_id_clears_missing_selection():
    _SelectionRegistry.stored = {"discord:user-1": 7}

    thread_id, selected_task_number = asyncio.run(
        resolve_selected_thread_id(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="retry",
            context=None,
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert thread_id == "thread-default"
    assert selected_task_number is None
    assert _SelectionRegistry.stored["discord:user-1"] is None


def test_resolve_selected_thread_id_downgrades_completed_task_to_inspect_only():
    class _CompletedTaskRegistry(_TaskRegistry):
        def __init__(self) -> None:
            self.tasks = {
                ("discord:user-1", 2): {
                    "task_number": 2,
                    "thread_id": "thread-2",
                    "status": "COMPLETED",
                }
            }

    _SelectionRegistry.stored = {"discord:user-1": 2}

    thread_id, selected_task_number = asyncio.run(
        resolve_selected_thread_id(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="retry",
            context=None,
            task_registry_cls=_CompletedTaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert thread_id == "thread-default"
    assert selected_task_number == 2


def test_resolve_turn_routing_allocates_new_thread_for_new_action_when_active_tasks_exist():
    class _ActiveTaskRegistry(_TaskRegistry):
        def __init__(self) -> None:
            self.tasks = {
                ("discord:user-1", 2): {
                    "task_number": 2,
                    "thread_id": "thread-2",
                    "status": "RUNNING",
                }
            }

    _SelectionRegistry.stored = {}

    resolution = asyncio.run(
        resolve_turn_routing(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="swap 1 eth to usdc",
            context=None,
            task_registry_cls=_ActiveTaskRegistry,
            selection_registry_cls=_SelectionRegistry,
            thread_id_factory=lambda: "thread-new",
        )
    )

    assert resolution.thread_id == "thread-new"
    assert resolution.selected_task_number is None
    assert resolution.allocated_new_thread is True


def test_resolve_turn_routing_allocates_new_thread_for_balance_action_when_active_tasks_exist():
    class _ActiveTaskRegistry(_TaskRegistry):
        def __init__(self) -> None:
            self.tasks = {
                ("discord:user-1", 2): {
                    "task_number": 2,
                    "thread_id": "thread-2",
                    "status": "RUNNING",
                }
            }

    _SelectionRegistry.stored = {}

    resolution = asyncio.run(
        resolve_turn_routing(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="sepolia balance",
            context=None,
            task_registry_cls=_ActiveTaskRegistry,
            selection_registry_cls=_SelectionRegistry,
            thread_id_factory=lambda: "thread-new",
        )
    )

    assert resolution.thread_id == "thread-new"
    assert resolution.selected_task_number is None
    assert resolution.allocated_new_thread is True


def test_persist_selection_for_thread_selects_latest_task_on_that_thread():
    _SelectionRegistry.stored = {}

    selected_task_number = asyncio.run(
        persist_selection_for_thread(
            provider="discord",
            provider_user_id="user-1",
            thread_id="thread-2",
            context=None,
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert selected_task_number == 2
    assert _SelectionRegistry.stored["discord:user-1"] == 2


def test_resolve_turn_routing_routes_task_specific_control_to_referenced_thread():
    _SelectionRegistry.stored = {}

    resolution = asyncio.run(
        resolve_turn_routing(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="retry task 2",
            context=None,
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
            thread_id_factory=lambda: "thread-new",
        )
    )

    assert resolution.thread_id == "thread-2"
    assert resolution.selected_task_number == 2
    assert resolution.allocated_new_thread is False


def test_resolve_turn_routing_routes_confirm_task_to_referenced_thread_even_when_other_task_selected():
    _SelectionRegistry.stored = {"discord:user-1": 3}

    resolution = asyncio.run(
        resolve_turn_routing(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="confirm task 2",
            context=None,
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
            thread_id_factory=lambda: "thread-new",
        )
    )

    assert resolution.thread_id == "thread-2"
    assert resolution.selected_task_number == 2
    assert resolution.allocated_new_thread is False


def test_resolve_turn_routing_routes_cancel_task_to_referenced_thread_even_when_other_task_selected():
    _SelectionRegistry.stored = {"discord:user-1": 3}

    resolution = asyncio.run(
        resolve_turn_routing(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="cancel task 2",
            context=None,
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
            thread_id_factory=lambda: "thread-new",
        )
    )

    assert resolution.thread_id == "thread-2"
    assert resolution.selected_task_number == 2
    assert resolution.allocated_new_thread is False


def test_resolve_turn_routing_routes_short_follow_up_to_only_live_task():
    class _SingleTaskRegistry(_TaskRegistry):
        def __init__(self) -> None:
            self.tasks = {
                ("discord:user-1", 2): {
                    "task_number": 2,
                    "thread_id": "thread-2",
                    "status": "WAITING_INPUT",
                }
            }

    _SelectionRegistry.stored = {}

    resolution = asyncio.run(
        resolve_turn_routing(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="somnia",
            context=None,
            task_registry_cls=_SingleTaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert resolution.thread_id == "thread-2"
    assert resolution.selected_task_number == 2
    assert resolution.blocked_message is None


def test_resolve_turn_routing_blocks_ambiguous_short_follow_up_with_clear_prompt():
    class _MultiTaskRegistry(_TaskRegistry):
        def __init__(self) -> None:
            self.tasks = {
                ("discord:user-1", 2): {
                    "task_number": 2,
                    "thread_id": "thread-2",
                    "status": "FAILED",
                    "title": "Swap STT to NIA",
                },
                ("discord:user-1", 3): {
                    "task_number": 3,
                    "thread_id": "thread-3",
                    "status": "WAITING_CONFIRMATION",
                    "title": "Bridge 100 USDC to Base",
                },
            }

    _SelectionRegistry.stored = {}

    resolution = asyncio.run(
        resolve_turn_routing(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="retry",
            context=None,
            task_registry_cls=_MultiTaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert resolution.thread_id == "thread-default"
    assert resolution.blocked_message is not None
    assert "which task do you mean" in resolution.blocked_message.lower()
    assert "task 2" in resolution.blocked_message.lower()
    assert "task 3" in resolution.blocked_message.lower()


def test_resolve_turn_routing_blocks_ambiguous_cancel_with_cancel_specific_hint():
    class _MultiTaskRegistry(_TaskRegistry):
        def __init__(self) -> None:
            self.tasks = {
                ("discord:user-1", 2): {
                    "task_number": 2,
                    "thread_id": "thread-2",
                    "status": "WAITING_EXTERNAL",
                    "title": "Swap STT to NIA",
                },
                ("discord:user-1", 3): {
                    "task_number": 3,
                    "thread_id": "thread-3",
                    "status": "WAITING_EXTERNAL",
                    "title": "Bridge 100 USDC to Base",
                },
            }

    _SelectionRegistry.stored = {}

    resolution = asyncio.run(
        resolve_turn_routing(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="cancel",
            context=None,
            task_registry_cls=_MultiTaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert resolution.thread_id == "thread-default"
    assert resolution.blocked_message is not None
    assert "which task do you mean" in resolution.blocked_message.lower()
    assert "cancel task <number>" in resolution.blocked_message.lower()
