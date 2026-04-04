import asyncio

from core.tasks.conversation_runtime import (
    finalize_conversation_turn,
    prepare_conversation_turn,
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
                "status": "WAITING_INPUT",
                "title": "Swap STT to NIA",
            }
        }

    async def get_task_by_number(self, conversation_id: str, *, task_number: int):
        return self.tasks.get((str(conversation_id), int(task_number)))

    async def list_recent(self, conversation_id: str, *, limit: int = 10):
        return [
            task
            for (stored_conversation_id, _task_number), task in self.tasks.items()
            if stored_conversation_id == str(conversation_id)
        ][:limit]

    async def get_latest_task_for_thread(self, conversation_id: str, *, thread_id: str):
        for (stored_conversation_id, _task_number), task in self.tasks.items():
            if stored_conversation_id == str(conversation_id) and task.get("thread_id") == str(thread_id):
                return task
        return None


def test_prepare_conversation_turn_resolves_context_and_thread_in_shared_service():
    prepared = asyncio.run(
        prepare_conversation_turn(
            provider="discord",
            provider_user_id="user-1",
            default_thread_id="thread-default",
            user_message="base",
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert prepared.conversation_id == "discord:user-1"
    assert prepared.thread_id == "thread-2"
    assert prepared.selected_task_number == 2
    assert prepared.context["conversation_id"] == "discord:user-1"


def test_finalize_conversation_turn_persists_selected_task_number_from_event():
    _SelectionRegistry.stored = {}

    selected_task_number = asyncio.run(
        finalize_conversation_turn(
            provider="discord",
            provider_user_id="user-1",
            conversation_id="discord:user-1",
            thread_id="thread-default",
            event_selected_task_number=3,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert selected_task_number == 3
    assert _SelectionRegistry.stored["discord:user-1"] == 3


def test_finalize_conversation_turn_clears_selection_when_event_requests_none():
    _SelectionRegistry.stored = {"discord:user-1": 4}

    selected_task_number = asyncio.run(
        finalize_conversation_turn(
            provider="discord",
            provider_user_id="user-1",
            conversation_id="discord:user-1",
            thread_id="thread-default",
            event_selected_task_number=None,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert selected_task_number is None
    assert _SelectionRegistry.stored["discord:user-1"] is None


def test_finalize_conversation_turn_falls_back_to_thread_selection_when_needed():
    _SelectionRegistry.stored = {}

    selected_task_number = asyncio.run(
        finalize_conversation_turn(
            provider="discord",
            provider_user_id="user-1",
            conversation_id="discord:user-1",
            thread_id="thread-2",
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert selected_task_number == 2
    assert _SelectionRegistry.stored["discord:user-1"] == 2
