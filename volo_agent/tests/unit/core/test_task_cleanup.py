import asyncio

from core.tasks.cleanup import run_terminal_task_cleanup
from core.tasks.updater import upsert_task_from_state


class _SelectionRegistry:
    selected_task_number: int | None = None
    clear_calls: list[tuple[str, int]] = []
    fail_lookup = False

    def __init__(self) -> None:
        pass

    async def clear_selected_task_number_if_matches(
        self,
        *,
        conversation_id: str,
        task_number: int,
    ) -> bool:
        type(self).clear_calls.append((str(conversation_id), int(task_number)))
        if type(self).selected_task_number == int(task_number):
            type(self).selected_task_number = None
            return True
        return False

    async def get_selected_task_number(self, conversation_id: str) -> int | None:
        if type(self).fail_lookup:
            raise RuntimeError("selection lookup failed")
        return type(self).selected_task_number


class _TaskRegistry:
    prune_calls: list[tuple[str, frozenset[int]]] = []
    backfill_calls: list[str] = []

    def __init__(self) -> None:
        pass

    async def backfill_failed_task_expiry_fields(
        self,
        conversation_id: str,
        *,
        limit: int = 25,
    ) -> int:
        self.backfill_calls.append(str(conversation_id))
        return 0

    async def prune_terminal_tasks(
        self,
        conversation_id: str,
        *,
        protected_task_numbers,
        now=None,
        limit: int = 50,
    ) -> int:
        self.prune_calls.append(
            (str(conversation_id), frozenset(protected_task_numbers or set()))
        )
        return 0


class _IdempotentTaskRegistry(_TaskRegistry):
    deleted = {"task-1"}
    prune_deleted_counts: list[int] = []

    async def prune_terminal_tasks(
        self,
        conversation_id: str,
        *,
        protected_task_numbers,
        now=None,
        limit: int = 50,
    ) -> int:
        await super().prune_terminal_tasks(
            conversation_id,
            protected_task_numbers=protected_task_numbers,
            now=now,
            limit=limit,
        )
        if "task-1" in self.deleted:
            self.deleted.remove("task-1")
            self.prune_deleted_counts.append(1)
            return 1
        self.prune_deleted_counts.append(0)
        return 0


class _UpdaterRegistry:
    async def upsert_execution_task(self, **kwargs):
        return {
            "conversation_id": kwargs["conversation_id"],
            "task_number": 3,
            "status": kwargs["status"],
        }


def test_run_terminal_task_cleanup_clears_terminal_selection_and_prunes():
    _SelectionRegistry.selected_task_number = 3
    _SelectionRegistry.clear_calls = []
    _SelectionRegistry.fail_lookup = False
    _TaskRegistry.prune_calls = []
    _TaskRegistry.backfill_calls = []

    asyncio.run(
        run_terminal_task_cleanup(
            task_record={
                "conversation_id": "discord:user-1",
                "task_number": 3,
                "status": "COMPLETED",
            },
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert _SelectionRegistry.selected_task_number is None
    assert _SelectionRegistry.clear_calls == [("discord:user-1", 3)]
    assert _TaskRegistry.backfill_calls == ["discord:user-1"]
    assert _TaskRegistry.prune_calls == [("discord:user-1", frozenset())]


def test_run_terminal_task_cleanup_protects_new_selection_during_prune():
    _SelectionRegistry.selected_task_number = 4
    _SelectionRegistry.clear_calls = []
    _SelectionRegistry.fail_lookup = False
    _TaskRegistry.prune_calls = []
    _TaskRegistry.backfill_calls = []

    asyncio.run(
        run_terminal_task_cleanup(
            task_record={
                "conversation_id": "discord:user-1",
                "task_number": 3,
                "status": "CANCELLED",
            },
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert _SelectionRegistry.clear_calls == [("discord:user-1", 3)]
    assert _TaskRegistry.prune_calls == [("discord:user-1", frozenset({4}))]


def test_run_terminal_task_cleanup_skips_prune_when_selection_lookup_fails():
    _SelectionRegistry.selected_task_number = 4
    _SelectionRegistry.clear_calls = []
    _SelectionRegistry.fail_lookup = True
    _TaskRegistry.prune_calls = []
    _TaskRegistry.backfill_calls = []

    asyncio.run(
        run_terminal_task_cleanup(
            task_record={
                "conversation_id": "discord:user-1",
                "task_number": 3,
                "status": "COMPLETED",
            },
            task_registry_cls=_TaskRegistry,
            selection_registry_cls=_SelectionRegistry,
        )
    )

    assert _TaskRegistry.backfill_calls == ["discord:user-1"]
    assert _TaskRegistry.prune_calls == []


def test_run_terminal_task_cleanup_is_idempotent():
    _SelectionRegistry.selected_task_number = 3
    _SelectionRegistry.clear_calls = []
    _SelectionRegistry.fail_lookup = False
    _IdempotentTaskRegistry.prune_calls = []
    _IdempotentTaskRegistry.backfill_calls = []
    _IdempotentTaskRegistry.deleted = {"task-1"}
    _IdempotentTaskRegistry.prune_deleted_counts = []

    for _ in range(2):
        asyncio.run(
            run_terminal_task_cleanup(
                task_record={
                    "conversation_id": "discord:user-1",
                    "task_number": 3,
                    "status": "COMPLETED",
                },
                task_registry_cls=_IdempotentTaskRegistry,
                selection_registry_cls=_SelectionRegistry,
            )
        )

    assert _SelectionRegistry.selected_task_number is None
    assert _IdempotentTaskRegistry.prune_deleted_counts == [1, 0]


def test_upsert_task_from_state_cleanup_failure_does_not_break_caller():
    state = {
        "provider": "discord",
        "user_id": "user-1",
        "context": {"conversation_id": "discord:user-1", "thread_id": "thread-1"},
        "execution_id": "exec-1",
        "user_info": {"volo_user_id": "volo-1"},
    }

    async def _run():
        from unittest.mock import patch

        with patch(
            "core.tasks.updater.schedule_terminal_task_cleanup",
            side_effect=RuntimeError("cleanup scheduling failed"),
        ):
            return await upsert_task_from_state(
                state,
                title="Task",
                status="COMPLETED",
                registry_cls=_UpdaterRegistry,
            )

    record = asyncio.run(_run())

    assert record == {
        "conversation_id": "discord:user-1",
        "task_number": 3,
        "status": "COMPLETED",
    }
