import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from core.tasks.registry import (
    ConversationTaskRegistry,
    draft_execution_id,
    resolve_conversation_id,
)


class _Cursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def sort(self, *_args, **_kwargs):
        return self

    def limit(self, limit):
        self._rows = self._rows[:limit]
        return self

    async def to_list(self, length):
        return self._rows[:length]


def test_resolve_conversation_id_prefers_explicit_context():
    conversation_id = resolve_conversation_id(
        provider="telegram",
        provider_user_id="u-1",
        context={"conversation_id": "chat-123"},
    )

    assert conversation_id == "chat-123"


def test_resolve_conversation_id_falls_back_to_provider_identity():
    conversation_id = resolve_conversation_id(
        provider="telegram",
        provider_user_id="u-1",
        context={},
    )

    assert conversation_id == "telegram:u-1"


def test_upsert_execution_task_assigns_incremental_task_numbers():
    tasks = AsyncMock()
    counters = AsyncMock()
    counters.find_one_and_update.side_effect = [
        {"conversation_id": "telegram:u-1", "next_task_number": 1},
        {"conversation_id": "telegram:u-1", "next_task_number": 2},
    ]
    tasks.find_one.side_effect = [None, None, None, None]

    with patch(
        "core.tasks.registry.AsyncMongoDB.get_db",
        return_value={
            "conversation_tasks": tasks,
            "conversation_task_counters": counters,
        },
    ):
        registry = ConversationTaskRegistry()

        first = asyncio.run(
            registry.upsert_execution_task(
                conversation_id="telegram:u-1",
                execution_id="exec-1",
                thread_id="thread-1",
                provider="telegram",
                provider_user_id="u-1",
                user_id="volo-1",
                title="Bridge 100 USDC to Base",
                status="RUNNING",
            )
        )
        second = asyncio.run(
            registry.upsert_execution_task(
                conversation_id="telegram:u-1",
                execution_id="exec-2",
                thread_id="thread-2",
                provider="telegram",
                provider_user_id="u-1",
                user_id="volo-1",
                title="Swap STT to NIA",
                status="RUNNING",
            )
        )

    assert first["task_number"] == 1
    assert second["task_number"] == 2
    assert tasks.insert_one.await_count == 2


def test_upsert_execution_task_promotes_existing_draft_task():
    tasks = AsyncMock()
    counters = AsyncMock()
    tasks.find_one.side_effect = [
        None,
        {
            "task_id": "task-1",
            "conversation_id": "telegram:u-1",
            "task_number": 1,
            "execution_id": draft_execution_id("thread-1"),
            "thread_id": "thread-1",
            "provider": "telegram",
            "provider_user_id": "u-1",
            "user_id": "volo-1",
            "title": "Swap 0.2 STT to NIA",
            "status": "WAITING_INPUT",
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00",
        },
    ]

    with patch(
        "core.tasks.registry.AsyncMongoDB.get_db",
        return_value={
            "conversation_tasks": tasks,
            "conversation_task_counters": counters,
        },
    ):
        registry = ConversationTaskRegistry()

        result = asyncio.run(
            registry.upsert_execution_task(
                conversation_id="telegram:u-1",
                execution_id="exec-1",
                thread_id="thread-1",
                provider="telegram",
                provider_user_id="u-1",
                user_id="volo-1",
                title="Swap 0.2 STT to NIA",
                status="WAITING_CONFIRMATION",
            )
        )

    assert result["execution_id"] == "exec-1"
    tasks.insert_one.assert_not_awaited()
    tasks.update_one.assert_awaited()


def test_upsert_execution_task_sets_failed_ttl_fields():
    tasks = AsyncMock()
    counters = AsyncMock()
    counters.find_one_and_update.return_value = {
        "conversation_id": "telegram:u-1",
        "next_task_number": 1,
    }
    tasks.find_one.return_value = None
    now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)

    with (
        patch(
            "core.tasks.registry.AsyncMongoDB.get_db",
            return_value={
                "conversation_tasks": tasks,
                "conversation_task_counters": counters,
            },
        ),
        patch("core.tasks.registry._utc_now", return_value=now),
    ):
        registry = ConversationTaskRegistry()
        record = asyncio.run(
            registry.upsert_execution_task(
                conversation_id="telegram:u-1",
                execution_id="exec-1",
                thread_id="thread-1",
                provider="telegram",
                provider_user_id="u-1",
                user_id="volo-1",
                title="Bridge funds",
                status="FAILED",
            )
        )

    assert record["terminal_at"] == now
    assert record["failed_expires_at"] == now + timedelta(days=3)
    inserted = tasks.insert_one.await_args.args[0]
    assert inserted["created_at_dt"] == now
    assert inserted["updated_at_dt"] == now
    assert inserted["terminal_at"] == now
    assert inserted["failed_expires_at"] == now + timedelta(days=3)


def test_upsert_execution_task_clears_failed_ttl_fields_when_task_reactivates():
    tasks = AsyncMock()
    counters = AsyncMock()
    tasks.find_one.return_value = {
        "task_id": "task-1",
        "conversation_id": "telegram:u-1",
        "task_number": 1,
        "execution_id": "exec-1",
        "thread_id": "thread-1",
        "provider": "telegram",
        "provider_user_id": "u-1",
        "user_id": "volo-1",
        "title": "Bridge funds",
        "status": "FAILED",
        "created_at": "2026-03-28T12:00:00+00:00",
        "updated_at": "2026-03-28T12:00:00+00:00",
        "terminal_at": datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
        "failed_expires_at": datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
    }

    with patch(
        "core.tasks.registry.AsyncMongoDB.get_db",
        return_value={
            "conversation_tasks": tasks,
            "conversation_task_counters": counters,
        },
    ):
        registry = ConversationTaskRegistry()
        result = asyncio.run(
            registry.upsert_execution_task(
                conversation_id="telegram:u-1",
                execution_id="exec-1",
                thread_id="thread-1",
                provider="telegram",
                provider_user_id="u-1",
                user_id="volo-1",
                title="Bridge funds",
                status="RUNNING",
            )
        )

    update_doc = tasks.update_one.await_args.args[1]
    assert update_doc["$unset"] == {"terminal_at": "", "failed_expires_at": ""}
    assert "terminal_at" not in result
    assert "failed_expires_at" not in result


def test_ensure_indexes_adds_failed_ttl_index():
    tasks = AsyncMock()
    counters = AsyncMock()

    with patch(
        "core.tasks.registry.AsyncMongoDB.get_db",
        return_value={
            "conversation_tasks": tasks,
            "conversation_task_counters": counters,
        },
    ):
        registry = ConversationTaskRegistry()
        asyncio.run(registry._ensure_indexes())

    models = tasks.create_indexes.await_args.args[0]
    ttl_model = next(
        model
        for model in models
        if getattr(model, "document", {}).get("name") == "ttl_failed_task_expires_at"
    )
    assert ttl_model.document["expireAfterSeconds"] == 0
    assert ttl_model.document["partialFilterExpression"] == {
        "failed_expires_at": {"$type": "date"}
    }
    assert list(ttl_model.document["key"].items()) == [("failed_expires_at", 1)]


def test_backfill_failed_task_expiry_fields_sets_bson_dates_from_iso_strings():
    tasks = AsyncMock()
    counters = AsyncMock()
    tasks.find = Mock(
        return_value=_Cursor(
        [
            {
                "task_id": "task-1",
                "updated_at": "2026-03-20T08:30:00+00:00",
                "created_at": "2026-03-19T08:30:00+00:00",
                "terminal_at": None,
            }
        ]
        )
    )
    tasks.update_one.return_value = SimpleNamespace(modified_count=1)

    with patch(
        "core.tasks.registry.AsyncMongoDB.get_db",
        return_value={
            "conversation_tasks": tasks,
            "conversation_task_counters": counters,
        },
    ):
        registry = ConversationTaskRegistry()
        updated = asyncio.run(
            registry.backfill_failed_task_expiry_fields("telegram:u-1")
        )

    assert updated == 1
    update_filter, update_doc = tasks.update_one.await_args.args
    assert update_filter["status"] == "FAILED"
    assert update_filter["failed_expires_at"] == {"$exists": False}
    assert update_doc["$set"]["terminal_at"] == datetime(
        2026, 3, 20, 8, 30, tzinfo=timezone.utc
    )
    assert update_doc["$set"]["failed_expires_at"] == datetime(
        2026, 3, 23, 8, 30, tzinfo=timezone.utc
    )


def test_completed_tasks_are_eligible_for_pruning_after_3_days():
    tasks = AsyncMock()
    counters = AsyncMock()
    tasks.find = Mock(return_value=_Cursor([{"task_id": "task-1", "task_number": 1}]))
    tasks.delete_many.return_value = SimpleNamespace(deleted_count=1)
    now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)

    with patch(
        "core.tasks.registry.AsyncMongoDB.get_db",
        return_value={
            "conversation_tasks": tasks,
            "conversation_task_counters": counters,
        },
    ):
        registry = ConversationTaskRegistry()
        deleted = asyncio.run(
            registry.prune_terminal_tasks(
                "telegram:u-1",
                protected_task_numbers={2},
                now=now,
            )
        )

    assert deleted == 1
    find_query = tasks.find.call_args.args[0]
    assert find_query["status"] == {"$in": ["CANCELLED", "COMPLETED"]}
    assert find_query["terminal_at"]["$lte"] == now - timedelta(days=3)
    assert find_query["task_number"] == {"$nin": [2]}
    delete_query = tasks.delete_many.await_args.args[0]
    assert delete_query["task_id"] == {"$in": ["task-1"]}
    assert delete_query["task_number"] == {"$nin": [2]}


def test_cancelled_tasks_are_eligible_for_pruning_after_3_days():
    tasks = AsyncMock()
    counters = AsyncMock()
    tasks.find = Mock(return_value=_Cursor([{"task_id": "task-9", "task_number": 9}]))
    tasks.delete_many.return_value = SimpleNamespace(deleted_count=1)
    now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)

    with patch(
        "core.tasks.registry.AsyncMongoDB.get_db",
        return_value={
            "conversation_tasks": tasks,
            "conversation_task_counters": counters,
        },
    ):
        registry = ConversationTaskRegistry()
        deleted = asyncio.run(
            registry.prune_terminal_tasks(
                "telegram:u-1",
                protected_task_numbers=set(),
                now=now,
            )
        )

    assert deleted == 1
    find_query = tasks.find.call_args.args[0]
    assert find_query["status"] == {"$in": ["CANCELLED", "COMPLETED"]}
    assert find_query["terminal_at"]["$lte"] == now - timedelta(days=3)
    delete_query = tasks.delete_many.await_args.args[0]
    assert delete_query["status"] == {"$in": ["CANCELLED", "COMPLETED"]}
    assert delete_query["task_id"] == {"$in": ["task-9"]}


def test_prune_terminal_tasks_never_targets_failed_or_active_statuses():
    tasks = AsyncMock()
    counters = AsyncMock()
    tasks.find = Mock(return_value=_Cursor([]))

    with patch(
        "core.tasks.registry.AsyncMongoDB.get_db",
        return_value={
            "conversation_tasks": tasks,
            "conversation_task_counters": counters,
        },
    ):
        registry = ConversationTaskRegistry()
        deleted = asyncio.run(registry.prune_terminal_tasks("telegram:u-1"))

    assert deleted == 0
    find_query = tasks.find.call_args.args[0]
    assert find_query["status"] == {"$in": ["CANCELLED", "COMPLETED"]}
    tasks.delete_many.assert_not_awaited()


def test_task_numbers_remain_monotonic_after_pruning():
    tasks = AsyncMock()
    counters = AsyncMock()
    counters.find_one_and_update.side_effect = [
        {"conversation_id": "telegram:u-1", "next_task_number": 1},
        {"conversation_id": "telegram:u-1", "next_task_number": 2},
        {"conversation_id": "telegram:u-1", "next_task_number": 3},
    ]
    tasks.find_one.side_effect = [None, None, None, None, None, None]
    tasks.find = Mock(return_value=_Cursor([{"task_id": "task-1", "task_number": 1}]))
    tasks.delete_many.return_value = SimpleNamespace(deleted_count=1)

    with patch(
        "core.tasks.registry.AsyncMongoDB.get_db",
        return_value={
            "conversation_tasks": tasks,
            "conversation_task_counters": counters,
        },
    ):
        registry = ConversationTaskRegistry()
        first = asyncio.run(
            registry.upsert_execution_task(
                conversation_id="telegram:u-1",
                execution_id="exec-1",
                thread_id="thread-1",
                provider="telegram",
                provider_user_id="u-1",
                user_id="volo-1",
                title="Task 1",
                status="COMPLETED",
            )
        )
        second = asyncio.run(
            registry.upsert_execution_task(
                conversation_id="telegram:u-1",
                execution_id="exec-2",
                thread_id="thread-2",
                provider="telegram",
                provider_user_id="u-1",
                user_id="volo-1",
                title="Task 2",
                status="COMPLETED",
            )
        )
        asyncio.run(registry.prune_terminal_tasks("telegram:u-1"))
        third = asyncio.run(
            registry.upsert_execution_task(
                conversation_id="telegram:u-1",
                execution_id="exec-3",
                thread_id="thread-3",
                provider="telegram",
                provider_user_id="u-1",
                user_id="volo-1",
                title="Task 3",
                status="RUNNING",
            )
        )

    assert first["task_number"] == 1
    assert second["task_number"] == 2
    assert third["task_number"] == 3
