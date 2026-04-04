import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from core.observer.trigger_registry import TriggerRegistry


class _FakeDB:
    def __init__(self, collection):
        self._collection = collection

    def __getitem__(self, _name):
        return self._collection


def _build_registry(collection):
    with patch(
        "core.observer.trigger_registry.AsyncMongoDB.get_db",
        return_value=_FakeDB(collection),
    ):
        return TriggerRegistry()


class _FrozenDateTime:
    _now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls._now
        return cls._now.astimezone(tz)


def test_register_trigger_inserts_document():
    collection = MagicMock()
    collection.create_indexes = AsyncMock()
    collection.insert_one = AsyncMock()

    registry = _build_registry(collection)

    with patch("core.observer.trigger_registry.datetime", _FrozenDateTime), patch(
        "core.observer.trigger_registry.uuid.uuid4", return_value="fixed-id"
    ):
        trigger_id = asyncio.run(
            registry.register_trigger(
                user_id="user-1",
                thread_id="thread-1",
                trigger_condition={"type": "price_below", "asset": "ETH", "target": 1000},
                payload={"intents": [{"intent_type": "swap"}]},
            )
        )

    assert trigger_id == "fixed-id"
    assert collection.create_indexes.await_count == 1
    assert collection.insert_one.await_count == 1
    models = collection.create_indexes.await_args.args[0]
    ttl_model = next(
        model
        for model in models
        if getattr(model, "document", {}).get("name") == "ttl_cleanup_after"
    )
    assert ttl_model.document["expireAfterSeconds"] == 0
    assert ttl_model.document["partialFilterExpression"] == {
        "cleanup_after": {"$type": "date"}
    }
    assert list(ttl_model.document["key"].items()) == [("cleanup_after", 1)]
    inserted = collection.insert_one.call_args[0][0]
    assert inserted["trigger_id"] == "fixed-id"
    assert inserted["user_id"] == "user-1"
    assert inserted["thread_id"] == "thread-1"
    assert inserted["status"] == "pending"
    assert inserted["created_at"] == _FrozenDateTime._now.isoformat()
    assert inserted["expires_at"] == (
        _FrozenDateTime._now + timedelta(days=7)
    ).isoformat()
    assert inserted["cleanup_after"] is None
    assert inserted["fire_count"] == 0


def test_mark_triggered_returns_true_on_update():
    collection = MagicMock()
    collection.create_indexes = AsyncMock()
    collection.update_one = AsyncMock(return_value=MagicMock(modified_count=1))

    registry = _build_registry(collection)

    with patch("core.observer.trigger_registry.datetime", _FrozenDateTime):
        result = asyncio.run(registry.mark_triggered("trigger-1"))

    assert result is True
    assert collection.update_one.await_count == 1
    update_doc = collection.update_one.await_args.args[1]
    assert update_doc["$set"]["status"] == "triggered"
    assert update_doc["$set"]["triggered_at"] == _FrozenDateTime._now.isoformat()
    assert update_doc["$set"]["cleanup_after"] == _FrozenDateTime._now + timedelta(
        days=1
    )


def test_mark_triggered_or_reschedule_updates_execute_at():
    collection = MagicMock()
    collection.create_indexes = AsyncMock()
    collection.update_one = AsyncMock(return_value=MagicMock(modified_count=1))

    registry = _build_registry(collection)

    with patch("core.observer.trigger_registry.datetime", _FrozenDateTime):
        result = asyncio.run(
            registry.mark_triggered_or_reschedule(
                "trigger-2", next_execute_at="2030-01-01T01:00:00+00:00"
            )
        )

    assert result is True
    assert collection.update_one.await_count == 1
    args, _kwargs = collection.update_one.call_args
    assert args[0]["trigger_id"] == "trigger-2"
    assert args[1]["$set"]["triggered_at"] == _FrozenDateTime._now.isoformat()
    assert (
        args[1]["$set"]["trigger_condition.execute_at"]
        == "2030-01-01T01:00:00+00:00"
    )
    assert "status" not in args[1]["$set"]
    assert "cleanup_after" not in args[1]["$set"]


def test_mark_failed_sets_delayed_cleanup():
    collection = MagicMock()
    collection.create_indexes = AsyncMock()
    collection.update_one = AsyncMock(return_value=MagicMock(modified_count=1))

    registry = _build_registry(collection)

    with patch("core.observer.trigger_registry.datetime", _FrozenDateTime):
        asyncio.run(registry.mark_failed("trigger-3", "boom"))

    update_doc = collection.update_one.await_args.args[1]
    assert update_doc["$set"]["status"] == "failed"
    assert update_doc["$set"]["error"] == "boom"
    assert update_doc["$set"]["triggered_at"] == _FrozenDateTime._now.isoformat()
    assert update_doc["$set"]["cleanup_after"] == _FrozenDateTime._now + timedelta(
        days=3
    )


def test_cancel_trigger_sets_delayed_cleanup():
    collection = MagicMock()
    collection.create_indexes = AsyncMock()
    collection.update_one = AsyncMock(return_value=MagicMock(modified_count=1))

    registry = _build_registry(collection)

    with patch("core.observer.trigger_registry.datetime", _FrozenDateTime):
        result = asyncio.run(registry.cancel_trigger("trigger-4", user_id="user-4"))

    assert result is True
    query = collection.update_one.await_args.args[0]
    update_doc = collection.update_one.await_args.args[1]
    assert query == {
        "trigger_id": "trigger-4",
        "status": "pending",
        "user_id": "user-4",
    }
    assert update_doc["$set"]["status"] == "cancelled"
    assert update_doc["$set"]["cancelled_at"] == _FrozenDateTime._now.isoformat()
    assert update_doc["$set"]["error"] == "user_cancelled"
    assert update_doc["$set"]["cleanup_after"] == _FrozenDateTime._now + timedelta(
        days=3
    )


def test_expire_old_triggers_sets_delayed_cleanup():
    collection = MagicMock()
    collection.create_indexes = AsyncMock()
    collection.update_many = AsyncMock(return_value=MagicMock(modified_count=2))

    registry = _build_registry(collection)

    with patch("core.observer.trigger_registry.datetime", _FrozenDateTime):
        expired = asyncio.run(registry.expire_old_triggers())

    assert expired == 2
    query = collection.update_many.await_args.args[0]
    update_doc = collection.update_many.await_args.args[1]
    assert query == {
        "status": "pending",
        "expires_at": {"$lt": _FrozenDateTime._now.isoformat()},
    }
    assert update_doc["$set"]["status"] == "expired"
    assert update_doc["$set"]["cleanup_after"] == _FrozenDateTime._now + timedelta(
        days=3
    )
