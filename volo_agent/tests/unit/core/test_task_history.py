import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.history.task_history import (
    TaskHistoryRegistry,
    _expiry_for_status,
    normalize_status,
    summarize_task,
    update_task_history_terminal_status,
    update_task_history_tx_hash,
)


def test_normalize_status_uppercases_and_defaults():
    assert normalize_status(None) == "UNKNOWN"
    assert normalize_status("pending") == "PENDING"


@pytest.mark.parametrize(
    ("status", "expected_days"),
    [
        ("SUCCESS", 3),
        ("FAILED", 3),
        ("PENDING", 5),
        ("PENDING_ON_CHAIN", 5),
        ("RESUBMITTED", 5),
        ("weird_new_status", 5),
    ],
)
def test_expiry_for_status_uses_retention_policy(status: str, expected_days: int):
    now = datetime(2026, 3, 31, 10, 0, tzinfo=timezone.utc)

    expires_at = _expiry_for_status(status, now)

    assert expires_at == now + timedelta(days=expected_days)
    assert expires_at.tzinfo == timezone.utc


def test_summarize_task_swap():
    summary = summarize_task(
        "swap",
        {
            "amount_in": 1,
            "token_in_symbol": "ETH",
            "token_out_symbol": "USDC",
            "chain": "base",
        },
    )
    assert "Swap 1 ETH to USDC on base" == summary


def test_summarize_task_bridge():
    summary = summarize_task(
        "bridge",
        {
            "amount": 0.5,
            "token_symbol": "ETH",
            "source_chain": "ethereum",
            "target_chain": "base",
        },
    )
    assert "Bridge 0.5 ETH from ethereum to base" == summary


def test_summarize_task_transfer():
    summary = summarize_task(
        "transfer",
        {
            "amount": 10,
            "asset_symbol": "USDC",
            "recipient": "0xabc",
            "network": "base",
        },
    )
    assert "Transfer 10 USDC to 0xabc on base" == summary


def test_record_event_sets_string_timestamps_and_bson_expiry():
    collection = AsyncMock()

    with patch(
        "core.history.task_history.AsyncMongoDB.get_collection",
        return_value=collection,
    ):
        registry = TaskHistoryRegistry()
        asyncio.run(
            registry.record_event(
                user_id="user-1",
                thread_id="thread-1",
                execution_id="exec-1",
                node_id="node-1",
                tool="bridge",
                status="pending_on_chain",
                summary="Bridge pending.",
                tx_hash="0xabc",
            )
        )

    inserted = collection.insert_one.await_args.args[0]
    created_at = datetime.fromisoformat(inserted["created_at"])
    updated_at = datetime.fromisoformat(inserted["updated_at"])

    assert isinstance(inserted["created_at"], str)
    assert isinstance(inserted["updated_at"], str)
    assert inserted["status"] == "PENDING_ON_CHAIN"
    assert inserted["expires_at"] == updated_at + timedelta(days=5)
    assert inserted["expires_at"].tzinfo == timezone.utc
    assert created_at.tzinfo == timezone.utc
    assert updated_at.tzinfo == timezone.utc


def test_ensure_indexes_adds_task_history_ttl_index():
    collection = AsyncMock()

    with patch(
        "core.history.task_history.AsyncMongoDB.get_collection",
        return_value=collection,
    ):
        registry = TaskHistoryRegistry()
        asyncio.run(registry._ensure_indexes())

    models = collection.create_indexes.await_args_list[0].args[0]
    ttl_model = next(
        model
        for model in models
        if getattr(model, "document", {}).get("name") == "ttl_task_history_expires_at"
    )
    assert ttl_model.document["expireAfterSeconds"] == 0
    assert ttl_model.document["partialFilterExpression"] == {
        "expires_at": {"$type": "date"}
    }
    assert list(ttl_model.document["key"].items()) == [("expires_at", 1)]


def test_update_task_history_tx_hash_refreshes_updated_at_and_expires_at():
    calls = {}

    class _Coll:
        async def find_one_and_update(self, query, update, sort=None):
            calls["query"] = query
            calls["update"] = update
            calls["sort"] = sort
            return {"_id": "x"}

    now_iso = "2026-03-31T10:00:00Z"
    updated = asyncio.run(
        update_task_history_tx_hash(
            collection=_Coll(),
            execution_id="exec-1",
            node_id="step_0",
            tx_hash="0xabc",
            now_iso=now_iso,
            status="resubmitted",
        )
    )

    assert updated is True
    assert calls["query"] == {
        "execution_id": "exec-1",
        "node_id": "step_0",
        "status": {"$in": ["PENDING", "PENDING_ON_CHAIN"]},
    }
    assert calls["sort"] == [("updated_at", -1)]
    assert calls["update"]["$set"]["tx_hash"] == "0xabc"
    assert calls["update"]["$set"]["status"] == "RESUBMITTED"
    assert calls["update"]["$set"]["updated_at"] == now_iso
    assert calls["update"]["$set"]["expires_at"] == datetime(
        2026, 4, 5, 10, 0, tzinfo=timezone.utc
    )


def test_update_task_history_terminal_status_refreshes_updated_at_and_expires_at():
    calls = {}

    class _Coll:
        async def find_one_and_update(self, query, update, sort=None):
            calls["query"] = query
            calls["update"] = update
            calls["sort"] = sort
            return {"_id": "x"}

    now_iso = "2026-04-01T14:00:00Z"
    updated = asyncio.run(
        update_task_history_terminal_status(
            collection=_Coll(),
            execution_id="exec-1",
            node_id="step_0",
            status="success",
            now_iso=now_iso,
            tx_hash="0xdef",
            summary="Bridge completed.",
        )
    )

    assert updated is True
    assert calls["query"] == {
        "execution_id": "exec-1",
        "node_id": "step_0",
        "status": {"$in": ["PENDING", "PENDING_ON_CHAIN", "RESUBMITTED"]},
    }
    assert calls["sort"] == [("updated_at", -1)]
    assert calls["update"]["$set"]["status"] == "SUCCESS"
    assert calls["update"]["$set"]["updated_at"] == now_iso
    assert calls["update"]["$set"]["tx_hash"] == "0xdef"
    assert calls["update"]["$set"]["summary"] == "Bridge completed."
    assert calls["update"]["$set"]["expires_at"] == datetime(
        2026, 4, 4, 14, 0, tzinfo=timezone.utc
    )
