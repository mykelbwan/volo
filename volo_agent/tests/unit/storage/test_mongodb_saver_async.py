import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.database.mongodb_saver_async import AsyncMongoDBSaver
from tests.unit.storage._mongodb_saver_fakes import (
    build_async_saver,
    make_checkpoint,
    make_config,
    make_metadata,
)


class _FakeDB:
    def __init__(self, collection):
        self._collection = collection

    def __getitem__(self, _name):
        return self._collection


def test_async_mongodb_saver_put_creates_indexes():
    collection = MagicMock()
    collection.create_index = AsyncMock()
    collection.update_one = AsyncMock()

    with patch(
        "core.database.mongodb_saver_async.AsyncMongoDB.get_db",
        return_value=_FakeDB(collection),
    ):
        saver = AsyncMongoDBSaver()

    checkpoint = {
        "v": 1,
        "id": "chk1",
        "ts": "2020-01-01T00:00:00Z",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
        "updated_channels": None,
    }
    metadata = {"source": "input", "step": -1, "parents": {}, "run_id": "run"}
    config = {"configurable": {"thread_id": "t1", "checkpoint_id": "chk0"}}

    asyncio.run(saver.aput(config, checkpoint, metadata, {}))

    assert collection.create_index.await_count >= 1
    assert collection.update_one.await_count >= 1
    assert any(
        kwargs.get("name") == "idx_write_pk" and kwargs.get("unique") is True
        for _args, kwargs in collection.create_index.await_args_list
    )


def test_async_mongodb_saver_sync_methods_raise_inside_running_loop():
    saver = object.__new__(AsyncMongoDBSaver)

    async def _run():
        try:
            saver.get_tuple({"configurable": {"thread_id": "t1"}})
        except RuntimeError as exc:
            return str(exc)
        raise AssertionError("Expected running-loop sync usage to fail.")

    message = asyncio.run(_run())

    assert "running event loop" in message
    assert "Use the async variants instead" in message


@pytest.mark.asyncio
async def test_async_mongodb_saver_aput_prunes_old_checkpoint_data_without_touching_adjacent_scope():
    saver, collections = build_async_saver()

    base_config = make_config("thread-1", "ns-1")
    other_thread_config = make_config("thread-2", "ns-1")
    other_ns_config = make_config("thread-1", "ns-2")

    checkpoint_1 = make_checkpoint(
        "001",
        channel_versions={"alpha": "v1"},
        channel_values={"alpha": {"step": 1}},
    )
    cfg_1 = await saver.aput(
        base_config,
        checkpoint_1,
        make_metadata(),
        checkpoint_1["channel_versions"],
    )
    await saver.aput_writes(cfg_1, [("pending", {"write": "old-1"})], "task-1")

    checkpoint_2 = make_checkpoint(
        "002",
        channel_versions={"beta": "v2"},
        channel_values={"beta": {"step": 2}},
    )
    cfg_2 = await saver.aput(
        base_config,
        checkpoint_2,
        make_metadata(),
        checkpoint_2["channel_versions"],
    )
    await saver.aput_writes(cfg_2, [("pending", {"write": "old-2"})], "task-2")

    other_thread_checkpoint = make_checkpoint(
        "100",
        channel_versions={"adjacent": "thread-keep"},
        channel_values={"adjacent": {"thread": True}},
    )
    other_thread_cfg = await saver.aput(
        other_thread_config,
        other_thread_checkpoint,
        make_metadata(),
        other_thread_checkpoint["channel_versions"],
    )
    await saver.aput_writes(
        other_thread_cfg,
        [("pending", {"write": "thread-keep"})],
        "task-thread",
    )

    other_ns_checkpoint = make_checkpoint(
        "100",
        channel_versions={"adjacent": "ns-keep"},
        channel_values={"adjacent": {"ns": True}},
    )
    other_ns_cfg = await saver.aput(
        other_ns_config,
        other_ns_checkpoint,
        make_metadata(),
        other_ns_checkpoint["channel_versions"],
    )
    await saver.aput_writes(
        other_ns_cfg,
        [("pending", {"write": "ns-keep"})],
        "task-ns",
    )

    collections.blobs.insert_one(
        {
            "thread_id": "thread-1",
            "checkpoint_ns": "ns-1",
            "channel": "orphan",
            "version": "legacy-blob",
            "type": "pickle",
            "blob": b"legacy",
        }
    )

    checkpoint_3 = make_checkpoint(
        "003",
        channel_versions={"gamma": "v3", "delta": "v4"},
        channel_values={"gamma": {"step": 3}, "delta": {"step": "latest"}},
    )
    cfg_3 = await saver.aput(
        base_config,
        checkpoint_3,
        make_metadata(),
        checkpoint_3["channel_versions"],
    )
    await saver.aput_writes(cfg_3, [("pending", {"write": "latest"})], "task-3")

    await saver._prune_checkpoints("thread-1", "ns-1")

    scoped_checkpoint_ids = sorted(
        doc["checkpoint_id"]
        for doc in collections.checkpoints.all_docs()
        if doc["thread_id"] == "thread-1" and doc["checkpoint_ns"] == "ns-1"
    )
    assert scoped_checkpoint_ids == ["003"]

    remaining_write_ids = sorted(
        doc["checkpoint_id"]
        for doc in collections.writes.all_docs()
        if doc["thread_id"] == "thread-1" and doc["checkpoint_ns"] == "ns-1"
    )
    assert remaining_write_ids == ["003"]

    remaining_blob_pairs = sorted(
        (doc["channel"], doc["version"])
        for doc in collections.blobs.all_docs()
        if doc["thread_id"] == "thread-1" and doc["checkpoint_ns"] == "ns-1"
    )
    assert remaining_blob_pairs == [("delta", "v4"), ("gamma", "v3")]

    assert sorted(
        doc["checkpoint_id"]
        for doc in collections.checkpoints.all_docs()
        if doc["thread_id"] == "thread-2" and doc["checkpoint_ns"] == "ns-1"
    ) == ["100"]
    assert sorted(
        doc["checkpoint_id"]
        for doc in collections.checkpoints.all_docs()
        if doc["thread_id"] == "thread-1" and doc["checkpoint_ns"] == "ns-2"
    ) == ["100"]

    latest_tuple = await saver.aget_tuple(make_config("thread-1", "ns-1"))
    assert latest_tuple is not None
    assert latest_tuple.checkpoint["id"] == "003"
    assert latest_tuple.checkpoint["channel_values"] == {
        "delta": {"step": "latest"},
        "gamma": {"step": 3},
    }


@pytest.mark.asyncio
async def test_async_mongodb_saver_aput_keeps_checkpoint_when_prune_cleanup_fails():
    saver, _collections = build_async_saver()
    checkpoint = make_checkpoint(
        "001",
        channel_versions={"alpha": "v1"},
        channel_values={"alpha": {"step": 1}},
    )

    with patch.object(
        saver,
        "_prune_checkpoints",
        AsyncMock(side_effect=RuntimeError("prune failed")),
    ):
        returned_config = await saver.aput(
            make_config("thread-1", "ns-1"),
            checkpoint,
            make_metadata(),
            checkpoint["channel_versions"],
        )

    loaded = await saver.aget_tuple(returned_config)

    assert loaded is not None
    assert loaded.checkpoint["id"] == "001"
    assert loaded.checkpoint["channel_values"] == {"alpha": {"step": 1}}


@pytest.mark.asyncio
async def test_async_mongodb_saver_prune_aborts_without_deletes_for_malformed_newest_checkpoint():
    saver, collections = build_async_saver()

    old_checkpoint = make_checkpoint(
        "001",
        channel_versions={"alpha": "v1"},
        channel_values={"alpha": {"step": 1}},
    )
    old_cfg = await saver.aput(
        make_config("thread-1", "ns-1"),
        old_checkpoint,
        make_metadata(),
        old_checkpoint["channel_versions"],
    )
    await saver.aput_writes(old_cfg, [("pending", {"write": "old"})], "task-old")

    metadata_type, metadata_blob = saver.serde.dumps_typed(make_metadata())
    collections.checkpoints.insert_one(
        {
            "thread_id": "thread-1",
            "checkpoint_ns": "ns-1",
            "checkpoint_id": "999",
            "type": "pickle",
            "checkpoint": b"not-a-valid-checkpoint",
            "metadata_type": metadata_type,
            "metadata": metadata_blob,
        }
    )
    collections.writes.insert_one(
        {
            "thread_id": "thread-1",
            "checkpoint_ns": "ns-1",
            "checkpoint_id": "999",
            "task_id": "task-new",
            "idx": 0,
            "channel": "pending",
            "type": "pickle",
            "value": b"new",
        }
    )
    collections.blobs.insert_one(
        {
            "thread_id": "thread-1",
            "checkpoint_ns": "ns-1",
            "channel": "legacy",
            "version": "legacy-v1",
            "type": "pickle",
            "blob": b"legacy",
        }
    )
    collections.blobs.insert_one(
        {
            "thread_id": "thread-1",
            "checkpoint_ns": "ns-1",
            "channel": "partial",
            "type": "pickle",
            "blob": b"partial",
        }
    )
    collections.blobs.insert_one(
        {
            "thread_id": "thread-1",
            "checkpoint_ns": "ns-2",
            "channel": "adjacent",
            "version": "keep",
            "type": "pickle",
            "blob": b"adjacent",
        }
    )

    await saver._prune_checkpoints("thread-1", "ns-1")

    scoped_checkpoints = sorted(
        doc["checkpoint_id"]
        for doc in collections.checkpoints.all_docs()
        if doc["thread_id"] == "thread-1" and doc["checkpoint_ns"] == "ns-1"
    )
    assert scoped_checkpoints == ["001", "999"]

    scoped_writes = sorted(
        doc["checkpoint_id"]
        for doc in collections.writes.all_docs()
        if doc["thread_id"] == "thread-1" and doc["checkpoint_ns"] == "ns-1"
    )
    assert scoped_writes == ["001", "999"]

    scoped_blobs = sorted(
        (
            doc.get("channel"),
            doc.get("version"),
        )
        for doc in collections.blobs.all_docs()
        if doc["thread_id"] == "thread-1" and doc["checkpoint_ns"] == "ns-1"
    )
    assert scoped_blobs == [("alpha", "v1"), ("legacy", "legacy-v1"), ("partial", None)]

    assert sorted(
        (doc["channel"], doc["version"])
        for doc in collections.blobs.all_docs()
        if doc["thread_id"] == "thread-1" and doc["checkpoint_ns"] == "ns-2"
    ) == [("adjacent", "keep")]
