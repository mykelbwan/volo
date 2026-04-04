import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from core.database.mongodb_saver import MongoDBSaver
from tests.unit.storage._mongodb_saver_fakes import (
    build_sync_saver,
    make_checkpoint,
    make_config,
    make_metadata,
)


class _DummyCollection(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


def test_mongodb_saver_creates_indexes_on_init():
    db = MagicMock()
    checkpoints = _DummyCollection()
    blobs = _DummyCollection()
    writes = _DummyCollection()

    def _getitem(name):
        if name == "lg_checkpoints":
            return checkpoints
        if name == "lg_checkpoint_blobs":
            return blobs
        if name == "lg_checkpoint_writes":
            return writes
        raise KeyError(name)

    db.__getitem__.side_effect = _getitem

    with patch("core.database.mongodb.MongoDB.get_db", return_value=db):
        _ = MongoDBSaver()

    assert checkpoints.create_index.called
    assert blobs.create_index.called
    assert writes.create_index.called
    assert any(
        kwargs.get("name") == "idx_write_pk" and kwargs.get("unique") is True
        for _args, kwargs in writes.create_index.call_args_list
    )


def test_mongodb_saver_async_wrappers_offload_to_thread():
    saver = object.__new__(MongoDBSaver)
    saver.get_tuple = MagicMock(return_value={"checkpoint": "ok"})
    saver.put = MagicMock(return_value={"configurable": {"thread_id": "t1"}})
    saver.put_writes = MagicMock(return_value=None)
    saver.delete_thread = MagicMock(return_value=None)
    to_thread = AsyncMock(
        side_effect=[
            {"checkpoint": "ok"},
            {"configurable": {"thread_id": "t1"}},
            None,
            None,
        ]
    )

    async def _run():
        with patch("core.database.mongodb_saver.asyncio.to_thread", to_thread):
            await saver.aget_tuple({"configurable": {"thread_id": "t1"}})
            await saver.aput(
                {"configurable": {"thread_id": "t1"}},
                {"id": "chk1"},
                {"source": "input"},
                {},
            )
            await saver.aput_writes(
                {"configurable": {"thread_id": "t1", "checkpoint_id": "chk1"}},
                [("channel", {"x": 1})],
                "task-1",
            )
            await saver.adelete_thread("t1")

    asyncio.run(_run())

    assert to_thread.await_count == 4
    saver.get_tuple.assert_not_called()
    saver.put.assert_not_called()
    saver.put_writes.assert_not_called()
    saver.delete_thread.assert_not_called()


def test_mongodb_saver_alist_offloads_to_thread():
    saver = object.__new__(MongoDBSaver)
    saver.list = MagicMock(return_value=iter([{"checkpoint": "ok"}]))
    to_thread = AsyncMock(return_value=[{"checkpoint": "ok"}])

    async def _run():
        with patch("core.database.mongodb_saver.asyncio.to_thread", to_thread):
            return [item async for item in saver.alist({"configurable": {"thread_id": "t1"}})]

    items = asyncio.run(_run())

    assert items == [{"checkpoint": "ok"}]
    to_thread.assert_awaited_once()
    saver.list.assert_not_called()


def test_mongodb_saver_load_writes_dedupes_legacy_duplicates():
    saver = object.__new__(MongoDBSaver)
    saver._writes = MagicMock()
    saver._writes.find.return_value = [
        {
            "task_id": "task-1",
            "idx": 0,
            "channel": "channel",
            "type": "json",
            "value": b"new",
        },
        {
            "task_id": "task-1",
            "idx": 0,
            "channel": "channel",
            "type": "json",
            "value": b"old",
        },
        {
            "task_id": "task-2",
            "idx": 1,
            "channel": "other",
            "type": "json",
            "value": b"value",
        },
    ]
    saver.serde = MagicMock()
    saver.serde.loads_typed.side_effect = ["new-value", "value"]

    pending = saver._load_writes("thread-1", "", "chk-1")

    assert pending == [
        ("task-1", "channel", "new-value"),
        ("task-2", "other", "value"),
    ]


def test_mongodb_saver_put_prunes_old_checkpoint_data_without_touching_adjacent_scope():
    saver, collections = build_sync_saver()

    base_config = make_config("thread-1", "ns-1")
    other_thread_config = make_config("thread-2", "ns-1")
    other_ns_config = make_config("thread-1", "ns-2")

    checkpoint_1 = make_checkpoint(
        "001",
        channel_versions={"alpha": "v1"},
        channel_values={"alpha": {"step": 1}},
    )
    cfg_1 = saver.put(
        base_config,
        checkpoint_1,
        make_metadata(),
        checkpoint_1["channel_versions"],
    )
    saver.put_writes(cfg_1, [("pending", {"write": "old-1"})], "task-1")

    checkpoint_2 = make_checkpoint(
        "002",
        channel_versions={"beta": "v2"},
        channel_values={"beta": {"step": 2}},
    )
    cfg_2 = saver.put(
        base_config,
        checkpoint_2,
        make_metadata(),
        checkpoint_2["channel_versions"],
    )
    saver.put_writes(cfg_2, [("pending", {"write": "old-2"})], "task-2")

    other_thread_checkpoint = make_checkpoint(
        "100",
        channel_versions={"adjacent": "thread-keep"},
        channel_values={"adjacent": {"thread": True}},
    )
    other_thread_cfg = saver.put(
        other_thread_config,
        other_thread_checkpoint,
        make_metadata(),
        other_thread_checkpoint["channel_versions"],
    )
    saver.put_writes(
        other_thread_cfg,
        [("pending", {"write": "thread-keep"})],
        "task-thread",
    )

    other_ns_checkpoint = make_checkpoint(
        "100",
        channel_versions={"adjacent": "ns-keep"},
        channel_values={"adjacent": {"ns": True}},
    )
    other_ns_cfg = saver.put(
        other_ns_config,
        other_ns_checkpoint,
        make_metadata(),
        other_ns_checkpoint["channel_versions"],
    )
    saver.put_writes(other_ns_cfg, [("pending", {"write": "ns-keep"})], "task-ns")

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
    cfg_3 = saver.put(
        base_config,
        checkpoint_3,
        make_metadata(),
        checkpoint_3["channel_versions"],
    )
    saver.put_writes(cfg_3, [("pending", {"write": "latest"})], "task-3")

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

    assert sorted(
        (doc["channel"], doc["version"])
        for doc in collections.blobs.all_docs()
        if doc["thread_id"] == "thread-2" and doc["checkpoint_ns"] == "ns-1"
    ) == [("adjacent", "thread-keep")]
    assert sorted(
        (doc["channel"], doc["version"])
        for doc in collections.blobs.all_docs()
        if doc["thread_id"] == "thread-1" and doc["checkpoint_ns"] == "ns-2"
    ) == [("adjacent", "ns-keep")]

    latest_tuple = saver.get_tuple(make_config("thread-1", "ns-1"))
    assert latest_tuple is not None
    assert latest_tuple.checkpoint["id"] == "003"
    assert latest_tuple.checkpoint["channel_values"] == {
        "delta": {"step": "latest"},
        "gamma": {"step": 3},
    }


def test_mongodb_saver_pruning_uses_checkpoint_id_order_and_is_idempotent():
    saver, collections = build_sync_saver()
    config = make_config("thread-1", "ns-1")

    highest_checkpoint = make_checkpoint(
        "zzz",
        channel_versions={"alpha": "keep"},
        channel_values={"alpha": {"winner": True}},
    )
    saver.put(
        config,
        highest_checkpoint,
        make_metadata(),
        highest_checkpoint["channel_versions"],
    )

    lower_checkpoint = make_checkpoint(
        "aaa",
        channel_versions={"beta": "remove"},
        channel_values={"beta": {"winner": False}},
    )
    saver.put(
        config,
        lower_checkpoint,
        make_metadata(),
        lower_checkpoint["channel_versions"],
    )

    before = {
        "checkpoints": collections.checkpoints.all_docs(),
        "blobs": collections.blobs.all_docs(),
        "writes": collections.writes.all_docs(),
    }

    saver._prune_checkpoints("thread-1", "ns-1")

    after = {
        "checkpoints": collections.checkpoints.all_docs(),
        "blobs": collections.blobs.all_docs(),
        "writes": collections.writes.all_docs(),
    }

    assert [doc["checkpoint_id"] for doc in before["checkpoints"]] == ["zzz"]
    assert sorted(
        (doc["channel"], doc["version"]) for doc in before["blobs"]
    ) == [("alpha", "keep")]
    assert after == before


def test_mongodb_saver_put_keeps_checkpoint_when_prune_cleanup_fails():
    saver, _collections = build_sync_saver()
    checkpoint = make_checkpoint(
        "001",
        channel_versions={"alpha": "v1"},
        channel_values={"alpha": {"step": 1}},
    )

    with patch.object(
        saver,
        "_prune_checkpoints",
        side_effect=RuntimeError("prune failed"),
    ):
        returned_config = saver.put(
            make_config("thread-1", "ns-1"),
            checkpoint,
            make_metadata(),
            checkpoint["channel_versions"],
        )

    loaded = saver.get_tuple(returned_config)

    assert loaded is not None
    assert loaded.checkpoint["id"] == "001"
    assert loaded.checkpoint["channel_values"] == {"alpha": {"step": 1}}


def test_mongodb_saver_prune_aborts_without_deletes_for_malformed_newest_checkpoint():
    saver, collections = build_sync_saver()

    old_checkpoint = make_checkpoint(
        "001",
        channel_versions={"alpha": "v1"},
        channel_values={"alpha": {"step": 1}},
    )
    old_cfg = saver.put(
        make_config("thread-1", "ns-1"),
        old_checkpoint,
        make_metadata(),
        old_checkpoint["channel_versions"],
    )
    saver.put_writes(old_cfg, [("pending", {"write": "old"})], "task-old")

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

    saver._prune_checkpoints("thread-1", "ns-1")

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
