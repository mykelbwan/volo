"""
AsyncMongoDBSaver – async LangGraph BaseCheckpointSaver backed by Motor.

This is the async counterpart to MongoDBSaver. Use it for async graph
execution to avoid blocking the event loop.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, AsyncIterator, Iterator, Optional, Sequence

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
    get_checkpoint_metadata,
)
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ASCENDING

from core.database.mongodb_async import AsyncMongoDB

_COL_CHECKPOINTS = "lg_checkpoints"
_COL_BLOBS = "lg_checkpoint_blobs"
_COL_WRITES = "lg_checkpoint_writes"

_DB_NAME = "auraagent"

logger = logging.getLogger(__name__)


class AsyncMongoDBSaver(BaseCheckpointSaver):
    """
    Async checkpointer backed by Motor (non-blocking).

    Sync methods are supported only when called outside a running event loop.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._db = AsyncMongoDB.get_db(_DB_NAME)
        self._checkpoints: AsyncIOMotorCollection = self._db[_COL_CHECKPOINTS]
        self._blobs: AsyncIOMotorCollection = self._db[_COL_BLOBS]
        self._writes: AsyncIOMotorCollection = self._db[_COL_WRITES]
        self._indexes_ready = False
        self._index_lock: Optional[asyncio.Lock] = None

    # ── Index setup ────────────────────────────────────────────────────────

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        if self._index_lock is None:
            self._index_lock = asyncio.Lock()

        async with self._index_lock:
            if self._indexes_ready:
                return
            await self._checkpoints.create_index(
                [
                    ("thread_id", ASCENDING),
                    ("checkpoint_ns", ASCENDING),
                    ("checkpoint_id", ASCENDING),
                ],
                unique=True,
                name="idx_checkpoint_pk",
            )
            await self._checkpoints.create_index(
                [
                    ("thread_id", ASCENDING),
                    ("checkpoint_ns", ASCENDING),
                ],
                name="idx_checkpoint_list",
            )
            await self._blobs.create_index(
                [
                    ("thread_id", ASCENDING),
                    ("checkpoint_ns", ASCENDING),
                    ("channel", ASCENDING),
                    ("version", ASCENDING),
                ],
                unique=True,
                name="idx_blob_pk",
            )
            await self._ensure_unique_write_index()
            self._indexes_ready = True

    async def _repair_duplicate_write_rows(self) -> None:
        cursor = self._writes.aggregate(
            [
                {"$sort": {"_id": 1}},
                {
                    "$group": {
                        "_id": {
                            "thread_id": "$thread_id",
                            "checkpoint_ns": "$checkpoint_ns",
                            "checkpoint_id": "$checkpoint_id",
                            "task_id": "$task_id",
                            "idx": "$idx",
                        },
                        "ids": {"$push": "$_id"},
                        "count": {"$sum": 1},
                    }
                },
                {"$match": {"count": {"$gt": 1}}},
            ]
        )
        for doc in await cursor.to_list(length=None):
            ids = doc.get("ids") or []
            if len(ids) <= 1:
                continue
            await self._writes.delete_many({"_id": {"$in": ids[:-1]}})

    async def _ensure_unique_write_index(self) -> None:
        spec = [
            ("thread_id", ASCENDING),
            ("checkpoint_ns", ASCENDING),
            ("checkpoint_id", ASCENDING),
            ("task_id", ASCENDING),
            ("idx", ASCENDING),
        ]
        try:
            await self._writes.create_index(
                spec,
                name="idx_write_pk",
                unique=True,
            )
        except Exception as exc:
            if "duplicate key" not in str(exc).lower():
                raise
            await self._repair_duplicate_write_rows()
            await self._writes.create_index(
                spec,
                name="idx_write_pk",
                unique=True,
            )

    # ── Version generator ────────────────────────────────────────────────────

    def get_next_version(self, current: Optional[str], channel: Any) -> str:  # type: ignore[override]
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(str(current).split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"

    # ── Internal helpers ────────────────────────────────────────────────────

    async def _load_checkpoint_channel_versions(
        self,
        doc: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        try:
            raw_checkpoint: Checkpoint = self.serde.loads_typed(
                (doc["type"], bytes(doc["checkpoint"]))
            )
        except Exception:
            return None

        if not isinstance(raw_checkpoint, dict):
            return None

        channel_versions = raw_checkpoint.get("channel_versions")
        if not isinstance(channel_versions, dict):
            return None
        return channel_versions

    async def _prune_checkpoints(self, thread_id: str, checkpoint_ns: str) -> None:
        scope = {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}
        newest_doc = await self._checkpoints.find_one(
            scope,
            projection={"checkpoint_id": 1, "type": 1, "checkpoint": 1},
            sort=[("checkpoint_id", -1)],
        )
        if newest_doc is None:
            return

        kept_checkpoint_id = newest_doc.get("checkpoint_id")
        if kept_checkpoint_id is None:
            return

        channel_versions = await self._load_checkpoint_channel_versions(newest_doc)
        if channel_versions is None:
            return

        referenced_pairs: set[tuple[str, Any]] = set()
        for channel, version in channel_versions.items():
            try:
                referenced_pairs.add((channel, version))
            except TypeError:
                return

        stale_docs = await self._checkpoints.find(
            scope,
            projection={"checkpoint_id": 1},
        ).to_list(length=None)
        stale_checkpoint_ids: list[str] = []
        for doc in stale_docs:
            checkpoint_id = doc.get("checkpoint_id")
            if checkpoint_id is None or checkpoint_id == kept_checkpoint_id:
                continue
            stale_checkpoint_ids.append(checkpoint_id)

        if stale_checkpoint_ids:
            delete_filter = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": {"$in": stale_checkpoint_ids},
            }
            await self._checkpoints.delete_many(delete_filter)
            await self._writes.delete_many(delete_filter)

        blob_docs = await self._blobs.find(
            scope,
            projection={"channel": 1, "version": 1},
        ).to_list(length=None)
        stale_blob_filters: list[dict[str, Any]] = []
        for doc in blob_docs:
            if "channel" not in doc or "version" not in doc:
                continue
            pair = (doc["channel"], doc["version"])
            if pair in referenced_pairs:
                continue
            # Only delete blobs with explicit pair fields to avoid matching
            # malformed legacy rows too broadly.
            stale_blob_filters.append(
                {"channel": doc["channel"], "version": doc["version"]}
            )

        if stale_blob_filters:
            await self._blobs.delete_many(
                {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "$or": stale_blob_filters,
                }
            )

    async def _load_blobs(
        self,
        thread_id: str,
        checkpoint_ns: str,
        channel_versions: dict[str, Any],
    ) -> dict[str, Any]:
        if not channel_versions:
            return {}

        or_clauses = [
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "channel": channel,
                "version": version,
            }
            for channel, version in channel_versions.items()
        ]

        cursor = self._blobs.find({"$or": or_clauses})
        docs = await cursor.to_list(length=None)

        channel_values: dict[str, Any] = {}
        for doc in docs:
            blob_type: str = doc["type"]
            blob_data: bytes = bytes(doc["blob"])
            if blob_type != "empty":
                channel_values[doc["channel"]] = self.serde.loads_typed(
                    (blob_type, blob_data)
                )
        return channel_values

    async def _load_writes(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> list[tuple[str, str, Any]]:
        cursor = self._writes.find(
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            },
            sort=[("task_id", ASCENDING), ("idx", ASCENDING), ("_id", -1)],
        )
        docs = await cursor.to_list(length=None)
        pending: list[tuple[str, str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for doc in docs:
            key = (str(doc["task_id"]), int(doc["idx"]))
            if key in seen:
                continue
            seen.add(key)
            value = self.serde.loads_typed((doc["type"], bytes(doc["value"])))
            pending.append((doc["task_id"], doc["channel"], value))
        return pending

    async def _doc_to_checkpoint_tuple(
        self,
        doc: dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> CheckpointTuple:
        thread_id: str = doc["thread_id"]
        checkpoint_ns: str = doc["checkpoint_ns"]
        checkpoint_id: str = doc["checkpoint_id"]
        parent_checkpoint_id: Optional[str] = doc.get("parent_checkpoint_id")

        raw_checkpoint: Checkpoint = self.serde.loads_typed(
            (doc["type"], bytes(doc["checkpoint"]))
        )
        metadata: CheckpointMetadata = self.serde.loads_typed(
            (doc["metadata_type"], bytes(doc["metadata"]))
        )

        raw_checkpoint["channel_values"] = await self._load_blobs(
            thread_id, checkpoint_ns, raw_checkpoint.get("channel_versions", {})
        )

        pending_writes = await self._load_writes(
            thread_id, checkpoint_ns, checkpoint_id
        )

        resolved_config: RunnableConfig = config or {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

        parent_config: Optional[RunnableConfig] = (
            {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": parent_checkpoint_id,
                }
            }
            if parent_checkpoint_id
            else None
        )

        return CheckpointTuple(
            config=resolved_config,
            checkpoint=raw_checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes,
        )

    # ── Sync wrappers (for non-async contexts only) ─────────────────────────

    def _run_sync(self, async_fn, *args, **kwargs):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(async_fn(*args, **kwargs))
        raise RuntimeError(
            "AsyncMongoDBSaver sync method called from a running event loop. "
            "Use the async variants instead. Recovery path: call aget_tuple/alist/aput/aput_writes/adelete_thread."
        )

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        return self._run_sync(self.aget_tuple, config)

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        return iter(self._run_sync(self._alist_to_list, config, filter, before, limit))

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return self._run_sync(self.aput, config, checkpoint, metadata, new_versions)

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        return self._run_sync(self.aput_writes, config, writes, task_id, task_path)

    def delete_thread(self, thread_id: str) -> None:
        return self._run_sync(self.adelete_thread, thread_id)

    # ── Async implementations ────────────────────────────────────────────────

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        await self._ensure_indexes()
        cfg = config.get("configurable") or {}
        thread_id: str = cfg["thread_id"]
        checkpoint_ns: str = cfg.get("checkpoint_ns", "")

        if checkpoint_id := get_checkpoint_id(config):
            doc = await self._checkpoints.find_one(
                {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            )
            if doc is None:
                return None
            return await self._doc_to_checkpoint_tuple(doc, config)

        doc = await self._checkpoints.find_one(
            {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns},
            sort=[("checkpoint_id", -1)],
        )
        if doc is None:
            return None
        return await self._doc_to_checkpoint_tuple(doc)

    async def _alist_to_list(
        self,
        config: Optional[RunnableConfig],
        filter: Optional[dict[str, Any]],
        before: Optional[RunnableConfig],
        limit: Optional[int],
    ) -> list[CheckpointTuple]:
        await self._ensure_indexes()
        query: dict[str, Any] = {}

        if config is not None:
            cfg = config.get("configurable") or {}
            query["thread_id"] = cfg["thread_id"]
            if checkpoint_ns := cfg.get("checkpoint_ns"):
                query["checkpoint_ns"] = checkpoint_ns
            if checkpoint_id := get_checkpoint_id(config):
                query["checkpoint_id"] = checkpoint_id

        if before is not None:
            if before_id := get_checkpoint_id(before):
                query["checkpoint_id"] = {"$lt": before_id}

        cursor = self._checkpoints.find(query, sort=[("checkpoint_id", -1)])
        if limit is not None:
            cursor = cursor.limit(limit)
        docs = await cursor.to_list(length=limit or None)

        items: list[CheckpointTuple] = []
        for doc in docs:
            if filter:
                metadata = self.serde.loads_typed(
                    (doc["metadata_type"], bytes(doc["metadata"]))
                )
                if not all(metadata.get(k) == v for k, v in filter.items()):
                    continue
            items.append(await self._doc_to_checkpoint_tuple(doc))
        return items

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        items = await self._alist_to_list(config, filter, before, limit)
        for item in items:
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        await self._ensure_indexes()
        cfg = config.get("configurable") or {}
        thread_id: str = cfg["thread_id"]
        checkpoint_ns: str = cfg.get("checkpoint_ns", "")
        parent_checkpoint_id: Optional[str] = cfg.get("checkpoint_id")

        c = checkpoint.copy()
        channel_values: dict[str, Any] = c.pop("channel_values", {})  # type: ignore[misc]

        for channel, version in new_versions.items():
            if channel in channel_values:
                blob_type, blob_data = self.serde.dumps_typed(channel_values[channel])
            else:
                blob_type, blob_data = "empty", b""

            await self._blobs.update_one(
                {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "channel": channel,
                    "version": version,
                },
                {
                    "$set": {
                        "type": blob_type,
                        "blob": blob_data,
                    }
                },
                upsert=True,
            )

        chk_type, chk_bytes = self.serde.dumps_typed(c)
        merged_metadata = get_checkpoint_metadata(config, metadata)
        meta_type, meta_bytes = self.serde.dumps_typed(merged_metadata)

        await self._checkpoints.update_one(
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            },
            {
                "$set": {
                    "parent_checkpoint_id": parent_checkpoint_id,
                    "type": chk_type,
                    "checkpoint": chk_bytes,
                    "metadata_type": meta_type,
                    "metadata": meta_bytes,
                }
            },
            upsert=True,
        )
        try:
            await self._prune_checkpoints(thread_id, checkpoint_ns)
        except Exception:
            logger.exception(
                "Failed to prune LangGraph checkpoints for thread_id=%s checkpoint_ns=%s",
                thread_id,
                checkpoint_ns,
            )

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await self._ensure_indexes()
        cfg = config.get("configurable") or {}
        thread_id: str = cfg["thread_id"]
        checkpoint_ns: str = cfg.get("checkpoint_ns", "")
        checkpoint_id: str = cfg["checkpoint_id"]

        for raw_idx, (channel, value) in enumerate(writes):
            idx: int = WRITES_IDX_MAP.get(channel, raw_idx)
            val_type, val_bytes = self.serde.dumps_typed(value)

            filter_doc = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "task_id": task_id,
                "idx": idx,
            }

            if idx >= 0:
                await self._writes.update_one(
                    filter_doc,
                    {
                        "$setOnInsert": {
                            "channel": channel,
                            "type": val_type,
                            "value": val_bytes,
                            "task_path": task_path,
                        }
                    },
                    upsert=True,
                )
            else:
                await self._writes.update_one(
                    filter_doc,
                    {
                        "$set": {
                            "channel": channel,
                            "type": val_type,
                            "value": val_bytes,
                            "task_path": task_path,
                        }
                    },
                    upsert=True,
                )

    async def adelete_thread(self, thread_id: str) -> None:
        await self._ensure_indexes()
        await self._checkpoints.delete_many({"thread_id": thread_id})
        await self._blobs.delete_many({"thread_id": thread_id})
        await self._writes.delete_many({"thread_id": thread_id})
