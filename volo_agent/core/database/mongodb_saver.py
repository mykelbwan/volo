"""
MongoDBSaver – a LangGraph BaseCheckpointSaver backed by MongoDB.

Uses the application-wide ``MongoDB`` singleton (``core.database.mongodb``)
so that all checkpointer I/O shares the same ``MongoClient`` connection pool
as the rest of the application.  No second client is ever created.

Three collections are used (all in the existing "auraagent" database):

  lg_checkpoints        – one doc per (thread_id, checkpoint_ns, checkpoint_id)
  lg_checkpoint_blobs   – one doc per (thread_id, checkpoint_ns, channel, version)
  lg_checkpoint_writes  – one doc per pending node write

This mirrors the InMemorySaver storage layout but persists across process
restarts, enabling event-driven execution and multi-instance deployments.
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
from pymongo import ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from core.database.mongodb import MongoDB

# ── Collection name constants ────────────────────────────────────────────────
_COL_CHECKPOINTS = "lg_checkpoints"
_COL_BLOBS = "lg_checkpoint_blobs"
_COL_WRITES = "lg_checkpoint_writes"

_DB_NAME = "auraagent"

logger = logging.getLogger(__name__)


class MongoDBSaver(BaseCheckpointSaver):
    """
    Persistent LangGraph checkpointer backed by MongoDB (pymongo).

    Usage
    -----
    Replace ``MemorySaver`` in graph.py::

        from core.database.mongodb_saver import MongoDBSaver
        checkpointer = MongoDBSaver()
        app = workflow.compile(checkpointer=checkpointer)

    The saver creates all necessary indexes on first instantiation.  It
    reuses the shared ``MongoDB`` singleton so no additional ``MongoClient``
    connections are opened.
    """

    _db: Database
    _checkpoints: Collection
    _blobs: Collection
    _writes: Collection

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Reuse the application-wide singleton — no new MongoClient created.
        self._db = MongoDB.get_db(_DB_NAME)
        self._checkpoints = self._db[_COL_CHECKPOINTS]
        self._blobs = self._db[_COL_BLOBS]
        self._writes = self._db[_COL_WRITES]
        self._ensure_indexes()

    # ── Index setup ──────────────────────────────────────────────────────────

    def _ensure_indexes(self) -> None:
        """Create compound indexes for efficient lookups (idempotent)."""
        # Checkpoint lookup by (thread, ns, id) – unique primary key
        self._checkpoints.create_index(
            [
                ("thread_id", ASCENDING),
                ("checkpoint_ns", ASCENDING),
                ("checkpoint_id", ASCENDING),
            ],
            unique=True,
            name="idx_checkpoint_pk",
        )
        # List queries need thread + ns + id sorted descending
        self._checkpoints.create_index(
            [
                ("thread_id", ASCENDING),
                ("checkpoint_ns", ASCENDING),
            ],
            name="idx_checkpoint_list",
        )
        # Blob lookup by (thread, ns, channel, version)
        self._blobs.create_index(
            [
                ("thread_id", ASCENDING),
                ("checkpoint_ns", ASCENDING),
                ("channel", ASCENDING),
                ("version", ASCENDING),
            ],
            unique=True,
            name="idx_blob_pk",
        )
        # Write lookup by (thread, ns, checkpoint_id)
        self._ensure_unique_write_index()

    def _repair_duplicate_write_rows(self) -> None:
        duplicates = self._writes.aggregate(
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
        for doc in duplicates:
            ids = doc.get("ids") or []
            if len(ids) <= 1:
                continue
            self._writes.delete_many({"_id": {"$in": ids[:-1]}})

    def _ensure_unique_write_index(self) -> None:
        spec = [
            ("thread_id", ASCENDING),
            ("checkpoint_ns", ASCENDING),
            ("checkpoint_id", ASCENDING),
            ("task_id", ASCENDING),
            ("idx", ASCENDING),
        ]
        try:
            self._writes.create_index(
                spec,
                name="idx_write_pk",
                unique=True,
            )
        except Exception as exc:
            if "duplicate key" not in str(exc).lower():
                raise
            self._repair_duplicate_write_rows()
            self._writes.create_index(
                spec,
                name="idx_write_pk",
                unique=True,
            )

    def _load_write_docs(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> list[dict[str, Any]]:
        return list(
            self._writes.find(
                {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                },
                sort=[("task_id", ASCENDING), ("idx", ASCENDING), ("_id", -1)],
            )
        )

    def _load_checkpoint_channel_versions(
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

    def _prune_checkpoints(self, thread_id: str, checkpoint_ns: str) -> None:
        scope = {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}
        newest_doc = self._checkpoints.find_one(
            scope,
            projection={"checkpoint_id": 1, "type": 1, "checkpoint": 1},
            sort=[("checkpoint_id", -1)],
        )
        if newest_doc is None:
            return

        kept_checkpoint_id = newest_doc.get("checkpoint_id")
        if kept_checkpoint_id is None:
            return

        channel_versions = self._load_checkpoint_channel_versions(newest_doc)
        if channel_versions is None:
            return

        referenced_pairs: set[tuple[str, Any]] = set()
        for channel, version in channel_versions.items():
            try:
                referenced_pairs.add((channel, version))
            except TypeError:
                return

        stale_checkpoint_ids: list[str] = []
        for doc in self._checkpoints.find(scope, projection={"checkpoint_id": 1}):
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
            self._checkpoints.delete_many(delete_filter)
            self._writes.delete_many(delete_filter)

        stale_blob_filters: list[dict[str, Any]] = []
        for doc in self._blobs.find(scope, projection={"channel": 1, "version": 1}):
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
            self._blobs.delete_many(
                {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "$or": stale_blob_filters,
                }
            )

    # ── Version generator ────────────────────────────────────────────────────

    def get_next_version(self, current: Optional[str], channel: Any) -> str:  # type: ignore[override]
        """Produce a monotonically increasing version string."""
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(str(current).split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _load_blobs(
        self,
        thread_id: str,
        checkpoint_ns: str,
        channel_versions: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Reconstruct channel_values for a checkpoint by fetching blobs from
        MongoDB that correspond to the channel versions stored in the checkpoint.
        """
        if not channel_versions:
            return {}

        # Build a query to fetch all relevant blobs in one round-trip
        or_clauses = [
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "channel": channel,
                "version": version,
            }
            for channel, version in channel_versions.items()
        ]

        blobs_cursor = self._blobs.find({"$or": or_clauses})

        channel_values: dict[str, Any] = {}
        for doc in blobs_cursor:
            blob_type: str = doc["type"]
            blob_data: bytes = bytes(doc["blob"])  # BSON Binary → bytes
            if blob_type != "empty":
                channel_values[doc["channel"]] = self.serde.loads_typed(
                    (blob_type, blob_data)
                )
        return channel_values

    def _load_writes(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> list[tuple[str, str, Any]]:
        """Fetch pending writes for a given checkpoint."""
        pending: list[tuple[str, str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for doc in self._load_write_docs(thread_id, checkpoint_ns, checkpoint_id):
            key = (str(doc["task_id"]), int(doc["idx"]))
            if key in seen:
                continue
            seen.add(key)
            value = self.serde.loads_typed((doc["type"], bytes(doc["value"])))
            pending.append((doc["task_id"], doc["channel"], value))
        return pending

    def _doc_to_checkpoint_tuple(
        self,
        doc: dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> CheckpointTuple:
        """Convert a MongoDB document into a CheckpointTuple."""
        thread_id: str = doc["thread_id"]
        checkpoint_ns: str = doc["checkpoint_ns"]
        checkpoint_id: str = doc["checkpoint_id"]
        parent_checkpoint_id: Optional[str] = doc.get("parent_checkpoint_id")

        # Deserialise checkpoint + metadata
        raw_checkpoint: Checkpoint = self.serde.loads_typed(
            (doc["type"], bytes(doc["checkpoint"]))
        )
        metadata: CheckpointMetadata = self.serde.loads_typed(
            (doc["metadata_type"], bytes(doc["metadata"]))
        )

        # Rehydrate channel values from blob store
        raw_checkpoint["channel_values"] = self._load_blobs(
            thread_id, checkpoint_ns, raw_checkpoint.get("channel_versions", {})
        )

        # Pending writes
        pending_writes = self._load_writes(thread_id, checkpoint_ns, checkpoint_id)

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

    # ── BaseCheckpointSaver interface ────────────────────────────────────────

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """
        Retrieve the latest (or specific) checkpoint for a thread.

        If config contains a ``checkpoint_id`` key, that exact checkpoint is
        returned; otherwise the checkpoint with the lexicographically greatest
        checkpoint_id (i.e. the most recent) is returned.
        """
        cfg = config.get("configurable") or {}
        thread_id: str = cfg["thread_id"]
        checkpoint_ns: str = cfg.get("checkpoint_ns", "")

        if checkpoint_id := get_checkpoint_id(config):
            doc = self._checkpoints.find_one(
                {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            )
            if doc is None:
                return None
            return self._doc_to_checkpoint_tuple(doc, config)

        # Latest checkpoint for this thread+ns (sort by checkpoint_id desc)
        doc = self._checkpoints.find_one(
            {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns},
            sort=[("checkpoint_id", -1)],
        )
        if doc is None:
            return None
        return self._doc_to_checkpoint_tuple(doc)

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        """
        Yield CheckpointTuples matching the given criteria, newest first.
        """
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

        for doc in cursor:
            # Apply metadata filter if provided
            if filter:
                metadata = self.serde.loads_typed(
                    (doc["metadata_type"], bytes(doc["metadata"]))
                )
                if not all(metadata.get(k) == v for k, v in filter.items()):
                    continue
            yield self._doc_to_checkpoint_tuple(doc)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """
        Persist a checkpoint to MongoDB.

        Channel values are stripped from the checkpoint and stored separately
        in ``lg_checkpoint_blobs`` (one document per channel version) to keep
        checkpoint documents small and queries fast.
        """
        cfg = config.get("configurable") or {}
        thread_id: str = cfg["thread_id"]
        checkpoint_ns: str = cfg.get("checkpoint_ns", "")
        parent_checkpoint_id: Optional[str] = cfg.get("checkpoint_id")

        # Pop channel_values before serialising the checkpoint document
        c = checkpoint.copy()
        channel_values: dict[str, Any] = c.pop("channel_values", {})  # type: ignore[misc]

        # ── Upsert blobs ─────────────────────────────────────────────────────
        for channel, version in new_versions.items():
            if channel in channel_values:
                blob_type, blob_data = self.serde.dumps_typed(channel_values[channel])
            else:
                blob_type, blob_data = "empty", b""

            self._blobs.update_one(
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

        # ── Serialise checkpoint + metadata ──────────────────────────────────
        chk_type, chk_bytes = self.serde.dumps_typed(c)
        merged_metadata = get_checkpoint_metadata(config, metadata)
        meta_type, meta_bytes = self.serde.dumps_typed(merged_metadata)

        # ── Upsert checkpoint document ────────────────────────────────────────
        self._checkpoints.update_one(
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
            self._prune_checkpoints(thread_id, checkpoint_ns)
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

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        Persist a list of intermediate node writes for a given checkpoint.
        Writes are idempotent: a write with the same (task_id, idx) pair is
        never overwritten unless it has a negative index (special channels).
        """
        cfg = config.get("configurable") or {}
        thread_id: str = cfg["thread_id"]
        checkpoint_ns: str = cfg.get("checkpoint_ns", "")
        checkpoint_id: str = cfg["checkpoint_id"]

        for raw_idx, (channel, value) in enumerate(writes):
            # Map special channels to reserved negative indices
            idx: int = WRITES_IDX_MAP.get(channel, raw_idx)
            val_type, val_bytes = self.serde.dumps_typed(value)

            filter_doc = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "task_id": task_id,
                "idx": idx,
            }

            # Positive-index writes are immutable once set; negative (special)
            # indices like __interrupt__ are always overwritten.
            if idx >= 0:
                self._writes.update_one(
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
                self._writes.update_one(
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

    # ── Async wrappers (run sync methods – suitable for I/O-light use) ───────

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        items = await asyncio.to_thread(
            lambda: list(self.list(config, filter=filter, before=before, limit=limit))
        )
        for item in items:
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return await asyncio.to_thread(
            self.put, config, checkpoint, metadata, new_versions
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await asyncio.to_thread(self.put_writes, config, writes, task_id, task_path)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def delete_thread(self, thread_id: str) -> None:
        """Remove all checkpoint data for a thread (useful in tests)."""
        self._checkpoints.delete_many({"thread_id": thread_id})
        self._blobs.delete_many({"thread_id": thread_id})
        self._writes.delete_many({"thread_id": thread_id})

    async def adelete_thread(self, thread_id: str) -> None:
        await asyncio.to_thread(self.delete_thread, thread_id)

    def close(self) -> None:
        """
        No-op: this saver uses the shared ``MongoDB`` singleton whose
        lifecycle is managed at the application level via ``MongoDB.close()``.
        Closing it here would break every other component in the process.
        """
