from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ASCENDING, IndexModel

from core.database.mongodb_async import AsyncMongoDB

_COLLECTION_NAME = "intent_triggers"
_DEFAULT_TTL_DAYS = 7
_TERMINAL_STATE_RETENTION_DAYS = {
    "triggered": 1,
    "failed": 3,
    "cancelled": 3,
    "expired": 3,
}


class TriggerRegistry:
    def __init__(self) -> None:
        db = AsyncMongoDB.get_db()
        self.collection: AsyncIOMotorCollection = db[_COLLECTION_NAME]
        self._indexes_ready = False
        self._index_lock: Optional[asyncio.Lock] = None

    @staticmethod
    def _terminal_retention_days(status: str) -> int:
        return _TERMINAL_STATE_RETENTION_DAYS[status]

    @classmethod
    def _terminal_cleanup_after(cls, status: str, now: datetime) -> datetime:
        return now + timedelta(days=cls._terminal_retention_days(status))

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return

        if self._index_lock is None:
            self._index_lock = asyncio.Lock()

        async with self._index_lock:
            if self._indexes_ready:
                return

            await self.collection.create_indexes(
                [
                    # Primary key
                    IndexModel(
                        [("trigger_id", ASCENDING)], unique=True, name="idx_trigger_id"
                    ),
                    # Observer fast path: fetch all pending price triggers
                    IndexModel(
                        [("status", ASCENDING), ("trigger_condition.type", ASCENDING)],
                        name="idx_status_type",
                    ),
                    # User-facing history queries
                    IndexModel(
                        [("user_id", ASCENDING), ("status", ASCENDING)],
                        name="idx_user_status",
                    ),
                    # Thread resume lookup
                    IndexModel([("thread_id", ASCENDING)], name="idx_thread_id"),
                    # Delayed cleanup for terminal triggers only. Pending
                    # records stay ineligible because cleanup_after is not a date.
                    IndexModel(
                        [("cleanup_after", ASCENDING)],
                        expireAfterSeconds=0,
                        partialFilterExpression={"cleanup_after": {"$type": "date"}},
                        name="ttl_cleanup_after",
                    ),
                ]
            )
            self._indexes_ready = True

    async def register_trigger(
        self,
        user_id: str,
        thread_id: str,
        trigger_condition: dict[str, Any],
        payload: dict[str, Any],
        ttl_days: int = _DEFAULT_TTL_DAYS,
    ) -> str:
        await self._ensure_indexes()
        now = datetime.now(tz=timezone.utc)
        trigger_id = str(uuid.uuid4())

        doc = {
            "trigger_id": trigger_id,
            "user_id": user_id,
            "thread_id": thread_id,
            "trigger_condition": trigger_condition,
            "status": "pending",
            "payload": payload,
            "created_at": now.isoformat(),
            "expires_at": (now + timedelta(days=ttl_days)).isoformat(),
            "cleanup_after": None,
            "triggered_at": None,
            "error": None,
            "fire_count": 0,
        }

        await self.collection.insert_one(doc)
        return trigger_id

    async def mark_triggered(self, trigger_id: str) -> bool:
        await self._ensure_indexes()
        now = datetime.now(tz=timezone.utc)
        result = await self.collection.update_one(
            {"trigger_id": trigger_id, "status": "pending"},
            {
                "$set": {
                    "status": "triggered",
                    "triggered_at": now.isoformat(),
                    "cleanup_after": self._terminal_cleanup_after("triggered", now),
                },
                "$inc": {"fire_count": 1},
            },
        )
        return result.modified_count > 0

    async def mark_triggered_or_reschedule(
        self, trigger_id: str, next_execute_at: Optional[str]
    ) -> bool:
        await self._ensure_indexes()
        now = datetime.now(tz=timezone.utc).isoformat()
        if next_execute_at:
            result = await self.collection.update_one(
                {"trigger_id": trigger_id, "status": "pending"},
                {
                    "$set": {
                        "triggered_at": now,
                        "trigger_condition.execute_at": next_execute_at,
                    },
                    "$inc": {"fire_count": 1},
                },
            )
            return result.modified_count > 0

        return await self.mark_triggered(trigger_id)

    async def mark_failed(self, trigger_id: str, error: str) -> None:
        await self._ensure_indexes()
        now = datetime.now(tz=timezone.utc)
        await self.collection.update_one(
            {"trigger_id": trigger_id},
            {
                "$set": {
                    "status": "failed",
                    "error": error,
                    "triggered_at": now.isoformat(),
                    "cleanup_after": self._terminal_cleanup_after("failed", now),
                }
            },
        )

    async def cancel_trigger(
        self, trigger_id: str, user_id: Optional[str] = None
    ) -> bool:
        await self._ensure_indexes()
        now = datetime.now(tz=timezone.utc)
        query: dict[str, Any] = {"trigger_id": trigger_id, "status": "pending"}
        if user_id:
            query["user_id"] = user_id
        result = await self.collection.update_one(
            query,
            {
                "$set": {
                    "status": "cancelled",
                    "cancelled_at": now.isoformat(),
                    "error": "user_cancelled",
                    "cleanup_after": self._terminal_cleanup_after("cancelled", now),
                }
            },
        )
        return result.modified_count > 0

    async def expire_old_triggers(self) -> int:
        await self._ensure_indexes()
        now = datetime.now(tz=timezone.utc)
        now_iso = now.isoformat()
        result = await self.collection.update_many(
            {"status": "pending", "expires_at": {"$lt": now_iso}},
            {
                "$set": {
                    "status": "expired",
                    "cleanup_after": self._terminal_cleanup_after("expired", now),
                }
            },
        )
        return result.modified_count

    async def get_pending_price_triggers(self) -> list[dict[str, Any]]:
        await self._ensure_indexes()
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        cursor = self.collection.find(
            {
                "status": "pending",
                "trigger_condition.type": {"$in": ["price_below", "price_above"]},
                "expires_at": {"$gt": now_iso},  # not yet expired
            },
            {"_id": 0},  # exclude internal MongoDB _id from results
        )
        return await cursor.to_list(length=None)

    async def get_pending_time_triggers(self) -> list[dict[str, Any]]:
        await self._ensure_indexes()
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        cursor = self.collection.find(
            {
                "status": "pending",
                "trigger_condition.type": "time_at",
                "trigger_condition.execute_at": {"$lte": now_iso},
                "expires_at": {"$gt": now_iso},
            },
            {"_id": 0},
        )
        return await cursor.to_list(length=None)

    async def get_trigger(self, trigger_id: str) -> Optional[dict[str, Any]]:
        await self._ensure_indexes()
        return await self.collection.find_one({"trigger_id": trigger_id}, {"_id": 0})

    async def get_triggers_for_user(
        self,
        user_id: str,
        status: Optional[str] = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        await self._ensure_indexes()
        query: dict[str, Any] = {"user_id": user_id}
        if status:
            query["status"] = status

        cursor = (
            self.collection.find(query, {"_id": 0}).sort("created_at", -1).limit(limit)
        )
        return await cursor.to_list(length=limit)

    async def get_triggers_for_thread(self, thread_id: str) -> list[dict[str, Any]]:
        await self._ensure_indexes()
        cursor = self.collection.find({"thread_id": thread_id}, {"_id": 0})
        return await cursor.to_list(length=None)

    # Diagnostic helpers 

    async def count_pending(self) -> int:
        await self._ensure_indexes()
        return await self.collection.count_documents({"status": "pending"})

    async def summary(self) -> dict[str, int]:
        await self._ensure_indexes()
        pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
        cursor = self.collection.aggregate(pipeline)
        docs = await cursor.to_list(length=None)
        return {doc["_id"]: doc["count"] for doc in docs}
