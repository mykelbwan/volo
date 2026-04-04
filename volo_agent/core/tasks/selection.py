from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pymongo import ASCENDING, IndexModel

from core.database.mongodb_async import AsyncMongoDB

_SELECTIONS_COLLECTION = "conversation_task_selections"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ConversationTaskSelectionRegistry:
    def __init__(self) -> None:
        self._collection = AsyncMongoDB.get_collection(_SELECTIONS_COLLECTION)
        self._indexes_ready = False
        self._index_lock: Optional[asyncio.Lock] = None

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        if self._index_lock is None:
            self._index_lock = asyncio.Lock()
        async with self._index_lock:
            if self._indexes_ready:
                return
            await self._collection.create_indexes(
                [
                    IndexModel(
                        [("conversation_id", ASCENDING)],
                        unique=True,
                        name="uniq_conversation_task_selection",
                    )
                ]
            )
            self._indexes_ready = True

    async def set_selected_task_number(
        self,
        *,
        conversation_id: str,
        task_number: int | None,
    ) -> None:
        await self._ensure_indexes()
        if task_number is None:
            await self._collection.delete_one({"conversation_id": str(conversation_id)})
            return
        await self._collection.update_one(
            {"conversation_id": str(conversation_id)},
            {
                "$set": {
                    "conversation_id": str(conversation_id),
                    "selected_task_number": int(task_number),
                    "updated_at": _utc_now_iso(),
                }
            },
            upsert=True,
        )

    async def get_selected_task_number(self, conversation_id: str) -> int | None:
        await self._ensure_indexes()
        doc = await self._collection.find_one(
            {"conversation_id": str(conversation_id)},
            {"_id": 0, "selected_task_number": 1},
        )
        if not isinstance(doc, dict):
            return None
        value = doc.get("selected_task_number")
        try:
            return int(value) if value is not None else None
        except Exception:
            return None

    async def clear_selected_task_number_if_matches(
        self,
        *,
        conversation_id: str,
        task_number: int,
    ) -> bool:
        await self._ensure_indexes()
        result = await self._collection.delete_one(
            {
                "conversation_id": str(conversation_id),
                "selected_task_number": int(task_number),
            }
        )
        return bool(getattr(result, "deleted_count", 0))
