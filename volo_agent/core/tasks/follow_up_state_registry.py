from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pymongo import ASCENDING, DESCENDING, IndexModel, ReturnDocument

from core.database.mongodb_async import AsyncMongoDB

_FOLLOW_UP_STATE_COLLECTION = "conversation_follow_up_state"
_FOLLOW_UP_STATE_TTL = timedelta(hours=24)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_scope_filter(
    *,
    conversation_id: str,
    mode: str,
    thread_id: str | None,
    selected_task_number: int | None,
) -> dict[str, Any]:
    selected_task_value: int | None
    try:
        selected_task_value = (
            int(selected_task_number) if selected_task_number is not None else None
        )
    except Exception:
        selected_task_value = None
    return {
        "conversation_id": str(conversation_id),
        "mode": str(mode).strip().lower(),
        "thread_id": str(thread_id or "").strip(),
        "selected_task_number": selected_task_value,
    }


class ConversationFollowUpStateRegistry:
    def __init__(self) -> None:
        self._collection = AsyncMongoDB.get_collection(_FOLLOW_UP_STATE_COLLECTION)
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
                        [
                            ("conversation_id", ASCENDING),
                            ("mode", ASCENDING),
                            ("thread_id", ASCENDING),
                            ("selected_task_number", ASCENDING),
                        ],
                        unique=True,
                        name="uniq_follow_up_state_scope",
                    ),
                    IndexModel(
                        [("expires_at", ASCENDING)],
                        expireAfterSeconds=0,
                        partialFilterExpression={"expires_at": {"$type": "date"}},
                        name="ttl_follow_up_state_expires_at",
                    ),
                    IndexModel(
                        [("conversation_id", ASCENDING), ("updated_at", DESCENDING)],
                        name="idx_follow_up_state_conversation_updated",
                    ),
                ]
            )
            self._indexes_ready = True

    async def set_state(
        self,
        *,
        conversation_id: str,
        mode: str,
        thread_id: str | None,
        selected_task_number: int | None,
        expected_slot: str | None,
        selected_task_reference: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        now = _utc_now()
        scope_filter = _to_scope_filter(
            conversation_id=conversation_id,
            mode=mode,
            thread_id=thread_id,
            selected_task_number=selected_task_number,
        )
        update_fields: dict[str, Any] = {
            **scope_filter,
            "expected_slot": str(expected_slot).strip().lower() if expected_slot else None,
            "updated_at": now.isoformat(),
            "expires_at": now + _FOLLOW_UP_STATE_TTL,
        }
        if selected_task_reference is not None:
            update_fields["selected_task_reference"] = dict(selected_task_reference)
        else:
            update_fields["selected_task_reference"] = None
        doc = await self._collection.find_one_and_update(
            scope_filter,
            {
                "$set": update_fields,
                "$setOnInsert": {"created_at": now.isoformat()},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
            projection={"_id": 0},
        )
        return dict(doc or update_fields)

    async def get_state(
        self,
        *,
        conversation_id: str,
        mode: str,
        thread_id: str | None,
        selected_task_number: int | None,
    ) -> dict[str, Any] | None:
        await self._ensure_indexes()
        scope_filter = _to_scope_filter(
            conversation_id=conversation_id,
            mode=mode,
            thread_id=thread_id,
            selected_task_number=selected_task_number,
        )
        doc = await self._collection.find_one(scope_filter, {"_id": 0})
        return dict(doc) if isinstance(doc, dict) else None

    async def clear_state(
        self,
        *,
        conversation_id: str,
        mode: str,
        thread_id: str | None,
        selected_task_number: int | None,
    ) -> bool:
        await self._ensure_indexes()
        scope_filter = _to_scope_filter(
            conversation_id=conversation_id,
            mode=mode,
            thread_id=thread_id,
            selected_task_number=selected_task_number,
        )
        result = await self._collection.delete_one(scope_filter)
        return bool(getattr(result, "deleted_count", 0))

