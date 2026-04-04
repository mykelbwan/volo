from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, Iterable, Optional
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING, IndexModel, ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure

from core.database.mongodb_async import AsyncMongoDB
from core.tasks.models import ConversationTaskRecord

_TASKS_COLLECTION = "conversation_tasks"
_COUNTERS_COLLECTION = "conversation_task_counters"
_FAILED_TASK_TTL = timedelta(days=3)
_TERMINAL_TASK_PRUNE_RETENTION = timedelta(days=3)
_ACTIVE_TASK_STATUSES = {
    "RUNNING",
    "WAITING_EXTERNAL",
    "WAITING_INPUT",
    "WAITING_CONFIRMATION",
    "WAITING_FUNDS",
}
_TERMINAL_TASK_STATUSES = {"FAILED", "COMPLETED", "CANCELLED"}
_COMPLETED_OR_CANCELLED_STATUSES = {"COMPLETED", "CANCELLED"}
_INDEX_CONFLICT_CODES = {85, 86}

_LOGGER = logging.getLogger("volo.tasks.registry")


def draft_execution_id(thread_id: str) -> str:
    return f"draft:{str(thread_id)}"


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_task_status(status: str | None) -> str:
    if not status:
        return "UNKNOWN"
    return str(status).strip().upper()


def resolve_conversation_id(
    *,
    provider: str | None,
    provider_user_id: str | None,
    context: Dict[str, Any] | None = None,
) -> str | None:
    ctx = context or {}
    explicit = str(ctx.get("conversation_id") or "").strip()
    if explicit:
        return explicit
    provider_value = str(provider or "").strip()
    provider_user_value = str(provider_user_id or "").strip()
    if provider_value and provider_user_value:
        return f"{provider_value}:{provider_user_value}"
    return None


class ConversationTaskRegistry:
    """
    Persist workflow-level tasks per conversation.
    """

    def __init__(self) -> None:
        db = AsyncMongoDB.get_db()
        self._collection = db[_TASKS_COLLECTION]
        self._counters = db[_COUNTERS_COLLECTION]
        self._indexes_ready = False
        self._index_lock: Optional[asyncio.Lock] = None

    @staticmethod
    def _is_index_conflict_error(exc: OperationFailure) -> bool:
        code = getattr(exc, "code", None)
        if code in _INDEX_CONFLICT_CODES:
            return True
        message = str(exc).lower()
        return (
            "indexoptionsconflict" in message
            or "indexkeyspecsconflict" in message
            or "existing index has the same name" in message
            or "already exists with different name" in message
        )

    async def _create_indexes_tolerant(
        self,
        collection: Any,
        indexes: list[IndexModel],
        *,
        collection_name: str,
    ) -> None:
        try:
            await collection.create_indexes(indexes)
            return
        except OperationFailure as exc:
            if not self._is_index_conflict_error(exc):
                raise

        for model in indexes:
            try:
                name = str(getattr(model, "document", {}).get("name") or "unknown")
            except Exception:
                name = "unknown"
            try:
                await collection.create_indexes([model])
            except OperationFailure as exc:
                if not self._is_index_conflict_error(exc):
                    raise
                _LOGGER.warning(
                    "index_conflict collection=%s index=%s detail=%s keeping_existing=true",
                    collection_name,
                    name,
                    exc,
                )

    def _status_field_updates(
        self,
        *,
        status: str,
        now_dt: datetime,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        set_fields: dict[str, Any] = {"updated_at_dt": now_dt}
        unset_fields: dict[str, str] = {}
        if status == "FAILED":
            set_fields["terminal_at"] = now_dt
            set_fields["failed_expires_at"] = now_dt + _FAILED_TASK_TTL
            return set_fields, unset_fields
        if status in _COMPLETED_OR_CANCELLED_STATUSES:
            set_fields["terminal_at"] = now_dt
            unset_fields["failed_expires_at"] = ""
            return set_fields, unset_fields
        unset_fields["terminal_at"] = ""
        unset_fields["failed_expires_at"] = ""
        return set_fields, unset_fields

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        if self._index_lock is None:
            self._index_lock = asyncio.Lock()
        async with self._index_lock:
            if self._indexes_ready:
                return
            await self._create_indexes_tolerant(
                self._collection,
                [
                    IndexModel(
                        [("conversation_id", ASCENDING), ("execution_id", ASCENDING)],
                        unique=True,
                        name="uniq_conversation_execution",
                    ),
                    IndexModel(
                        [("conversation_id", ASCENDING), ("task_number", ASCENDING)],
                        unique=True,
                        name="uniq_conversation_task_number",
                    ),
                    IndexModel(
                        [("conversation_id", ASCENDING), ("updated_at", DESCENDING)],
                        name="idx_conversation_updated",
                    ),
                    IndexModel(
                        [("conversation_id", ASCENDING), ("thread_id", ASCENDING), ("updated_at", DESCENDING)],
                        name="idx_conversation_thread_updated",
                    ),
                    IndexModel(
                        [("failed_expires_at", ASCENDING)],
                        expireAfterSeconds=0,
                        partialFilterExpression={"failed_expires_at": {"$type": "date"}},
                        name="ttl_failed_task_expires_at",
                    ),
                ],
                collection_name=_TASKS_COLLECTION,
            )
            await self._create_indexes_tolerant(
                self._counters,
                [
                    IndexModel(
                        [("conversation_id", ASCENDING)],
                        unique=True,
                        name="uniq_conversation_counter",
                    )
                ],
                collection_name=_COUNTERS_COLLECTION,
            )
            self._indexes_ready = True

    async def _next_task_number(self, conversation_id: str) -> int:
        doc = await self._counters.find_one_and_update(
            {"conversation_id": str(conversation_id)},
            {"$inc": {"next_task_number": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if not isinstance(doc, dict):
            return 1
        try:
            return max(1, int(doc.get("next_task_number", 1)))
        except Exception:
            return 1

    async def upsert_execution_task(
        self,
        *,
        conversation_id: str,
        execution_id: str,
        thread_id: str,
        provider: str,
        provider_user_id: str,
        user_id: str,
        title: str,
        status: str,
        latest_summary: str | None = None,
        tool: str | None = None,
        tx_hash: str | None = None,
        error_category: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        await self._ensure_indexes()
        conversation_key = str(conversation_id)
        execution_key = str(execution_id)
        normalized_status = normalize_task_status(status)
        now_dt = _utc_now()
        now_iso = now_dt.isoformat()
        update_fields: Dict[str, Any] = {
            "thread_id": str(thread_id),
            "provider": str(provider),
            "provider_user_id": str(provider_user_id),
            "user_id": str(user_id),
            "title": str(title),
            "status": normalized_status,
            "updated_at": now_iso,
        }
        status_fields, unset_fields = self._status_field_updates(
            status=normalized_status,
            now_dt=now_dt,
        )
        update_fields.update(status_fields)
        if latest_summary:
            update_fields["latest_summary"] = str(latest_summary)
        if tool:
            update_fields["tool"] = str(tool)
        if tx_hash:
            update_fields["tx_hash"] = str(tx_hash)
        if error_category:
            update_fields["error_category"] = str(error_category)
        if meta is not None:
            update_fields["meta"] = meta

        existing = await self._collection.find_one(
            {"conversation_id": conversation_key, "execution_id": execution_key},
            {"_id": 0},
        )
        if existing:
            update_doc: Dict[str, Any] = {"$set": update_fields}
            if unset_fields:
                update_doc["$unset"] = unset_fields
            await self._collection.update_one(
                {"conversation_id": conversation_key, "execution_id": execution_key},
                update_doc,
            )
            existing.update(update_fields)
            for field_name in unset_fields:
                existing.pop(field_name, None)
            return existing

        if execution_key != draft_execution_id(thread_id):
            draft = await self._collection.find_one(
                {
                    "conversation_id": conversation_key,
                    "execution_id": draft_execution_id(thread_id),
                    "thread_id": str(thread_id),
                },
                {"_id": 0},
            )
            if draft:
                promoted_fields = {**update_fields, "execution_id": execution_key}
                promoted_update: Dict[str, Any] = {"$set": promoted_fields}
                if unset_fields:
                    promoted_update["$unset"] = unset_fields
                await self._collection.update_one(
                    {
                        "conversation_id": conversation_key,
                        "execution_id": draft_execution_id(thread_id),
                        "thread_id": str(thread_id),
                    },
                    promoted_update,
                )
                draft.update(promoted_fields)
                for field_name in unset_fields:
                    draft.pop(field_name, None)
                return draft

        task_number = await self._next_task_number(conversation_key)
        record = ConversationTaskRecord(
            task_id=str(uuid4()),
            conversation_id=conversation_key,
            task_number=task_number,
            execution_id=execution_key,
            thread_id=str(thread_id),
            provider=str(provider),
            provider_user_id=str(provider_user_id),
            user_id=str(user_id),
            title=str(title),
            status=normalized_status,
            created_at=now_iso,
            updated_at=now_iso,
            created_at_dt=now_dt,
            updated_at_dt=now_dt,
            terminal_at=update_fields.get("terminal_at"),
            failed_expires_at=update_fields.get("failed_expires_at"),
            latest_summary=str(latest_summary) if latest_summary else None,
            tool=str(tool) if tool else None,
            tx_hash=str(tx_hash) if tx_hash else None,
            error_category=str(error_category) if error_category else None,
            meta=meta,
        )
        doc = asdict(record)
        try:
            await self._collection.insert_one(doc)
            return doc
        except DuplicateKeyError:
            update_doc = {"$set": update_fields}
            if unset_fields:
                update_doc["$unset"] = unset_fields
            await self._collection.update_one(
                {"conversation_id": conversation_key, "execution_id": execution_key},
                update_doc,
            )
            current = await self._collection.find_one(
                {"conversation_id": conversation_key, "execution_id": execution_key},
                {"_id": 0},
            )
            return current or doc

    async def backfill_failed_task_expiry_fields(
        self,
        conversation_id: str,
        *,
        limit: int = 25,
    ) -> int:
        await self._ensure_indexes()
        cursor = self._collection.find(
            {
                "conversation_id": str(conversation_id),
                "status": "FAILED",
                "failed_expires_at": {"$exists": False},
            },
            {
                "_id": 0,
                "task_id": 1,
                "updated_at": 1,
                "created_at": 1,
                "terminal_at": 1,
            },
        ).limit(limit)
        rows = await cursor.to_list(length=limit)
        updated = 0
        for row in rows:
            task_id = str(row.get("task_id") or "").strip()
            if not task_id:
                continue
            terminal_at = row.get("terminal_at")
            terminal_dt = (
                terminal_at.astimezone(timezone.utc)
                if isinstance(terminal_at, datetime) and terminal_at.tzinfo
                else (
                    terminal_at.replace(tzinfo=timezone.utc)
                    if isinstance(terminal_at, datetime)
                    else None
                )
            )
            base_dt = (
                terminal_dt
                or _parse_iso_datetime(row.get("updated_at"))
                or _parse_iso_datetime(row.get("created_at"))
            )
            if base_dt is None:
                continue
            set_fields: Dict[str, Any] = {
                "failed_expires_at": base_dt + _FAILED_TASK_TTL,
            }
            if terminal_dt is None:
                set_fields["terminal_at"] = base_dt
            result = await self._collection.update_one(
                {
                    "conversation_id": str(conversation_id),
                    "task_id": task_id,
                    "status": "FAILED",
                    "failed_expires_at": {"$exists": False},
                },
                {"$set": set_fields},
            )
            updated += int(getattr(result, "modified_count", 0) or 0)
        return updated

    async def prune_terminal_tasks(
        self,
        conversation_id: str,
        *,
        protected_task_numbers: Iterable[int] | None = None,
        now: datetime | None = None,
        limit: int = 50,
    ) -> int:
        await self._ensure_indexes()
        protected_numbers = {
            int(value)
            for value in (protected_task_numbers or [])
            if value is not None
        }
        prune_before = (now or _utc_now()) - _TERMINAL_TASK_PRUNE_RETENTION
        query: Dict[str, Any] = {
            "conversation_id": str(conversation_id),
            "status": {"$in": sorted(_COMPLETED_OR_CANCELLED_STATUSES)},
            "terminal_at": {"$lte": prune_before},
        }
        if protected_numbers:
            query["task_number"] = {"$nin": sorted(protected_numbers)}
        cursor = (
            self._collection.find(
                query,
                {"_id": 0, "task_id": 1, "task_number": 1},
            )
            .sort("terminal_at", ASCENDING)
            .limit(limit)
        )
        rows = await cursor.to_list(length=limit)
        task_ids = [
            str(row.get("task_id") or "").strip()
            for row in rows
            if str(row.get("task_id") or "").strip()
        ]
        if not task_ids:
            return 0
        delete_query: Dict[str, Any] = {
            "conversation_id": str(conversation_id),
            "task_id": {"$in": task_ids},
            "status": {"$in": sorted(_COMPLETED_OR_CANCELLED_STATUSES)},
        }
        if protected_numbers:
            delete_query["task_number"] = {"$nin": sorted(protected_numbers)}
        result = await self._collection.delete_many(delete_query)
        return int(getattr(result, "deleted_count", 0) or 0)

    async def list_recent(
        self,
        conversation_id: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        await self._ensure_indexes()
        cursor = (
            self._collection.find(
                {"conversation_id": str(conversation_id)},
                {"_id": 0},
            )
            .sort("updated_at", DESCENDING)
            .limit(limit)
        )
        return await cursor.to_list(length=limit)

    async def get_task_by_number(
        self,
        conversation_id: str,
        *,
        task_number: int,
    ) -> dict[str, Any] | None:
        await self._ensure_indexes()
        return await self._collection.find_one(
            {
                "conversation_id": str(conversation_id),
                "task_number": int(task_number),
            },
            {"_id": 0},
        )

    async def get_latest_task_for_thread(
        self,
        conversation_id: str,
        *,
        thread_id: str,
    ) -> dict[str, Any] | None:
        await self._ensure_indexes()
        cursor = (
            self._collection.find(
                {
                    "conversation_id": str(conversation_id),
                    "thread_id": str(thread_id),
                },
                {"_id": 0},
            )
            .sort("updated_at", DESCENDING)
            .limit(1)
        )
        rows = await cursor.to_list(length=1)
        return rows[0] if rows else None

    async def list_active(
        self,
        conversation_id: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        await self._ensure_indexes()
        cursor = (
            self._collection.find(
                {
                    "conversation_id": str(conversation_id),
                    "status": {"$in": sorted(_ACTIVE_TASK_STATUSES)},
                },
                {"_id": 0},
            )
            .sort("updated_at", DESCENDING)
            .limit(limit)
        )
        return await cursor.to_list(length=limit)
