from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from pymongo import ASCENDING, IndexModel
from pymongo.errors import OperationFailure

from core.database.mongodb_async import AsyncMongoDB

_COLLECTION_NAME = "task_history"
_SHORT_RETENTION = timedelta(days=3)
_DEFAULT_RETENTION = timedelta(days=5)
_SHORT_RETENTION_STATUSES = {"SUCCESS", "FAILED"}
_DEFAULT_RETENTION_STATUSES = {"PENDING", "PENDING_ON_CHAIN", "RESUBMITTED"}
_INDEX_CONFLICT_CODES = {85, 86}


def _ensure_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_utc_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return None
    return _ensure_utc_datetime(parsed)


def normalize_status(status: str | None) -> str:
    if not status:
        return "UNKNOWN"
    return str(status).strip().upper()


def _expiry_for_status(status: str, now_dt: datetime) -> datetime:
    normalized = normalize_status(status)
    current_dt = _ensure_utc_datetime(now_dt)
    if normalized in _SHORT_RETENTION_STATUSES:
        return current_dt + _SHORT_RETENTION
    if normalized in _DEFAULT_RETENTION_STATUSES:
        return current_dt + _DEFAULT_RETENTION
    return current_dt + _DEFAULT_RETENTION


def task_history_status_fields(
    status: str,
    now_dt: datetime,
    *,
    updated_at: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_status(status)
    current_dt = _ensure_utc_datetime(now_dt)
    return {
        "status": normalized,
        "updated_at": updated_at or current_dt.isoformat(),
        "expires_at": _expiry_for_status(normalized, current_dt),
    }


def summarize_task(tool: str | None, args: Dict[str, Any] | None) -> str:
    if not tool:
        return "Transaction with current settings"
    tool_lower = str(tool).strip().lower()
    args = args or {}

    if tool_lower == "swap":
        return (
            f"Swap {args.get('amount_in')} {args.get('token_in_symbol')} "
            f"to {args.get('token_out_symbol')} on {args.get('chain')}"
        )
    if tool_lower == "bridge":
        return (
            f"Bridge {args.get('amount')} {args.get('token_symbol')} "
            f"from {args.get('source_chain')} to {args.get('target_chain')}"
        )
    if tool_lower == "transfer":
        return (
            f"Transfer {args.get('amount')} "
            f"{args.get('asset_symbol') or args.get('token_symbol')} "
            f"to {args.get('recipient')} on {args.get('network') or args.get('chain')}"
        )
    if tool_lower == "unwrap":
        amount = args.get("amount")
        if amount is None:
            amount = "all available"
        return (
            f"Unwrap {amount} "
            f"{args.get('wrapped_token_symbol') or args.get('token_symbol') or 'wrapped native'} "
            f"on {args.get('network') or args.get('chain')}"
        )

    return f"{tool_lower.capitalize()} with current settings"


@dataclass(frozen=True)
class TaskHistoryRecord:
    user_id: str
    thread_id: str
    execution_id: str
    node_id: str
    tool: str
    status: str
    summary: str
    created_at: str
    updated_at: str
    expires_at: Optional[datetime] = None
    # Persisted as "chain" for storage compatibility with existing task records.
    chain: Optional[str] = None
    protocol: Optional[str] = None
    tx_hash: Optional[str] = None
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


class TaskHistoryRegistry:
    def __init__(self) -> None:
        self._collection = AsyncMongoDB.get_collection(_COLLECTION_NAME)
        self._indexes_ready = False
        self._index_lock = None

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

    async def _create_indexes_tolerant(self, indexes: list[IndexModel]) -> None:
        try:
            await self._collection.create_indexes(indexes)
            return
        except OperationFailure as exc:
            if not self._is_index_conflict_error(exc):
                raise

        for model in indexes:
            try:
                await self._collection.create_indexes([model])
            except OperationFailure as exc:
                if not self._is_index_conflict_error(exc):
                    raise

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        if self._index_lock is None:
            self._index_lock = asyncio.Lock()

        async with self._index_lock:
            if self._indexes_ready:
                return
            await self._create_indexes_tolerant(
                [
                    IndexModel([("user_id", ASCENDING), ("created_at", ASCENDING)]),
                    IndexModel([("user_id", ASCENDING), ("status", ASCENDING)]),
                    IndexModel([("thread_id", ASCENDING)]),
                    IndexModel([("execution_id", ASCENDING)]),
                    IndexModel(
                        [("expires_at", ASCENDING)],
                        expireAfterSeconds=0,
                        partialFilterExpression={"expires_at": {"$type": "date"}},
                        name="ttl_task_history_expires_at",
                    ),
                ]
            )
            self._indexes_ready = True

    async def record_event(
        self,
        *,
        user_id: str,
        thread_id: str,
        execution_id: str,
        node_id: str,
        tool: str,
        status: str,
        summary: str,
        chain: Optional[str] = None,
        protocol: Optional[str] = None,
        tx_hash: Optional[str] = None,
        error_category: Optional[str] = None,
        error_message: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> str:
        await self._ensure_indexes()
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()
        task_id = str(uuid4())
        status_fields = task_history_status_fields(status, now_dt, updated_at=now_iso)
        record = TaskHistoryRecord(
            user_id=user_id,
            thread_id=thread_id,
            execution_id=execution_id,
            node_id=node_id,
            tool=tool,
            status=str(status_fields["status"]),
            summary=summary,
            created_at=now_iso,
            updated_at=str(status_fields["updated_at"]),
            expires_at=status_fields["expires_at"],
            chain=chain,
            protocol=protocol,
            tx_hash=tx_hash,
            error_category=error_category,
            error_message=error_message,
            meta=meta,
        )
        await self._collection.insert_one(record.__dict__)
        return task_id

    async def list_recent(
        self,
        user_id: str,
        *,
        limit: int = 20,
        status: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        await self._ensure_indexes()
        query: dict[str, Any] = {"user_id": user_id}
        if status:
            query["status"] = normalize_status(status)
        cursor = (
            self._collection.find(query, {"_id": 0}).sort("created_at", -1).limit(limit)
        )
        return await cursor.to_list(length=limit)


async def update_task_history_tx_hash(
    *,
    collection: Any,
    execution_id: Optional[str],
    node_id: Optional[str],
    tx_hash: str,
    now_iso: str,
    status: str = "RESUBMITTED",
) -> bool:
    if not execution_id or not node_id:
        return False
    now_dt = _parse_utc_datetime(now_iso) or datetime.now(timezone.utc)
    updated_at = now_iso if str(now_iso or "").strip() else now_dt.isoformat()
    set_fields = {
        "tx_hash": str(tx_hash),
        **task_history_status_fields(status, now_dt, updated_at=updated_at),
    }
    result = await collection.find_one_and_update(
        {
            "execution_id": str(execution_id),
            "node_id": str(node_id),
            "status": {"$in": ["PENDING", "PENDING_ON_CHAIN"]},
        },
        {"$set": set_fields},
        sort=[("updated_at", -1)],
    )
    return bool(result)


async def update_task_history_terminal_status(
    *,
    collection: Any,
    execution_id: Optional[str],
    node_id: Optional[str],
    status: str,
    now_iso: str,
    tx_hash: Optional[str] = None,
    summary: Optional[str] = None,
) -> bool:
    if not execution_id or not node_id:
        return False
    now_dt = _parse_utc_datetime(now_iso) or datetime.now(timezone.utc)
    updated_at = now_iso if str(now_iso or "").strip() else now_dt.isoformat()
    set_payload: Dict[str, Any] = task_history_status_fields(
        status,
        now_dt,
        updated_at=updated_at,
    )
    if tx_hash:
        set_payload["tx_hash"] = str(tx_hash)
    if summary:
        set_payload["summary"] = str(summary)
    result = await collection.find_one_and_update(
        {
            "execution_id": str(execution_id),
            "node_id": str(node_id),
            "status": {"$in": ["PENDING", "PENDING_ON_CHAIN", "RESUBMITTED"]},
        },
        {"$set": set_payload},
        sort=[("updated_at", -1)],
    )
    return bool(result)
