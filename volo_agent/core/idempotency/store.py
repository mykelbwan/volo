from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional, Tuple

from pymongo import ASCENDING

from core.database.mongodb import MongoDB
from core.database.mongodb_async import AsyncMongoDB

_DEFAULT_COLLECTION = "idempotency_records"
_ALLOWED_STATUSES = frozenset({"pending", "success", "failed"})

logger = logging.getLogger("volo.idempotency")


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _coerce_non_empty_str(value: Any, *, fallback: str) -> str:
    if isinstance(value, str) and value.strip():
        return value
    return fallback


def _coerce_status(value: Any, *, fallback: str = "pending") -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _ALLOWED_STATUSES:
            return normalized
    return fallback


def _coerce_datetime(value: Any, *, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        normalized = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return fallback
        return (
            parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        )
    return fallback


def _coerce_dict_or_none(value: Any) -> Optional[Dict[str, Any]]:
    return value if isinstance(value, dict) else None


def _coerce_str_or_none(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value else None


def _get_int_env(key: str, default: int) -> int:
    raw = os.getenv(key, "")
    if raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


PENDING_TTL_SECONDS = _get_int_env("IDEMPOTENCY_TTL_PENDING_SECONDS", 24 * 60 * 60)
SUCCESS_TTL_SECONDS = _get_int_env("IDEMPOTENCY_TTL_SUCCESS_SECONDS", 30 * 24 * 60 * 60)
FAILED_TTL_SECONDS = _get_int_env("IDEMPOTENCY_TTL_FAILED_SECONDS", 7 * 24 * 60 * 60)
PENDING_RECLAIM_SECONDS = _get_int_env("IDEMPOTENCY_PENDING_RECLAIM_SECONDS", 5 * 60)


@dataclass
class IdempotencyRecord:
    key: str
    status: str
    created_at: datetime
    expires_at: datetime
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    tx_hash: Optional[str] = None


def compute_args_hash(args: Dict[str, Any]) -> str:
    payload = json.dumps(args, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_idempotency_key(
    *,
    scope_id: str,
    node_id: str,
    tool: str,
    args_hash: str,
) -> str:
    raw = f"{scope_id}:{node_id}:{tool}:{args_hash}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class IdempotencyStore:
    def __init__(self, collection_name: str = _DEFAULT_COLLECTION) -> None:
        self._collection = MongoDB.get_collection(collection_name)
        self._async_collection = AsyncMongoDB.get_collection(collection_name)
        self._indexes_ready = False
        self._async_indexes_ready = False
        self._async_index_lock: asyncio.Lock | None = None

    def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self._collection.create_index(
            [("key", ASCENDING)], unique=True, name="idempotency_key_unique"
        )
        # TTL index uses expires_at field.
        self._collection.create_index(
            [("expires_at", ASCENDING)],
            expireAfterSeconds=0,
            name="idempotency_expires_at_ttl",
        )
        self._indexes_ready = True

    async def _ensure_indexes_async(self) -> None:
        if self._async_indexes_ready:
            return
        if self._async_index_lock is None:
            self._async_index_lock = asyncio.Lock()
        async with self._async_index_lock:
            if self._async_indexes_ready:
                return
            await self._async_collection.create_index(
                [("key", ASCENDING)],
                unique=True,
                name="idempotency_key_unique",
            )
            await self._async_collection.create_index(
                [("expires_at", ASCENDING)],
                expireAfterSeconds=0,
                name="idempotency_expires_at_ttl",
            )
            self._async_indexes_ready = True

    @staticmethod
    def _claim_doc(
        *,
        key: str,
        metadata: Dict[str, Any],
        now: datetime,
        ttl_seconds: int,
    ) -> Dict[str, Any]:
        return {
            "key": key,
            "status": "pending",
            "created_at": now,
            "updated_at": now,
            "expires_at": now + timedelta(seconds=ttl_seconds),
            "metadata": metadata,
        }

    @staticmethod
    def _reclaim_filter(doc: Mapping[str, Any], *, key: str) -> Dict[str, Any]:
        reclaim_filter: Dict[str, Any] = {
            "key": key,
            "status": doc.get("status"),
            "expires_at": doc.get("expires_at"),
            "tx_hash": doc.get("tx_hash"),
        }
        if "updated_at" in doc:
            reclaim_filter["updated_at"] = doc.get("updated_at")
        elif "created_at" in doc:
            reclaim_filter["created_at"] = doc.get("created_at")
        return reclaim_filter

    @staticmethod
    def _reclaim_update(
        *,
        metadata: Dict[str, Any],
        now: datetime,
        ttl_seconds: int,
    ) -> Dict[str, Any]:
        return {
            "$set": {
                "status": "pending",
                "updated_at": now,
                "expires_at": now + timedelta(seconds=ttl_seconds),
                "metadata": metadata,
                "error": None,
                "result": None,
                "tx_hash": None,
            }
        }

    @staticmethod
    def _is_reclaimable_doc(doc: Mapping[str, Any], *, now: datetime) -> bool:
        status = _coerce_status(doc.get("status"), fallback="pending")
        expires_at = _coerce_datetime(doc.get("expires_at"), fallback=now)
        if expires_at <= now:
            return True

        tx_hash = _coerce_str_or_none(doc.get("tx_hash"))
        if status == "failed" and tx_hash is None:
            return True

        if status != "pending" or tx_hash is not None:
            return False

        updated_at = _coerce_datetime(
            doc.get("updated_at"),
            fallback=_coerce_datetime(doc.get("created_at"), fallback=now),
        )
        age_seconds = max((now - updated_at).total_seconds(), 0.0)
        return age_seconds >= PENDING_RECLAIM_SECONDS

    @staticmethod
    def _record_from_doc(
        doc: Mapping[str, Any],
        *,
        fallback_key: str,
        now: datetime,
        fallback_expires_at: datetime,
    ) -> IdempotencyRecord:
        raw_key = doc.get("key")
        raw_status = doc.get("status")
        raw_created_at = doc.get("created_at")
        raw_expires_at = doc.get("expires_at")

        key = _coerce_non_empty_str(raw_key, fallback=fallback_key)
        status = _coerce_status(raw_status, fallback="pending")
        created_at = _coerce_datetime(raw_created_at, fallback=now)
        expires_at = _coerce_datetime(raw_expires_at, fallback=fallback_expires_at)

        issues: list[str] = []
        if key == fallback_key and not (isinstance(raw_key, str) and raw_key.strip()):
            issues.append("missing/invalid key")
        if status == "pending" and not (
            isinstance(raw_status, str)
            and raw_status.strip().lower() in _ALLOWED_STATUSES
        ):
            issues.append("missing/invalid status")
        if created_at == now and not isinstance(raw_created_at, datetime):
            issues.append("missing/invalid created_at")
        if expires_at == fallback_expires_at and not isinstance(
            raw_expires_at, datetime
        ):
            issues.append("missing/invalid expires_at")
        if issues:
            logger.warning(
                "Recovered malformed idempotency record for key %s (%s). "
                "Recovery path: replay the request; if this repeats, delete the stale idempotency document.",
                fallback_key,
                ", ".join(issues),
            )

        return IdempotencyRecord(
            key=key,
            status=status,
            created_at=created_at,
            expires_at=expires_at,
            result=_coerce_dict_or_none(doc.get("result")),
            error=_coerce_str_or_none(doc.get("error")),
            metadata=_coerce_dict_or_none(doc.get("metadata")),
            tx_hash=_coerce_str_or_none(doc.get("tx_hash")),
        )

    def claim(
        self,
        *,
        key: str,
        metadata: Dict[str, Any],
        ttl_seconds: int = PENDING_TTL_SECONDS,
    ) -> Tuple[IdempotencyRecord, bool]:
        """
        Claim an idempotency key. Returns (record, claimed).
        """
        self._ensure_indexes()
        now = _utcnow()
        doc = self._claim_doc(
            key=key,
            metadata=metadata,
            now=now,
            ttl_seconds=ttl_seconds,
        )
        update_result = self._collection.update_one(
            {"key": key},
            {"$setOnInsert": doc},
            upsert=True,
        )
        claimed = update_result.upserted_id is not None
        result = self._collection.find_one({"key": key}) or doc
        if (not claimed) and self._is_reclaimable_doc(result, now=now):
            reclaimed = self._collection.update_one(
                self._reclaim_filter(result, key=key),
                self._reclaim_update(
                    metadata=metadata,
                    now=now,
                    ttl_seconds=ttl_seconds,
                ),
            )
            if reclaimed.modified_count:
                claimed = True
                logger.info(
                    "idempotency_reclaimed key=%s prior_status=%s had_tx_hash=%s",
                    key,
                    result.get("status"),
                    bool(_coerce_str_or_none(result.get("tx_hash"))),
                )
                result = self._collection.find_one({"key": key}) or {
                    **dict(result),
                    **doc,
                    "created_at": result.get("created_at", now),
                }
        record = self._record_from_doc(
            result,
            fallback_key=key,
            now=now,
            fallback_expires_at=doc["expires_at"],
        )
        return record, claimed

    async def aclaim(
        self,
        *,
        key: str,
        metadata: Dict[str, Any],
        ttl_seconds: int = PENDING_TTL_SECONDS,
    ) -> Tuple[IdempotencyRecord, bool]:
        await self._ensure_indexes_async()
        now = _utcnow()
        doc = self._claim_doc(
            key=key,
            metadata=metadata,
            now=now,
            ttl_seconds=ttl_seconds,
        )
        update_result = await self._async_collection.update_one(
            {"key": key},
            {"$setOnInsert": doc},
            upsert=True,
        )
        claimed = update_result.upserted_id is not None
        result = await self._async_collection.find_one({"key": key}) or doc
        if (not claimed) and self._is_reclaimable_doc(result, now=now):
            reclaimed = await self._async_collection.update_one(
                self._reclaim_filter(result, key=key),
                self._reclaim_update(
                    metadata=metadata,
                    now=now,
                    ttl_seconds=ttl_seconds,
                ),
            )
            if reclaimed.modified_count:
                claimed = True
                logger.info(
                    "idempotency_reclaimed key=%s prior_status=%s had_tx_hash=%s",
                    key,
                    result.get("status"),
                    bool(_coerce_str_or_none(result.get("tx_hash"))),
                )
                result = await self._async_collection.find_one({"key": key}) or {
                    **dict(result),
                    **doc,
                    "created_at": result.get("created_at", now),
                }
        return (
            self._record_from_doc(
                result,
                fallback_key=key,
                now=now,
                fallback_expires_at=doc["expires_at"],
            ),
            claimed,
        )

    def get(self, *, key: str) -> Optional[IdempotencyRecord]:
        self._ensure_indexes()
        now = _utcnow()
        fallback_expires_at = now + timedelta(seconds=PENDING_TTL_SECONDS)
        result = self._collection.find_one({"key": key})
        if result is None:
            return None
        return self._record_from_doc(
            result,
            fallback_key=key,
            now=now,
            fallback_expires_at=fallback_expires_at,
        )

    async def aget(self, *, key: str) -> Optional[IdempotencyRecord]:
        await self._ensure_indexes_async()
        now = _utcnow()
        fallback_expires_at = now + timedelta(seconds=PENDING_TTL_SECONDS)
        result = await self._async_collection.find_one({"key": key})
        if result is None:
            return None
        return self._record_from_doc(
            result,
            fallback_key=key,
            now=now,
            fallback_expires_at=fallback_expires_at,
        )

    def mark_inflight(
        self,
        *,
        key: str,
        tx_hash: str,
        result: Optional[Dict[str, Any]] = None,
        ttl_seconds: int = PENDING_TTL_SECONDS,
    ) -> None:
        self._ensure_indexes()
        now = _utcnow()
        update_fields: Dict[str, Any] = {
            "tx_hash": tx_hash,
            "updated_at": now,
            "expires_at": now + timedelta(seconds=ttl_seconds),
        }
        if result is not None:
            update_fields["result"] = result
        self._collection.update_one(
            {"key": key},
            {"$set": update_fields},
        )

    async def amark_inflight(
        self,
        *,
        key: str,
        tx_hash: str,
        result: Optional[Dict[str, Any]] = None,
        ttl_seconds: int = PENDING_TTL_SECONDS,
    ) -> None:
        await self._ensure_indexes_async()
        now = _utcnow()
        update_fields: Dict[str, Any] = {
            "tx_hash": tx_hash,
            "updated_at": now,
            "expires_at": now + timedelta(seconds=ttl_seconds),
        }
        if result is not None:
            update_fields["result"] = result
        await self._async_collection.update_one(
            {"key": key},
            {"$set": update_fields},
        )

    def mark_success(
        self,
        *,
        key: str,
        result: Dict[str, Any],
        ttl_seconds: int = SUCCESS_TTL_SECONDS,
    ) -> None:
        self._ensure_indexes()
        now = _utcnow()
        tx_hash = result.get("tx_hash") if isinstance(result, dict) else None
        self._collection.update_one(
            {"key": key},
            {
                "$set": {
                    "status": "success",
                    "result": result,
                    "tx_hash": tx_hash,
                    "error": None,
                    "updated_at": now,
                    "expires_at": now + timedelta(seconds=ttl_seconds),
                }
            },
        )

    async def amark_success(
        self,
        *,
        key: str,
        result: Dict[str, Any],
        ttl_seconds: int = SUCCESS_TTL_SECONDS,
    ) -> None:
        await self._ensure_indexes_async()
        now = _utcnow()
        tx_hash = result.get("tx_hash") if isinstance(result, dict) else None
        await self._async_collection.update_one(
            {"key": key},
            {
                "$set": {
                    "status": "success",
                    "result": result,
                    "tx_hash": tx_hash,
                    "error": None,
                    "updated_at": now,
                    "expires_at": now + timedelta(seconds=ttl_seconds),
                }
            },
        )

    def mark_failed(
        self,
        *,
        key: str,
        error: str,
        ttl_seconds: int = FAILED_TTL_SECONDS,
    ) -> None:
        self._ensure_indexes()
        now = _utcnow()
        self._collection.update_one(
            {"key": key},
            {
                "$set": {
                    "status": "failed",
                    "error": error,
                    "updated_at": now,
                    "expires_at": now + timedelta(seconds=ttl_seconds),
                }
            },
        )

    async def amark_failed(
        self,
        *,
        key: str,
        error: str,
        ttl_seconds: int = FAILED_TTL_SECONDS,
    ) -> None:
        await self._ensure_indexes_async()
        now = _utcnow()
        await self._async_collection.update_one(
            {"key": key},
            {
                "$set": {
                    "status": "failed",
                    "error": error,
                    "updated_at": now,
                    "expires_at": now + timedelta(seconds=ttl_seconds),
                }
            },
        )
