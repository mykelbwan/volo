from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, Iterable, List, cast

try:
    from bson.decimal128 import Decimal128
    from pymongo import ASCENDING, IndexModel, ReturnDocument, UpdateOne
    from pymongo.errors import (
        DuplicateKeyError as _PyMongoDuplicateKeyError,
        OperationFailure as _PyMongoOperationFailure,
    )
except Exception:  # pragma: no cover - optional dependency
    Decimal128 = None  # type: ignore[assignment]
    ASCENDING = 1  # type: ignore[assignment]
    IndexModel = None  # type: ignore[assignment]
    ReturnDocument = None  # type: ignore[assignment]
    UpdateOne = None  # type: ignore[assignment]

    class _FallbackDuplicateKeyError(Exception):
        pass

    class _FallbackOperationFailure(Exception):
        code = None

    DuplicateKeyError: type[Exception] = _FallbackDuplicateKeyError
    OperationFailure: type[Exception] = _FallbackOperationFailure
else:
    DuplicateKeyError = _PyMongoDuplicateKeyError
    OperationFailure = _PyMongoOperationFailure


from core.database.mongodb_async import AsyncMongoDB
from core.reservations.models import (
    FundsWaitRecord,
    ReservationRecord,
    iso_utc,
    parse_iso,
    utc_now,
)

_LOCKS_COLLECTION = "wallet_reservation_locks"
_RECORDS_COLLECTION = "wallet_reservations"
_WAITS_COLLECTION = "wallet_funds_waits"
_TOTALS_COLLECTION = "wallet_reservation_resource_totals"
_WAIT_HEADS_COLLECTION = "wallet_reservation_wait_heads"
_WALLET_STATE_COLLECTION = "wallet_reservation_wallet_states"
_DELETE_AFTER_FIELD = "delete_after"
_INDEX_CONFLICT_CODES = {85, 86}
_LOGGER = logging.getLogger("volo.reservations.store")


def _mongo_ttl_datetime(value: object) -> object | None:
    parsed = parse_iso(value)
    return parsed if parsed is not None else None


def _to_mongo_units(value: object) -> object:
    if Decimal128 is not None:
        try:
            return Decimal128(str(value))
        except Exception:
            pass
    try:
        return int(str(value))
    except Exception:
        return 0


def _from_mongo_units(value: object) -> int:
    if value is None:
        return 0
    try:
        return int(str(value))
    except Exception:
        return 0


def _to_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        try:
            return int(normalized)
        except Exception:
            return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _mongo_decimal_convert(field_ref: str) -> Dict[str, Any]:
    return {
        "$convert": {
            "input": {"$ifNull": [field_ref, 0]},
            "to": "decimal",
            "onError": Decimal128("0") if Decimal128 is not None else 0,
            "onNull": Decimal128("0") if Decimal128 is not None else 0,
        }
    }


def _build_mongo_partial_update(
    updates: Dict[str, object],
) -> Dict[str, Dict[str, object]]:
    set_payload: Dict[str, object] = {}
    unset_payload: Dict[str, object] = {}
    for key, value in dict(updates).items():
        if key != _DELETE_AFTER_FIELD:
            set_payload[key] = value
            continue
        ttl_value = _mongo_ttl_datetime(value)
        if ttl_value is None:
            unset_payload[_DELETE_AFTER_FIELD] = ""
        else:
            set_payload[_DELETE_AFTER_FIELD] = ttl_value
    update_doc: Dict[str, Dict[str, object]] = {}
    if set_payload:
        update_doc["$set"] = set_payload
    if unset_payload:
        update_doc["$unset"] = unset_payload
    return update_doc


def _build_mongo_full_update(
    payload: Dict[str, object],
) -> Dict[str, Dict[str, object]]:
    set_payload = dict(payload)
    ttl_value = _mongo_ttl_datetime(set_payload.pop(_DELETE_AFTER_FIELD, None))
    update_doc: Dict[str, Dict[str, object]] = {"$set": set_payload}
    if ttl_value is None:
        update_doc["$unset"] = {_DELETE_AFTER_FIELD: ""}
    else:
        update_doc["$set"][_DELETE_AFTER_FIELD] = ttl_value
    return update_doc


class ReservationStore:
    async def acquire_wallet_lock(
        self, *, wallet_scope: str, owner: str, ttl_seconds: int
    ) -> bool:
        raise NotImplementedError

    async def release_wallet_lock(self, *, wallet_scope: str, owner: str) -> None:
        raise NotImplementedError

    async def get_record(self, reservation_id: str) -> ReservationRecord | None:
        raise NotImplementedError

    async def list_records(
        self,
        *,
        wallet_scope: str,
        statuses: Iterable[str] | None = None,
    ) -> List[ReservationRecord]:
        raise NotImplementedError

    async def save_record(self, record: ReservationRecord) -> None:
        raise NotImplementedError

    async def update_record(
        self, reservation_id: str, *, updates: Dict[str, object]
    ) -> None:
        raise NotImplementedError

    async def get_wait(self, wait_id: str) -> FundsWaitRecord | None:
        raise NotImplementedError

    async def list_waits(
        self,
        *,
        wallet_scope: str | None = None,
        statuses: Iterable[str] | None = None,
        exclude_wait_ids: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> List[FundsWaitRecord]:
        raise NotImplementedError

    async def save_wait(self, wait: FundsWaitRecord) -> None:
        raise NotImplementedError

    async def update_wait(self, wait_id: str, *, updates: Dict[str, object]) -> None:
        raise NotImplementedError

    async def transition_wait(
        self,
        wait_id: str,
        *,
        from_statuses: Iterable[str],
        updates: Dict[str, object],
    ) -> FundsWaitRecord | None:
        raise NotImplementedError

    async def expire_records(
        self,
        *,
        wallet_scope: str,
        statuses: Iterable[str],
        expires_before,
        terminal_status: str,
        delete_after: str | None = None,
        reason: str | None = None,
        updated_at: str | None = None,
    ) -> None:
        raise NotImplementedError

    async def requeue_waits(
        self,
        *,
        wallet_scope: str,
        from_status: str,
        ready_before,
        updated_at: str | None = None,
    ) -> None:
        raise NotImplementedError

    async def get_resource_totals(
        self,
        *,
        wallet_scope: str,
        resource_keys: Iterable[str],
    ) -> Dict[str, int]:
        raise NotImplementedError

    async def adjust_resource_totals(
        self,
        *,
        wallet_scope: str,
        deltas_by_key: Dict[str, int],
        updated_at: str | None = None,
    ) -> None:
        """Deprecated: use adjust_wallet_state_resource_totals instead."""
        raise NotImplementedError

    async def replace_resource_totals(
        self,
        *,
        wallet_scope: str,
        totals_by_key: Dict[str, int],
        updated_at: str | None = None,
    ) -> None:
        """Deprecated: use update_wallet_state instead."""
        raise NotImplementedError

    async def get_wait_heads(
        self,
        *,
        wallet_scope: str,
        resource_keys: Iterable[str],
    ) -> Dict[str, Dict[str, object]]:
        raise NotImplementedError

    async def replace_wait_heads(
        self,
        *,
        wallet_scope: str,
        resource_keys: Iterable[str],
        heads_by_key: Dict[str, Dict[str, object] | None],
        updated_at: str | None = None,
    ) -> None:
        """Deprecated: use update_wallet_state instead."""
        raise NotImplementedError

    async def get_wallet_state(self, *, wallet_scope: str) -> Dict[str, object] | None:
        raise NotImplementedError

    async def update_wallet_state(
        self,
        *,
        wallet_scope: str,
        resource_totals: Dict[str, int] | None = None,
        wait_heads: Dict[str, Dict[str, object] | None] | None = None,
        active_holders: Dict[str, Dict[str, object] | None] | None = None,
        updated_at: str | None = None,
    ) -> None:
        raise NotImplementedError

    async def adjust_wallet_state_resource_totals(
        self,
        *,
        wallet_scope: str,
        deltas_by_key: Dict[str, int],
        updated_at: str | None = None,
    ) -> None:
        raise NotImplementedError


class MongoReservationStore(ReservationStore):
    def __init__(self) -> None:
        if IndexModel is None or ReturnDocument is None:
            raise RuntimeError("pymongo is not installed")
        db = AsyncMongoDB.get_db()
        self._locks = db[_LOCKS_COLLECTION]
        self._records = db[_RECORDS_COLLECTION]
        self._waits = db[_WAITS_COLLECTION]
        self._totals = db[_TOTALS_COLLECTION]
        self._wait_heads = db[_WAIT_HEADS_COLLECTION]
        self._wallet_states = db[_WALLET_STATE_COLLECTION]
        self._indexes_ready = False
        self._index_lock: asyncio.Lock | None = None

    @staticmethod
    def _is_index_conflict_error(exc: Exception) -> bool:
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
        collection,
        indexes: list[object],
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

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        if IndexModel is None:
            raise RuntimeError("pymongo is not installed")
        if self._index_lock is None:
            self._index_lock = asyncio.Lock()
        async with self._index_lock:
            if self._indexes_ready:
                return
            await self._create_indexes_tolerant(
                self._locks,
                [
                    IndexModel(
                        [("wallet_scope", ASCENDING)],
                        unique=True,
                        name="uniq_wallet_reservation_lock_scope",
                    ),
                    IndexModel(
                        [("expires_at", ASCENDING)],
                        name="idx_wallet_reservation_lock_expires",
                    ),
                ],
                collection_name=_LOCKS_COLLECTION,
            )
            await self._create_indexes_tolerant(
                self._records,
                [
                    IndexModel(
                        [("reservation_id", ASCENDING)],
                        unique=True,
                        name="uniq_wallet_reservation_id",
                    ),
                    IndexModel(
                        [("wallet_scope", ASCENDING), ("status", ASCENDING)],
                        name="idx_wallet_reservation_scope_status",
                    ),
                    IndexModel(
                        [("wallet_scope", ASCENDING), ("expires_at", ASCENDING)],
                        name="idx_wallet_reservation_scope_expires",
                    ),
                    IndexModel(
                        [("execution_id", ASCENDING), ("node_id", ASCENDING)],
                        name="idx_wallet_reservation_execution_node",
                    ),
                    IndexModel(
                        [(_DELETE_AFTER_FIELD, ASCENDING)],
                        expireAfterSeconds=0,
                        partialFilterExpression={
                            _DELETE_AFTER_FIELD: {"$type": "date"}
                        },
                        name="ttl_wallet_reservation_delete_after",
                    ),
                ],
                collection_name=_RECORDS_COLLECTION,
            )
            await self._create_indexes_tolerant(
                self._waits,
                [
                    IndexModel(
                        [("wait_id", ASCENDING)],
                        unique=True,
                        name="uniq_wallet_funds_wait_id",
                    ),
                    IndexModel(
                        [
                            ("wallet_scope", ASCENDING),
                            ("status", ASCENDING),
                            ("created_at", ASCENDING),
                        ],
                        name="idx_wallet_funds_wait_scope_status_created",
                    ),
                    IndexModel(
                        [
                            ("wallet_scope", ASCENDING),
                            ("status", ASCENDING),
                            ("resume_after", ASCENDING),
                        ],
                        name="idx_wallet_funds_wait_scope_status_resume_after",
                    ),
                    IndexModel(
                        [("status", ASCENDING), ("resume_after", ASCENDING)],
                        name="idx_wallet_funds_wait_status_resume_after",
                    ),
                    IndexModel(
                        [("execution_id", ASCENDING), ("node_id", ASCENDING)],
                        name="idx_wallet_funds_wait_execution_node",
                    ),
                    IndexModel(
                        [(_DELETE_AFTER_FIELD, ASCENDING)],
                        expireAfterSeconds=0,
                        partialFilterExpression={
                            _DELETE_AFTER_FIELD: {"$type": "date"}
                        },
                        name="ttl_wallet_funds_wait_delete_after",
                    ),
                ],
                collection_name=_WAITS_COLLECTION,
            )
            await self._create_indexes_tolerant(
                self._wallet_states,
                [
                    IndexModel(
                        [("wallet_scope", ASCENDING)],
                        unique=True,
                        name="uniq_wallet_reservation_wallet_state_scope",
                    ),
                    IndexModel(
                        [("updated_at", ASCENDING)],
                        name="idx_wallet_reservation_wallet_state_updated",
                    ),
                ],
                collection_name=_WALLET_STATE_COLLECTION,
            )
            self._indexes_ready = True

    async def acquire_wallet_lock(
        self, *, wallet_scope: str, owner: str, ttl_seconds: int
    ) -> bool:
        await self._ensure_indexes()
        if ReturnDocument is None:
            raise RuntimeError("pymongo is not installed")
        now = utc_now()
        expires = now + timedelta(seconds=max(1, ttl_seconds))
        query = {
            "wallet_scope": str(wallet_scope),
            "$or": [
                {"owner": str(owner)},
                {"expires_at": {"$lte": now}},
            ],
        }
        update = {
            "$set": {
                "wallet_scope": str(wallet_scope),
                "owner": str(owner),
                "expires_at": expires,
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        }
        try:
            doc = await self._locks.find_one_and_update(
                query,
                update,
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError:
            return False
        return isinstance(doc, dict) and str(doc.get("owner") or "") == str(owner)

    async def release_wallet_lock(self, *, wallet_scope: str, owner: str) -> None:
        await self._ensure_indexes()
        await self._locks.delete_one(
            {"wallet_scope": str(wallet_scope), "owner": str(owner)}
        )

    async def get_record(self, reservation_id: str) -> ReservationRecord | None:
        await self._ensure_indexes()
        doc = await self._records.find_one(
            {"reservation_id": str(reservation_id)},
            {"_id": 0},
        )
        if not isinstance(doc, dict):
            return None
        return ReservationRecord.from_dict(doc)

    async def list_records(
        self,
        *,
        wallet_scope: str,
        statuses: Iterable[str] | None = None,
    ) -> List[ReservationRecord]:
        await self._ensure_indexes()
        query: Dict[str, object] = {"wallet_scope": str(wallet_scope)}
        if statuses is not None:
            query["status"] = {"$in": sorted({str(item).lower() for item in statuses})}
        cursor = self._records.find(
            query,
            {"_id": 0},
        )
        docs = await cursor.to_list(length=1000)
        return [
            ReservationRecord.from_dict(doc) for doc in docs if isinstance(doc, dict)
        ]

    async def save_record(self, record: ReservationRecord) -> None:
        await self._ensure_indexes()
        payload = record.to_dict()
        await self._records.update_one(
            {"reservation_id": record.reservation_id},
            _build_mongo_full_update(payload),
            upsert=True,
        )

    async def update_record(
        self, reservation_id: str, *, updates: Dict[str, object]
    ) -> None:
        await self._ensure_indexes()
        await self._records.update_one(
            {"reservation_id": str(reservation_id)},
            _build_mongo_partial_update(updates),
        )

    async def get_wait(self, wait_id: str) -> FundsWaitRecord | None:
        await self._ensure_indexes()
        doc = await self._waits.find_one({"wait_id": str(wait_id)}, {"_id": 0})
        if not isinstance(doc, dict):
            return None
        return FundsWaitRecord.from_dict(doc)

    async def list_waits(
        self,
        *,
        wallet_scope: str | None = None,
        statuses: Iterable[str] | None = None,
        exclude_wait_ids: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> List[FundsWaitRecord]:
        await self._ensure_indexes()
        query: Dict[str, object] = {}
        if wallet_scope is not None:
            query["wallet_scope"] = str(wallet_scope)
        if statuses is not None:
            query["status"] = {"$in": sorted({str(item).lower() for item in statuses})}
        if exclude_wait_ids is not None:
            excluded = sorted(
                {str(item).strip() for item in exclude_wait_ids if str(item).strip()}
            )
        else:
            excluded = []
        cursor = self._waits.find(
            query,
            {"_id": 0},
        ).sort("created_at", ASCENDING)
        if limit is not None and limit > 0:
            cursor = cursor.limit(int(limit))
        docs = await cursor.to_list(length=1000)
        waits = [
            FundsWaitRecord.from_dict(doc) for doc in docs if isinstance(doc, dict)
        ]
        if excluded:
            excluded_wait_ids = set(excluded)
            waits = [wait for wait in waits if wait.wait_id not in excluded_wait_ids]
        return waits

    async def save_wait(self, wait: FundsWaitRecord) -> None:
        await self._ensure_indexes()
        await self._waits.update_one(
            {"wait_id": str(wait.wait_id)},
            _build_mongo_full_update(wait.to_dict()),
            upsert=True,
        )

    async def update_wait(self, wait_id: str, *, updates: Dict[str, object]) -> None:
        await self._ensure_indexes()
        await self._waits.update_one(
            {"wait_id": str(wait_id)},
            _build_mongo_partial_update(updates),
        )

    async def transition_wait(
        self,
        wait_id: str,
        *,
        from_statuses: Iterable[str],
        updates: Dict[str, object],
    ) -> FundsWaitRecord | None:
        await self._ensure_indexes()
        if ReturnDocument is None:
            raise RuntimeError("pymongo is not installed")
        doc = await self._waits.find_one_and_update(
            {
                "wait_id": str(wait_id),
                "status": {
                    "$in": sorted({str(item).lower() for item in from_statuses})
                },
            },
            _build_mongo_partial_update(updates),
            return_document=ReturnDocument.AFTER,
        )
        if not isinstance(doc, dict):
            return None
        doc.pop("_id", None)
        return FundsWaitRecord.from_dict(doc)

    async def expire_records(
        self,
        *,
        wallet_scope: str,
        statuses: Iterable[str],
        expires_before,
        terminal_status: str,
        delete_after: str | None = None,
        reason: str | None = None,
        updated_at: str | None = None,
    ) -> None:
        await self._ensure_indexes()
        expires_before_iso = iso_utc(expires_before)
        updates: Dict[str, object] = {
            "status": str(terminal_status).strip().lower(),
            "updated_at": updated_at or expires_before_iso,
            "expires_at": expires_before_iso,
            _DELETE_AFTER_FIELD: delete_after,
        }
        if reason is not None:
            updates["reason"] = reason
        await self._records.update_many(
            {
                "wallet_scope": str(wallet_scope),
                "status": {"$in": sorted({str(item).lower() for item in statuses})},
                "expires_at": {"$lte": expires_before_iso},
            },
            _build_mongo_partial_update(updates),
        )

    async def requeue_waits(
        self,
        *,
        wallet_scope: str,
        from_status: str,
        ready_before,
        updated_at: str | None = None,
    ) -> None:
        await self._ensure_indexes()
        ready_before_iso = iso_utc(ready_before)
        await self._waits.update_many(
            {
                "wallet_scope": str(wallet_scope),
                "status": str(from_status).strip().lower(),
                "resume_after": {"$lte": ready_before_iso},
            },
            {
                "$set": {
                    "status": "queued",
                    "updated_at": updated_at or ready_before_iso,
                },
                "$unset": {_DELETE_AFTER_FIELD: ""},
            },
        )

    async def get_resource_totals(
        self,
        *,
        wallet_scope: str,
        resource_keys: Iterable[str],
    ) -> Dict[str, int]:
        await self._ensure_indexes()
        normalized_keys = sorted(
            {str(item).strip().lower() for item in resource_keys if str(item).strip()}
        )
        if not normalized_keys:
            return {}
        wallet_state = await self.get_wallet_state(wallet_scope=wallet_scope)
        wallet_totals: Dict[str, int] = {}
        resource_totals_raw: Dict[object, object] | None = None
        if isinstance(wallet_state, dict):
            maybe_resource_totals = wallet_state.get("resource_totals")
            if isinstance(maybe_resource_totals, dict):
                resource_totals_raw = maybe_resource_totals
        if resource_totals_raw is not None:
            wallet_totals = {
                str(key).strip().lower(): _from_mongo_units(value)
                for key, value in resource_totals_raw.items()
                if str(key).strip()
            }

        result: Dict[str, int] = {}
        missing_keys: List[str] = []
        for resource_key in normalized_keys:
            if resource_key in wallet_totals:
                result[resource_key] = wallet_totals[resource_key]
            else:
                missing_keys.append(resource_key)

        if not missing_keys:
            return result

        cursor = self._totals.find(
            {
                "wallet_scope": str(wallet_scope),
                "resource_key": {"$in": missing_keys},
            },
            {
                "_id": 0,
                "resource_key": 1,
                "reserved_base_units": 1,
            },
        )
        docs = await cursor.to_list(length=1000)
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            resource_key = str(doc.get("resource_key") or "").strip().lower()
            if resource_key in missing_keys:
                result[resource_key] = max(
                    0, _from_mongo_units(doc.get("reserved_base_units"))
                )

        for resource_key in missing_keys:
            if resource_key not in result:
                result[resource_key] = 0

        return result

    async def get_wait_heads(
        self,
        *,
        wallet_scope: str,
        resource_keys: Iterable[str],
    ) -> Dict[str, Dict[str, object]]:
        await self._ensure_indexes()
        normalized_keys = sorted(
            {str(item).strip().lower() for item in resource_keys if str(item).strip()}
        )
        if not normalized_keys:
            return {}
        wallet_state = await self.get_wallet_state(wallet_scope=wallet_scope)
        wallet_heads: Dict[str, Dict[str, object]] = {}
        wait_heads_raw: Dict[object, object] | None = None
        if isinstance(wallet_state, dict):
            maybe_wait_heads = wallet_state.get("wait_heads")
            if isinstance(maybe_wait_heads, dict):
                wait_heads_raw = maybe_wait_heads
        if wait_heads_raw is not None:
            wallet_heads = {
                str(key).strip().lower(): dict(value)
                for key, value in wait_heads_raw.items()
                if str(key).strip() and isinstance(value, dict)
            }

        result: Dict[str, Dict[str, object]] = {}
        missing_keys: List[str] = []
        for resource_key in normalized_keys:
            if resource_key in wallet_heads:
                result[resource_key] = wallet_heads[resource_key]
            else:
                missing_keys.append(resource_key)

        if not missing_keys:
            return result

        cursor = self._wait_heads.find(
            {
                "wallet_scope": str(wallet_scope),
                "resource_key": {"$in": missing_keys},
            },
            {"_id": 0},
        )
        docs = await cursor.to_list(length=1000)
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            resource_key = str(doc.get("resource_key") or "").strip().lower()
            if resource_key in missing_keys:
                result[resource_key] = doc

        return result

    async def get_wallet_state(self, *, wallet_scope: str) -> Dict[str, object] | None:
        await self._ensure_indexes()
        doc = await self._wallet_states.find_one(
            {"wallet_scope": str(wallet_scope)},
            {"_id": 0},
        )
        if not isinstance(doc, dict):
            return None
        return doc

    async def update_wallet_state(
        self,
        *,
        wallet_scope: str,
        resource_totals: Dict[str, int] | None = None,
        wait_heads: Dict[str, Dict[str, object] | None] | None = None,
        active_holders: Dict[str, Dict[str, object] | None] | None = None,
        updated_at: str | None = None,
    ) -> None:
        await self._ensure_indexes()
        now_iso = updated_at or iso_utc()
        set_payload: Dict[str, object] = {
            "wallet_scope": str(wallet_scope),
            "updated_at": now_iso,
        }
        for resource_key, total in (resource_totals or {}).items():
            normalized_key = str(resource_key).strip().lower()
            if not normalized_key:
                continue
            try:
                total_units = int(total)
            except Exception:
                total_units = 0
            set_payload[f"resource_totals.{normalized_key}"] = _to_mongo_units(
                max(0, total_units)
            )
        for resource_key, head in (wait_heads or {}).items():
            normalized_key = str(resource_key).strip().lower()
            if not normalized_key:
                continue
            set_payload[f"wait_heads.{normalized_key}"] = {
                "head_wait_id": (
                    str(head.get("head_wait_id") or "").strip()
                    if isinstance(head, dict) and head.get("head_wait_id")
                    else None
                ),
                "task_number": (
                    _to_optional_int(head.get("task_number"))
                    if isinstance(head, dict)
                    else None
                ),
                "title": (
                    str(head.get("title")).strip()
                    if isinstance(head, dict) and head.get("title") is not None
                    else None
                ),
                "head_created_at": (
                    str(head.get("head_created_at")).strip()
                    if isinstance(head, dict) and head.get("head_created_at")
                    else None
                ),
                "status": (
                    str(head.get("status")).strip().lower()
                    if isinstance(head, dict) and head.get("status")
                    else None
                ),
            }
        for resource_key, holder in (active_holders or {}).items():
            normalized_key = str(resource_key).strip().lower()
            if not normalized_key:
                continue
            set_payload[f"active_holders.{normalized_key}"] = (
                {
                    "reservation_id": (
                        str(holder.get("reservation_id") or "").strip()
                        if isinstance(holder, dict) and holder.get("reservation_id")
                        else None
                    ),
                    "execution_id": (
                        str(holder.get("execution_id") or "").strip()
                        if isinstance(holder, dict) and holder.get("execution_id")
                        else None
                    ),
                    "thread_id": (
                        str(holder.get("thread_id") or "").strip()
                        if isinstance(holder, dict) and holder.get("thread_id")
                        else None
                    ),
                    "task_number": (
                        _to_optional_int(holder.get("task_number"))
                        if isinstance(holder, dict)
                        else None
                    ),
                    "title": (
                        str(holder.get("title")).strip()
                        if isinstance(holder, dict) and holder.get("title") is not None
                        else None
                    ),
                    "node_id": (
                        str(holder.get("node_id")).strip()
                        if isinstance(holder, dict)
                        and holder.get("node_id") is not None
                        else None
                    ),
                    "status": (
                        str(holder.get("status")).strip().lower()
                        if isinstance(holder, dict) and holder.get("status")
                        else None
                    ),
                    "required_base_units": _to_mongo_units(
                        str(holder.get("required_base_units") or "0").strip()
                    ),
                }
                if isinstance(holder, dict)
                else None
            )
        await self._wallet_states.update_one(
            {"wallet_scope": str(wallet_scope)},
            {
                "$set": set_payload,
                "$setOnInsert": {"created_at": now_iso},
            },
            upsert=True,
        )

    async def adjust_wallet_state_resource_totals(
        self,
        *,
        wallet_scope: str,
        deltas_by_key: Dict[str, int],
        updated_at: str | None = None,
    ) -> None:
        await self._ensure_indexes()
        now_iso = updated_at or iso_utc()
        set_payload: Dict[str, object] = {
            "wallet_scope": str(wallet_scope),
            "updated_at": now_iso,
            "created_at": {"$ifNull": ["$created_at", now_iso]},
        }
        has_updates = False
        for resource_key, delta in (deltas_by_key or {}).items():
            normalized_key = str(resource_key).strip().lower()
            if not normalized_key:
                continue
            try:
                delta_units = int(delta)
            except Exception:
                delta_units = 0
            if delta_units == 0:
                continue
            has_updates = True
            set_payload[f"resource_totals.{normalized_key}"] = {
                "$max": [
                    Decimal128("0") if Decimal128 is not None else 0,
                    {
                        "$add": [
                            _mongo_decimal_convert(
                                f"$resource_totals.{normalized_key}"
                            ),
                            _to_mongo_units(delta_units),
                        ]
                    },
                ]
            }
        if not has_updates:
            return
        await self._wallet_states.update_one(
            {"wallet_scope": str(wallet_scope)},
            [{"$set": set_payload}],
            upsert=True,
        )


@dataclass
class _InMemoryLock:
    owner: str
    expires_at: str


class InMemoryReservationStore(ReservationStore):
    def __init__(self) -> None:
        self._mutex = asyncio.Lock()
        self._locks: Dict[str, _InMemoryLock] = {}
        self._records: Dict[str, ReservationRecord] = {}
        self._waits: Dict[str, FundsWaitRecord] = {}
        self._resource_totals: Dict[str, Dict[str, int]] = {}
        self._wait_heads: Dict[str, Dict[str, Dict[str, object]]] = {}
        self._wallet_states: Dict[str, Dict[str, object]] = {}

    async def acquire_wallet_lock(
        self, *, wallet_scope: str, owner: str, ttl_seconds: int
    ) -> bool:
        now = utc_now()
        expires = now + timedelta(seconds=max(1, ttl_seconds))
        async with self._mutex:
            current = self._locks.get(str(wallet_scope))
            current_expiry = parse_iso(current.expires_at) if current else None
            if (
                current is None
                or current.owner == str(owner)
                or (current_expiry is not None and current_expiry <= now)
            ):
                self._locks[str(wallet_scope)] = _InMemoryLock(
                    owner=str(owner),
                    expires_at=iso_utc(expires),
                )
                return True
            return False

    async def release_wallet_lock(self, *, wallet_scope: str, owner: str) -> None:
        async with self._mutex:
            current = self._locks.get(str(wallet_scope))
            if current and current.owner == str(owner):
                self._locks.pop(str(wallet_scope), None)

    async def get_record(self, reservation_id: str) -> ReservationRecord | None:
        async with self._mutex:
            return self._records.get(str(reservation_id))

    async def list_records(
        self,
        *,
        wallet_scope: str,
        statuses: Iterable[str] | None = None,
    ) -> List[ReservationRecord]:
        async with self._mutex:
            status_filter = (
                {str(item).lower() for item in statuses}
                if statuses is not None
                else None
            )
            records = []
            for record in self._records.values():
                if record.wallet_scope != str(wallet_scope):
                    continue
                if status_filter is not None and record.status not in status_filter:
                    continue
                records.append(record)
            return list(records)

    async def save_record(self, record: ReservationRecord) -> None:
        async with self._mutex:
            self._records[str(record.reservation_id)] = record

    async def update_record(
        self, reservation_id: str, *, updates: Dict[str, object]
    ) -> None:
        async with self._mutex:
            current = self._records.get(str(reservation_id))
            if current is None:
                return
            payload = current.to_dict()
            payload.update(dict(updates))
            self._records[str(reservation_id)] = ReservationRecord.from_dict(payload)

    async def get_wait(self, wait_id: str) -> FundsWaitRecord | None:
        async with self._mutex:
            return self._waits.get(str(wait_id))

    async def list_waits(
        self,
        *,
        wallet_scope: str | None = None,
        statuses: Iterable[str] | None = None,
        exclude_wait_ids: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> List[FundsWaitRecord]:
        async with self._mutex:
            status_filter = (
                {str(item).lower() for item in statuses}
                if statuses is not None
                else None
            )
            excluded_wait_ids = (
                {str(item).strip() for item in exclude_wait_ids if str(item).strip()}
                if exclude_wait_ids is not None
                else None
            )
            waits: List[FundsWaitRecord] = []
            for wait in self._waits.values():
                if wallet_scope is not None and wait.wallet_scope != str(wallet_scope):
                    continue
                if status_filter is not None and wait.status not in status_filter:
                    continue
                if excluded_wait_ids is not None and wait.wait_id in excluded_wait_ids:
                    continue
                waits.append(wait)
            waits.sort(
                key=lambda item: (
                    parse_iso(item.created_at) or utc_now(),
                    item.wait_id,
                )
            )
            if limit is not None and limit > 0:
                return waits[: int(limit)]
            return list(waits)

    async def save_wait(self, wait: FundsWaitRecord) -> None:
        async with self._mutex:
            self._waits[str(wait.wait_id)] = wait

    async def update_wait(self, wait_id: str, *, updates: Dict[str, object]) -> None:
        async with self._mutex:
            current = self._waits.get(str(wait_id))
            if current is None:
                return
            payload = current.to_dict()
            payload.update(dict(updates))
            self._waits[str(wait_id)] = FundsWaitRecord.from_dict(payload)

    async def transition_wait(
        self,
        wait_id: str,
        *,
        from_statuses: Iterable[str],
        updates: Dict[str, object],
    ) -> FundsWaitRecord | None:
        async with self._mutex:
            current = self._waits.get(str(wait_id))
            allowed = {str(item).lower() for item in from_statuses}
            if current is None or current.status not in allowed:
                return None
            payload = current.to_dict()
            payload.update(dict(updates))
            updated = FundsWaitRecord.from_dict(payload)
            self._waits[str(wait_id)] = updated
            return updated

    async def expire_records(
        self,
        *,
        wallet_scope: str,
        statuses: Iterable[str],
        expires_before,
        terminal_status: str,
        delete_after: str | None = None,
        reason: str | None = None,
        updated_at: str | None = None,
    ) -> None:
        now_iso = updated_at or iso_utc(expires_before)
        status_filter = {str(item).lower() for item in statuses}
        async with self._mutex:
            for reservation_id, record in list(self._records.items()):
                if (
                    record.wallet_scope != str(wallet_scope)
                    or record.status not in status_filter
                ):
                    continue
                expires_at = parse_iso(record.expires_at)
                if expires_at is None or expires_at > expires_before:
                    continue
                payload = record.to_dict()
                payload.update(
                    {
                        "status": str(terminal_status).strip().lower(),
                        "updated_at": now_iso,
                        "expires_at": iso_utc(expires_before),
                        "delete_after": delete_after,
                        "reason": reason,
                    }
                )
                self._records[reservation_id] = ReservationRecord.from_dict(payload)

    async def requeue_waits(
        self,
        *,
        wallet_scope: str,
        from_status: str,
        ready_before,
        updated_at: str | None = None,
    ) -> None:
        now_iso = updated_at or iso_utc(ready_before)
        normalized_from_status = str(from_status).strip().lower()
        async with self._mutex:
            for wait_id, wait in list(self._waits.items()):
                if (
                    wait.wallet_scope != str(wallet_scope)
                    or wait.status != normalized_from_status
                ):
                    continue
                resume_after = parse_iso(wait.resume_after)
                if resume_after is None or resume_after > ready_before:
                    continue
                payload = wait.to_dict()
                payload.update(
                    {
                        "status": "queued",
                        "updated_at": now_iso,
                        "delete_after": None,
                    }
                )
                self._waits[wait_id] = FundsWaitRecord.from_dict(payload)

    async def get_resource_totals(
        self,
        *,
        wallet_scope: str,
        resource_keys: Iterable[str],
    ) -> Dict[str, int]:
        normalized_keys = [
            str(item).strip().lower() for item in resource_keys if str(item).strip()
        ]
        async with self._mutex:
            wallet_state = self._wallet_states.get(str(wallet_scope))
            wallet_totals: Dict[object, object] = {}
            if isinstance(wallet_state, dict):
                maybe_wallet_totals = wallet_state.get("resource_totals")
                if isinstance(maybe_wallet_totals, dict):
                    wallet_totals = maybe_wallet_totals
            totals_by_wallet = self._resource_totals.get(str(wallet_scope), {})
            result: Dict[str, int] = {}
            for resource_key in normalized_keys:
                if resource_key in wallet_totals:
                    total_value = wallet_totals.get(resource_key)
                elif resource_key in totals_by_wallet:
                    total_value = totals_by_wallet.get(resource_key, 0)
                else:
                    continue
                total_units = _to_optional_int(total_value) or 0
                result[resource_key] = max(0, total_units)
            return result

    async def adjust_resource_totals(
        self,
        *,
        wallet_scope: str,
        deltas_by_key: Dict[str, int],
        updated_at: str | None = None,
    ) -> None:
        async with self._mutex:
            totals_by_wallet = self._resource_totals.setdefault(str(wallet_scope), {})
            for resource_key, delta in (deltas_by_key or {}).items():
                normalized_key = str(resource_key).strip().lower()
                if not normalized_key:
                    continue
                try:
                    delta_units = int(delta)
                except Exception:
                    delta_units = 0
                current = int(totals_by_wallet.get(normalized_key, 0))
                totals_by_wallet[normalized_key] = max(0, current + delta_units)

    async def replace_resource_totals(
        self,
        *,
        wallet_scope: str,
        totals_by_key: Dict[str, int],
        updated_at: str | None = None,
    ) -> None:
        async with self._mutex:
            totals_by_wallet = self._resource_totals.setdefault(str(wallet_scope), {})
            for resource_key, total in (totals_by_key or {}).items():
                normalized_key = str(resource_key).strip().lower()
                if not normalized_key:
                    continue
                try:
                    total_units = int(total)
                except Exception:
                    total_units = 0
                totals_by_wallet[normalized_key] = max(0, total_units)

    async def get_wait_heads(
        self,
        *,
        wallet_scope: str,
        resource_keys: Iterable[str],
    ) -> Dict[str, Dict[str, object]]:
        normalized_keys = [
            str(item).strip().lower() for item in resource_keys if str(item).strip()
        ]
        async with self._mutex:
            wallet_state = self._wallet_states.get(str(wallet_scope))
            wallet_heads: Dict[object, object] = {}
            if isinstance(wallet_state, dict):
                maybe_wallet_heads = wallet_state.get("wait_heads")
                if isinstance(maybe_wallet_heads, dict):
                    wallet_heads = maybe_wallet_heads
            heads_by_wallet = self._wait_heads.get(str(wallet_scope), {})
            result: Dict[str, Dict[str, object]] = {}
            for resource_key in normalized_keys:
                if resource_key in wallet_heads:
                    head_value = wallet_heads.get(resource_key)
                    if isinstance(head_value, dict):
                        result[resource_key] = dict(head_value)
                    continue
                if resource_key in heads_by_wallet:
                    result[resource_key] = dict(heads_by_wallet[resource_key])
            return result

    async def replace_wait_heads(
        self,
        *,
        wallet_scope: str,
        resource_keys: Iterable[str],
        heads_by_key: Dict[str, Dict[str, object] | None],
        updated_at: str | None = None,
    ) -> None:
        now_iso = updated_at or iso_utc()
        async with self._mutex:
            heads_by_wallet = self._wait_heads.setdefault(str(wallet_scope), {})
            for resource_key in {
                str(item).strip().lower() for item in resource_keys if str(item).strip()
            }:
                head = heads_by_key.get(resource_key)
                heads_by_wallet[resource_key] = {
                    "wallet_scope": str(wallet_scope),
                    "resource_key": resource_key,
                    "head_wait_id": (
                        str(head.get("head_wait_id") or "").strip()
                        if isinstance(head, dict) and head.get("head_wait_id")
                        else None
                    ),
                    "task_number": (
                        _to_optional_int(head.get("task_number"))
                        if isinstance(head, dict)
                        else None
                    ),
                    "title": (
                        str(head.get("title")).strip()
                        if isinstance(head, dict) and head.get("title") is not None
                        else None
                    ),
                    "head_created_at": (
                        str(head.get("head_created_at")).strip()
                        if isinstance(head, dict) and head.get("head_created_at")
                        else None
                    ),
                    "status": (
                        str(head.get("status")).strip().lower()
                        if isinstance(head, dict) and head.get("status")
                        else None
                    ),
                    "updated_at": now_iso,
                }

    async def get_wallet_state(self, *, wallet_scope: str) -> Dict[str, object] | None:
        async with self._mutex:
            state = self._wallet_states.get(str(wallet_scope))
            if state is None:
                return None
            resource_totals = state.get("resource_totals")
            wait_heads = state.get("wait_heads")
            active_holders = state.get("active_holders")
            return {
                "wallet_scope": str(wallet_scope),
                "resource_totals": (
                    dict(resource_totals) if isinstance(resource_totals, dict) else {}
                ),
                "wait_heads": dict(wait_heads) if isinstance(wait_heads, dict) else {},
                "active_holders": (
                    dict(active_holders) if isinstance(active_holders, dict) else {}
                ),
                "updated_at": state.get("updated_at"),
            }

    async def update_wallet_state(
        self,
        *,
        wallet_scope: str,
        resource_totals: Dict[str, int] | None = None,
        wait_heads: Dict[str, Dict[str, object] | None] | None = None,
        active_holders: Dict[str, Dict[str, object] | None] | None = None,
        updated_at: str | None = None,
    ) -> None:
        now_iso = updated_at or iso_utc()
        async with self._mutex:
            state = self._wallet_states.setdefault(
                str(wallet_scope),
                {
                    "wallet_scope": str(wallet_scope),
                    "resource_totals": {},
                    "wait_heads": {},
                    "active_holders": {},
                    "updated_at": now_iso,
                },
            )
            resource_totals_obj = state.get("resource_totals")
            if not isinstance(resource_totals_obj, dict):
                resource_totals_obj = {}
                state["resource_totals"] = resource_totals_obj
            resource_totals_map: Dict[str, int] = cast(
                Dict[str, int], resource_totals_obj
            )

            wait_heads_obj = state.get("wait_heads")
            if not isinstance(wait_heads_obj, dict):
                wait_heads_obj = {}
                state["wait_heads"] = wait_heads_obj
            wait_heads_map: Dict[str, Dict[str, object]] = cast(
                Dict[str, Dict[str, object]], wait_heads_obj
            )

            active_holders_obj = state.get("active_holders")
            if not isinstance(active_holders_obj, dict):
                active_holders_obj = {}
                state["active_holders"] = active_holders_obj
            active_holders_map: Dict[str, Dict[str, object] | None] = cast(
                Dict[str, Dict[str, object] | None], active_holders_obj
            )
            for resource_key, total in (resource_totals or {}).items():
                normalized_key = str(resource_key).strip().lower()
                if not normalized_key:
                    continue
                try:
                    total_units = int(total)
                except Exception:
                    total_units = 0
                resource_totals_map[normalized_key] = max(0, total_units)
            for resource_key, head in (wait_heads or {}).items():
                normalized_key = str(resource_key).strip().lower()
                if not normalized_key:
                    continue
                wait_heads_map[normalized_key] = {
                    "head_wait_id": (
                        str(head.get("head_wait_id") or "").strip()
                        if isinstance(head, dict) and head.get("head_wait_id")
                        else None
                    ),
                    "task_number": (
                        _to_optional_int(head.get("task_number"))
                        if isinstance(head, dict)
                        else None
                    ),
                    "title": (
                        str(head.get("title")).strip()
                        if isinstance(head, dict) and head.get("title") is not None
                        else None
                    ),
                    "head_created_at": (
                        str(head.get("head_created_at")).strip()
                        if isinstance(head, dict) and head.get("head_created_at")
                        else None
                    ),
                    "status": (
                        str(head.get("status")).strip().lower()
                        if isinstance(head, dict) and head.get("status")
                        else None
                    ),
                }
            for resource_key, holder in (active_holders or {}).items():
                normalized_key = str(resource_key).strip().lower()
                if not normalized_key:
                    continue
                active_holders_map[normalized_key] = (
                    {
                        "reservation_id": (
                            str(holder.get("reservation_id") or "").strip()
                            if isinstance(holder, dict) and holder.get("reservation_id")
                            else None
                        ),
                        "execution_id": (
                            str(holder.get("execution_id") or "").strip()
                            if isinstance(holder, dict) and holder.get("execution_id")
                            else None
                        ),
                        "thread_id": (
                            str(holder.get("thread_id") or "").strip()
                            if isinstance(holder, dict) and holder.get("thread_id")
                            else None
                        ),
                        "task_number": (
                            _to_optional_int(holder.get("task_number"))
                            if isinstance(holder, dict)
                            else None
                        ),
                        "title": (
                            str(holder.get("title")).strip()
                            if isinstance(holder, dict)
                            and holder.get("title") is not None
                            else None
                        ),
                        "node_id": (
                            str(holder.get("node_id")).strip()
                            if isinstance(holder, dict)
                            and holder.get("node_id") is not None
                            else None
                        ),
                        "status": (
                            str(holder.get("status")).strip().lower()
                            if isinstance(holder, dict) and holder.get("status")
                            else None
                        ),
                        "required_base_units": (
                            str(holder.get("required_base_units")).strip()
                            if isinstance(holder, dict)
                            and holder.get("required_base_units") is not None
                            else "0"
                        ),
                    }
                    if isinstance(holder, dict)
                    else None
                )
            state["updated_at"] = now_iso

    async def adjust_wallet_state_resource_totals(
        self,
        *,
        wallet_scope: str,
        deltas_by_key: Dict[str, int],
        updated_at: str | None = None,
    ) -> None:
        now_iso = updated_at or iso_utc()
        async with self._mutex:
            state = self._wallet_states.setdefault(
                str(wallet_scope),
                {
                    "wallet_scope": str(wallet_scope),
                    "resource_totals": {},
                    "wait_heads": {},
                    "active_holders": {},
                    "updated_at": now_iso,
                },
            )
            resource_totals_obj = state.get("resource_totals")
            if not isinstance(resource_totals_obj, dict):
                resource_totals_obj = {}
                state["resource_totals"] = resource_totals_obj
            resource_totals_map: Dict[str, int] = cast(
                Dict[str, int], resource_totals_obj
            )
            for resource_key, delta in (deltas_by_key or {}).items():
                normalized_key = str(resource_key).strip().lower()
                if not normalized_key:
                    continue
                try:
                    delta_units = int(delta)
                except Exception:
                    delta_units = 0
                current = int(resource_totals_map.get(normalized_key, 0) or 0)
                resource_totals_map[normalized_key] = max(0, current + delta_units)
            state["updated_at"] = now_iso
