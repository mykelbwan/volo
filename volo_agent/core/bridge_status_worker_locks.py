from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pymongo import ASCENDING, IndexModel, ReturnDocument
from pymongo.errors import DuplicateKeyError

from core.database.mongodb_async import AsyncMongoDB

LOCK_COLLECTION = "bridge_status_worker_locks"


async def ensure_bridge_status_worker_lock_indexes() -> None:
    locks = AsyncMongoDB.get_collection(LOCK_COLLECTION)
    await locks.create_indexes(
        [
            IndexModel(
                [("expires_at", ASCENDING)],
                expireAfterSeconds=0,
                name="ttl_bridge_status_worker_lock_expires_at",
            )
        ]
    )


async def acquire_bridge_status_worker_lock(
    lock_id: str,
    owner: str,
    ttl_seconds: int,
) -> bool:
    locks = AsyncMongoDB.get_collection(LOCK_COLLECTION)
    now_dt = datetime.now(timezone.utc)
    now_ts = int(now_dt.timestamp())
    expiry = now_dt + timedelta(seconds=max(1, ttl_seconds))
    try:
        doc = await locks.find_one_and_update(
            {
                "_id": lock_id,
                "$or": [
                    {"expires_at": {"$lte": now_dt}},
                    {"expires_at": {"$type": "number", "$lte": now_ts}},
                    {"owner": owner},
                ],
            },
            {"$set": {"owner": owner, "expires_at": expiry}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        return False
    return bool(doc and doc.get("owner") == owner)
