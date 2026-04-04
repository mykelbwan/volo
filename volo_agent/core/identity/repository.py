from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from pymongo import ASCENDING, IndexModel, ReturnDocument
from pymongo.errors import OperationFailure

from core.database.mongodb_async import AsyncMongoDB

_USERS_COLLECTION = "users"
_LINK_TOKENS_COLLECTION = "link_tokens"
_INDEX_CONFLICT_CODES = {85, 86}

_LOGGER = logging.getLogger("volo.identity.repository")


class IdentityRepository:
    _indexes_ready = False

    def __init__(self) -> None:
        db = AsyncMongoDB.get_db()
        self.users = db[_USERS_COLLECTION]
        self.link_tokens = db[_LINK_TOKENS_COLLECTION]
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

        # Fallback path: try each index independently so one conflicting legacy
        # index cannot block startup or wallet provisioning.
        for model in indexes:
            name = None
            try:
                # IndexModel.document is part of PyMongo's public API.
                name = str(getattr(model, "document", {}).get("name"))
            except Exception:
                name = None
            try:
                await collection.create_indexes([model])
            except OperationFailure as exc:
                if not self._is_index_conflict_error(exc):
                    raise
                _LOGGER.warning(
                    "index_conflict collection=%s index=%s detail=%s keeping_existing=true",
                    collection_name,
                    name or "unknown",
                    exc,
                )

    async def ensure_indexes(self) -> None:
        if IdentityRepository._indexes_ready:
            return
        if self._index_lock is None:
            self._index_lock = asyncio.Lock()
        async with self._index_lock:
            if IdentityRepository._indexes_ready:
                return
            await self._create_indexes_tolerant(
                self.users,
                [
                    IndexModel(
                        [
                            ("identities.provider", ASCENDING),
                            ("identities.provider_user_id", ASCENDING),
                        ],
                        unique=True,
                        sparse=True,
                        name="uniq_identity_provider_id",
                    ),
                    IndexModel([("volo_user_id", ASCENDING)], name="idx_volo_user_id"),
                ],
                collection_name=_USERS_COLLECTION,
            )
            await self._create_indexes_tolerant(
                self.link_tokens,
                [
                    IndexModel(
                        [("token", ASCENDING)],
                        unique=True,
                        name="uniq_link_token",
                    ),
                    IndexModel(
                        [("expires_at", ASCENDING)],
                        expireAfterSeconds=0,
                        name="ttl_link_token_expires",
                    ),
                    IndexModel(
                        [("volo_user_id", ASCENDING), ("status", ASCENDING)],
                        name="idx_link_token_user_status",
                    ),
                ],
                collection_name=_LINK_TOKENS_COLLECTION,
            )
            IdentityRepository._indexes_ready = True

    async def get_user_by_identity(
        self, provider: str, provider_user_id: str
    ) -> Optional[dict[str, Any]]:
        return await self.users.find_one(
            {
                "identities": {
                    "$elemMatch": {
                        "provider": provider,
                        "provider_user_id": provider_user_id,
                    }
                }
            }
        )

    async def get_user_by_volo_id(self, volo_user_id: str) -> Optional[dict[str, Any]]:
        return await self.users.find_one({"volo_user_id": volo_user_id})

    async def insert_user(self, doc: dict[str, Any]) -> None:
        await self.ensure_indexes()
        await self.users.insert_one(doc)

    async def update_user(
        self, query: dict[str, Any], update: dict[str, Any], **kwargs: Any
    ) -> Any:
        await self.ensure_indexes()
        return await self.users.update_one(query, update, **kwargs)

    async def claim_link_token(
        self,
        token: str,
        *,
        provider: str,
        provider_user_id: str,
        now: Any,
    ) -> Optional[dict[str, Any]]:
        await self.ensure_indexes()
        return await self.link_tokens.find_one_and_update(
            {
                "token": token,
                "expires_at": {"$gt": now},
                "$or": [
                    {"status": "issued"},
                    {"status": {"$exists": False}, "used_at": {"$in": [None, False]}},
                ],
            },
            {
                "$set": {
                    "status": "used",
                    "used_at": now,
                    "used_by_provider": provider,
                    "used_by_provider_user_id": provider_user_id,
                }
            },
            return_document=ReturnDocument.AFTER,
        )

    async def get_link_token(self, token: str) -> Optional[dict[str, Any]]:
        await self.ensure_indexes()
        return await self.link_tokens.find_one({"token": token})

    async def revoke_issued_link_tokens(self, volo_user_id: str, now: Any) -> None:
        await self.ensure_indexes()
        await self.link_tokens.update_many(
            {"volo_user_id": volo_user_id, "status": "issued"},
            {
                "$set": {
                    "status": "revoked",
                    "revoked_at": now,
                    "revoked_reason": "reissued",
                }
            },
        )

    async def insert_link_token(self, doc: dict[str, Any]) -> None:
        await self.ensure_indexes()
        await self.link_tokens.insert_one(doc)
