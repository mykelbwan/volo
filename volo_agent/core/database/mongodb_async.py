from __future__ import annotations

import threading
from typing import Optional

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)

from core.database.mongodb import _CLIENT_KWARGS, _DEFAULT_DB, _build_uri


class AsyncMongoDB:
    _client: Optional[AsyncIOMotorClient] = None
    _client_lock = threading.Lock()

    @classmethod
    def get_client(cls) -> AsyncIOMotorClient:
        if cls._client is None:
            with cls._client_lock:
                if cls._client is None:
                    uri = _build_uri()
                    cls._client = AsyncIOMotorClient(uri, **_CLIENT_KWARGS)
        return cls._client

    @classmethod
    def get_db(cls, db_name: str = _DEFAULT_DB) -> AsyncIOMotorDatabase:
        return cls.get_client()[db_name]

    @classmethod
    def get_collection(
        cls,
        collection_name: str,
        db_name: str = _DEFAULT_DB,
    ) -> AsyncIOMotorCollection:
        return cls.get_db(db_name)[collection_name]

    @classmethod
    async def ping(cls) -> bool:
        try:
            await cls.get_client().admin.command("ping")
            return True
        except Exception:
            return False

    @classmethod
    def close(cls) -> None:
        with cls._client_lock:
            if cls._client is not None:
                cls._client.close()
                cls._client = None
