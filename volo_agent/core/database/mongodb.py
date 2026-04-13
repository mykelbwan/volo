from __future__ import annotations

import threading
from typing import Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from config.env import MONGODB_URI

_DEFAULT_DB = "auraagent"
_CLIENT_KWARGS = {
    "maxPoolSize": 50,
    "serverSelectionTimeoutMS": 5_000,
    "connectTimeoutMS": 5_000,
    "socketTimeoutMS": 30_000,
    "retryWrites": True,
    "retryReads": True,
}


def _build_uri() -> str:
    if MONGODB_URI:
        return MONGODB_URI

    raise ValueError(
        "MongoDB connection is not configured. "
        "Recovery path: set MONGODB_URI in your environment/.env, then restart the service."
    )


class MongoDB:
    _client: Optional[MongoClient] = None
    _client_lock = threading.Lock()

    @classmethod
    def get_client(cls) -> MongoClient:
        if cls._client is None:
            with cls._client_lock:
                if cls._client is None:
                    uri = _build_uri()
                    cls._client = MongoClient(uri, **_CLIENT_KWARGS)
        return cls._client

    @classmethod
    def get_db(cls, db_name: str = _DEFAULT_DB) -> Database:
        return cls.get_client()[db_name]

    @classmethod
    def get_collection(
        cls,
        collection_name: str,
        db_name: str = _DEFAULT_DB,
    ) -> Collection:
        return cls.get_db(db_name)[collection_name]

    @classmethod
    def ping(cls) -> bool:
        try:
            cls.get_client().admin.command("ping")
            return True
        except Exception:
            return False

    @classmethod
    def close(cls) -> None:
        with cls._client_lock:
            if cls._client is not None:
                cls._client.close()
                cls._client = None
