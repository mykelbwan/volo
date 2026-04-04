"""
core.database.mongodb – application-wide MongoDB connection singleton.

A single ``MongoClient`` instance is shared across the entire process.
``pymongo.MongoClient`` maintains an internal connection pool and is
fully thread-safe; creating one client per request (or per class
instantiation) is an anti-pattern that causes high latency and can
exhaust file-descriptor limits under load.

Usage
-----
::

    from core.database.mongodb import MongoDB

    # Get a database handle (creates the client on first call)
    db = MongoDB.get_db()                     # default: "auraagent"
    db = MongoDB.get_db("other_db")           # any database on the same server

    # Get the raw MongoClient when you need direct control
    # (e.g. MongoDBSaver, Motor async wrapper, admin commands)
    client = MongoDB.get_client()

    # Shortcut: get a collection directly
    col = MongoDB.get_collection("users")
    col = MongoDB.get_collection("checkpoints", db_name="langgraph")

    # Explicit shutdown (call once at process exit, not per-request)
    MongoDB.close()

Connection URI resolution (in priority order)
---------------------------------------------
1. ``MONGODB_URI`` env var   – full connection string, used as-is.
2. ValueError raised if neither is set.
"""

from __future__ import annotations

import threading
from typing import Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from config.env import MONGODB_URI

# ── Constants ─────────────────────────────────────────────────────────────────

_DEFAULT_DB = "auraagent"

# pymongo connection-pool tuning defaults.
# maxPoolSize: cap concurrent connections per host (default 100 is fine for most cases).
# serverSelectionTimeoutMS: fail fast when the cluster is unreachable.
# connectTimeoutMS: TCP handshake deadline.
# socketTimeoutMS: individual operation deadline.
_CLIENT_KWARGS = {
    "maxPoolSize": 50,
    "serverSelectionTimeoutMS": 5_000,
    "connectTimeoutMS": 5_000,
    "socketTimeoutMS": 30_000,
    "retryWrites": True,
    "retryReads": True,
}


# ── URI builder ────────────────────────────────────────────────────────────────


def _build_uri() -> str:
    """
    Resolve the MongoDB connection URI from environment variables.

    Raises
    ------
    ValueError
        If neither ``MONGODB_URI`` nor ``MONGODB_ATLAS_PASSWORD`` is set.
    """
    if MONGODB_URI:
        return MONGODB_URI

    raise ValueError(
        "MongoDB connection is not configured. "
        "Recovery path: set MONGODB_URI in your environment/.env, then restart the service."
    )


# ── Singleton ─────────────────────────────────────────────────────────────────


class MongoDB:
    """
    Process-level MongoDB connection singleton.

    The underlying ``MongoClient`` is created on the first call to
    ``get_client()`` (or any helper that delegates to it) and reused for
    the lifetime of the process.  All public methods are classmethods so no
    instantiation is required.

    Thread-safety
    -------------
    ``MongoClient`` is thread-safe.  Lazy initialisation is protected by
    ``_client_lock`` so concurrent first-use callers still converge on a
    single shared client instance.
    """

    _client: Optional[MongoClient] = None
    _client_lock = threading.Lock()

    # ── Core accessors ────────────────────────────────────────────────────────

    @classmethod
    def get_client(cls) -> MongoClient:
        """
        Return the shared ``MongoClient``, creating it on the first call.

        Returns
        -------
        MongoClient
            The singleton client instance.

        Raises
        ------
        ValueError
            If the connection URI cannot be resolved from env vars.
        """
        if cls._client is None:
            with cls._client_lock:
                if cls._client is None:
                    uri = _build_uri()
                    cls._client = MongoClient(uri, **_CLIENT_KWARGS)
        return cls._client

    @classmethod
    def get_db(cls, db_name: str = _DEFAULT_DB) -> Database:
        """
        Return a ``Database`` handle for *db_name*.

        This method is safe to call with different ``db_name`` values from
        the same process — each call returns ``client[db_name]`` via the
        shared client, so no extra network connections are opened.

        Parameters
        ----------
        db_name:
            MongoDB database name.  Defaults to ``"auraagent"``.

        Returns
        -------
        pymongo.database.Database
        """
        return cls.get_client()[db_name]

    @classmethod
    def get_collection(
        cls,
        collection_name: str,
        db_name: str = _DEFAULT_DB,
    ) -> Collection:
        """
        Return a ``Collection`` handle — a convenience shortcut for
        ``MongoDB.get_db(db_name)[collection_name]``.

        Parameters
        ----------
        collection_name:
            Name of the MongoDB collection.
        db_name:
            Database that owns the collection.  Defaults to ``"auraagent"``.

        Returns
        -------
        pymongo.collection.Collection
        """
        return cls.get_db(db_name)[collection_name]

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    @classmethod
    def ping(cls) -> bool:
        """
        Send a lightweight ``ping`` command to verify connectivity.

        Returns
        -------
        bool
            ``True`` if the server responded, ``False`` on any error.
        """
        try:
            cls.get_client().admin.command("ping")
            return True
        except Exception:
            return False

    @classmethod
    def close(cls) -> None:
        """
        Close the shared ``MongoClient`` and release all pooled connections.

        Call this once at clean process shutdown (e.g. in an ``atexit``
        handler or a FastAPI/Starlette ``on_event("shutdown")`` hook).
        Do **not** call it on every request — that would defeat the purpose
        of connection pooling.
        """
        with cls._client_lock:
            if cls._client is not None:
                cls._client.close()
                cls._client = None
