from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, NoReturn, Optional

from pymongo import ASCENDING

from core.database.mongodb_async import AsyncMongoDB

_DEFAULTS_COLLECTION = "policy_defaults"
_USER_POLICIES_COLLECTION = "user_policies"
_DEFAULT_POLICY_NAME = "guardrails"

logger = logging.getLogger("volo.policy_store")


class PolicyStoreError(RuntimeError):
    """Policy storage failure that should stop guardrail-dependent flows."""


class PolicyStoreUnavailableError(PolicyStoreError):
    """Raised when policy storage cannot be reached safely."""


class PolicyStoreDataError(PolicyStoreError):
    """Raised when stored policy data is malformed."""


class PolicyStore:
    """
    Mongo-backed policy store for guardrail defaults and per-user overrides.
    """

    _async_indexes_ensured = False
    _async_indexes_lock: asyncio.Lock | None = None
    _retry_after_monotonic = 0.0
    _failure_retry_seconds = 60.0

    def __init__(self) -> None:
        self.async_db = None
        self.async_defaults_collection = None
        self.async_user_policies_collection = None

    @classmethod
    def _db_temporarily_unavailable(cls) -> bool:
        return time.monotonic() < cls._retry_after_monotonic

    @classmethod
    def _mark_db_unavailable(cls) -> None:
        cls._retry_after_monotonic = time.monotonic() + cls._failure_retry_seconds

    @classmethod
    def _raise_unavailable(
        cls, detail: str, exc: Exception | None = None
    ) -> NoReturn:
        cls._mark_db_unavailable()
        if exc is not None:
            logger.warning("%s: %s", detail, exc)
        raise PolicyStoreUnavailableError(detail) from exc

    @staticmethod
    def _extract_policy(doc: Any, *, scope: str) -> Optional[Dict[str, Any]]:
        if not doc:
            return None
        policy = doc.get("policy")
        if not isinstance(policy, dict):
            raise PolicyStoreDataError(f"{scope} policy document is malformed.")
        return policy

    def _get_async_db(self):
        if self.async_db is None:
            self.async_db = AsyncMongoDB.get_db()
        return self.async_db

    def _get_async_defaults_collection(self):
        if self.async_defaults_collection is None:
            self.async_defaults_collection = self._get_async_db()[
                _DEFAULTS_COLLECTION
            ]
        return self.async_defaults_collection

    def _get_async_user_policies_collection(self):
        if self.async_user_policies_collection is None:
            self.async_user_policies_collection = self._get_async_db()[
                _USER_POLICIES_COLLECTION
            ]
        return self.async_user_policies_collection

    async def _ensure_indexes_async(self) -> None:
        if PolicyStore._async_indexes_ensured:
            return
        if PolicyStore._db_temporarily_unavailable():
            return
        if PolicyStore._async_indexes_lock is None:
            PolicyStore._async_indexes_lock = asyncio.Lock()
        async with PolicyStore._async_indexes_lock:
            if PolicyStore._async_indexes_ensured:
                return
            if PolicyStore._db_temporarily_unavailable():
                return
            try:
                await self._get_async_defaults_collection().create_index(
                    [("name", ASCENDING)],
                    unique=True,
                    name="uniq_policy_default_name",
                )
                await self._get_async_user_policies_collection().create_index(
                    [("volo_user_id", ASCENDING)],
                    unique=True,
                    name="uniq_user_policy",
                )
                PolicyStore._async_indexes_ensured = True
            except Exception as exc:
                PolicyStore._raise_unavailable(
                    "Policy storage is temporarily unavailable.", exc
                )

    async def aget_default_policy(self) -> Optional[Dict[str, Any]]:
        if PolicyStore._db_temporarily_unavailable():
            raise PolicyStoreUnavailableError(
                "Policy storage is temporarily unavailable."
            )
        await self._ensure_indexes_async()
        try:
            doc = await self._get_async_defaults_collection().find_one(
                {"name": _DEFAULT_POLICY_NAME}
            )
        except Exception as exc:
            PolicyStore._raise_unavailable(
                "Policy storage is temporarily unavailable.", exc
            )
        return self._extract_policy(doc, scope="default")

    async def aget_user_policy(self, volo_user_id: str) -> Optional[Dict[str, Any]]:
        if PolicyStore._db_temporarily_unavailable():
            raise PolicyStoreUnavailableError(
                "Policy storage is temporarily unavailable."
            )
        await self._ensure_indexes_async()
        try:
            doc = await self._get_async_user_policies_collection().find_one(
                {"volo_user_id": str(volo_user_id)}
            )
        except Exception as exc:
            PolicyStore._raise_unavailable(
                "Policy storage is temporarily unavailable.", exc
            )
        return self._extract_policy(doc, scope="user")

    async def aget_effective_policy(
        self, volo_user_id: str
    ) -> Optional[Dict[str, Any]]:
        base = await self.aget_default_policy() or {}
        override = await self.aget_user_policy(volo_user_id) or {}
        if not base and not override:
            return None
        merged = dict(base)
        merged.update(override)
        return merged
