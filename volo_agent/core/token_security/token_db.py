from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ASCENDING, IndexModel
from pymongo.collection import Collection

from core.database.mongodb import MongoDB
from core.database.mongodb_async import AsyncMongoDB

logger = logging.getLogger(__name__)

_COLLECTION_NAME = "token_registry"
_INDEX_MODELS = [
    IndexModel(
        [("_reg_key", ASCENDING)],
        unique=True,
        name="idx_reg_key",
    ),
    IndexModel(
        [("symbol", ASCENDING)],
        name="idx_symbol",
    ),
    IndexModel(
        [("aliases", ASCENDING)],
        name="idx_aliases",
    ),
    IndexModel(
        [("chain_id", ASCENDING)],
        name="idx_chain_id",
    ),
    IndexModel(
        [("chain_id", ASCENDING), ("address_lower", ASCENDING)],
        name="idx_chain_address_lower",
    ),
]


def _reg_key(symbol: str, chain_id: int) -> str:
    return f"{symbol.upper()}:{chain_id}"


@dataclass
class TokenRegistryEntry:
    symbol: str
    chain_name: str
    chain_id: int
    address: str
    decimals: int
    name: Optional[str] = None
    aliases: list[str] = field(default_factory=list)
    is_active: bool = True
    source: str = "registry"
    added_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @property
    def reg_key(self) -> str:
        """Stable compound lookup key for this entry."""
        return _reg_key(self.symbol, self.chain_id)

    def to_doc(self) -> dict:
        """Serialise to a plain dict for a MongoDB upsert."""
        return {
            "_reg_key": self.reg_key,
            "symbol": self.symbol.upper(),
            "name": self.name,
            "chain_name": self.chain_name.lower(),
            "chain_id": self.chain_id,
            "address": self.address,
            "address_lower": self.address.strip().lower(),
            "decimals": self.decimals,
            "aliases": [a.lower() for a in self.aliases],
            "is_active": self.is_active,
            "source": self.source,
            "added_at": self.added_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_doc(cls, doc: dict) -> "TokenRegistryEntry":
        """Reconstruct a TokenRegistryEntry from a raw MongoDB document."""

        # Ensure timestamps are timezone-aware (MongoDB stores UTC)
        def _aware(dt: object) -> datetime:
            if isinstance(dt, datetime) and dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            if isinstance(dt, datetime):
                return dt
            return datetime.now(tz=timezone.utc)

        return cls(
            symbol=doc["symbol"],
            name=doc.get("name"),
            chain_name=doc["chain_name"],
            chain_id=doc["chain_id"],
            address=doc["address"],
            decimals=doc.get("decimals", 18),
            aliases=doc.get("aliases", []),
            is_active=doc.get("is_active", True),
            source=doc.get("source", "registry"),
            added_at=_aware(doc.get("added_at")),
            updated_at=_aware(doc.get("updated_at")),
        )


class TokenRegistryDB:
    def __init__(self, collection: Optional[Collection] = None) -> None:
        self._col: Collection = collection or MongoDB.get_collection(_COLLECTION_NAME)
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self._col.create_indexes(_INDEX_MODELS)
        except Exception as exc:
            # Non-fatal — stale indexes won't break reads/writes.
            logger.warning("TokenRegistryDB: index creation warning: %s", exc)

    def get(self, symbol: str, chain_id: int) -> Optional[TokenRegistryEntry]:
        key = _reg_key(symbol, chain_id)
        doc = self._col.find_one({"_reg_key": key, "is_active": True})
        if doc is None:
            return None
        try:
            return TokenRegistryEntry.from_doc(doc)
        except Exception as exc:
            logger.warning(
                "TokenRegistryDB.get: failed to deserialise doc for %s: %s",
                key,
                exc,
            )
            return None

    def get_by_alias(self, alias: str, chain_id: int) -> Optional[TokenRegistryEntry]:
        doc = self._col.find_one(
            {
                "aliases": alias.lower(),
                "chain_id": chain_id,
                "is_active": True,
            }
        )
        if doc is None:
            return None
        try:
            return TokenRegistryEntry.from_doc(doc)
        except Exception as exc:
            logger.warning(
                "TokenRegistryDB.get_by_alias: failed to deserialise "
                "alias=%s chain_id=%d: %s",
                alias,
                chain_id,
                exc,
            )
            return None

    def get_by_address(
        self, address: str, chain_id: int
    ) -> Optional[TokenRegistryEntry]:
        normalized_address = address.strip().lower()
        if not normalized_address:
            return None

        doc = self._col.find_one(
            {
                "address_lower": normalized_address,
                "chain_id": chain_id,
                "is_active": True,
            }
        )
        if doc is None:
            doc = self._col.find_one(
                {
                    "address": {
                        "$regex": f"^{re.escape(address.strip())}$",
                        "$options": "i",
                    },
                    "chain_id": chain_id,
                    "is_active": True,
                }
            )
        if doc is None:
            return None
        try:
            return TokenRegistryEntry.from_doc(doc)
        except Exception as exc:
            logger.warning(
                "TokenRegistryDB.get_by_address: failed to deserialise address=%s: %s",
                address,
                exc,
            )
            return None

    def upsert(self, entry: TokenRegistryEntry) -> None:
        """Insert or replace a token registry entry."""
        doc = entry.to_doc()
        self._col.replace_one({"_reg_key": entry.reg_key}, doc, upsert=True)

    def list_active_entries(self) -> list[TokenRegistryEntry]:
        docs = self._col.find({"is_active": True})
        entries: list[TokenRegistryEntry] = []
        for doc in docs:
            try:
                entries.append(TokenRegistryEntry.from_doc(doc))
            except Exception as exc:
                logger.warning(
                    "TokenRegistryDB.list_active_entries: failed to deserialise entry: %s",
                    exc,
                )
        return entries


_REGISTRY: Optional[TokenRegistryDB] = None
_ASYNC_REGISTRY: Optional["AsyncTokenRegistryDB"] = None


class AsyncTokenRegistryDB:
    def __init__(self, collection: Optional[AsyncIOMotorCollection] = None) -> None:
        self._col: AsyncIOMotorCollection = collection or AsyncMongoDB.get_collection(
            _COLLECTION_NAME
        )
        self._indexes_ready = False
        self._indexes_lock = asyncio.Lock()

    async def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        async with self._indexes_lock:
            if self._indexes_ready:
                return
            try:
                await self._col.create_indexes(_INDEX_MODELS)
                self._indexes_ready = True
            except Exception as exc:
                # Non-fatal — stale indexes won't break reads/writes.
                logger.warning("AsyncTokenRegistryDB: index creation warning: %s", exc)

    async def get(self, symbol: str, chain_id: int) -> Optional[TokenRegistryEntry]:
        await self.ensure_indexes()
        key = _reg_key(symbol, chain_id)
        doc = await self._col.find_one({"_reg_key": key, "is_active": True})
        if doc is None:
            return None
        try:
            return TokenRegistryEntry.from_doc(doc)
        except Exception as exc:
            logger.warning(
                "AsyncTokenRegistryDB.get: failed to deserialise doc for %s: %s",
                key,
                exc,
            )
            return None

    async def get_by_alias(
        self, alias: str, chain_id: int
    ) -> Optional[TokenRegistryEntry]:
        await self.ensure_indexes()
        doc = await self._col.find_one(
            {
                "aliases": alias.lower(),
                "chain_id": chain_id,
                "is_active": True,
            }
        )
        if doc is None:
            return None
        try:
            return TokenRegistryEntry.from_doc(doc)
        except Exception as exc:
            logger.warning(
                "AsyncTokenRegistryDB.get_by_alias: failed to deserialise "
                "alias=%s chain_id=%d: %s",
                alias,
                chain_id,
                exc,
            )
            return None

    async def get_by_address(
        self, address: str, chain_id: int
    ) -> Optional[TokenRegistryEntry]:
        await self.ensure_indexes()
        normalized_address = address.strip().lower()
        if not normalized_address:
            return None

        doc = await self._col.find_one(
            {
                "address_lower": normalized_address,
                "chain_id": chain_id,
                "is_active": True,
            }
        )
        if doc is None:
            doc = await self._col.find_one(
                {
                    "address": {
                        "$regex": f"^{re.escape(address.strip())}$",
                        "$options": "i",
                    },
                    "chain_id": chain_id,
                    "is_active": True,
                }
            )
        if doc is None:
            return None
        try:
            return TokenRegistryEntry.from_doc(doc)
        except Exception as exc:
            logger.warning(
                "AsyncTokenRegistryDB.get_by_address: failed to deserialise "
                "address=%s: %s",
                address,
                exc,
            )
            return None

    async def upsert(self, entry: TokenRegistryEntry) -> None:
        """Insert or replace a token registry entry asynchronously."""
        await self.ensure_indexes()
        doc = entry.to_doc()
        await self._col.replace_one({"_reg_key": entry.reg_key}, doc, upsert=True)

    async def list_active_entries(self) -> list[TokenRegistryEntry]:
        await self.ensure_indexes()
        cursor = self._col.find({"is_active": True})
        entries: list[TokenRegistryEntry] = []
        async for doc in cursor:
            try:
                entries.append(TokenRegistryEntry.from_doc(doc))
            except Exception as exc:
                logger.warning(
                    "AsyncTokenRegistryDB.list_active_entries: failed to deserialise entry: %s",
                    exc,
                )
        return entries


def get_token_registry() -> TokenRegistryDB:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = TokenRegistryDB()
    return _REGISTRY


def get_async_token_registry() -> AsyncTokenRegistryDB:
    global _ASYNC_REGISTRY
    if _ASYNC_REGISTRY is None:
        _ASYNC_REGISTRY = AsyncTokenRegistryDB()
    return _ASYNC_REGISTRY
