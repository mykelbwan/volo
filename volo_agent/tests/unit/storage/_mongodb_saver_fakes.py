import copy
import pickle
from dataclasses import dataclass
from typing import Any

from core.database.mongodb_saver import MongoDBSaver
from core.database.mongodb_saver_async import AsyncMongoDBSaver


class PickleSerde:
    def dumps_typed(self, value: Any) -> tuple[str, bytes]:
        return ("pickle", pickle.dumps(value))

    def loads_typed(self, typed: tuple[str, bytes]) -> Any:
        _type, data = typed
        return pickle.loads(bytes(data))


class FakeDeleteResult:
    def __init__(self, deleted_count: int) -> None:
        self.deleted_count = deleted_count


class FakeUpdateResult:
    def __init__(self, upserted_id: Any = None) -> None:
        self.upserted_id = upserted_id


class FakeCursor:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = docs

    def limit(self, limit: int) -> "FakeCursor":
        return FakeCursor(self._docs[:limit])

    def __iter__(self):
        return iter(copy.deepcopy(self._docs))


class FakeAsyncCursor:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = docs

    def limit(self, limit: int) -> "FakeAsyncCursor":
        return FakeAsyncCursor(self._docs[:limit])

    async def to_list(self, length: int | None = None) -> list[dict[str, Any]]:
        docs = self._docs if length is None else self._docs[:length]
        return copy.deepcopy(docs)


class FakeCollection:
    def __init__(self) -> None:
        self.docs: list[dict[str, Any]] = []
        self.indexes: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self._next_id = 1

    def create_index(self, keys, **kwargs):
        self.indexes.append((tuple(keys), kwargs))
        return kwargs.get("name", "idx")

    def insert_one(self, doc: dict[str, Any]) -> None:
        self.docs.append(self._copy_with_id(doc))

    def update_one(self, filter_doc: dict[str, Any], update: dict[str, Any], upsert: bool = False):
        for doc in self.docs:
            if self._matches(doc, filter_doc):
                if "$set" in update:
                    doc.update(copy.deepcopy(update["$set"]))
                return FakeUpdateResult()

        if not upsert:
            return FakeUpdateResult()

        new_doc = {
            key: value
            for key, value in filter_doc.items()
            if not isinstance(value, dict)
        }
        if "$setOnInsert" in update:
            new_doc.update(copy.deepcopy(update["$setOnInsert"]))
        if "$set" in update:
            new_doc.update(copy.deepcopy(update["$set"]))
        new_doc = self._copy_with_id(new_doc)
        self.docs.append(new_doc)
        return FakeUpdateResult(upserted_id=new_doc["_id"])

    def find_one(
        self,
        query: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
    ) -> dict[str, Any] | None:
        docs = self._find_docs(query, projection=projection, sort=sort)
        return docs[0] if docs else None

    def find(
        self,
        query: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
    ) -> FakeCursor:
        return FakeCursor(self._find_docs(query, projection=projection, sort=sort))

    def delete_many(self, query: dict[str, Any]) -> FakeDeleteResult:
        kept_docs: list[dict[str, Any]] = []
        deleted_count = 0
        for doc in self.docs:
            if self._matches(doc, query):
                deleted_count += 1
                continue
            kept_docs.append(doc)
        self.docs = kept_docs
        return FakeDeleteResult(deleted_count)

    def all_docs(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self.docs)

    def _find_docs(
        self,
        query: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
    ) -> list[dict[str, Any]]:
        docs = [copy.deepcopy(doc) for doc in self.docs if self._matches(doc, query)]
        if sort:
            for field, direction in reversed(sort):
                docs.sort(
                    key=lambda doc: (field not in doc, doc.get(field)),
                    reverse=direction < 0,
                )
        if projection is not None:
            included_fields = {field for field, include in projection.items() if include}
            docs = [
                {field: doc[field] for field in included_fields if field in doc}
                for doc in docs
            ]
        return docs

    def _copy_with_id(self, doc: dict[str, Any]) -> dict[str, Any]:
        copied = copy.deepcopy(doc)
        if "_id" not in copied:
            copied["_id"] = self._next_id
            self._next_id += 1
        return copied

    def _matches(self, doc: dict[str, Any], query: dict[str, Any]) -> bool:
        for key, expected in query.items():
            if key == "$or":
                if not any(self._matches(doc, clause) for clause in expected):
                    return False
                continue

            actual = doc.get(key)
            if isinstance(expected, dict):
                for operator, value in expected.items():
                    if operator == "$in":
                        if actual not in value:
                            return False
                    elif operator == "$lt":
                        if actual is None or actual >= value:
                            return False
                    else:
                        raise NotImplementedError(operator)
                continue

            if actual != expected:
                return False

        return True


class FakeAsyncCollection:
    def __init__(self, sync_collection: FakeCollection) -> None:
        self._sync = sync_collection

    async def create_index(self, keys, **kwargs):
        return self._sync.create_index(keys, **kwargs)

    async def update_one(self, filter_doc: dict[str, Any], update: dict[str, Any], upsert: bool = False):
        return self._sync.update_one(filter_doc, update, upsert=upsert)

    async def find_one(
        self,
        query: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
    ) -> dict[str, Any] | None:
        return self._sync.find_one(query, projection=projection, sort=sort)

    def find(
        self,
        query: dict[str, Any],
        projection: dict[str, Any] | None = None,
        sort: list[tuple[str, int]] | None = None,
    ) -> FakeAsyncCursor:
        return FakeAsyncCursor(self._sync._find_docs(query, projection=projection, sort=sort))

    async def delete_many(self, query: dict[str, Any]) -> FakeDeleteResult:
        return self._sync.delete_many(query)


@dataclass
class FakeSaverCollections:
    checkpoints: FakeCollection
    blobs: FakeCollection
    writes: FakeCollection


def build_sync_saver() -> tuple[MongoDBSaver, FakeSaverCollections]:
    collections = FakeSaverCollections(
        checkpoints=FakeCollection(),
        blobs=FakeCollection(),
        writes=FakeCollection(),
    )
    saver = object.__new__(MongoDBSaver)
    saver._checkpoints = collections.checkpoints
    saver._blobs = collections.blobs
    saver._writes = collections.writes
    saver.serde = PickleSerde()
    return saver, collections


def build_async_saver() -> tuple[AsyncMongoDBSaver, FakeSaverCollections]:
    collections = FakeSaverCollections(
        checkpoints=FakeCollection(),
        blobs=FakeCollection(),
        writes=FakeCollection(),
    )
    saver = object.__new__(AsyncMongoDBSaver)
    saver._checkpoints = FakeAsyncCollection(collections.checkpoints)
    saver._blobs = FakeAsyncCollection(collections.blobs)
    saver._writes = FakeAsyncCollection(collections.writes)
    saver.serde = PickleSerde()
    saver._indexes_ready = True
    saver._index_lock = None
    return saver, collections


def make_checkpoint(
    checkpoint_id: str,
    *,
    channel_versions: dict[str, Any],
    channel_values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2020-01-01T00:00:00Z",
        "channel_values": channel_values or {},
        "channel_versions": channel_versions,
        "versions_seen": {},
        "updated_channels": None,
    }


def make_metadata() -> dict[str, Any]:
    return {"source": "input", "step": -1, "parents": {}, "run_id": "run-1"}


def make_config(
    thread_id: str,
    checkpoint_ns: str = "",
    checkpoint_id: str | None = None,
) -> dict[str, Any]:
    configurable = {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}
