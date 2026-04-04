import asyncio
from unittest.mock import AsyncMock, patch

from pymongo import ASCENDING, IndexModel
from pymongo.errors import OperationFailure

from core.reservations.store import MongoReservationStore


def _mock_db():
    return {
        "wallet_reservation_locks": AsyncMock(),
        "wallet_reservations": AsyncMock(),
        "wallet_funds_waits": AsyncMock(),
        "wallet_reservation_resource_totals": AsyncMock(),
        "wallet_reservation_wait_heads": AsyncMock(),
        "wallet_reservation_wallet_states": AsyncMock(),
    }


def test_mongo_reservation_store_ensure_indexes_adds_ttl_indexes():
    db = _mock_db()

    with patch("core.reservations.store.AsyncMongoDB.get_db", return_value=db):
        store = MongoReservationStore()
        asyncio.run(store._ensure_indexes())

    record_models = db["wallet_reservations"].create_indexes.await_args.args[0]
    record_ttl_model = next(
        model
        for model in record_models
        if getattr(model, "document", {}).get("name")
        == "ttl_wallet_reservation_delete_after"
    )
    assert record_ttl_model.document["expireAfterSeconds"] == 0
    assert record_ttl_model.document["partialFilterExpression"] == {
        "delete_after": {"$type": "date"}
    }
    assert list(record_ttl_model.document["key"].items()) == [("delete_after", 1)]

    wait_models = db["wallet_funds_waits"].create_indexes.await_args.args[0]
    wait_ttl_model = next(
        model
        for model in wait_models
        if getattr(model, "document", {}).get("name")
        == "ttl_wallet_funds_wait_delete_after"
    )
    assert wait_ttl_model.document["expireAfterSeconds"] == 0
    assert wait_ttl_model.document["partialFilterExpression"] == {
        "delete_after": {"$type": "date"}
    }
    assert list(wait_ttl_model.document["key"].items()) == [("delete_after", 1)]


def test_mongo_reservation_store_tolerates_existing_index_conflicts():
    db = _mock_db()

    with patch("core.reservations.store.AsyncMongoDB.get_db", return_value=db):
        store = MongoReservationStore()

    collection = AsyncMock()
    collection.create_indexes.side_effect = [
        OperationFailure("index conflict", code=85),
        OperationFailure("index conflict", code=85),
    ]
    indexes = [IndexModel([("delete_after", ASCENDING)], name="ttl_wallet_reservation_delete_after")]

    asyncio.run(
        store._create_indexes_tolerant(
            collection,
            indexes,
            collection_name="wallet_reservations",
        )
    )

    assert collection.create_indexes.await_count == 2
