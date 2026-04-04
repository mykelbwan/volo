from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

from core.database.mongodb import MongoDB
from core.database.mongodb_async import AsyncMongoDB


def test_mongodb_get_client_is_singleton_under_race():
    MongoDB.close()
    created: list[object] = []
    barrier = threading.Barrier(8)

    def _build() -> None:
        barrier.wait()
        MongoDB.get_client()

    def _fake_client(*_args, **_kwargs):
        time.sleep(0.01)
        client = MagicMock()
        created.append(client)
        return client

    with (
        patch("core.database.mongodb._build_uri", return_value="mongodb://unit-test"),
        patch("core.database.mongodb.MongoClient", side_effect=_fake_client),
    ):
        threads = [threading.Thread(target=_build) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert len(created) == 1
    MongoDB.close()


def test_async_mongodb_get_client_is_singleton_under_race():
    AsyncMongoDB.close()
    created: list[object] = []
    barrier = threading.Barrier(8)

    def _build() -> None:
        barrier.wait()
        AsyncMongoDB.get_client()

    def _fake_client(*_args, **_kwargs):
        time.sleep(0.01)
        client = MagicMock()
        created.append(client)
        return client

    with (
        patch(
            "core.database.mongodb_async._build_uri",
            return_value="mongodb://unit-test",
        ),
        patch(
            "core.database.mongodb_async.AsyncIOMotorClient",
            side_effect=_fake_client,
        ),
    ):
        threads = [threading.Thread(target=_build) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert len(created) == 1
    AsyncMongoDB.close()


def test_mongodb_close_closes_and_clears_singleton():
    fake_client = MagicMock()
    MongoDB.close()
    MongoDB._client = fake_client

    MongoDB.close()

    fake_client.close.assert_called_once()
    assert MongoDB._client is None


def test_async_mongodb_close_closes_and_clears_singleton():
    fake_client = MagicMock()
    AsyncMongoDB.close()
    AsyncMongoDB._client = fake_client

    AsyncMongoDB.close()

    fake_client.close.assert_called_once()
    assert AsyncMongoDB._client is None
