from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from pymongo.errors import OperationFailure

from core.identity import (
    AsyncIdentityService,
    ExpiredLinkTokenError,
    LastIdentityError,
    UsedLinkTokenError,
)
from core.identity.repository import IdentityRepository
from tests.support.async_identity_mocks import make_identity_db_mocks, modified_result


def test_generate_link_token_inserts_expiry():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, _users_col, tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        svc = AsyncIdentityService()
        token = asyncio.run(svc.generate_link_token("volo-1", ttl_seconds=60))

        doc = tokens_col.insert_one.await_args.args[0]

        assert doc["token"] == token
        assert doc["volo_user_id"] == "volo-1"
        assert doc["status"] == "issued"
        assert doc["expires_at"] > doc["created_at"]
        tokens_col.update_many.assert_awaited_once()


def test_identity_repository_indexes_created_once():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, users_col, tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        repo_one = IdentityRepository()
        repo_two = IdentityRepository()
        asyncio.run(repo_one.ensure_indexes())
        asyncio.run(repo_two.ensure_indexes())

        users_col.create_indexes.assert_awaited_once()
        tokens_col.create_indexes.assert_awaited_once()


def test_identity_repository_indexes_tolerate_legacy_name_conflict():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, users_col, tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        conflict = OperationFailure(
            "Requested index has conflicting key for existing name",
            code=86,
        )
        users_col.create_indexes.side_effect = [conflict, None, None]
        tokens_col.create_indexes.side_effect = [conflict, None, None, None]

        repo = IdentityRepository()
        asyncio.run(repo.ensure_indexes())

        assert IdentityRepository._indexes_ready is True
        assert users_col.create_indexes.await_count == 3
        assert tokens_col.create_indexes.await_count == 4


def test_link_identity_by_token_success():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, users_col, tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        tokens_col.find_one_and_update.return_value = {
            "token": "ABCD1234",
            "volo_user_id": "volo-1",
        }
        users_col.find_one.side_effect = [
            None,
            {
                "volo_user_id": "volo-1",
                "sub_org_id": "sub",
                "sender_address": "0xabc",
                "identities": [],
            },
            {
                "volo_user_id": "volo-1",
                "sub_org_id": "sub",
                "sender_address": "0xabc",
                "identities": [
                    {
                        "provider": "discord",
                        "provider_user_id": "u-1",
                        "username": "alice",
                    }
                ],
            },
        ]
        users_col.update_one.return_value = modified_result(1)

        svc = AsyncIdentityService()
        user = asyncio.run(
            svc.link_identity_by_token("abcd1234", "discord", "u-1", username="alice")
        )

        users_col.update_one.assert_awaited()
        assert user["volo_user_id"] == "volo-1"
        assert user["sender_address"] == "0xabc"


def test_link_identity_by_token_idempotent_for_same_user():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, users_col, tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        tokens_col.find_one_and_update.return_value = {
            "token": "ABCD1234",
            "volo_user_id": "volo-1",
        }
        users_col.find_one.return_value = {
            "volo_user_id": "volo-1",
            "sub_org_id": "sub",
            "sender_address": "0xabc",
            "identities": [{"provider": "discord", "provider_user_id": "u-1"}],
        }

        svc = AsyncIdentityService()
        user = asyncio.run(svc.link_identity_by_token("ABCD1234", "discord", "u-1"))

        users_col.update_one.assert_not_awaited()
        assert user["volo_user_id"] == "volo-1"


def test_link_identity_by_token_conflict():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, users_col, tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        tokens_col.find_one_and_update.return_value = {
            "token": "ABCD1234",
            "volo_user_id": "volo-1",
        }
        users_col.find_one.return_value = {
            "volo_user_id": "volo-2",
            "identities": [{"provider": "discord", "provider_user_id": "u-1"}],
        }

        svc = AsyncIdentityService()
        with pytest.raises(ValueError):
            asyncio.run(svc.link_identity_by_token("ABCD1234", "discord", "u-1"))

        users_col.update_one.assert_not_awaited()


def test_link_identity_by_token_expired_returns_clear_error():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, _users_col, tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        tokens_col.find_one_and_update.return_value = None
        tokens_col.find_one.return_value = {
            "token": "ABCD1234",
            "volo_user_id": "volo-1",
            "status": "issued",
            "expires_at": datetime.utcnow() - timedelta(seconds=1),
        }

        svc = AsyncIdentityService()
        with pytest.raises(ExpiredLinkTokenError) as exc:
            asyncio.run(svc.link_identity_by_token("ABCD1234", "discord", "u-1"))

        assert "expired" in exc.value.user_message.lower()


def test_link_identity_by_token_used_returns_clear_error():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, _users_col, tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        tokens_col.find_one_and_update.return_value = None
        tokens_col.find_one.return_value = {
            "token": "ABCD1234",
            "volo_user_id": "volo-1",
            "status": "used",
            "used_at": datetime.utcnow(),
        }

        svc = AsyncIdentityService()
        with pytest.raises(UsedLinkTokenError) as exc:
            asyncio.run(svc.link_identity_by_token("ABCD1234", "discord", "u-1"))

        assert "already been used" in exc.value.user_message.lower()


def test_unlink_identity_blocks_last_identity():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, users_col, _tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        users_col.find_one.return_value = {
            "volo_user_id": "volo-1",
            "identities": [{"provider": "discord", "provider_user_id": "u-1"}],
        }

        svc = AsyncIdentityService()
        with pytest.raises(LastIdentityError):
            asyncio.run(svc.unlink_identity("discord", "u-1"))

        users_col.update_one.assert_not_awaited()


def test_unlink_identity_success():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, users_col, _tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        users_col.find_one.side_effect = [
            {
                "volo_user_id": "volo-1",
                "identities": [
                    {"provider": "discord", "provider_user_id": "u-1"},
                    {"provider": "telegram", "provider_user_id": "u-2"},
                ],
            },
            {
                "volo_user_id": "volo-1",
                "identities": [{"provider": "telegram", "provider_user_id": "u-2"}],
            },
        ]
        users_col.update_one.return_value = modified_result(1)

        svc = AsyncIdentityService()
        user = asyncio.run(svc.unlink_identity("discord", "u-1"))

        users_col.update_one.assert_awaited()
        assert user["volo_user_id"] == "volo-1"


def test_unlink_primary_identity_promotes_another_identity():
    IdentityRepository._indexes_ready = False
    with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
        mock_db, users_col, _tokens_col = make_identity_db_mocks()
        mock_get_db.return_value = mock_db

        users_col.find_one.side_effect = [
            {
                "volo_user_id": "volo-1",
                "identities": [
                    {
                        "provider": "discord",
                        "provider_user_id": "u-1",
                        "is_primary": True,
                    },
                    {
                        "provider": "telegram",
                        "provider_user_id": "u-2",
                        "is_primary": False,
                    },
                ],
            },
            {
                "volo_user_id": "volo-1",
                "identities": [
                    {
                        "provider": "telegram",
                        "provider_user_id": "u-2",
                        "is_primary": True,
                    }
                ],
            },
        ]
        users_col.update_one.return_value = modified_result(1)

        svc = AsyncIdentityService()
        user = asyncio.run(svc.unlink_identity("discord", "u-1"))

        assert users_col.update_one.await_args.kwargs["array_filters"] == [
            {
                "replacement.provider": "telegram",
                "replacement.provider_user_id": "u-2",
            }
        ]
        assert user["identities"][0]["is_primary"] is True
