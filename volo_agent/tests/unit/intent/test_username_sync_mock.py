from __future__ import annotations

import asyncio
import uuid
from unittest.mock import patch

from core.identity import AsyncIdentityService
from core.identity.repository import IdentityRepository
from tests.support.async_identity_mocks import make_identity_db_mocks


def test_username_sync_logic():
    async def _run() -> None:
        IdentityRepository._indexes_ready = False
        with patch("core.database.mongodb_async.AsyncMongoDB.get_db") as mock_get_db:
            mock_db, users_collection, _tokens_col = make_identity_db_mocks()
            mock_get_db.return_value = mock_db

            user_service = AsyncIdentityService(
                provision_evm=lambda _uid: {"sub_org_id": "sub_123", "address": "0xabc"},
                provision_solana=lambda _uid: {
                    "sub_org_id": "sol_sub_123",
                    "address": "So1anaAddress",
                },
            )
            provider = "telegram"
            provider_user_id = "12345"
            volo_user_id = str(uuid.uuid4())

            users_collection.find_one.return_value = None
            await user_service.register_user(
                provider, provider_user_id, username="alice"
            )

            inserted_user = users_collection.insert_one.await_args.args[0]
            assert inserted_user["identities"][0]["username"] == "alice"
            assert inserted_user["identities"][0]["username_history"] == []

            users_collection.insert_one.reset_mock()
            users_collection.update_one.reset_mock()

            existing_user = {
                "volo_user_id": volo_user_id,
                "sub_org_id": "sub_123",
                "sender_address": "0xabc",
                "evm_sub_org_id": "sub_123",
                "evm_address": "0xabc",
                "solana_sub_org_id": "sol_sub_123",
                "solana_address": "So1anaAddress",
                "identities": [
                    {
                        "provider": provider,
                        "provider_user_id": provider_user_id,
                        "username": "alice",
                        "username_history": [],
                    }
                ],
            }
            users_collection.find_one.return_value = existing_user

            await user_service.sync_username(
                existing_user, provider, provider_user_id, "alice_new"
            )

            update_query, update_ops = users_collection.update_one.await_args.args
            assert update_query["volo_user_id"] == volo_user_id
            assert update_ops["$set"]["identities.$.username"] == "alice_new"
            assert (
                update_ops["$push"]["identities.$.username_history"]["username"]
                == "alice"
            )

            users_collection.update_one.reset_mock()
            users_collection.find_one.return_value = {
                **existing_user,
                "identities": [
                    {
                        "provider": provider,
                        "provider_user_id": provider_user_id,
                        "username": "alice_new",
                        "username_history": [
                            {"username": "alice", "changed_at": "ignored-in-test"}
                        ],
                    }
                ],
            }

            await user_service.sync_username(
                users_collection.find_one.return_value,
                provider,
                provider_user_id,
                "alice_new",
            )

            users_collection.update_one.assert_not_awaited()

    asyncio.run(_run())
