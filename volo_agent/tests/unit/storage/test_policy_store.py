from __future__ import annotations

import asyncio
import importlib
from unittest.mock import AsyncMock, patch

import core.security.policy_store as policy_store_module


def _reload_policy_store():
    module = importlib.reload(policy_store_module)
    module.PolicyStore._async_indexes_ensured = False
    module.PolicyStore._async_indexes_lock = None
    module.PolicyStore._retry_after_monotonic = 0.0
    return module


def test_policy_store_constructor_is_lazy():
    mod = _reload_policy_store()

    with patch.object(
        mod.AsyncMongoDB,
        "get_db",
        side_effect=AssertionError("constructor should not touch MongoDB"),
    ):
        store = mod.PolicyStore()

    assert store.async_db is None
    assert store.async_defaults_collection is None
    assert store.async_user_policies_collection is None


def test_policy_store_aget_effective_policy_uses_async_collections_only():
    mod = _reload_policy_store()
    defaults = AsyncMock()
    defaults.create_index = AsyncMock(return_value="defaults_idx")
    defaults.find_one = AsyncMock(return_value={"policy": {"max_amount": 10}})
    user_policies = AsyncMock()
    user_policies.create_index = AsyncMock(return_value="user_idx")
    user_policies.find_one = AsyncMock(return_value={"policy": {"allow_bridge": True}})
    fake_db = {
        "policy_defaults": defaults,
        "user_policies": user_policies,
    }

    with patch.object(mod.AsyncMongoDB, "get_db", return_value=fake_db):
        policy = asyncio.run(mod.PolicyStore().aget_effective_policy("user-1"))

    assert policy == {"max_amount": 10, "allow_bridge": True}
    defaults.create_index.assert_awaited_once()
    user_policies.create_index.assert_awaited_once()
    defaults.find_one.assert_awaited_once_with({"name": "guardrails"})
    user_policies.find_one.assert_awaited_once_with({"volo_user_id": "user-1"})


def test_policy_store_rejects_malformed_policy_documents():
    mod = _reload_policy_store()
    defaults = AsyncMock()
    defaults.create_index = AsyncMock(return_value="defaults_idx")
    defaults.find_one = AsyncMock(return_value={"policy": "not-a-dict"})
    user_policies = AsyncMock()
    user_policies.create_index = AsyncMock(return_value="user_idx")
    user_policies.find_one = AsyncMock(return_value=None)
    fake_db = {
        "policy_defaults": defaults,
        "user_policies": user_policies,
    }

    with patch.object(mod.AsyncMongoDB, "get_db", return_value=fake_db):
        try:
            asyncio.run(mod.PolicyStore().aget_effective_policy("user-1"))
        except mod.PolicyStoreDataError as exc:
            assert "malformed" in str(exc)
        else:
            raise AssertionError("Expected malformed policy documents to fail closed.")
