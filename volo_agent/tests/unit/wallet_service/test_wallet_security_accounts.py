from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from typing import Any

import pytest

from tests.unit.wallet_service._wallet_security_helpers import FakeChain, fake_chain, fake_users
from wallet_service.common.cdp_helpers import (
    build_deterministic_account_name,
    managed_cdp_client,
    run_async,
)
from wallet_service.evm.create_sub_org import create_sub_org as create_evm_sub_org


def test_account_name_hashing_prevents_similar_user_collisions(
    fake_users: list[str],
) -> None:
    # Vulnerability: account name collisions from lossy normalization.
    account_names = [build_deterministic_account_name(user_id) for user_id in fake_users]

    assert len(account_names) == len(set(account_names))
    assert all(name.startswith("volo-") for name in account_names)
    assert (
        build_deterministic_account_name("user-1")
        != build_deterministic_account_name("user1")
    )


def test_create_sub_org_rejects_forced_collision_account_reuse() -> None:
    # Vulnerability: attacker forces "already exists" reuse and gets another user's wallet.
    victim_name = build_deterministic_account_name("user-1")

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "wallet_service.evm.create_sub_org.create_evm_account",
            lambda _name: (_ for _ in ()).throw(RuntimeError("account already exists")),
        )
        monkeypatch.setattr(
            "wallet_service.evm.create_sub_org.get_evm_account",
            lambda _name: SimpleNamespace(name=victim_name, address="0xvictim"),
        )

        with pytest.raises(RuntimeError, match="unexpected EVM account identifier"):
            create_evm_sub_org("user1")


@pytest.mark.asyncio
async def test_run_async_fails_closed_inside_event_loop_without_zombie_tasks() -> None:
    # Vulnerability: async/sync bridge leaks scheduled tasks when called from a live loop.
    started = False
    before_tasks = {
        id(task) for task in asyncio.all_tasks() if task is not asyncio.current_task()
    }

    async def _marker() -> None:
        nonlocal started
        started = True
        await asyncio.sleep(0.1)

    with pytest.raises(RuntimeError, match="running event loop"):
        run_async(_marker())

    await asyncio.sleep(0)
    after_tasks = {
        id(task) for task in asyncio.all_tasks() if task is not asyncio.current_task()
    }

    assert started is False
    assert after_tasks == before_tasks


@pytest.mark.asyncio
async def test_managed_cdp_client_passes_explicit_credentials_without_env_backfill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Vulnerability: credential handling mutates process env and leaks across requests.
    captured_kwargs: dict[str, Any] = {}
    sentinel_env = "unchanged"

    class _FakeClient:
        def __init__(self, **kwargs: Any) -> None:
            captured_kwargs.update(kwargs)

        async def close(self) -> None:
            return None

    monkeypatch.setenv("CDP_API_KEY_ID", sentinel_env)
    monkeypatch.setenv("CDP_API_KEY_SECRET", sentinel_env)
    monkeypatch.setenv("CDP_WALLET_SECRET", sentinel_env)
    monkeypatch.delenv("DISABLE_CDP_USAGE_TRACKING", raising=False)
    monkeypatch.delenv("DISABLE_CDP_ERROR_REPORTING", raising=False)
    monkeypatch.setattr(
        "wallet_service.common.cdp_helpers.get_cdp_client_config",
        lambda: SimpleNamespace(
            api_key_id="request-key",
            api_key_secret="request-secret",
            wallet_secret="request-wallet",
        ),
    )
    monkeypatch.setattr(
        "wallet_service.common.cdp_helpers._cdp_client_cls",
        lambda: _FakeClient,
    )

    async with managed_cdp_client():
        pass

    assert captured_kwargs == {
        "api_key_id": "request-key",
        "api_key_secret": "request-secret",
        "wallet_secret": "request-wallet",
    }
    assert "DISABLE_CDP_USAGE_TRACKING" not in os.environ
    assert "DISABLE_CDP_ERROR_REPORTING" not in os.environ
    assert os.environ["CDP_API_KEY_ID"] == sentinel_env
