from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

from core.identity.service import AsyncIdentityService


def _make_repo() -> MagicMock:
    repo = MagicMock()
    repo.update_user = AsyncMock()
    return repo


def test_ensure_multi_chain_wallets_respects_retry_backoff_by_default():
    repo = _make_repo()
    solana_provisioner = MagicMock()
    solana_provisioner.chain_label = "Solana"
    solana_provisioner.provision = MagicMock(
        return_value={"sub_org_id": "sol-sub", "address": "So1anaAddress"}
    )
    svc = AsyncIdentityService(repository=repo, solana_provisioner=solana_provisioner)
    now_iso = datetime.utcnow().isoformat()
    user = {
        "volo_user_id": "volo-1",
        "sub_org_id": "sub-1",
        "sender_address": "0xabc",
        "evm_sub_org_id": "sub-1",
        "evm_address": "0xabc",
        "solana_sub_org_id": None,
        "solana_address": None,
        "metadata": {"solana_provision_last_failed_at": now_iso},
    }

    updated = asyncio.run(svc.ensure_multi_chain_wallets(user))

    assert updated["solana_address"] is None
    solana_provisioner.provision.assert_not_called()
    repo.update_user.assert_not_awaited()


def test_ensure_multi_chain_wallets_force_retry_bypasses_backoff():
    repo = _make_repo()
    solana_provisioner = MagicMock()
    solana_provisioner.chain_label = "Solana"
    solana_provisioner.provision = MagicMock(
        return_value={"sub_org_id": "sol-sub", "address": "So1anaAddress"}
    )
    svc = AsyncIdentityService(repository=repo, solana_provisioner=solana_provisioner)
    now_iso = datetime.utcnow().isoformat()
    user = {
        "volo_user_id": "volo-1",
        "sub_org_id": "sub-1",
        "sender_address": "0xabc",
        "evm_sub_org_id": "sub-1",
        "evm_address": "0xabc",
        "solana_sub_org_id": None,
        "solana_address": None,
        "metadata": {"solana_provision_last_failed_at": now_iso},
    }

    updated = asyncio.run(
        svc.ensure_multi_chain_wallets(user, force_solana_retry=True)
    )

    assert updated["solana_address"] == "So1anaAddress"
    solana_provisioner.provision.assert_called_once_with("volo-1")
    repo.update_user.assert_awaited_once()
