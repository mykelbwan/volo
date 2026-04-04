from __future__ import annotations

import asyncio
import base64
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from solders.keypair import Keypair

from tests.unit.wallet_service._wallet_security_helpers import (
    ASYNC_TEST_TIMEOUT,
    SOLANA_TEST_BLOCKHASH,
)
from wallet_service.solana.native_transfer import execute_native_transfer


@pytest.mark.asyncio
async def test_solana_replay_protection_keeps_same_blockhash_burst_unique(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Vulnerability: Solana duplicate-hash replay when identical transfers reuse a blockhash.
    signed_payloads: list[str] = []

    async def _sign(
        _account_ref: str,
        serialized: str,
        *,
        sign_with: str | None = None,
    ) -> str:
        assert sign_with
        signed_payloads.append(serialized)
        return serialized

    async def _send(serialized: str, *, network: str | None = None) -> str:
        assert network == "solana"
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    monkeypatch.setattr(
        "wallet_service.solana.native_transfer.get_solana_chain",
        lambda _network: SimpleNamespace(rpc_url="https://solana.rpc.test"),
    )
    monkeypatch.setattr(
        "wallet_service.solana.native_transfer.get_shared_solana_client",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        "wallet_service.solana.native_transfer.get_cached_latest_blockhash",
        AsyncMock(return_value=SOLANA_TEST_BLOCKHASH),
    )
    monkeypatch.setattr(
        "wallet_service.solana.native_transfer.sign_transaction_async",
        _sign,
    )
    monkeypatch.setattr(
        "wallet_service.solana.native_transfer.send_solana_transaction_async",
        _send,
    )

    sender = str(Keypair().pubkey())
    recipient = str(Keypair().pubkey())
    signatures = await asyncio.wait_for(
        asyncio.gather(
            *[
                execute_native_transfer(
                    sender=sender,
                    sub_org_id="sol-sub-org",
                    recipient=recipient,
                    amount_native="0.000001",
                    network="solana",
                )
                for _ in range(50)
            ]
        ),
        timeout=ASYNC_TEST_TIMEOUT,
    )

    assert len(signatures) == 50
    assert len(set(signatures)) == 50
    assert len(set(signed_payloads)) == 50


@pytest.mark.asyncio
async def test_same_solana_transfer_bytes_change_when_only_memo_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Vulnerability: replay remains possible if the memo/nonce instruction disappears.
    from wallet_service.solana.native_transfer import _build_native_transfer_tx_async

    monkeypatch.setattr(
        "wallet_service.solana.native_transfer.get_shared_solana_client",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        "wallet_service.solana.native_transfer.get_cached_latest_blockhash",
        AsyncMock(return_value=SOLANA_TEST_BLOCKHASH),
    )

    sender = str(Keypair().pubkey())
    recipient = str(Keypair().pubkey())

    tx_a = await _build_native_transfer_tx_async(
        sender,
        recipient,
        1_000,
        rpc_url="https://solana.rpc.test",
        memo_text="memo-a",
    )
    tx_b = await _build_native_transfer_tx_async(
        sender,
        recipient,
        1_000,
        rpc_url="https://solana.rpc.test",
        memo_text="memo-b",
    )

    assert tx_a.message.recent_blockhash == SOLANA_TEST_BLOCKHASH
    assert tx_b.message.recent_blockhash == SOLANA_TEST_BLOCKHASH
    assert base64.b64encode(bytes(tx_a)) != base64.b64encode(bytes(tx_b))
