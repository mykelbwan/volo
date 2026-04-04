from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import Message
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction

from wallet_service.solana import cdp_utils
from wallet_service.solana.native_transfer import (
    _build_native_transfer_tx_async,
    execute_native_transfer,
)

ASYNC_TEST_TIMEOUT = 5.0
_NON_PLACEHOLDER_BLOCKHASH = Hash.from_string("4vJ9JU1bJJHzAq4xV7hW4Y3mJrLrM8VgHkL8V5x6wZ7G")


def _serialize_transfer_tx(*, signed: bool) -> str:
    sender = Keypair()
    recipient = Keypair().pubkey()
    instruction = transfer(
        TransferParams(
            from_pubkey=sender.pubkey(),
            to_pubkey=recipient,
            lamports=1,
        )
    )
    message = Message.new_with_blockhash(
        [instruction],
        sender.pubkey(),
        _NON_PLACEHOLDER_BLOCKHASH,
    )
    tx = (
        Transaction([sender], message, _NON_PLACEHOLDER_BLOCKHASH)
        if signed
        else Transaction.new_unsigned(message)
    )
    return base64.b64encode(bytes(tx)).decode("utf-8")


class _FakeAsyncClient:
    def __init__(self, rpc_url: str) -> None:
        self.rpc_url = rpc_url

    async def __aenter__(self) -> "_FakeAsyncClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def get_latest_blockhash(self) -> SimpleNamespace:
        return SimpleNamespace(
            value=SimpleNamespace(blockhash=_NON_PLACEHOLDER_BLOCKHASH)
        )


def _fake_managed_client(send_mock: AsyncMock):
    @asynccontextmanager
    async def _manager():
        yield SimpleNamespace(
            solana=SimpleNamespace(send_transaction=send_mock)
        )

    return _manager


@pytest.mark.asyncio
async def test_signed_tx_required_before_send(monkeypatch: pytest.MonkeyPatch) -> None:
    # Catch the previous bug where unsigned Solana transactions reached broadcast.
    async def exercise() -> None:
        send_mock = AsyncMock(return_value="sig")
        monkeypatch.setattr(
            cdp_utils,
            "managed_cdp_client",
            _fake_managed_client(send_mock),
        )

        with pytest.raises(ValueError, match="signed"):
            await cdp_utils.send_solana_transaction_async(
                _serialize_transfer_tx(signed=False),
                network="solana",
            )

        send_mock.assert_not_awaited()

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_send_solana_transaction_propagates_expired_blockhash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catch stale-blockhash regressions: validation should pass for a signed tx,
    # then the downstream rejection must surface cleanly.
    async def exercise() -> None:
        send_mock = AsyncMock(side_effect=RuntimeError("Blockhash not found"))
        monkeypatch.setattr(
            cdp_utils,
            "managed_cdp_client",
            _fake_managed_client(send_mock),
        )

        with pytest.raises(RuntimeError, match="Blockhash not found"):
            await cdp_utils.send_solana_transaction_async(
                _serialize_transfer_tx(signed=True),
                network="solana-devnet",
            )

        send_mock.assert_awaited_once()

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_send_solana_transaction_accepts_sdk_response_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        send_mock = AsyncMock(
            return_value=SimpleNamespace(transaction_signature="solana-sig-123")
        )
        monkeypatch.setattr(
            cdp_utils,
            "managed_cdp_client",
            _fake_managed_client(send_mock),
        )

        signature = await cdp_utils.send_solana_transaction_async(
            _serialize_transfer_tx(signed=True),
            network="solana-devnet",
        )

        assert signature == "solana-sig-123"
        send_mock.assert_awaited_once()

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_native_transfer_builder_uses_mocked_recent_blockhash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catch placeholder-blockhash regressions by asserting the tx uses the RPC value.
    async def exercise() -> None:
        monkeypatch.setattr(
            "solana.rpc.async_api.AsyncClient",
            _FakeAsyncClient,
        )

        sender = str(Keypair().pubkey())
        recipient = str(Keypair().pubkey())
        tx = await _build_native_transfer_tx_async(
            sender,
            recipient,
            123,
            rpc_url="https://rpc.test",
            memo_text="memo-1",
        )

        assert tx.message.recent_blockhash == _NON_PLACEHOLDER_BLOCKHASH
        assert (
            str(tx.message.recent_blockhash)
            != "SysvarRecentB1ockHashes11111111111111111111"
        )

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_native_transfer_stops_before_send_when_signing_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catch invalid-signer paths: broadcast must never run if signing fails.
    async def exercise() -> None:
        send_mock = AsyncMock()
        monkeypatch.setattr(
            "solana.rpc.async_api.AsyncClient",
            _FakeAsyncClient,
        )
        monkeypatch.setattr(
            "wallet_service.solana.native_transfer.get_solana_chain",
            lambda _network: SimpleNamespace(rpc_url="https://rpc.test"),
        )
        monkeypatch.setattr(
            "wallet_service.solana.native_transfer.sign_transaction_async",
            AsyncMock(side_effect=ValueError("invalid signer")),
        )
        monkeypatch.setattr(
            "wallet_service.solana.native_transfer.send_solana_transaction_async",
            send_mock,
        )

        with pytest.raises(ValueError, match="invalid signer"):
            await execute_native_transfer(
                sender=str(Keypair().pubkey()),
                sub_org_id="sol-sub-org",
                recipient=str(Keypair().pubkey()),
                amount_native="0.000001",
                network="solana",
            )

        send_mock.assert_not_awaited()

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)
