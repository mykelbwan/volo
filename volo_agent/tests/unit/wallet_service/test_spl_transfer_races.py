from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from solders.hash import Hash
from solders.keypair import Keypair

from wallet_service.solana import rpc_client
from wallet_service.solana import spl_transfer
from wallet_service.solana.spl_transfer import (
    _build_spl_transfer_tx_async,
    _get_cached_mint_decimals,
    execute_spl_transfer,
)

ASYNC_TEST_TIMEOUT = 5.0
TEST_BLOCKHASH = Hash.from_string("8qbHbw2BbbTHBW1s7WcTLs8x2Wc6N8qF8a5n1Yp8wQkX")


class _FakeAsyncClient:
    def __init__(self, rpc_url: str) -> None:
        self.rpc_url = rpc_url

    async def __aenter__(self) -> "_FakeAsyncClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def get_latest_blockhash(self) -> SimpleNamespace:
        return SimpleNamespace(value=SimpleNamespace(blockhash=TEST_BLOCKHASH))

    async def get_account_info(self, _pubkey: object) -> object:
        raise AssertionError("decimals were provided; mint RPC lookup should not run")


class _FakeTx:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __bytes__(self) -> bytes:
        return self._payload


@pytest.fixture(autouse=True)
def _clear_solana_caches() -> None:
    with rpc_client._SHARED_SOLANA_CLIENTS_LOCK:
        rpc_client._SHARED_SOLANA_CLIENTS.clear()
    with rpc_client._BLOCKHASH_CACHE_LOCK:
        rpc_client._BLOCKHASH_CACHE.clear()
        rpc_client._BLOCKHASH_IN_FLIGHT.clear()
    with spl_transfer._TOKEN_DECIMALS_CACHE_LOCK:
        spl_transfer._TOKEN_DECIMALS_CACHE.clear()
        spl_transfer._TOKEN_DECIMALS_IN_FLIGHT.clear()


@pytest.mark.asyncio
async def test_latest_blockhash_cache_refresh_is_singleflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _DelayedBlockhashAsyncClient:
        call_count = 0

        def __init__(self, rpc_url: str) -> None:
            self.rpc_url = rpc_url

        async def get_latest_blockhash(self) -> SimpleNamespace:
            type(self).call_count += 1
            await asyncio.sleep(0.05)
            return SimpleNamespace(value=SimpleNamespace(blockhash=TEST_BLOCKHASH))

    async def exercise() -> None:
        monkeypatch.setattr(
            "solana.rpc.async_api.AsyncClient",
            _DelayedBlockhashAsyncClient,
        )

        results = await asyncio.gather(
            *[
                rpc_client.get_cached_latest_blockhash("https://rpc.test")
                for _ in range(40)
            ]
        )

        assert _DelayedBlockhashAsyncClient.call_count == 1
        assert results == [TEST_BLOCKHASH] * 40

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_mint_decimals_cache_refresh_is_singleflight() -> None:
    class _DelayedMintInfoClient:
        def __init__(self) -> None:
            self.call_count = 0

        async def get_account_info(self, _pubkey: object) -> object:
            self.call_count += 1
            await asyncio.sleep(0.05)
            raw = bytearray(82)
            raw[44] = 6
            return SimpleNamespace(value=SimpleNamespace(data=bytes(raw)))

    async def exercise() -> None:
        client = _DelayedMintInfoClient()
        mint_address = str(Keypair().pubkey())

        results = await asyncio.gather(
            *[
                _get_cached_mint_decimals(client, object(), mint_address)
                for _ in range(40)
            ]
        )

        assert client.call_count == 1
        assert results == [6] * 40

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_concurrent_spl_transfers_do_not_silent_double_spend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catch TOCTOU balance bugs by forcing two transfers to race on the same
    # balance after build/sign; one must fail visibly instead of both "succeeding".
    async def exercise() -> None:
        send_lock = asyncio.Lock()
        remaining = {"raw_amount": 100}
        send_mock = AsyncMock()

        async def _fake_build(**_kwargs: object) -> _FakeTx:
            return _FakeTx(b"unsigned-spl")

        async def _fake_send(_tx_b64: str, *, network: str | None = None) -> str:
            assert network == "solana"
            async with send_lock:
                if remaining["raw_amount"] < 60:
                    raise RuntimeError("insufficient funds after concurrent state change")
                remaining["raw_amount"] -= 60
                return f"sig-{remaining['raw_amount']}"

        send_mock.side_effect = _fake_send
        monkeypatch.setattr(
            "wallet_service.solana.spl_transfer._build_spl_transfer_tx_async",
            _fake_build,
        )
        monkeypatch.setattr(
            "wallet_service.solana.spl_transfer.sign_transaction_async",
            AsyncMock(return_value="signed-spl"),
        )
        monkeypatch.setattr(
            "wallet_service.solana.spl_transfer.send_solana_transaction_async",
            send_mock,
        )

        sender = str(Keypair().pubkey())
        recipient = str(Keypair().pubkey())
        mint = str(Keypair().pubkey())

        results = await asyncio.gather(
            execute_spl_transfer(
                sender=sender,
                sub_org_id="sol-sub-org",
                recipient=recipient,
                mint_address=mint,
                amount="0.00006",
                rpc_url="https://rpc.test",
                network="solana",
                decimals=6,
            ),
            execute_spl_transfer(
                sender=sender,
                sub_org_id="sol-sub-org",
                recipient=recipient,
                mint_address=mint,
                amount="0.00006",
                rpc_url="https://rpc.test",
                network="solana",
                decimals=6,
            ),
            return_exceptions=True,
        )

        successes = [item for item in results if isinstance(item, str)]
        failures = [item for item in results if isinstance(item, Exception)]

        assert len(successes) == 1
        assert len(failures) == 1
        assert "insufficient funds" in str(failures[0]).lower()
        assert send_mock.await_count == 2

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_spl_builder_uses_idempotent_ata_instruction_under_parallel_builds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catch the old ATA race by asserting every build emits the idempotent
    # create instruction instead of the non-idempotent variant.
    async def exercise() -> None:
        monkeypatch.setattr(
            "solana.rpc.async_api.AsyncClient",
            _FakeAsyncClient,
        )

        sender = str(Keypair().pubkey())
        recipient = str(Keypair().pubkey())
        mint = str(Keypair().pubkey())

        txs = await asyncio.gather(
            *[
                _build_spl_transfer_tx_async(
                    rpc_url="https://rpc.test",
                    sender=sender,
                    recipient=recipient,
                    mint_address=mint,
                    amount="0.000001",
                    decimals=6,
                    memo_text="memo-test",
                )
                for _ in range(8)
            ]
        )

        for tx in txs:
            assert tx.message.recent_blockhash == TEST_BLOCKHASH
            assert tx.data(0) == b"\x01"

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)
