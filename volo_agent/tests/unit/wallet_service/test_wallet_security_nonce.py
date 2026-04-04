from __future__ import annotations

import asyncio
import hashlib
from contextlib import asynccontextmanager
from decimal import Decimal

import pytest
from unittest.mock import AsyncMock

from tests.unit.wallet_service._wallet_security_helpers import (
    ASYNC_TEST_TIMEOUT,
    FakeAsyncWeb3,
    FakeChain,
    FakeEvmEth,
    InMemoryIdempotencyStore,
    patch_native_transfer_idempotency,
    fake_chain,
    AtomicFakeNonceManager,
)
from wallet_service.evm.native_transfer import execute_native_transfer


@asynccontextmanager
async def _noop_wallet_lock(*_args: object, **_kwargs: object):
    class _HeldLock:
        async def ensure_held(self) -> None:
            return None

    yield _HeldLock()


@pytest.mark.asyncio
async def test_evm_native_transfer_uses_strictly_increasing_nonces_under_50_way_concurrency(
    monkeypatch: pytest.MonkeyPatch,
    fake_chain: FakeChain,
) -> None:
    # Vulnerability: concurrent sends desynchronize nonce state and double-use a nonce.
    manager = AtomicFakeNonceManager(initial_nonce=7)
    w3 = FakeAsyncWeb3(FakeEvmEth(pending_nonce=7, delay_ticks=2))
    seen_nonces: list[int] = []

    async def _sign(_sub_org: str, unsigned_tx: dict[str, object], _sign_with: str) -> str:
        seen_nonces.append(int(unsigned_tx["nonce"]))
        return f"signed-{unsigned_tx['nonce']}"

    async def _broadcast(_w3: object, signed_tx: str) -> str:
        await asyncio.sleep(0)
        return f"0x{hashlib.sha256(signed_tx.encode('utf-8')).hexdigest()}"

    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_chain_by_name",
        lambda _name: fake_chain,
    )
    monkeypatch.setattr("wallet_service.evm.native_transfer.wallet_lock", _noop_wallet_lock)
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_shared_async_web3",
        AsyncMock(return_value=w3),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_async_nonce_manager",
        AsyncMock(return_value=manager),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.gas_price_cache.get_wei",
        AsyncMock(return_value=35_000_000_000),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.sign_transaction_async",
        _sign,
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.async_broadcast_evm",
        _broadcast,
    )

    hashes = await asyncio.wait_for(
        asyncio.gather(
            *[
                execute_native_transfer(
                    to="0x00000000000000000000000000000000000000dd",
                    amount_native=Decimal("0.0001"),
                    chain_name="ethereum",
                    sender="0x00000000000000000000000000000000000000cc",
                    sub_org_id="sub-org",
                )
                for _ in range(50)
            ]
        ),
        timeout=ASYNC_TEST_TIMEOUT,
    )

    assert len(set(seen_nonces)) == 50
    assert sorted(seen_nonces) == list(range(7, 57))
    assert len(set(hashes)) == 50


@pytest.mark.asyncio
async def test_signing_failure_rolls_nonce_back_for_next_evm_transfer(
    monkeypatch: pytest.MonkeyPatch,
    fake_chain: FakeChain,
) -> None:
    # Vulnerability: a failed sign consumes the nonce and causes the next send to skip.
    manager = AtomicFakeNonceManager(initial_nonce=11)
    w3 = FakeAsyncWeb3(FakeEvmEth(pending_nonce=11))
    seen_nonces: list[int] = []

    async def _sign(_sub_org: str, unsigned_tx: dict[str, object], _sign_with: str) -> str:
        seen_nonces.append(int(unsigned_tx["nonce"]))
        if len(seen_nonces) == 1:
            raise RuntimeError("cdp signing failed")
        return "signed-ok"

    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_chain_by_name",
        lambda _name: fake_chain,
    )
    monkeypatch.setattr("wallet_service.evm.native_transfer.wallet_lock", _noop_wallet_lock)
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_shared_async_web3",
        AsyncMock(return_value=w3),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_async_nonce_manager",
        AsyncMock(return_value=manager),
    )
    monkeypatch.setattr(
        "wallet_service.evm.nonce_manager.get_async_nonce_manager",
        AsyncMock(return_value=manager),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.gas_price_cache.get_wei",
        AsyncMock(return_value=20_000_000_000),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.sign_transaction_async",
        _sign,
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.async_broadcast_evm",
        AsyncMock(return_value="0xabc123"),
    )

    with pytest.raises(RuntimeError, match="cdp signing failed"):
        await asyncio.wait_for(
            execute_native_transfer(
                to="0x00000000000000000000000000000000000000aa",
                amount_native=Decimal("0.0001"),
                chain_name="ethereum",
                sender="0x00000000000000000000000000000000000000bb",
                sub_org_id="sub-org",
            ),
            timeout=ASYNC_TEST_TIMEOUT,
        )

    tx_hash = await asyncio.wait_for(
        execute_native_transfer(
            to="0x00000000000000000000000000000000000000aa",
            amount_native=Decimal("0.0001"),
            chain_name="ethereum",
            sender="0x00000000000000000000000000000000000000bb",
            sub_org_id="sub-org",
        ),
        timeout=ASYNC_TEST_TIMEOUT,
    )

    assert tx_hash == "0xabc123"
    assert seen_nonces == [11, 11]


@pytest.mark.asyncio
async def test_successful_idempotent_retries_replay_same_hash_without_second_broadcast(
    monkeypatch: pytest.MonkeyPatch,
    fake_chain: FakeChain,
) -> None:
    # Vulnerability: repeated same-intent retries can double-spend after the first success.
    store = InMemoryIdempotencyStore()
    patch_native_transfer_idempotency(monkeypatch, store)

    manager = AtomicFakeNonceManager(initial_nonce=21)
    w3 = FakeAsyncWeb3(FakeEvmEth(pending_nonce=21))
    broadcast_count = {"value": 0}

    async def _broadcast(_w3: object, _signed_tx: str) -> str:
        broadcast_count["value"] += 1
        return "0xidemtx"

    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_chain_by_name",
        lambda _name: fake_chain,
    )
    monkeypatch.setattr("wallet_service.evm.native_transfer.wallet_lock", _noop_wallet_lock)
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_shared_async_web3",
        AsyncMock(return_value=w3),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_async_nonce_manager",
        AsyncMock(return_value=manager),
    )
    monkeypatch.setattr(
        "wallet_service.evm.nonce_manager.get_async_nonce_manager",
        AsyncMock(return_value=manager),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.gas_price_cache.get_wei",
        AsyncMock(return_value=25_000_000_000),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.sign_transaction_async",
        AsyncMock(return_value="signed-once"),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.async_broadcast_evm",
        _broadcast,
    )

    first_hash = await execute_native_transfer(
        to="0x00000000000000000000000000000000000000aa",
        amount_native=Decimal("0.0003"),
        chain_name="ethereum",
        sender="0x00000000000000000000000000000000000000bb",
        sub_org_id="sub-org",
        idempotency_key="payment-123",
    )
    retries = await asyncio.wait_for(
        asyncio.gather(
            *[
                execute_native_transfer(
                    to="0x00000000000000000000000000000000000000aa",
                    amount_native=Decimal("0.0003"),
                    chain_name="ethereum",
                    sender="0x00000000000000000000000000000000000000bb",
                    sub_org_id="sub-org",
                    idempotency_key="payment-123",
                )
                for _ in range(50)
            ]
        ),
        timeout=ASYNC_TEST_TIMEOUT,
    )

    assert first_hash == "0xidemtx"
    assert set(retries) == {"0xidemtx"}
    assert broadcast_count["value"] == 1


@pytest.mark.asyncio
async def test_different_idempotency_keys_produce_distinct_transactions(
    monkeypatch: pytest.MonkeyPatch,
    fake_chain: FakeChain,
) -> None:
    # Vulnerability: idempotency scope is too broad and merges separate payment intents.
    store = InMemoryIdempotencyStore()
    patch_native_transfer_idempotency(monkeypatch, store)

    manager = AtomicFakeNonceManager(initial_nonce=31)
    w3 = FakeAsyncWeb3(FakeEvmEth(pending_nonce=31))
    seen_hashes: list[str] = []

    async def _broadcast(_w3: object, signed_tx: str) -> str:
        tx_hash = f"0x{hashlib.sha256(signed_tx.encode('utf-8')).hexdigest()}"
        seen_hashes.append(tx_hash)
        return tx_hash

    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_chain_by_name",
        lambda _name: fake_chain,
    )
    monkeypatch.setattr("wallet_service.evm.native_transfer.wallet_lock", _noop_wallet_lock)
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_shared_async_web3",
        AsyncMock(return_value=w3),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_async_nonce_manager",
        AsyncMock(return_value=manager),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.gas_price_cache.get_wei",
        AsyncMock(return_value=25_000_000_000),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.sign_transaction_async",
        lambda _sub_org, unsigned_tx, _sign_with: asyncio.sleep(
            0,
            result=f"signed-{unsigned_tx['nonce']}",
        ),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.async_broadcast_evm",
        _broadcast,
    )

    hashes = [
        await execute_native_transfer(
            to="0x00000000000000000000000000000000000000aa",
            amount_native=Decimal("0.0003"),
            chain_name="ethereum",
            sender="0x00000000000000000000000000000000000000bb",
            sub_org_id="sub-org",
            idempotency_key=key,
        )
        for key in ("payment-a", "payment-b", "payment-c")
    ]

    assert len(set(hashes)) == 3
    assert hashes == seen_hashes
