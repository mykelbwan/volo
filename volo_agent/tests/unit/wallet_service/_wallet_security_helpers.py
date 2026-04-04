from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from solders.hash import Hash

from core.idempotency.store import IdempotencyRecord
from wallet_service.common import transfer_idempotency as transfer_idempotency_module

ASYNC_TEST_TIMEOUT = 10.0
SOLANA_TEST_BLOCKHASH = Hash.from_string(
    "4vJ9JU1bJJHzAq4xV7hW4Y3mJrLrM8VgHkL8V5x6wZ7G"
)


@dataclass(frozen=True)
class FakeChain:
    name: str
    chain_id: int
    rpc_url: str
    explorer_url: str


class FakeEvmEth:
    def __init__(
        self,
        *,
        pending_nonce: int,
        delay_ticks: int = 0,
        base_fee_per_gas: int = 30_000_000_000,
        max_priority_fee: int = 50_000_000_000,
    ) -> None:
        self._pending_nonce = pending_nonce
        self._delay_ticks = delay_ticks
        self._base_fee_per_gas = base_fee_per_gas
        self._max_priority_fee = max_priority_fee

    async def get_transaction_count(self, _sender: str, _block: str) -> int:
        for _ in range(self._delay_ticks):
            await asyncio.sleep(0)
        return self._pending_nonce

    async def get_block(self, _tag: str) -> dict[str, int]:
        return {"baseFeePerGas": self._base_fee_per_gas}

    @property
    def max_priority_fee(self) -> int:
        return self._max_priority_fee


class FakeAsyncWeb3:
    def __init__(self, eth: FakeEvmEth) -> None:
        self.eth = eth

    @staticmethod
    def to_checksum_address(address: str) -> str:
        return address.lower()


class StaticNonceManager:
    def __init__(self, nonce: int) -> None:
        self._nonce = nonce

    async def allocate_safe(self, *_args: object, **_kwargs: object) -> int:
        return self._nonce

    async def rollback(self, *_args: object, **_kwargs: object) -> int:
        return self._nonce


class AtomicFakeNonceManager:
    def __init__(self, initial_nonce: int) -> None:
        self._nonce = initial_nonce
        self._lock = asyncio.Lock()

    async def allocate(self, *_args: object, **_kwargs: object) -> int:
        async with self._lock:
            val = self._nonce
            self._nonce += 1
            return val

    async def allocate_safe(self, *_args: object, **_kwargs: object) -> int:
        return await self.allocate()

    async def peek(self, *_args: object, **_kwargs: object) -> int:
        async with self._lock:
            return self._nonce

    async def reset(self, _sender: str, _chain_id: int, w3: Any) -> int:
        async with self._lock:
            # In real life reset fetches from RPC.
            # Here we simulate by calling get_transaction_count on the fake w3.
            self._nonce = await w3.eth.get_transaction_count(_sender, "pending")
            return self._nonce

    async def rollback(self, _sender: str, _chain_id: int, nonce: int, _w3: Any) -> int:
        async with self._lock:
            # Simple rollback: if it's the most recent one, move back.
            if self._nonce == nonce + 1:
                self._nonce = nonce
            return self._nonce


class InMemoryIdempotencyStore:
    def __init__(self) -> None:
        self._records: dict[str, IdempotencyRecord] = {}
        self._lock = asyncio.Lock()

    async def aclaim(
        self,
        *,
        key: str,
        metadata: dict[str, Any],
        ttl_seconds: int = 60,
    ) -> tuple[IdempotencyRecord, bool]:
        async with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                return existing, False

            now = datetime.now(timezone.utc)
            record = IdempotencyRecord(
                key=key,
                status="pending",
                created_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
                metadata=dict(metadata),
            )
            self._records[key] = record
            return record, True

    async def aget(self, *, key: str):
        async with self._lock:
            return self._records.get(key)

    async def amark_inflight(
        self,
        *,
        key: str,
        tx_hash: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        async with self._lock:
            record = self._records[key]
            self._records[key] = IdempotencyRecord(
                key=record.key,
                status="pending",
                created_at=record.created_at,
                expires_at=record.expires_at,
                result=dict(result) if result is not None else record.result,
                error=record.error,
                metadata=record.metadata,
                tx_hash=tx_hash,
            )

    async def amark_success(self, *, key: str, result: dict[str, Any]) -> None:
        async with self._lock:
            record = self._records[key]
            tx_hash = str(result.get("tx_hash") or record.tx_hash or "")
            self._records[key] = IdempotencyRecord(
                key=record.key,
                status="success",
                created_at=record.created_at,
                expires_at=record.expires_at,
                result=dict(result),
                error=None,
                metadata=record.metadata,
                tx_hash=tx_hash or None,
            )

    async def amark_failed(self, *, key: str, error: str) -> None:
        async with self._lock:
            record = self._records[key]
            self._records[key] = IdempotencyRecord(
                key=record.key,
                status="failed",
                created_at=record.created_at,
                expires_at=record.expires_at,
                result=record.result,
                error=str(error),
                metadata=record.metadata,
                tx_hash=record.tx_hash,
            )


@pytest.fixture
def fake_users() -> list[str]:
    return ["user-1", "user1", "USER1", "User1", "user_1", "user-01"]


@pytest.fixture
def fake_chain() -> FakeChain:
    return FakeChain(
        name="ethereum",
        chain_id=1,
        rpc_url="https://rpc.test",
        explorer_url="https://explorer.test",
    )


def evm_transfer_params(*, asset_ref: str | None) -> dict[str, Any]:
    return {
        "asset_symbol": "ETH" if asset_ref in (None, "", "native") else "USDC",
        "asset_ref": asset_ref,
        "amount": "1.25",
        "recipient": "0x00000000000000000000000000000000000000aa",
        "network": "ethereum",
        "sub_org_id": "sub-org",
        "sender": "0x00000000000000000000000000000000000000bb",
        "decimals": 6,
    }


def patch_native_transfer_idempotency(
    monkeypatch: pytest.MonkeyPatch, store: InMemoryIdempotencyStore
) -> None:
    async def _claim(**kwargs: Any):
        return await transfer_idempotency_module.claim_transfer_idempotency(
            store=store,
            **kwargs,
        )

    async def _mark_inflight(claim: Any, *, tx_hash: str) -> None:
        await transfer_idempotency_module.mark_transfer_inflight(
            claim,
            tx_hash=tx_hash,
            store=store,
        )

    async def _mark_success(
        claim: Any,
        *,
        tx_hash: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        await transfer_idempotency_module.mark_transfer_success(
            claim,
            tx_hash=tx_hash,
            result=result,
            store=store,
        )

    async def _mark_failed(claim: Any, *, error: str) -> None:
        await transfer_idempotency_module.mark_transfer_failed(
            claim,
            error=error,
            store=store,
        )

    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.claim_transfer_idempotency",
        _claim,
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.mark_transfer_inflight",
        _mark_inflight,
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.mark_transfer_success",
        _mark_success,
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.mark_transfer_failed",
        _mark_failed,
    )
