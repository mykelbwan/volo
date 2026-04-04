from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tool_nodes.dex import swap as swap_module
from tool_nodes.dex.swap_executor import SwapResult, execute_swap
from tool_nodes.dex.swap_simulator_v2 import SwapQuoteV2
from tool_nodes.wallet import transfer as transfer_module
from tests.unit.wallet_service._wallet_security_helpers import InMemoryIdempotencyStore
from wallet_service.common import transfer_idempotency as transfer_idempotency_module


class _DummySwapChain:
    chain_id = 1
    name = "Ethereum"
    rpc_url = "https://rpc.test"
    v3_quoter = None
    v3_router = None
    v2_router = "0xrouter"
    v2_factory = "0xfactory"
    wrapped_native = "0xwrap"
    supports_native_swaps = True
    explorer_url = "https://etherscan.io"


class _DummyWeb3:
    def __init__(self) -> None:
        self.eth = SimpleNamespace()

    @staticmethod
    def to_checksum_address(address: str) -> str:
        return address.lower()


class _DummyNonceManager:
    def __init__(self) -> None:
        self._next = 7

    async def pending(self, *_args, **_kwargs) -> int:
        return self._next

    async def allocate_safe(self, *_args, **_kwargs) -> int:
        current = self._next
        self._next += 1
        return current


class _SharedTestLock:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.active = 0
        self.max_active = 0

    @asynccontextmanager
    async def manager(self, *_args, **_kwargs):
        await self._lock.acquire()
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            yield SimpleNamespace(ensure_held=self._ensure_held)
        finally:
            self.active -= 1
            self._lock.release()

    async def _ensure_held(self) -> None:
        return None


def _swap_params() -> dict[str, object]:
    return {
        "token_in_address": "0x1111111111111111111111111111111111111111",
        "token_out_address": "0x2222222222222222222222222222222222222222",
        "amount_in": "1",
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": "0xsender",
        "token_in_symbol": "USDC",
        "token_out_symbol": "ETH",
    }


def _swap_quote() -> SwapQuoteV2:
    return SwapQuoteV2(
        token_in="0x1111111111111111111111111111111111111111",
        token_out="0x2222222222222222222222222222222222222222",
        amount_in=Decimal("1"),
        amount_out=Decimal("2"),
        amount_out_minimum=Decimal("1.9"),
        decimals_in=6,
        decimals_out=18,
        slippage_pct=Decimal("0.5"),
        price_impact_pct=Decimal("0.1"),
        path=[
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
        ],
        gas_estimate=150_000,
        needs_approval=False,
        allowance=10**30,
        chain_id=1,
        chain_name="Ethereum",
        dex_name="Uniswap V2",
    )


def _swap_result(tx_hash: str = "0xswap") -> SwapResult:
    return SwapResult(
        tx_hash=tx_hash,
        approve_hash=None,
        token_in="0x1111111111111111111111111111111111111111",
        token_out="0x2222222222222222222222222222222222222222",
        amount_in=Decimal("1"),
        amount_out_minimum=Decimal("1.9"),
        chain_id=1,
        chain_name="Ethereum",
        protocol="v2",
        router="0xrouter",
    )


def _patch_swap_idempotency(
    monkeypatch: pytest.MonkeyPatch,
    store: InMemoryIdempotencyStore,
) -> None:
    async def _claim(**kwargs):
        return await transfer_idempotency_module.claim_transfer_idempotency(
            store=store, **kwargs
        )

    async def _load(claim, **kwargs):
        return await transfer_idempotency_module.load_transfer_idempotency_claim(
            claim, store=store, **kwargs
        )

    async def _mark_inflight(claim, *, tx_hash: str, result=None):
        await transfer_idempotency_module.mark_transfer_inflight(
            claim, tx_hash=tx_hash, result=result, store=store
        )

    async def _mark_success(claim, *, tx_hash: str, result=None):
        await transfer_idempotency_module.mark_transfer_success(
            claim, tx_hash=tx_hash, result=result, store=store
        )

    async def _mark_failed(claim, *, error: str):
        await transfer_idempotency_module.mark_transfer_failed(
            claim, error=error, store=store
        )

    monkeypatch.setattr(swap_module, "claim_transfer_idempotency", _claim)
    monkeypatch.setattr(swap_module, "load_transfer_idempotency_claim", _load)
    monkeypatch.setattr(swap_module, "mark_transfer_inflight", _mark_inflight)
    monkeypatch.setattr(swap_module, "mark_transfer_success", _mark_success)
    monkeypatch.setattr(swap_module, "mark_transfer_failed", _mark_failed)


@pytest.mark.asyncio
async def test_swap_token_is_idempotent_and_executes_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryIdempotencyStore()
    _patch_swap_idempotency(monkeypatch, store)
    monkeypatch.setattr(swap_module, "get_chain_by_name", lambda _name: _DummySwapChain())
    monkeypatch.setattr(
        swap_module.gas_price_cache,
        "get_gwei",
        AsyncMock(return_value=5),
    )
    monkeypatch.setattr(swap_module, "simulate_swap_v2", AsyncMock(return_value=_swap_quote()))
    execute_mock = AsyncMock(return_value=_swap_result())
    monkeypatch.setattr(swap_module, "execute_swap", execute_mock)

    params = _swap_params()
    params["idempotency_key"] = "swap-intent-1"
    first = await swap_module.swap_token(params)
    second = await swap_module.swap_token(dict(params))

    assert first["tx_hash"] == "0xswap"
    assert second["tx_hash"] == "0xswap"
    assert execute_mock.await_count == 1


@pytest.mark.asyncio
async def test_swap_token_allows_identical_requests_without_external_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryIdempotencyStore()
    _patch_swap_idempotency(monkeypatch, store)
    monkeypatch.setattr(swap_module, "get_chain_by_name", lambda _name: _DummySwapChain())
    monkeypatch.setattr(
        swap_module.gas_price_cache,
        "get_gwei",
        AsyncMock(return_value=5),
    )
    monkeypatch.setattr(swap_module, "simulate_swap_v2", AsyncMock(return_value=_swap_quote()))
    execute_mock = AsyncMock(side_effect=[_swap_result("0xone"), _swap_result("0xtwo")])
    monkeypatch.setattr(swap_module, "execute_swap", execute_mock)

    first = await swap_module.swap_token(_swap_params())
    second = await swap_module.swap_token(_swap_params())

    assert first["tx_hash"] == "0xone"
    assert second["tx_hash"] == "0xtwo"
    assert execute_mock.await_count == 2


@pytest.mark.asyncio
async def test_swap_token_retry_replays_persisted_broadcast_after_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryIdempotencyStore()
    _patch_swap_idempotency(monkeypatch, store)
    monkeypatch.setattr(swap_module, "get_chain_by_name", lambda _name: _DummySwapChain())
    monkeypatch.setattr(
        swap_module.gas_price_cache,
        "get_gwei",
        AsyncMock(return_value=5),
    )
    monkeypatch.setattr(swap_module, "simulate_swap_v2", AsyncMock(return_value=_swap_quote()))

    execute_calls = 0

    async def _crashing_execute_swap(*, persist_broadcast=None, **_kwargs):
        nonlocal execute_calls
        execute_calls += 1
        result = _swap_result("0xcrash")
        assert persist_broadcast is not None
        await persist_broadcast(result)
        raise RuntimeError("worker crashed after broadcast")

    monkeypatch.setattr(swap_module, "execute_swap", _crashing_execute_swap)
    params = _swap_params()
    params["idempotency_key"] = "swap-crash-1"

    with pytest.raises(RuntimeError, match="worker crashed after broadcast"):
        await swap_module.swap_token(params)

    replayed = await swap_module.swap_token(dict(params))

    assert execute_calls == 1
    assert replayed["tx_hash"] == "0xcrash"
    assert replayed["status"] == "pending"


@pytest.mark.asyncio
async def test_swap_and_transfer_share_wallet_lock_without_interleaving(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared_lock = _SharedTestLock()
    nonce_manager = _DummyNonceManager()
    chain = _DummySwapChain()

    monkeypatch.setattr("tool_nodes.dex.swap_executor.wallet_lock", shared_lock.manager)
    monkeypatch.setattr("tool_nodes.wallet.transfer.wallet_lock", shared_lock.manager)
    monkeypatch.setattr("tool_nodes.dex.swap_executor.get_chain_by_id", lambda _chain_id: chain)
    monkeypatch.setattr(
        "tool_nodes.dex.swap_executor.get_router_capabilities",
        lambda *_args, **_kwargs: SimpleNamespace(
            supports_native_swaps=True,
            last_checked="2026-01-01T00:00:00",
        ),
    )
    monkeypatch.setattr("tool_nodes.dex.swap_executor.make_async_web3", lambda _rpc: _DummyWeb3())
    monkeypatch.setattr(
        "tool_nodes.dex.swap_executor.get_async_nonce_manager",
        AsyncMock(return_value=nonce_manager),
    )
    monkeypatch.setattr(
        "tool_nodes.dex.swap_executor._build_v2_swap_tx",
        AsyncMock(return_value={"tx": "swap"}),
    )

    async def _slow_sign(*_args, **_kwargs):
        await asyncio.sleep(0.02)
        return "signed"

    async def _slow_broadcast(*_args, **_kwargs):
        await asyncio.sleep(0.02)
        return "0xhash"

    monkeypatch.setattr("tool_nodes.dex.swap_executor.sign_transaction_async", _slow_sign)
    monkeypatch.setattr("tool_nodes.dex.swap_executor.async_broadcast_evm", _slow_broadcast)
    monkeypatch.setattr(
        "tool_nodes.dex.swap_executor.async_await_evm_receipt",
        AsyncMock(return_value=None),
    )

    monkeypatch.setattr(transfer_module, "get_chain_by_name", lambda _name: chain)
    monkeypatch.setattr(
        transfer_module.gas_price_cache,
        "get_wei",
        AsyncMock(return_value=1_000_000_000),
    )
    monkeypatch.setattr(transfer_module, "make_async_web3", lambda _rpc: _DummyWeb3())
    monkeypatch.setattr(
        transfer_module,
        "get_async_nonce_manager",
        AsyncMock(return_value=nonce_manager),
    )
    monkeypatch.setattr(
        transfer_module,
        "estimate_eip1559_fees",
        AsyncMock(return_value=(1, 1)),
    )
    monkeypatch.setattr(transfer_module, "build_erc20_tx", lambda **_kwargs: {"tx": "transfer"})
    monkeypatch.setattr(transfer_module, "sign_transaction_async", _slow_sign)
    monkeypatch.setattr(transfer_module, "async_broadcast_evm", _slow_broadcast)

    quote = _swap_quote()
    transfer_params = {
        "token_symbol": "USDC",
        "token_address": "0xusdc",
        "amount": "5",
        "recipient": "0xabc",
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": "0xsender",
        "decimals": 6,
    }

    await asyncio.gather(
        execute_swap(quote=quote, sub_org_id="sub", sender="0xsender"),
        transfer_module.transfer_token(transfer_params),
    )

    assert shared_lock.max_active == 1
