from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.utils.errors import NonRetryableError
from tests.unit.wallet_service._wallet_security_helpers import InMemoryIdempotencyStore
from tool_nodes.solana import swap as solana_swap_module
from wallet_service.common import transfer_idempotency as transfer_idempotency_module


@dataclass
class _DummySolanaChain:
    chain_id: int = 101
    name: str = "Solana"
    network: str = "solana"
    rpc_url: str = "https://rpc.test"
    explorer_url: str = "https://explorer.test"


@dataclass
class _DummyQuote:
    aggregator: str = "jupiter"
    amount_out: Decimal = Decimal("20")
    amount_out_min: Decimal = Decimal("19.5")
    swap_transaction: str = "base64-tx"
    price_impact_pct: Decimal = Decimal("0.1")


def _params() -> dict[str, object]:
    return {
        "token_in_symbol": "SOL",
        "token_out_symbol": "USDC",
        "token_in_mint": "So11111111111111111111111111111111111111112",
        "token_out_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "amount_in": "1.25",
        "network": "solana",
        "sub_org_id": "sub",
        "sender": "sender1111111111111111111111111111111111111",
    }


def _patch_idempotency(
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

    monkeypatch.setattr(solana_swap_module, "claim_transfer_idempotency", _claim)
    monkeypatch.setattr(solana_swap_module, "load_transfer_idempotency_claim", _load)
    monkeypatch.setattr(solana_swap_module, "mark_transfer_inflight", _mark_inflight)
    monkeypatch.setattr(solana_swap_module, "mark_transfer_success", _mark_success)
    monkeypatch.setattr(solana_swap_module, "mark_transfer_failed", _mark_failed)


@pytest.mark.asyncio
async def test_solana_swap_rejects_untrusted_precomputed_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryIdempotencyStore()
    _patch_idempotency(monkeypatch, store)
    monkeypatch.setattr(solana_swap_module, "get_solana_chain", lambda _network: _DummySolanaChain())

    params = _params()
    params["_route_meta"] = {
        "network": "solana",
        "input_mint": params["token_in_mint"],
        "output_mint": params["token_out_mint"],
        "swap_transaction": "base64-unsigned",
    }

    with pytest.raises(
        NonRetryableError,
        match="Untrusted precomputed transaction data is not allowed",
    ):
        await solana_swap_module.solana_swap_token(params)


@pytest.mark.asyncio
async def test_solana_swap_identical_requests_without_external_idempotency_key_deduplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryIdempotencyStore()
    _patch_idempotency(monkeypatch, store)
    monkeypatch.setattr(solana_swap_module, "get_solana_chain", lambda _network: _DummySolanaChain())
    monkeypatch.setattr(
        solana_swap_module,
        "fetch_solana_token_decimals",
        AsyncMock(side_effect=[9, 6, 9, 6]),
    )
    monkeypatch.setattr(
        solana_swap_module,
        "_get_aggregators",
        lambda: [SimpleNamespace(TIMEOUT_SECONDS=1.0, name="jupiter")],
    )
    monkeypatch.setattr(
        solana_swap_module,
        "_fetch_quote_with_timeout",
        AsyncMock(side_effect=[_DummyQuote(), _DummyQuote()]),
    )
    monkeypatch.setattr(
        solana_swap_module,
        "pick_best_solana_swap",
        lambda quotes, _ledger: (quotes[0], 1.0),
    )
    monkeypatch.setattr(
        solana_swap_module,
        "sign_transaction_async",
        AsyncMock(side_effect=["signed-1", "signed-2"]),
    )
    send_mock = AsyncMock(side_effect=["sig-1", "sig-2"])
    monkeypatch.setattr(
        solana_swap_module,
        "send_solana_transaction_async",
        send_mock,
    )

    first = await solana_swap_module.solana_swap_token(_params())
    second = await solana_swap_module.solana_swap_token(_params())

    assert first["signature"] == "sig-1"
    assert second["signature"] == "sig-1"
    assert send_mock.await_count == 1
