from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from core.utils.errors import DeterminismViolationError
from tool_nodes.dex import swap as swap_module
from tool_nodes.dex.swap_simulator_v3 import SimulationError

_SIMULATION_DELAY_SECONDS = 0.1


class _DummyChain:
    def __init__(self, *, v3: bool = True, v2: bool = True) -> None:
        self.chain_id = 1
        self.name = "Ethereum"
        self.v3_quoter = "0xquoter" if v3 else None
        self.v3_router = "0xrouter" if v3 else None
        self.v2_router = "0xv2router" if v2 else None
        self.v2_factory = "0xv2factory" if v2 else None
        self.explorer_url = "https://etherscan.io"


@dataclass
class _DummyQuote:
    route: str = "single-hop"
    path: list[str] | None = None
    amount_out: Decimal = Decimal("2")


@dataclass
class _DummyResult:
    protocol: str
    tx_hash: str
    approve_hash: str | None
    amount_in: Decimal
    amount_out_minimum: Decimal
    chain_name: str


def _swap_params() -> dict[str, object]:
    return {
        "token_in_address": "0xaaa",
        "token_out_address": "0xbbb",
        "amount_in": 1,
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": "0xsender",
        "token_in_symbol": "ETH",
        "token_out_symbol": "USDC",
    }


def _patch_idempotency(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        swap_module,
        "claim_transfer_idempotency",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        swap_module,
        "load_transfer_idempotency_claim",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        swap_module,
        "mark_transfer_inflight",
        AsyncMock(),
    )
    monkeypatch.setattr(
        swap_module,
        "mark_transfer_success",
        AsyncMock(),
    )
    monkeypatch.setattr(
        swap_module,
        "mark_transfer_failed",
        AsyncMock(),
    )


@pytest.mark.asyncio
async def test_swap_token_runs_v2_and_v3_simulations_in_parallel_for_v2_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    monkeypatch.setattr(swap_module, "get_chain_by_name", lambda _name: _DummyChain())
    monkeypatch.setattr(swap_module.gas_price_cache, "get_gwei", AsyncMock(return_value=5))

    async def _slow_v3_failure(**_kwargs):
        await asyncio.sleep(_SIMULATION_DELAY_SECONDS)
        return SimulationError(reason="NO_LIQ", message="no liquidity")

    async def _slow_v2(**_kwargs):
        await asyncio.sleep(_SIMULATION_DELAY_SECONDS)
        return _DummyQuote(route="direct", path=["0xaaa", "0xbbb"])

    monkeypatch.setattr(swap_module, "simulate_swap", _slow_v3_failure)
    monkeypatch.setattr(swap_module, "simulate_swap_v2", _slow_v2)
    execute_swap = AsyncMock(
        return_value=_DummyResult(
            protocol="v2",
            tx_hash="0xhash",
            approve_hash=None,
            amount_in=Decimal("1"),
            amount_out_minimum=Decimal("1.9"),
            chain_name="Ethereum",
        )
    )
    monkeypatch.setattr(swap_module, "execute_swap", execute_swap)

    started = time.perf_counter()
    result = await swap_module.swap_token(
        {
            **_swap_params(),
            "_fallback_policy": {
                "allow_fallback": True,
                "reason": "PLANNER_OVERRIDE",
            },
        }
    )
    elapsed = time.perf_counter() - started

    assert elapsed < (_SIMULATION_DELAY_SECONDS * 1.6)
    assert result["protocol"] == "v2"
    assert execute_swap.await_count == 1


@pytest.mark.asyncio
async def test_swap_token_still_prefers_v3_quote_when_both_simulations_succeed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    monkeypatch.setattr(swap_module, "get_chain_by_name", lambda _name: _DummyChain())
    monkeypatch.setattr(swap_module.gas_price_cache, "get_gwei", AsyncMock(return_value=5))

    async def _slow_v3_quote(**_kwargs):
        await asyncio.sleep(_SIMULATION_DELAY_SECONDS)
        return _DummyQuote(route="single-hop", path=["0xaaa", "0xbbb"])

    async def _slow_v2_quote(**_kwargs):
        await asyncio.sleep(_SIMULATION_DELAY_SECONDS)
        return _DummyQuote(route="direct", path=["0xaaa", "0xbbb"])

    monkeypatch.setattr(swap_module, "simulate_swap", _slow_v3_quote)
    monkeypatch.setattr(swap_module, "simulate_swap_v2", _slow_v2_quote)
    monkeypatch.setattr(
        swap_module,
        "execute_swap",
        AsyncMock(
            return_value=_DummyResult(
                protocol="v3",
                tx_hash="0xhashv3",
                approve_hash=None,
                amount_in=Decimal("1"),
                amount_out_minimum=Decimal("1.9"),
                chain_name="Ethereum",
            )
        ),
    )

    result = await swap_module.swap_token(_swap_params())

    assert result["protocol"] == "v3"
    assert result["tx_hash"] == "0xhashv3"


@pytest.mark.asyncio
async def test_swap_token_does_not_start_v2_fallback_without_explicit_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    monkeypatch.setattr(swap_module, "get_chain_by_name", lambda _name: _DummyChain())
    monkeypatch.setattr(swap_module.gas_price_cache, "get_gwei", AsyncMock(return_value=5))

    async def _v3_failure(**_kwargs):
        return SimulationError(reason="NO_LIQ", message="no liquidity")

    v2_simulation = AsyncMock(return_value=_DummyQuote(route="direct", path=["0xaaa", "0xbbb"]))

    monkeypatch.setattr(swap_module, "simulate_swap", _v3_failure)
    monkeypatch.setattr(swap_module, "simulate_swap_v2", v2_simulation)
    monkeypatch.setattr(swap_module, "execute_swap", AsyncMock())

    with pytest.raises(DeterminismViolationError):
        await swap_module.swap_token(_swap_params())

    v2_simulation.assert_not_awaited()
