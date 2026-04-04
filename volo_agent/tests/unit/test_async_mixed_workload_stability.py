from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from weakref import WeakKeyDictionary

import pytest

import core.token_security.registry_lookup as registry_lookup
from tool_nodes.bridge import bridge_tool
from tool_nodes.bridge.executors.across_executor import AcrossBridgeResult
from tool_nodes.bridge.simulators.across_simulator import AcrossBridgeQuote
from tool_nodes.dex import swap as swap_module
from tool_nodes.dex.swap_simulator_v3 import SimulationError

_ROUNDS = 4
_LOOKUP_COUNT = 18
_SWAP_COUNT = 12
_BRIDGE_COUNT = 12


class _FakeAsyncRegistry:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    async def get(self, symbol: str, chain_id: int):
        self.calls.append((symbol, chain_id))
        await asyncio.sleep(0.002)
        decimals = {"USDC": 6, "WETH": 18, "WBTC": 8}.get(symbol.upper(), 18)
        return SimpleNamespace(decimals=decimals)

    async def get_by_alias(self, symbol: str, chain_id: int):
        self.calls.append((symbol, chain_id))
        await asyncio.sleep(0.001)
        return None


class _DummySwapChain:
    def __init__(self) -> None:
        self.chain_id = 1
        self.name = "Ethereum"
        self.v3_quoter = "0xquoter"
        self.v3_router = "0xrouter"
        self.v2_router = "0xv2router"
        self.v2_factory = "0xv2factory"
        self.explorer_url = "https://etherscan.io"


class _DummyBridgeChain:
    def __init__(self, chain_id: int, name: str) -> None:
        self.chain_id = chain_id
        self.name = name
        self.explorer_url = "https://etherscan.io"
        self.is_testnet = False


def _swap_params(index: int, *, allow_fallback: bool) -> dict[str, object]:
    params: dict[str, object] = {
        "token_in_address": "0xaaa",
        "token_out_address": "0xbbb",
        "amount_in": 1,
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": f"0xswap{index}",
        "token_in_symbol": "ETH",
        "token_out_symbol": "USDC",
    }
    if allow_fallback:
        params["_fallback_policy"] = {
            "allow_fallback": True,
            "reason": "PLANNER_OVERRIDE",
        }
    return params


def _bridge_params(index: int) -> dict[str, object]:
    return {
        "token_symbol": "USDC",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": f"0xbridge{index}",
        "recipient": f"0xrecipient{index}",
    }


def _bridge_route(index: int):
    from config.bridge_registry import BridgeRoute

    return (
        "cfg",
        BridgeRoute(
            protocol="across",
            source_chain_id=1,
            dest_chain_id=8453,
            token_symbol="USDC",
            source_contract=f"0xsource{index}",
            dest_contract=f"0xdest{index}",
            input_token=f"0x{index + 1:040x}",
            output_token=f"0x{index + 101:040x}",
        ),
    )


def _bridge_quote(amount_out: Decimal) -> AcrossBridgeQuote:
    return AcrossBridgeQuote(
        protocol="across",
        token_symbol="USDC",
        input_token="0xinput",
        output_token="0xoutput",
        source_chain_id=1,
        dest_chain_id=8453,
        source_chain_name="Ethereum",
        dest_chain_name="Base",
        input_amount=Decimal("100"),
        output_amount=amount_out,
        total_fee=Decimal("1"),
        total_fee_pct=Decimal("0.01"),
        lp_fee=Decimal("0.5"),
        relayer_fee=Decimal("0.4"),
        gas_fee=Decimal("0.1"),
        input_decimals=6,
        output_decimals=6,
        quote_timestamp=1,
        fill_deadline=2,
        exclusivity_deadline=3,
        exclusive_relayer="0xrelayer",
        spoke_pool="0xpool",
        is_native_input=False,
        avg_fill_time_seconds=120,
    )


def _patch_swap_idempotency(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(swap_module, "claim_transfer_idempotency", AsyncMock(return_value=None))
    monkeypatch.setattr(swap_module, "load_transfer_idempotency_claim", AsyncMock(return_value=None))
    monkeypatch.setattr(swap_module, "mark_transfer_inflight", AsyncMock())
    monkeypatch.setattr(swap_module, "mark_transfer_success", AsyncMock())
    monkeypatch.setattr(swap_module, "mark_transfer_failed", AsyncMock())


def _patch_bridge_idempotency(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bridge_tool, "claim_transfer_idempotency", AsyncMock(return_value=None))
    monkeypatch.setattr(bridge_tool, "load_transfer_idempotency_claim", AsyncMock(return_value=None))
    monkeypatch.setattr(bridge_tool, "mark_transfer_success", AsyncMock())
    monkeypatch.setattr(bridge_tool, "mark_transfer_failed", AsyncMock())


@pytest.mark.asyncio
async def test_mixed_async_workloads_remain_stable_under_repeated_high_concurrency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_registry = _FakeAsyncRegistry()
    _patch_swap_idempotency(monkeypatch)
    _patch_bridge_idempotency(monkeypatch)

    registry_lookup._LOOKUP_CACHE.clear()
    monkeypatch.setattr(registry_lookup, "get_async_token_registry", lambda: fake_registry)

    monkeypatch.setattr(swap_module, "get_chain_by_name", lambda _name: _DummySwapChain())
    monkeypatch.setattr(swap_module.gas_price_cache, "get_gwei", AsyncMock(return_value=5))

    async def _simulate_swap(**kwargs):
        await asyncio.sleep(0.004)
        sender = str(kwargs["sender"])
        if sender.endswith(("1", "3", "5", "7", "9")):
            return SimulationError(reason="NO_LIQ", message="no liquidity")
        return SimpleNamespace(route="single-hop", path=["0xaaa", "0xbbb"], amount_out=Decimal("2"))

    async def _simulate_swap_v2(**_kwargs):
        await asyncio.sleep(0.004)
        return SimpleNamespace(route="direct", path=["0xaaa", "0xbbb"], amount_out=Decimal("1.9"))

    async def _execute_swap(*, quote, sender: str, **_kwargs):
        await asyncio.sleep(0.002)
        protocol = "v2" if getattr(quote, "route", "") == "direct" else "v3"
        return SimpleNamespace(
            protocol=protocol,
            tx_hash=f"tx-{sender}",
            approve_hash=None,
            amount_in=Decimal("1"),
            amount_out_minimum=Decimal("1.8"),
            chain_name="Ethereum",
        )

    monkeypatch.setattr(swap_module, "simulate_swap", _simulate_swap)
    monkeypatch.setattr(swap_module, "simulate_swap_v2", _simulate_swap_v2)
    monkeypatch.setattr(swap_module, "execute_swap", _execute_swap)

    monkeypatch.setattr(
        bridge_tool,
        "get_chain_by_name",
        lambda name: (
            _DummyBridgeChain(1, "Ethereum")
            if name == "ethereum"
            else _DummyBridgeChain(8453, "Base")
        ),
    )
    monkeypatch.setattr(
        bridge_tool,
        "get_routes",
        lambda **_kwargs: [_bridge_route(index) for index in range(6)],
    )
    monkeypatch.setattr(bridge_tool, "get_dynamic_protocols", lambda: [])
    monkeypatch.setattr(
        bridge_tool,
        "fetch_across_available_routes_async",
        AsyncMock(return_value=[_bridge_route(index + 100)[1] for index in range(6)]),
    )
    monkeypatch.setattr(bridge_tool, "_get_route_decimals", AsyncMock(return_value=(6, 6)))
    monkeypatch.setattr(bridge_tool.gas_price_cache, "get_gwei", AsyncMock(return_value=5))
    monkeypatch.setattr(bridge_tool, "_BRIDGE_SIMULATION_MAX_CONCURRENCY", 8)
    monkeypatch.setattr(bridge_tool, "_BRIDGE_SIMULATION_SEMAPHORES", WeakKeyDictionary())

    async def _simulate_bridge(route_tuple, *_args, **_kwargs):
        await asyncio.sleep(0.004)
        route = route_tuple[1]
        route_index = int(route.source_contract.replace("0xsource", "") or "0")
        return _bridge_quote(Decimal("110") - Decimal(route_index % 10))

    async def _execute_bridge(*args, **kwargs):
        sender = str(kwargs.get("sender") or args[2])
        await asyncio.sleep(0.002)
        return AcrossBridgeResult(
            tx_hash=f"bridge-{sender}",
            approve_hash=None,
            source_chain_id=1,
            dest_chain_id=8453,
            source_chain_name="Ethereum",
            dest_chain_name="Base",
            token_symbol="USDC",
            input_amount=Decimal("100"),
            output_amount=Decimal("99"),
            recipient=sender.replace("bridge", "recipient"),
            spoke_pool="0xpool",
            fill_deadline=2,
            estimated_fill_time="~2 minutes",
            status="pending",
        )

    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _simulate_bridge)
    monkeypatch.setattr(bridge_tool, "execute_across_bridge", _execute_bridge)

    @asynccontextmanager
    async def _wallet_lock_cm(*_args, **_kwargs):
        yield None

    monkeypatch.setattr(bridge_tool, "wallet_lock", _wallet_lock_cm)

    async def _run_round(round_index: int) -> None:
        lookup_symbols = ["USDC", "WETH", "WBTC"] * (_LOOKUP_COUNT // 3)
        lookup_tasks = [
            registry_lookup.get_registry_decimals_by_symbol_async(symbol, 8453)
            for symbol in lookup_symbols
        ]
        swap_tasks = [
            swap_module.swap_token(
                _swap_params(
                    index=(round_index * _SWAP_COUNT) + index,
                    allow_fallback=(index % 2 == 1),
                )
            )
            for index in range(_SWAP_COUNT)
        ]
        bridge_tasks = [
            bridge_tool.bridge_token(_bridge_params((round_index * _BRIDGE_COUNT) + index))
            for index in range(_BRIDGE_COUNT)
        ]

        results = await asyncio.wait_for(
            asyncio.gather(*(lookup_tasks + swap_tasks + bridge_tasks)),
            timeout=5.0,
        )

        lookup_results = results[: len(lookup_tasks)]
        swap_results = results[len(lookup_tasks) : len(lookup_tasks) + len(swap_tasks)]
        bridge_results = results[-len(bridge_tasks) :]

        assert sorted(set(lookup_results)) == [6, 8, 18]
        for index, swap_result in enumerate(swap_results):
            expected_protocol = "v2" if index % 2 == 1 else "v3"
            assert swap_result["protocol"] == expected_protocol
            assert swap_result["tx_hash"].startswith("tx-0xswap")
        for bridge_result in bridge_results:
            assert bridge_result["protocol"] == "across"
            assert bridge_result["status"] == "pending"
            assert bridge_result["tx_hash"].startswith("bridge-0xbridge")

    for round_index in range(_ROUNDS):
        registry_lookup._LOOKUP_CACHE.clear()
        bridge_tool._BRIDGE_SIMULATION_SEMAPHORES = WeakKeyDictionary()
        await _run_round(round_index)
