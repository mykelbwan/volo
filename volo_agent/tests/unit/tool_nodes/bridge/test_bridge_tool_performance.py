from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import AsyncMock
from weakref import WeakKeyDictionary

import pytest

from config.bridge_registry import BridgeRoute
from tool_nodes.bridge import bridge_tool
from tool_nodes.bridge.executors.across_executor import AcrossBridgeResult
from tool_nodes.bridge.simulators.across_simulator import (
    AcrossBridgeQuote,
    AcrossSimulationError,
)

_DECIMALS_DELAY_SECONDS = 0.05
_ROUTE_COUNT = 10
_FETCH_DELAY_SECONDS = 0.1
_SIMULATION_DELAY_SECONDS = 0.1
_GAS_DELAY_SECONDS = 0.1


@asynccontextmanager
async def _noop_wallet_lock(*_args, **_kwargs):
    yield None


class _DummyChain:
    def __init__(self, chain_id: int, name: str) -> None:
        self.chain_id = chain_id
        self.name = name
        self.explorer_url = "https://etherscan.io"
        self.is_testnet = False


def _bridge_params() -> dict[str, object]:
    return {
        "token_symbol": "USDC",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }


def _route(index: int) -> BridgeRoute:
    return BridgeRoute(
        protocol="across",
        source_chain_id=1,
        dest_chain_id=8453,
        token_symbol="USDC",
        source_contract=f"0xsource{index}",
        dest_contract=f"0xdest{index}",
        input_token=f"0x{index + 1:040x}",
        output_token=f"0x{index + 101:040x}",
    )


def _quote(output_amount: Decimal) -> AcrossBridgeQuote:
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
        output_amount=output_amount,
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


def _patch_idempotency(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        bridge_tool,
        "claim_transfer_idempotency",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        bridge_tool,
        "load_transfer_idempotency_claim",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        bridge_tool,
        "mark_transfer_success",
        AsyncMock(),
    )
    monkeypatch.setattr(
        bridge_tool,
        "mark_transfer_failed",
        AsyncMock(),
    )


@pytest.mark.asyncio
async def test_bridge_token_prefetches_route_decimals_in_parallel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    routes = [("cfg", _route(index)) for index in range(_ROUTE_COUNT)]
    decimals_calls: list[tuple[int, str]] = []

    monkeypatch.setattr(
        bridge_tool,
        "get_chain_by_name",
        lambda name: (
            _DummyChain(1, "Ethereum")
            if name == "ethereum"
            else _DummyChain(8453, "Base")
        ),
    )
    monkeypatch.setattr(bridge_tool, "get_routes", lambda **_kwargs: routes)
    monkeypatch.setattr(bridge_tool, "get_dynamic_protocols", lambda: [])
    monkeypatch.setattr(
        bridge_tool,
        "fetch_across_available_routes_async",
        AsyncMock(return_value=[]),
    )

    async def _slow_get_route_decimals(route: BridgeRoute, token_symbol: str) -> tuple[int, int]:
        decimals_calls.append((route.source_chain_id, token_symbol))
        await asyncio.sleep(_DECIMALS_DELAY_SECONDS)
        return (6, 6)

    async def _fake_simulate_route(route_tuple, *_args, **_kwargs):
        route = route_tuple[1]
        return _quote(Decimal("100") - Decimal(route.source_contract[-1]))

    monkeypatch.setattr(bridge_tool, "_get_route_decimals", _slow_get_route_decimals)
    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _fake_simulate_route)
    monkeypatch.setattr(
        bridge_tool.gas_price_cache,
        "get_gwei",
        AsyncMock(return_value=5),
    )
    monkeypatch.setattr(
        bridge_tool,
        "execute_across_bridge",
        AsyncMock(
            return_value=AcrossBridgeResult(
                tx_hash="0xhash",
                approve_hash=None,
                source_chain_id=1,
                dest_chain_id=8453,
                source_chain_name="Ethereum",
                dest_chain_name="Base",
                token_symbol="USDC",
                input_amount=Decimal("100"),
                output_amount=Decimal("99"),
                recipient="0xsender",
                spoke_pool="0xpool",
                fill_deadline=2,
                estimated_fill_time="~2 minutes",
                status="pending",
            )
        ),
    )
    monkeypatch.setattr(bridge_tool, "wallet_lock", _noop_wallet_lock)

    started = time.perf_counter()
    result = await bridge_tool.bridge_token(_bridge_params())
    elapsed = time.perf_counter() - started

    assert elapsed < (_DECIMALS_DELAY_SECONDS * 2.5)
    assert len(decimals_calls) == _ROUTE_COUNT
    assert result["protocol"] == "across"


@pytest.mark.asyncio
async def test_bridge_token_overlaps_dynamic_route_fetch_with_static_simulation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    routes = [("cfg", _route(0))]
    timeline: dict[str, float] = {}

    monkeypatch.setattr(
        bridge_tool,
        "get_chain_by_name",
        lambda name: (
            _DummyChain(1, "Ethereum")
            if name == "ethereum"
            else _DummyChain(8453, "Base")
        ),
    )
    monkeypatch.setattr(bridge_tool, "get_routes", lambda **_kwargs: routes)
    monkeypatch.setattr(bridge_tool, "get_dynamic_protocols", lambda: [])

    async def _slow_fetch(*_args, **_kwargs):
        timeline["fetch_start"] = time.perf_counter()
        await asyncio.sleep(_FETCH_DELAY_SECONDS)
        timeline["fetch_end"] = time.perf_counter()
        return []

    async def _fast_route_decimals(*_args, **_kwargs) -> tuple[int, int]:
        return (6, 6)

    async def _slow_simulate_route(*_args, **_kwargs):
        timeline["simulate_start"] = time.perf_counter()
        await asyncio.sleep(_SIMULATION_DELAY_SECONDS)
        timeline["simulate_end"] = time.perf_counter()
        return _quote(Decimal("99"))

    monkeypatch.setattr(
        bridge_tool,
        "fetch_across_available_routes_async",
        _slow_fetch,
    )
    monkeypatch.setattr(bridge_tool, "_get_route_decimals", _fast_route_decimals)
    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _slow_simulate_route)
    monkeypatch.setattr(
        bridge_tool.gas_price_cache,
        "get_gwei",
        AsyncMock(return_value=5),
    )
    monkeypatch.setattr(
        bridge_tool,
        "execute_across_bridge",
        AsyncMock(
            return_value=AcrossBridgeResult(
                tx_hash="0xhash",
                approve_hash=None,
                source_chain_id=1,
                dest_chain_id=8453,
                source_chain_name="Ethereum",
                dest_chain_name="Base",
                token_symbol="USDC",
                input_amount=Decimal("100"),
                output_amount=Decimal("99"),
                recipient="0xsender",
                spoke_pool="0xpool",
                fill_deadline=2,
                estimated_fill_time="~2 minutes",
                status="pending",
            )
        ),
    )
    monkeypatch.setattr(bridge_tool, "wallet_lock", _noop_wallet_lock)

    started = time.perf_counter()
    result = await bridge_tool.bridge_token(_bridge_params())
    elapsed = time.perf_counter() - started

    assert elapsed < (_FETCH_DELAY_SECONDS + _SIMULATION_DELAY_SECONDS * 0.6)
    assert timeline["simulate_start"] < timeline["fetch_end"]
    assert result["protocol"] == "across"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("request_count", "max_elapsed_seconds", "min_peak_concurrency"),
    [
        (25, 0.25, 12),
        (500, 3.5, 100),
    ],
)
async def test_bridge_token_overlap_path_holds_under_concurrent_request_load(
    monkeypatch: pytest.MonkeyPatch,
    request_count: int,
    max_elapsed_seconds: float,
    min_peak_concurrency: int,
) -> None:
    _patch_idempotency(monkeypatch)
    routes = [("cfg", _route(0))]
    fetch_active = 0
    fetch_max = 0
    simulate_active = 0
    simulate_max = 0
    lock = asyncio.Lock()

    monkeypatch.setattr(
        bridge_tool,
        "get_chain_by_name",
        lambda name: (
            _DummyChain(1, "Ethereum")
            if name == "ethereum"
            else _DummyChain(8453, "Base")
        ),
    )
    monkeypatch.setattr(bridge_tool, "get_routes", lambda **_kwargs: routes)
    monkeypatch.setattr(bridge_tool, "get_dynamic_protocols", lambda: [])
    monkeypatch.setattr(bridge_tool, "_get_route_decimals", AsyncMock(return_value=(6, 6)))

    async def _slow_fetch(*_args, **_kwargs):
        nonlocal fetch_active, fetch_max
        async with lock:
            fetch_active += 1
            fetch_max = max(fetch_max, fetch_active)
        try:
            await asyncio.sleep(_FETCH_DELAY_SECONDS)
            return []
        finally:
            async with lock:
                fetch_active -= 1

    async def _slow_simulate_route(*_args, **_kwargs):
        nonlocal simulate_active, simulate_max
        async with lock:
            simulate_active += 1
            simulate_max = max(simulate_max, simulate_active)
        try:
            await asyncio.sleep(_SIMULATION_DELAY_SECONDS)
            return _quote(Decimal("99"))
        finally:
            async with lock:
                simulate_active -= 1

    async def _execute(*_args, **_kwargs):
        return AcrossBridgeResult(
            tx_hash="0xhash",
            approve_hash=None,
            source_chain_id=1,
            dest_chain_id=8453,
            source_chain_name="Ethereum",
            dest_chain_name="Base",
            token_symbol="USDC",
            input_amount=Decimal("100"),
            output_amount=Decimal("99"),
            recipient="0xsender",
            spoke_pool="0xpool",
            fill_deadline=2,
            estimated_fill_time="~2 minutes",
            status="pending",
        )

    monkeypatch.setattr(bridge_tool, "fetch_across_available_routes_async", _slow_fetch)
    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _slow_simulate_route)
    monkeypatch.setattr(bridge_tool.gas_price_cache, "get_gwei", AsyncMock(return_value=5))
    monkeypatch.setattr(bridge_tool, "execute_across_bridge", _execute)
    monkeypatch.setattr(bridge_tool, "wallet_lock", _noop_wallet_lock)

    async def _request(index: int) -> dict[str, object]:
        params = dict(_bridge_params())
        params["sender"] = f"0xsender{index}"
        params["recipient"] = f"0xrecipient{index}"
        return await bridge_tool.bridge_token(params)

    started = time.perf_counter()
    results = await asyncio.gather(*[_request(i) for i in range(request_count)])
    elapsed = time.perf_counter() - started

    simulation_cap = getattr(bridge_tool, "_BRIDGE_SIMULATION_MAX_CONCURRENCY", request_count)

    assert len(results) == request_count
    assert all(result["protocol"] == "across" for result in results)
    assert fetch_max >= min_peak_concurrency
    assert simulate_max >= min(min_peak_concurrency, simulation_cap)
    assert elapsed < max_elapsed_seconds


@pytest.mark.asyncio
async def test_bridge_token_prefetches_gas_price_before_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    routes = [("cfg", _route(0))]
    timeline: dict[str, float] = {}
    gas_calls = 0

    monkeypatch.setattr(
        bridge_tool,
        "get_chain_by_name",
        lambda name: (
            _DummyChain(1, "Ethereum")
            if name == "ethereum"
            else _DummyChain(8453, "Base")
        ),
    )
    monkeypatch.setattr(bridge_tool, "get_routes", lambda **_kwargs: routes)
    monkeypatch.setattr(bridge_tool, "get_dynamic_protocols", lambda: [])
    monkeypatch.setattr(
        bridge_tool,
        "fetch_across_available_routes_async",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(bridge_tool, "_get_route_decimals", AsyncMock(return_value=(6, 6)))

    async def _slow_simulate_route(*_args, **_kwargs):
        timeline["simulate_start"] = time.perf_counter()
        await asyncio.sleep(_SIMULATION_DELAY_SECONDS)
        timeline["simulate_end"] = time.perf_counter()
        return _quote(Decimal("99"))

    async def _slow_get_gwei(*_args, **_kwargs):
        nonlocal gas_calls
        gas_calls += 1
        timeline["gas_start"] = time.perf_counter()
        await asyncio.sleep(_GAS_DELAY_SECONDS)
        timeline["gas_end"] = time.perf_counter()
        return 5

    async def _execute(*_args, **_kwargs):
        timeline["execute_start"] = time.perf_counter()
        return AcrossBridgeResult(
            tx_hash="0xhash",
            approve_hash=None,
            source_chain_id=1,
            dest_chain_id=8453,
            source_chain_name="Ethereum",
            dest_chain_name="Base",
            token_symbol="USDC",
            input_amount=Decimal("100"),
            output_amount=Decimal("99"),
            recipient="0xsender",
            spoke_pool="0xpool",
            fill_deadline=2,
            estimated_fill_time="~2 minutes",
            status="pending",
        )

    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _slow_simulate_route)
    monkeypatch.setattr(bridge_tool.gas_price_cache, "get_gwei", _slow_get_gwei)
    monkeypatch.setattr(bridge_tool, "execute_across_bridge", _execute)
    monkeypatch.setattr(bridge_tool, "wallet_lock", _noop_wallet_lock)

    started = time.perf_counter()
    result = await bridge_tool.bridge_token(_bridge_params())
    elapsed = time.perf_counter() - started

    assert gas_calls == 1
    assert timeline["gas_start"] < timeline["execute_start"]
    assert timeline["gas_start"] < timeline["simulate_end"]
    assert elapsed < (_SIMULATION_DELAY_SECONDS + _GAS_DELAY_SECONDS * 0.6)
    assert result["protocol"] == "across"


@pytest.mark.asyncio
async def test_bridge_simulations_do_not_fan_out_without_a_global_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    route_count = 100
    routes = [("cfg", _route(index)) for index in range(route_count)]
    active = 0
    max_active = 0
    counter_lock = asyncio.Lock()

    monkeypatch.setattr(
        bridge_tool,
        "get_chain_by_name",
        lambda name: (
            _DummyChain(1, "Ethereum")
            if name == "ethereum"
            else _DummyChain(8453, "Base")
        ),
    )
    monkeypatch.setattr(bridge_tool, "get_routes", lambda **_kwargs: routes)
    monkeypatch.setattr(bridge_tool, "get_dynamic_protocols", lambda: [])
    monkeypatch.setattr(
        bridge_tool,
        "fetch_across_available_routes_async",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(bridge_tool, "_get_route_decimals", AsyncMock(return_value=(6, 6)))
    monkeypatch.setattr(
        bridge_tool.gas_price_cache,
        "get_gwei",
        AsyncMock(return_value=5),
    )

    async def _tracked_simulation(*_args, **_kwargs):
        nonlocal active, max_active
        async with counter_lock:
            active += 1
            max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.05)
            return _quote(Decimal("99"))
        finally:
            async with counter_lock:
                active -= 1

    async def _execute(*_args, **_kwargs):
        return AcrossBridgeResult(
            tx_hash="0xhash",
            approve_hash=None,
            source_chain_id=1,
            dest_chain_id=8453,
            source_chain_name="Ethereum",
            dest_chain_name="Base",
            token_symbol="USDC",
            input_amount=Decimal("100"),
            output_amount=Decimal("99"),
            recipient="0xsender",
            spoke_pool="0xpool",
            fill_deadline=2,
            estimated_fill_time="~2 minutes",
            status="pending",
        )

    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _tracked_simulation)
    monkeypatch.setattr(bridge_tool, "execute_across_bridge", _execute)
    monkeypatch.setattr(bridge_tool, "wallet_lock", _noop_wallet_lock)

    result = await bridge_tool.bridge_token(_bridge_params())
    max_allowed = getattr(bridge_tool, "_BRIDGE_SIMULATION_MAX_CONCURRENCY", 32)

    assert result["protocol"] == "across"
    assert max_active <= max_allowed


@pytest.mark.asyncio
async def test_bridge_simulation_limit_applies_across_static_and_dynamic_routes_under_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    static_routes = [("cfg", _route(index)) for index in range(12)]
    dynamic_routes = [_route(index + 100) for index in range(12)]
    active = 0
    max_active = 0
    counter_lock = asyncio.Lock()
    request_count = 20
    simulation_cap = 8

    monkeypatch.setattr(bridge_tool, "_BRIDGE_SIMULATION_MAX_CONCURRENCY", simulation_cap)
    monkeypatch.setattr(bridge_tool, "_BRIDGE_SIMULATION_SEMAPHORES", WeakKeyDictionary())
    monkeypatch.setattr(
        bridge_tool,
        "get_chain_by_name",
        lambda name: (
            _DummyChain(1, "Ethereum")
            if name == "ethereum"
            else _DummyChain(8453, "Base")
        ),
    )
    monkeypatch.setattr(bridge_tool, "get_routes", lambda **_kwargs: list(static_routes))
    monkeypatch.setattr(bridge_tool, "get_dynamic_protocols", lambda: [])
    monkeypatch.setattr(
        bridge_tool,
        "fetch_across_available_routes_async",
        AsyncMock(return_value=dynamic_routes),
    )
    monkeypatch.setattr(bridge_tool, "_get_route_decimals", AsyncMock(return_value=(6, 6)))
    monkeypatch.setattr(bridge_tool.gas_price_cache, "get_gwei", AsyncMock(return_value=5))

    async def _tracked_simulation(*_args, **_kwargs):
        nonlocal active, max_active
        async with counter_lock:
            active += 1
            max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.02)
            return _quote(Decimal("99"))
        finally:
            async with counter_lock:
                active -= 1

    async def _execute(*_args, **_kwargs):
        return AcrossBridgeResult(
            tx_hash="0xhash",
            approve_hash=None,
            source_chain_id=1,
            dest_chain_id=8453,
            source_chain_name="Ethereum",
            dest_chain_name="Base",
            token_symbol="USDC",
            input_amount=Decimal("100"),
            output_amount=Decimal("99"),
            recipient="0xsender",
            spoke_pool="0xpool",
            fill_deadline=2,
            estimated_fill_time="~2 minutes",
            status="pending",
        )

    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _tracked_simulation)
    monkeypatch.setattr(bridge_tool, "execute_across_bridge", _execute)
    monkeypatch.setattr(bridge_tool, "wallet_lock", _noop_wallet_lock)

    async def _request(index: int) -> dict[str, object]:
        params = dict(_bridge_params())
        params["sender"] = f"0xsender{index}"
        params["recipient"] = f"0xrecipient{index}"
        return await bridge_tool.bridge_token(params)

    results = await asyncio.gather(*[_request(index) for index in range(request_count)])

    assert len(results) == request_count
    assert all(result["protocol"] == "across" for result in results)
    assert max_active <= simulation_cap


@pytest.mark.asyncio
async def test_bridge_simulation_limit_releases_permits_after_request_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    route_count = 24
    simulation_cap = 6
    routes = [("cfg", _route(index)) for index in range(route_count)]
    started = 0
    active = 0
    counter_lock = asyncio.Lock()
    simulation_gate = asyncio.Event()

    monkeypatch.setattr(bridge_tool, "_BRIDGE_SIMULATION_MAX_CONCURRENCY", simulation_cap)
    monkeypatch.setattr(bridge_tool, "_BRIDGE_SIMULATION_SEMAPHORES", WeakKeyDictionary())
    monkeypatch.setattr(
        bridge_tool,
        "get_chain_by_name",
        lambda name: (
            _DummyChain(1, "Ethereum")
            if name == "ethereum"
            else _DummyChain(8453, "Base")
        ),
    )
    monkeypatch.setattr(bridge_tool, "get_routes", lambda **_kwargs: routes)
    monkeypatch.setattr(bridge_tool, "get_dynamic_protocols", lambda: [])
    monkeypatch.setattr(
        bridge_tool,
        "fetch_across_available_routes_async",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(bridge_tool, "_get_route_decimals", AsyncMock(return_value=(6, 6)))
    monkeypatch.setattr(bridge_tool.gas_price_cache, "get_gwei", AsyncMock(return_value=5))

    async def _blocked_simulation(*_args, **_kwargs):
        nonlocal started, active
        async with counter_lock:
            started += 1
            active += 1
        try:
            await simulation_gate.wait()
            return _quote(Decimal("99"))
        finally:
            async with counter_lock:
                active -= 1

    async def _fast_simulation(*_args, **_kwargs):
        await asyncio.sleep(0.01)
        return _quote(Decimal("99"))

    async def _execute(*_args, **_kwargs):
        return AcrossBridgeResult(
            tx_hash="0xhash",
            approve_hash=None,
            source_chain_id=1,
            dest_chain_id=8453,
            source_chain_name="Ethereum",
            dest_chain_name="Base",
            token_symbol="USDC",
            input_amount=Decimal("100"),
            output_amount=Decimal("99"),
            recipient="0xsender",
            spoke_pool="0xpool",
            fill_deadline=2,
            estimated_fill_time="~2 minutes",
            status="pending",
        )

    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _blocked_simulation)
    monkeypatch.setattr(bridge_tool, "execute_across_bridge", _execute)
    monkeypatch.setattr(bridge_tool, "wallet_lock", _noop_wallet_lock)

    first_request = asyncio.create_task(bridge_tool.bridge_token(_bridge_params()))
    deadline = time.perf_counter() + 1.0
    while started < simulation_cap:
        if time.perf_counter() > deadline:
            raise AssertionError("Timed out waiting for simulations to saturate the semaphore")
        await asyncio.sleep(0.01)

    first_request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_request

    simulation_gate.set()
    deadline = time.perf_counter() + 1.0
    while active:
        if time.perf_counter() > deadline:
            raise AssertionError("Cancelled simulations did not release the semaphore")
        await asyncio.sleep(0.01)

    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _fast_simulation)
    second_result = await asyncio.wait_for(bridge_tool.bridge_token(_bridge_params()), timeout=0.5)

    assert second_result["protocol"] == "across"


@pytest.mark.asyncio
async def test_bridge_simulation_limit_holds_under_jitter_and_partial_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    simulation_cap = 10
    request_count = 40
    static_routes = [("cfg", _route(index)) for index in range(12)]
    dynamic_routes = [_route(index + 100) for index in range(12)]
    active = 0
    max_active = 0
    lock = asyncio.Lock()
    fetch_count = 0

    monkeypatch.setattr(bridge_tool, "_BRIDGE_SIMULATION_MAX_CONCURRENCY", simulation_cap)
    monkeypatch.setattr(bridge_tool, "_BRIDGE_SIMULATION_SEMAPHORES", WeakKeyDictionary())
    monkeypatch.setattr(
        bridge_tool,
        "get_chain_by_name",
        lambda name: (
            _DummyChain(1, "Ethereum")
            if name == "ethereum"
            else _DummyChain(8453, "Base")
        ),
    )
    monkeypatch.setattr(bridge_tool, "get_routes", lambda **_kwargs: list(static_routes))
    monkeypatch.setattr(bridge_tool, "get_dynamic_protocols", lambda: [])
    monkeypatch.setattr(bridge_tool, "_get_route_decimals", AsyncMock(return_value=(6, 6)))
    monkeypatch.setattr(bridge_tool.gas_price_cache, "get_gwei", AsyncMock(return_value=5))

    async def _jittery_fetch(*_args, **_kwargs):
        nonlocal fetch_count
        fetch_count += 1
        await asyncio.sleep(0.005 * ((fetch_count % 4) + 1))
        if fetch_count % 7 == 0:
            raise RuntimeError("temporary across route fetch failure")
        return list(dynamic_routes)

    async def _jittery_simulation(route_tuple, *_args, **_kwargs):
        nonlocal active, max_active
        route = route_tuple[1]
        route_index = int(route.source_contract.replace("0xsource", "") or "0")
        async with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.003 * ((route_index % 5) + 1))
            if route_index % 11 == 0:
                return AcrossSimulationError(reason="NO_LIQ", message="no liquidity")
            if route_index % 13 == 0:
                raise RuntimeError("simulator crashed")
            return _quote(Decimal("120") - Decimal(route_index % 17))
        finally:
            async with lock:
                active -= 1

    async def _execute(*_args, **_kwargs):
        return AcrossBridgeResult(
            tx_hash="0xhash",
            approve_hash=None,
            source_chain_id=1,
            dest_chain_id=8453,
            source_chain_name="Ethereum",
            dest_chain_name="Base",
            token_symbol="USDC",
            input_amount=Decimal("100"),
            output_amount=Decimal("99"),
            recipient="0xsender",
            spoke_pool="0xpool",
            fill_deadline=2,
            estimated_fill_time="~2 minutes",
            status="pending",
        )

    monkeypatch.setattr(bridge_tool, "fetch_across_available_routes_async", _jittery_fetch)
    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _jittery_simulation)
    monkeypatch.setattr(bridge_tool, "execute_across_bridge", _execute)
    monkeypatch.setattr(bridge_tool, "wallet_lock", _noop_wallet_lock)

    async def _request(index: int) -> dict[str, object]:
        params = dict(_bridge_params())
        params["sender"] = f"0xsender{index}"
        params["recipient"] = f"0xrecipient{index}"
        return await bridge_tool.bridge_token(params)

    results = await asyncio.wait_for(
        asyncio.gather(*[_request(index) for index in range(request_count)]),
        timeout=5.0,
    )

    assert len(results) == request_count
    assert all(result["protocol"] == "across" for result in results)
    assert max_active <= simulation_cap


@pytest.mark.asyncio
async def test_bridge_simulation_limit_survives_cancellation_storm_across_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_idempotency(monkeypatch)
    simulation_cap = 8
    route_count = 20
    request_count = 16
    cancel_count = 8
    routes = [("cfg", _route(index)) for index in range(route_count)]
    started = 0
    max_active = 0
    active = 0
    counter_lock = asyncio.Lock()
    gate = asyncio.Event()

    monkeypatch.setattr(bridge_tool, "_BRIDGE_SIMULATION_MAX_CONCURRENCY", simulation_cap)
    monkeypatch.setattr(bridge_tool, "_BRIDGE_SIMULATION_SEMAPHORES", WeakKeyDictionary())
    monkeypatch.setattr(
        bridge_tool,
        "get_chain_by_name",
        lambda name: (
            _DummyChain(1, "Ethereum")
            if name == "ethereum"
            else _DummyChain(8453, "Base")
        ),
    )
    monkeypatch.setattr(bridge_tool, "get_routes", lambda **_kwargs: routes)
    monkeypatch.setattr(bridge_tool, "get_dynamic_protocols", lambda: [])
    monkeypatch.setattr(
        bridge_tool,
        "fetch_across_available_routes_async",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(bridge_tool, "_get_route_decimals", AsyncMock(return_value=(6, 6)))
    monkeypatch.setattr(bridge_tool.gas_price_cache, "get_gwei", AsyncMock(return_value=5))

    async def _blocked_simulation(*_args, **_kwargs):
        nonlocal started, max_active, active
        async with counter_lock:
            started += 1
            active += 1
            max_active = max(max_active, active)
        try:
            await gate.wait()
            return _quote(Decimal("99"))
        finally:
            async with counter_lock:
                active -= 1

    async def _execute(*_args, **_kwargs):
        return AcrossBridgeResult(
            tx_hash="0xhash",
            approve_hash=None,
            source_chain_id=1,
            dest_chain_id=8453,
            source_chain_name="Ethereum",
            dest_chain_name="Base",
            token_symbol="USDC",
            input_amount=Decimal("100"),
            output_amount=Decimal("99"),
            recipient="0xsender",
            spoke_pool="0xpool",
            fill_deadline=2,
            estimated_fill_time="~2 minutes",
            status="pending",
        )

    monkeypatch.setattr(bridge_tool, "_simulate_route_async", _blocked_simulation)
    monkeypatch.setattr(bridge_tool, "execute_across_bridge", _execute)
    monkeypatch.setattr(bridge_tool, "wallet_lock", _noop_wallet_lock)

    async def _request(index: int) -> dict[str, object]:
        params = dict(_bridge_params())
        params["sender"] = f"0xsender{index}"
        params["recipient"] = f"0xrecipient{index}"
        return await bridge_tool.bridge_token(params)

    tasks = [asyncio.create_task(_request(index)) for index in range(request_count)]
    deadline = time.perf_counter() + 1.0
    while started < simulation_cap:
        if time.perf_counter() > deadline:
            raise AssertionError("Timed out waiting for cancellation-storm saturation")
        await asyncio.sleep(0.01)

    for task in tasks[:cancel_count]:
        task.cancel()

    for task in tasks[:cancel_count]:
        with pytest.raises(asyncio.CancelledError):
            await task

    gate.set()

    follow_up = asyncio.create_task(_request(999))
    survivors = await asyncio.wait_for(
        asyncio.gather(*tasks[cancel_count:], follow_up),
        timeout=2.0,
    )

    assert len(survivors) == (request_count - cancel_count + 1)
    assert all(result["protocol"] == "across" for result in survivors)
    assert max_active <= simulation_cap
