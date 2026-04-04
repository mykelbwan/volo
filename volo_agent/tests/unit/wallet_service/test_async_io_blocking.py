from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.utils import http
from wallet_service.common import cdp_helpers
from wallet_service.evm.cdp_utils import get_evm_account
from wallet_service.evm.gas_price import GasPriceCache

from tests.unit.wallet_service._stress_helpers import FakeHTTPResponse, Heartbeat

ASYNC_TEST_TIMEOUT = 10.0
TEST_GAS_PRICE = 456_789_123


class _SlowAsyncClient:
    def __init__(self, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds
        self.is_closed = False
        self.calls = 0

    async def request(self, method: str, url: str, **_kwargs: object) -> FakeHTTPResponse:
        self.calls += 1
        await asyncio.sleep(self.delay_seconds)
        return FakeHTTPResponse(status_code=200, payload={"ok": True}, text="ok")

    async def aclose(self) -> None:
        self.is_closed = True


@pytest.fixture(autouse=True)
def _clear_http_clients() -> None:
    with http._ASYNC_CLIENTS_LOCK:
        http._ASYNC_CLIENTS.clear()
    yield
    with http._ASYNC_CLIENTS_LOCK:
        http._ASYNC_CLIENTS.clear()


@pytest.mark.asyncio
async def test_async_http_load_keeps_event_loop_responsive_under_concurrency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        client = _SlowAsyncClient(delay_seconds=0.02)
        heartbeat = Heartbeat()
        heartbeat_task = asyncio.create_task(heartbeat.run())

        monkeypatch.setattr(http, "_build_async_client", lambda: client)
        monkeypatch.setattr(http, "record_external_call", lambda **_kwargs: None)

        started = time.perf_counter()
        try:
            await asyncio.gather(
                *[
                    http.async_request_json(
                        "POST",
                        "https://rpc.test",
                        service="rpc",
                        json={"i": index},
                    )
                    for index in range(60)
                ]
            )
        finally:
            heartbeat.stop()
            await heartbeat_task

        elapsed = time.perf_counter() - started

        assert client.calls == 60
        assert elapsed < 0.30
        assert heartbeat.ticks > 20

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_async_gas_price_refresh_under_contention_avoids_thread_usage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        cache = GasPriceCache()
        chain = SimpleNamespace(chain_id=1, rpc_url="https://rpc.test")
        heartbeat = Heartbeat()
        heartbeat_task = asyncio.create_task(heartbeat.run())
        state = {"fetches": 0}

        async def _fake_fetch(_chain: object) -> int:
            state["fetches"] += 1
            await asyncio.sleep(0.03)
            return TEST_GAS_PRICE

        def _forbidden_thread(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("async gas-price path must not spawn threads")

        monkeypatch.setattr("wallet_service.evm.gas_price.get_async_redis", AsyncMock(return_value=None))
        monkeypatch.setattr(cache, "_resolve_chain", lambda *_args, **_kwargs: chain)
        monkeypatch.setattr(cache, "_fetch_async", _fake_fetch)
        monkeypatch.setattr("wallet_service.evm.gas_price.threading.Thread", _forbidden_thread)

        try:
            results = await asyncio.gather(
                *[cache.get_wei(chain_id=1) for _ in range(200)]
            )
        finally:
            heartbeat.stop()
            await heartbeat_task

        assert results == [TEST_GAS_PRICE] * 200
        assert state["fetches"] == 1
        assert cache._refreshing == {}
        assert heartbeat.ticks > 20

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_async_gas_price_waiters_recover_after_timeout_without_deadlock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        cache = GasPriceCache()
        chain = SimpleNamespace(chain_id=8453, rpc_url="https://rpc.test")
        state = {"fetches": 0}

        async def _flaky_fetch(_chain: object) -> int:
            state["fetches"] += 1
            await asyncio.sleep(0.02)
            if state["fetches"] == 1:
                raise TimeoutError("gas rpc timed out")
            return TEST_GAS_PRICE + 1

        monkeypatch.setattr("wallet_service.evm.gas_price.get_async_redis", AsyncMock(return_value=None))
        monkeypatch.setattr(cache, "_resolve_chain", lambda *_args, **_kwargs: chain)
        monkeypatch.setattr(cache, "_fetch_async", _flaky_fetch)

        first_wave = await asyncio.gather(
            *[cache.get_wei(chain_id=8453) for _ in range(80)],
            return_exceptions=True,
        )

        failures = [item for item in first_wave if isinstance(item, TimeoutError)]
        successes = [item for item in first_wave if isinstance(item, int)]

        assert failures
        assert successes
        assert set(successes) == {TEST_GAS_PRICE + 1}
        assert cache._refreshing == {}

        second_wave = await asyncio.gather(
            *[cache.get_wei(chain_id=8453) for _ in range(80)]
        )

        assert second_wave == [TEST_GAS_PRICE + 1] * 80
        assert state["fetches"] == 2
        assert cache._refreshing == {}

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_sync_to_async_bridge_handles_high_frequency_nested_calls_without_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        for _ in range(4):
            with pytest.raises(RuntimeError, match="running event loop"):
                get_evm_account("sub-org")

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)
