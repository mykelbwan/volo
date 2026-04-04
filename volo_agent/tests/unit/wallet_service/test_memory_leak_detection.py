from __future__ import annotations

import asyncio
import gc
import sys
import time
import tracemalloc
from types import SimpleNamespace

import pytest

from core.utils import evm_async, http
from wallet_service.solana import rpc_client

from tests.unit.wallet_service._stress_helpers import sleep_ticks

ASYNC_TEST_TIMEOUT = 15.0


class _FakeAsyncClient:
    def __init__(self) -> None:
        self.is_closed = False

    async def aclose(self) -> None:
        self.is_closed = True


class _FakeAiohttpSession:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _FakeClientTimeout:
    def __init__(self, *, total: int) -> None:
        self.total = total


class _FakeTCPConnector:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


@pytest.fixture(autouse=True)
def _clear_shared_state() -> None:
    with http._ASYNC_CLIENTS_LOCK:
        http._ASYNC_CLIENTS.clear()
    with evm_async._SHARED_AIOHTTP_SESSIONS_LOCK:
        evm_async._SHARED_AIOHTTP_SESSIONS.clear()
    with rpc_client._SHARED_SOLANA_CLIENTS_LOCK:
        rpc_client._SHARED_SOLANA_CLIENTS.clear()
    yield
    with http._ASYNC_CLIENTS_LOCK:
        http._ASYNC_CLIENTS.clear()
    with evm_async._SHARED_AIOHTTP_SESSIONS_LOCK:
        evm_async._SHARED_AIOHTTP_SESSIONS.clear()
    with rpc_client._SHARED_SOLANA_CLIENTS_LOCK:
        rpc_client._SHARED_SOLANA_CLIENTS.clear()


@pytest.mark.asyncio
async def test_shared_transport_pools_do_not_grow_across_recreate_close_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        fake_aiohttp = SimpleNamespace(
            ClientTimeout=_FakeClientTimeout,
            TCPConnector=_FakeTCPConnector,
            ClientSession=_FakeAiohttpSession,
        )

        monkeypatch.setitem(sys.modules, "aiohttp", fake_aiohttp)
        monkeypatch.setattr(http, "_build_async_client", _FakeAsyncClient)

        tracemalloc.start()
        baseline, _ = tracemalloc.get_traced_memory()
        try:
            for _ in range(40):
                client = await http._get_shared_async_client()
                session = await evm_async._get_shared_aiohttp_session()

                assert client is not None
                assert session is not None
                assert len(http._ASYNC_CLIENTS) <= 1
                assert len(evm_async._SHARED_AIOHTTP_SESSIONS) <= 1

                await http.close_shared_async_http_clients()
                await evm_async.close_shared_async_web3_sessions()
                gc.collect()

                assert http._ASYNC_CLIENTS == {}
                assert evm_async._SHARED_AIOHTTP_SESSIONS == {}

            current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        assert current - baseline < 800_000
        assert peak - baseline < 1_600_000

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)
