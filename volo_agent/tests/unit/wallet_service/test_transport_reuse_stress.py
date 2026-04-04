from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

import httpx
import pytest

from core.utils import evm_async, http
from wallet_service.solana import rpc_client

from tests.unit.wallet_service._stress_helpers import FakeHTTPResponse, sleep_ticks

ASYNC_TEST_TIMEOUT = 10.0


class _FakeAsyncClient:
    def __init__(self, *, delay_ticks: int = 0, exc: Exception | None = None) -> None:
        self.delay_ticks = delay_ticks
        self.exc = exc
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.is_closed = False

    async def request(self, method: str, url: str, **kwargs: object) -> FakeHTTPResponse:
        self.calls.append((method, url, dict(kwargs)))
        await sleep_ticks(self.delay_ticks)
        if self.exc is not None:
            raise self.exc
        return FakeHTTPResponse(status_code=200, payload={"ok": True}, text="ok")

    async def aclose(self) -> None:
        self.is_closed = True


class _FakeAiohttpSession:
    created = 0

    def __init__(self, *args: object, **kwargs: object) -> None:
        type(self).created += 1
        self.args = args
        self.kwargs = kwargs
        self.closed = False
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class _FakeClientTimeout:
    def __init__(self, *, total: int) -> None:
        self.total = total


class _FakeTCPConnector:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


@pytest.fixture(autouse=True)
def _clear_shared_transports() -> None:
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
async def test_shared_async_http_client_reused_under_500_parallel_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        created: list[_FakeAsyncClient] = []

        def _build() -> _FakeAsyncClient:
            client = _FakeAsyncClient(delay_ticks=2)
            created.append(client)
            return client

        monkeypatch.setattr(http, "_build_async_client", _build)
        monkeypatch.setattr(http, "record_external_call", lambda **_kwargs: None)

        responses = await asyncio.gather(
            *[
                http.async_request_json(
                    "POST",
                    "https://rpc.test",
                    service="rpc",
                    json={"i": index},
                )
                for index in range(500)
            ]
        )

        assert len(created) == 1
        assert len(http._ASYNC_CLIENTS) == 1
        assert created[0].is_closed is False
        assert len(created[0].calls) == 500
        assert all(response.status_code == 200 for response in responses)

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_async_http_client_replaced_once_after_closed_session_detected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        created: list[_FakeAsyncClient] = []

        def _build() -> _FakeAsyncClient:
            client = _FakeAsyncClient()
            created.append(client)
            return client

        monkeypatch.setattr(http, "_build_async_client", _build)

        first = await http._get_shared_async_client()
        first.is_closed = True
        replacements = await asyncio.gather(
            *[http._get_shared_async_client() for _ in range(200)]
        )

        assert len(created) == 2
        assert all(client is created[1] for client in replacements)
        assert created[1] is not first
        assert len(http._ASYNC_CLIENTS) == 1

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_async_request_json_dns_failures_do_not_create_client_explosion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        created: list[_FakeAsyncClient] = []
        original_sleep = asyncio.sleep

        def _build() -> _FakeAsyncClient:
            exc = httpx.ConnectError(
                "dns failed",
                request=httpx.Request("POST", "https://rpc.test"),
            )
            client = _FakeAsyncClient(exc=exc)
            created.append(client)
            return client

        async def _zero_backoff(_seconds: float) -> None:
            await original_sleep(0)

        monkeypatch.setattr(http, "_build_async_client", _build)
        monkeypatch.setattr(http, "record_external_call", lambda **_kwargs: None)
        monkeypatch.setattr(http.asyncio, "sleep", _zero_backoff)

        results = await asyncio.gather(
            *[
                http.async_request_json(
                    "POST",
                    "https://rpc.test",
                    service="rpc",
                    json={"i": index},
                )
                for index in range(40)
            ],
            return_exceptions=True,
        )

        assert len(created) == 1
        assert len(http._ASYNC_CLIENTS) == 1
        assert all(isinstance(result, httpx.ConnectError) for result in results)
        assert len(created[0].calls) == 40 * 3

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_shared_aiohttp_session_reused_across_many_async_web3_builds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        attached_sessions: list[object] = []
        _FakeAiohttpSession.created = 0

        fake_aiohttp = SimpleNamespace(
            ClientTimeout=_FakeClientTimeout,
            TCPConnector=_FakeTCPConnector,
            ClientSession=_FakeAiohttpSession,
        )
        monkeypatch.setitem(sys.modules, "aiohttp", fake_aiohttp)

        def _build_async_web3(_rpc_url: str) -> object:
            provider = SimpleNamespace()

            async def _cache_async_session(session: object) -> None:
                await sleep_ticks(1)
                attached_sessions.append(session)

            provider.cache_async_session = _cache_async_session
            return SimpleNamespace(provider=provider)

        monkeypatch.setattr(evm_async, "_build_async_web3", _build_async_web3)

        web3s = await asyncio.gather(
            *[evm_async.get_shared_async_web3("https://rpc.test") for _ in range(200)]
        )

        assert len(web3s) == 200
        assert _FakeAiohttpSession.created == 1
        assert len({id(session) for session in attached_sessions}) == 1
        assert len(evm_async._SHARED_AIOHTTP_SESSIONS) == 1

        session = next(iter(evm_async._SHARED_AIOHTTP_SESSIONS.values()))
        await evm_async.close_shared_async_web3_sessions()

        assert session.closed is True
        assert session.close_calls == 1

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_close_shared_solana_clients_survives_partial_close_failures() -> None:
    async def exercise() -> None:
        closed: list[str] = []

        class _Client:
            def __init__(self, name: str, *, fail: bool = False) -> None:
                self.name = name
                self.fail = fail

            async def close(self) -> None:
                await sleep_ticks(2)
                closed.append(self.name)
                if self.fail:
                    raise RuntimeError(f"{self.name} close failed")

        with rpc_client._SHARED_SOLANA_CLIENTS_LOCK:
            rpc_client._SHARED_SOLANA_CLIENTS[(1, "https://rpc-a.test")] = _Client("a")
            rpc_client._SHARED_SOLANA_CLIENTS[(1, "https://rpc-b.test")] = _Client("b", fail=True)
            rpc_client._SHARED_SOLANA_CLIENTS[(1, "https://rpc-c.test")] = _Client("c")

        await rpc_client.close_shared_solana_clients()

        assert sorted(closed) == ["a", "b", "c"]
        assert rpc_client._SHARED_SOLANA_CLIENTS == {}

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)
