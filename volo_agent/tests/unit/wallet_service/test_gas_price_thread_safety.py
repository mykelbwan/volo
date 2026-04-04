from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from wallet_service.evm.gas_price import GasPriceCache

TEST_GAS_PRICE = 987_654_321


async def _sleep_ticks(ticks: int) -> None:
    for _ in range(ticks):
        await asyncio.sleep(0)


@dataclass
class _DummyChain:
    chain_id: int = 1
    rpc_url: str = "https://rpc.test"


def _configure_cache(monkeypatch: pytest.MonkeyPatch) -> tuple[GasPriceCache, dict[str, int]]:
    cache = GasPriceCache()
    chain = _DummyChain()
    state = {"fetches": 0}
    fetch_lock = threading.Lock()

    async def _fake_fetch(_chain: _DummyChain) -> int:
        assert _chain.chain_id == chain.chain_id
        with fetch_lock:
            state["fetches"] += 1
        await _sleep_ticks(2)
        return TEST_GAS_PRICE

    monkeypatch.setattr(
        "wallet_service.evm.gas_price.get_async_redis",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(cache, "_resolve_chain", lambda *_args, **_kwargs: chain)
    monkeypatch.setattr(cache, "_fetch_async", _fake_fetch)
    return cache, state


def test_get_wei_async_and_sync_share_cache_across_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catch the old cross-event-loop lock bug by mixing sync bridge calls with
    # async calls running in a separate event loop and many worker threads.
    cache, state = _configure_cache(monkeypatch)
    errors: list[BaseException] = []
    async_results: list[int] = []

    def _async_worker() -> None:
        async def _runner() -> None:
            for _ in range(200):
                async_results.append(await cache.get_wei(chain_id=1))
                assert cache.get_wei_sync(chain_id=1) == TEST_GAS_PRICE

        try:
            asyncio.run(asyncio.wait_for(_runner(), timeout=5.0))
        except BaseException as exc:  # pragma: no cover - failure path only
            errors.append(exc)

    async_thread = threading.Thread(target=_async_worker, daemon=True)
    async_thread.start()

    def _sync_worker() -> list[int]:
        return [cache.get_wei_sync(chain_id=1) for _ in range(150)]

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_sync_worker) for _ in range(8)]
        sync_results = []
        for future in futures:
            sync_results.extend(future.result(timeout=5.0))

    async_thread.join(timeout=5.0)

    assert not async_thread.is_alive()
    assert not errors
    assert set(sync_results) == {TEST_GAS_PRICE}
    assert set(async_results) == {TEST_GAS_PRICE}
    assert state["fetches"] >= 1


def test_gas_price_cache_invalidate_under_parallel_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catch races between invalidation and active readers that previously could
    # wedge shared state or surface cross-thread runtime errors.
    cache, state = _configure_cache(monkeypatch)
    failures: list[BaseException] = []

    assert cache.get_wei_sync(chain_id=1) == TEST_GAS_PRICE
    cache.invalidate(chain_id=1)

    def _sync_reader() -> None:
        try:
            for _ in range(250):
                assert cache.get_wei_sync(chain_id=1) == TEST_GAS_PRICE
        except BaseException as exc:  # pragma: no cover - failure path only
            failures.append(exc)

    def _invalidator() -> None:
        try:
            for _ in range(120):
                cache.invalidate(chain_id=1)
        except BaseException as exc:  # pragma: no cover - failure path only
            failures.append(exc)

    def _async_reader() -> None:
        async def _runner() -> None:
            for _ in range(250):
                assert await cache.get_wei(chain_id=1) == TEST_GAS_PRICE

        try:
            asyncio.run(asyncio.wait_for(_runner(), timeout=5.0))
        except BaseException as exc:  # pragma: no cover - failure path only
            failures.append(exc)

    async_thread = threading.Thread(target=_async_reader, daemon=True)
    invalidator_thread = threading.Thread(target=_invalidator, daemon=True)
    async_thread.start()
    invalidator_thread.start()

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_sync_reader) for _ in range(6)]
        for future in futures:
            future.result(timeout=5.0)

    async_thread.join(timeout=5.0)
    invalidator_thread.join(timeout=5.0)

    assert not async_thread.is_alive()
    assert not invalidator_thread.is_alive()
    assert not failures
    assert state["fetches"] > 1
