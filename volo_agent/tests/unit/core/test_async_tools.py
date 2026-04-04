from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from core.utils import async_tools


def test_run_blocking_returns_value():
    async def _run():
        with ThreadPoolExecutor(max_workers=1) as executor:
            return await async_tools.run_blocking(
                lambda: 123,
                timeout=1.0,
                executor=executor,
            )

    assert asyncio.run(_run()) == 123


def test_run_blocking_timeout_race_returns_completed_result(monkeypatch):
    async def _fake_wait_for(task, timeout):  # noqa: ARG001
        await task
        raise asyncio.TimeoutError

    monkeypatch.setattr(async_tools.asyncio, "wait_for", _fake_wait_for)

    async def _run():
        with ThreadPoolExecutor(max_workers=1) as executor:
            return await async_tools.run_blocking(
                lambda: "ok",
                timeout=1.0,
                executor=executor,
            )

    assert asyncio.run(_run()) == "ok"


def test_run_blocking_no_timeout_stalls_across_event_loops():
    async def _run_once(index: int) -> int:
        return await async_tools.run_blocking(lambda: index, timeout=0.2)

    for expected in range(5):
        assert asyncio.run(_run_once(expected)) == expected


def test_run_blocking_rejects_async_callable():
    async def _async_fn():
        return "ok"

    async def _run():
        with ThreadPoolExecutor(max_workers=1) as executor:
            return await async_tools.run_blocking(
                _async_fn,
                timeout=1.0,
                executor=executor,
            )

    with pytest.raises(TypeError, match="cannot run async callables"):
        asyncio.run(_run())


def test_run_blocking_rejects_awaitable_result():
    async def _inner():
        return "ok"

    def _returns_coroutine():
        return _inner()

    async def _run():
        with ThreadPoolExecutor(max_workers=1) as executor:
            return await async_tools.run_blocking(
                _returns_coroutine,
                timeout=1.0,
                executor=executor,
            )

    with pytest.raises(TypeError, match="cannot run async callables"):
        asyncio.run(_run())
