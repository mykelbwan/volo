from __future__ import annotations

import asyncio
import time

import pytest

from core.token_security.token_db import AsyncTokenRegistryDB

_CHAIN_ID = 8453
_DELAY_SECONDS = 0.05
_TASK_COUNT = 20


def _build_registry_doc(symbol: str, chain_id: int = _CHAIN_ID) -> dict[str, object]:
    return {
        "symbol": symbol,
        "name": f"{symbol} Token",
        "chain_name": "base",
        "chain_id": chain_id,
        "address": f"0x{symbol.lower():0<40}"[:42],
        "decimals": 6,
        "aliases": [symbol.lower()],
        "is_active": True,
    }


class _BaseAsyncCollection:
    def __init__(self, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds
        self.create_indexes_calls = 0
        self.find_one_calls = 0

    async def create_indexes(self, _models) -> None:
        self.create_indexes_calls += 1

    def _doc_for_query(self, query: dict[str, object]) -> dict[str, object]:
        reg_key = query.get("_reg_key")
        if isinstance(reg_key, str):
            symbol = reg_key.split(":", 1)[0]
            chain_id = int(reg_key.split(":", 1)[1])
            return _build_registry_doc(symbol, chain_id)

        alias = query.get("aliases")
        symbol = str(alias or "UNKNOWN").upper()
        chain_id = int(query.get("chain_id") or _CHAIN_ID)
        return _build_registry_doc(symbol, chain_id)


class _BlockingAsyncCollection(_BaseAsyncCollection):
    async def find_one(self, query: dict[str, object]) -> dict[str, object]:
        self.find_one_calls += 1
        time.sleep(self.delay_seconds)
        return self._doc_for_query(query)


class _OffloadedAsyncCollection(_BaseAsyncCollection):
    async def find_one(self, query: dict[str, object]) -> dict[str, object]:
        self.find_one_calls += 1
        await asyncio.sleep(self.delay_seconds)
        return self._doc_for_query(query)


async def _measure_runtime(awaitables: list[asyncio.Future]) -> tuple[float, list[object]]:
    started = time.perf_counter()
    results = await asyncio.gather(*awaitables)
    return time.perf_counter() - started, results


@pytest.mark.asyncio
async def test_async_token_registry_get_exposes_blocking_collection_serialization():
    registry = AsyncTokenRegistryDB(collection=_BlockingAsyncCollection(_DELAY_SECONDS))

    elapsed, results = await asyncio.wait_for(
        _measure_runtime([registry.get(f"TOKEN{i}", _CHAIN_ID) for i in range(_TASK_COUNT)]),
        timeout=5.0,
    )

    assert all(result is not None for result in results)
    assert elapsed >= _TASK_COUNT * _DELAY_SECONDS * 0.75


@pytest.mark.asyncio
async def test_async_token_registry_get_stays_concurrent_when_db_io_is_offloaded():
    registry = AsyncTokenRegistryDB(collection=_OffloadedAsyncCollection(_DELAY_SECONDS))

    elapsed, results = await asyncio.wait_for(
        _measure_runtime([registry.get(f"TOKEN{i}", _CHAIN_ID) for i in range(_TASK_COUNT)]),
        timeout=5.0,
    )

    assert all(result is not None for result in results)
    assert elapsed < _TASK_COUNT * _DELAY_SECONDS * 0.35
