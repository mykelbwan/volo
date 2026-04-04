from __future__ import annotations

import asyncio
import time

import pytest

from wallet_service.evm import nonce_manager as nonce_manager_module
from wallet_service.evm.nonce_manager import AsyncUpstashNonceManager

from tests.unit.wallet_service._stress_helpers import sleep_ticks

ASYNC_TEST_TIMEOUT = 10.0


class _FakeEth:
    def __init__(self, pending_values: list[int], *, delay_ticks: int = 0) -> None:
        self._pending_values = pending_values
        self._delay_ticks = delay_ticks
        self._call_index = 0
        self.call_count = 0
        self.inflight = 0
        self.max_inflight = 0

    async def get_transaction_count(self, _sender: str, _block: str) -> int:
        index = min(self._call_index, len(self._pending_values) - 1)
        self._call_index += 1
        self.call_count += 1
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            await sleep_ticks(self._delay_ticks)
            return self._pending_values[index]
        finally:
            self.inflight -= 1


class _FakeWeb3:
    def __init__(self, eth: _FakeEth) -> None:
        self.eth = eth

    @staticmethod
    def to_checksum_address(sender: str) -> str:
        return sender.upper()


class _FakeUpstash:
    def __init__(self, *, delay_ticks: int = 0, fail: bool = False) -> None:
        self._delay_ticks = delay_ticks
        self._fail = fail
        self._values: dict[str, int] = {}
        self._syncs: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self.call_count = 0

    async def eval(self, script: str, *, keys: list[str], args: list[object]) -> int:
        self.call_count += 1
        await sleep_ticks(self._delay_ticks)
        if self._fail:
            raise TimeoutError("upstash timed out")

        key = keys[0]
        sync_key = keys[1]
        async with self._lock:
            current = self._values.get(key)
            if "failed_nonce" in script:
                failed_nonce = int(args[0])
                rpc_pending = int(args[1])
                now = int(args[2])
                if current == failed_nonce + 1 and failed_nonce >= rpc_pending:
                    self._values[key] = failed_nonce
                    self._syncs[sync_key] = now
                    return failed_nonce
                floor_nonce = max(current or 0, rpc_pending)
                self._values[key] = floor_nonce
                self._syncs[sync_key] = now
                return floor_nonce
            if "return -1" in script and "next_nonce + 1" in script:
                now = int(args[0])
                reconcile_after = int(args[3])
                last_sync = self._syncs.get(sync_key)
                if current is None or last_sync is None or (now - last_sync) >= reconcile_after:
                    return -1
                self._values[key] = current + 1
                self._syncs[sync_key] = now
                return current
            if "return -1" in script:
                now = int(args[0])
                reconcile_after = int(args[3])
                last_sync = self._syncs.get(sync_key)
                if current is None or last_sync is None or (now - last_sync) >= reconcile_after:
                    return -1
                self._syncs[sync_key] = now
                return current
            if 'redis.call("SET", key, pending, "EX", ttl)' in script:
                pending = int(args[0])
                now = int(args[1])
                self._values[key] = pending
                self._syncs[sync_key] = now
                return pending
            if "next_nonce + 1" in script:
                rpc_pending = int(args[0])
                now = int(args[1])
                next_nonce = rpc_pending if current is None else max(current, rpc_pending)
                self._values[key] = next_nonce + 1
                self._syncs[sync_key] = now
                return next_nonce
            rpc_pending = int(args[0])
            now = int(args[1])
            next_nonce = rpc_pending if current is None else max(current, rpc_pending)
            self._values[key] = next_nonce
            self._syncs[sync_key] = now
            return next_nonce


@pytest.mark.asyncio
async def test_upstash_nonce_manager_raises_when_redis_fails() -> None:
    async def exercise() -> None:
        manager = AsyncUpstashNonceManager(_FakeUpstash(delay_ticks=1, fail=True))
        eth = _FakeEth([301], delay_ticks=2)
        w3 = _FakeWeb3(eth)
        with pytest.raises(RuntimeError, match="Redis nonce allocation is unavailable"):
            await manager.allocate_safe("0xdef", 10, w3)

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_upstash_nonce_manager_is_unique_across_ten_parallel_instances() -> None:
    async def exercise() -> None:
        upstash = _FakeUpstash(delay_ticks=1)
        managers = [AsyncUpstashNonceManager(upstash) for _ in range(10)]
        eth = _FakeEth([501], delay_ticks=2)
        w3 = _FakeWeb3(eth)
        start = asyncio.Event()

        async def worker(manager: AsyncUpstashNonceManager) -> int:
            await start.wait()
            return await manager.allocate_safe("0xmulti", 137, w3)

        tasks = [asyncio.create_task(worker(manager)) for manager in managers]
        start.set()
        results = await asyncio.gather(*tasks)

        assert len(results) == len(set(results))
        assert sorted(results) == list(range(501, 511))
        assert upstash.call_count >= 10

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)
