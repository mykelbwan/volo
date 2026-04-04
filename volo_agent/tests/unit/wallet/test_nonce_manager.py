import asyncio
from unittest.mock import MagicMock

import pytest

from wallet_service.evm.nonce_manager import (
    AsyncUpstashNonceManager,
)


class _DummyUpstash:
    def __init__(self, *, fail: bool = False) -> None:
        self._fail = fail
        self.values: dict[str, int] = {}
        self.syncs: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def eval(self, script, *, keys, args):
        if self._fail:
            raise RuntimeError("upstash unavailable")
        key = keys[0]
        sync_key = keys[1]
        async with self._lock:
            current = self.values.get(key)
            if "failed_nonce" in script:
                failed_nonce = int(args[0])
                rpc_pending = int(args[1])
                now = int(args[2])
                if current == failed_nonce + 1 and failed_nonce >= rpc_pending:
                    self.values[key] = failed_nonce
                    self.syncs[sync_key] = now
                    return failed_nonce
                floor_nonce = max(current or 0, rpc_pending)
                self.values[key] = floor_nonce
                self.syncs[sync_key] = now
                return floor_nonce
            if "return -1" in script and "next_nonce + 1" in script:
                now = int(args[0])
                reconcile_after = int(args[3])
                last_sync = self.syncs.get(sync_key)
                if current is None or last_sync is None or (now - last_sync) >= reconcile_after:
                    return -1
                self.values[key] = current + 1
                self.syncs[sync_key] = now
                return current
            if "return -1" in script:
                now = int(args[0])
                reconcile_after = int(args[3])
                last_sync = self.syncs.get(sync_key)
                if current is None or last_sync is None or (now - last_sync) >= reconcile_after:
                    return -1
                self.syncs[sync_key] = now
                return current
            if 'redis.call("SET", key, pending, "EX", ttl)' in script:
                pending = int(args[0])
                now = int(args[1])
                self.values[key] = pending
                self.syncs[sync_key] = now
                return pending
            if "next_nonce + 1" in script:
                pending = int(args[0])
                now = int(args[1])
                next_nonce = pending if current is None else max(current, pending)
                self.values[key] = next_nonce + 1
                self.syncs[sync_key] = now
                return next_nonce
            pending = int(args[0])
            now = int(args[1])
            next_nonce = pending if current is None else max(current, pending)
            self.values[key] = next_nonce
            self.syncs[sync_key] = now
            return next_nonce


def test_upstash_nonce_manager_pending_returns_chain_value():
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda v: v
    w3.eth.get_transaction_count.return_value = 7
    nm = AsyncUpstashNonceManager(_DummyUpstash(), prefix="test")
    pending = asyncio.run(nm.pending("0xabc", 1, w3))
    assert pending == 7


def test_upstash_nonce_manager_allocate_safe_prefers_redis_cursor_until_reconciliation():
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda v: v
    w3.eth.get_transaction_count.return_value = 4

    nm = AsyncUpstashNonceManager(_DummyUpstash(), prefix="test")
    nm._now_seconds = lambda: 100
    first = asyncio.run(nm.allocate_safe("0xabc", 1, w3))
    assert first == 4

    w3.eth.get_transaction_count.return_value = 9
    nm._now_seconds = lambda: 101
    second = asyncio.run(nm.allocate_safe("0xabc", 1, w3))
    assert second == 5


def test_upstash_nonce_manager_allocate_safe_reconciles_when_sync_is_stale():
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda v: v
    w3.eth.get_transaction_count.return_value = 4

    nm = AsyncUpstashNonceManager(_DummyUpstash(), prefix="test")
    nm._now_seconds = lambda: 100
    nm._reconcile_interval_seconds = 1

    assert asyncio.run(nm.allocate_safe("0xabc", 1, w3)) == 4

    w3.eth.get_transaction_count.return_value = 9
    nm._now_seconds = lambda: 102
    assert asyncio.run(nm.allocate_safe("0xabc", 1, w3)) == 9


def test_upstash_nonce_manager_allocate_is_unique_under_concurrency():
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda v: v
    w3.eth.get_transaction_count.return_value = 11

    nm = AsyncUpstashNonceManager(_DummyUpstash(), prefix="test")

    async def _allocate_many():
        return await asyncio.gather(
            *[nm.allocate("0xabc", 1, w3) for _ in range(8)]
        )

    allocated = asyncio.run(_allocate_many())
    assert sorted(allocated) == list(range(11, 19))


def test_upstash_nonce_manager_raises_when_redis_fails():
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda v: v
    w3.eth.get_transaction_count.return_value = 21
    nm = AsyncUpstashNonceManager(_DummyUpstash(fail=True), prefix="test")

    with pytest.raises(RuntimeError, match="Redis nonce allocation is unavailable"):
        asyncio.run(nm.allocate("0xabc", 1, w3))


def test_upstash_nonce_manager_rollback_releases_latest_failed_reservation():
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda v: v
    w3.eth.get_transaction_count.return_value = 30

    nm = AsyncUpstashNonceManager(_DummyUpstash(), prefix="test")
    reserved = asyncio.run(nm.allocate_safe("0xabc", 1, w3))
    assert reserved == 30

    rolled_back = asyncio.run(nm.rollback("0xabc", 1, reserved, w3))
    assert rolled_back == 30
    assert asyncio.run(nm.allocate_safe("0xabc", 1, w3)) == 30
