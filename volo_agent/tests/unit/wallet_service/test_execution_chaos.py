from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from core.database.mongodb import MongoDB
from core.database.mongodb_async import AsyncMongoDB
from core.idempotency.store import IdempotencyStore
from wallet_service.common import wallet_lock as wallet_lock_module
from wallet_service.common.transfer_idempotency import (
    claim_transfer_idempotency,
    load_transfer_idempotency_claim,
    mark_transfer_failed,
    mark_transfer_inflight,
    mark_transfer_success,
)
from wallet_service.common.wallet_lock import WalletLock
from wallet_service.evm.nonce_manager import AsyncUpstashNonceManager


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


async def _sleep_ticks(ticks: int) -> None:
    for _ in range(max(0, int(ticks))):
        await asyncio.sleep(0)


class _StaticEth:
    def __init__(self, pending_nonce: int) -> None:
        self._pending_nonce = int(pending_nonce)

    async def get_transaction_count(self, _sender: str, _block: str) -> int:
        await asyncio.sleep(0)
        return self._pending_nonce


class _StaticWeb3:
    def __init__(self, pending_nonce: int) -> None:
        self.eth = _StaticEth(pending_nonce)

    @staticmethod
    def to_checksum_address(address: str) -> str:
        return str(address).lower()


@dataclass
class _UpdateResult:
    upserted_id: Any = None
    modified_count: int = 0


class _InMemoryCollection:
    def __init__(self) -> None:
        self._docs: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    def create_index(self, *_args: Any, **_kwargs: Any) -> str:
        return "ok"

    def _matches(self, doc: dict[str, Any], query: dict[str, Any]) -> bool:
        for key, expected in query.items():
            actual = doc.get(key)
            if expected is None:
                if actual is not None:
                    return False
                continue
            if actual != expected:
                return False
        return True

    def _apply_update(
        self,
        existing: dict[str, Any] | None,
        update: dict[str, Any],
        *,
        upsert: bool,
    ) -> tuple[dict[str, Any] | None, _UpdateResult]:
        doc = dict(existing or {})
        created = False
        modified = False
        if existing is None and upsert:
            created = True
            if "$setOnInsert" in update:
                doc.update(dict(update["$setOnInsert"]))
        elif existing is None:
            return None, _UpdateResult()

        if "$set" in update:
            for key, value in dict(update["$set"]).items():
                if doc.get(key) != value:
                    modified = True
                doc[key] = value
        if created:
            modified = True
        return doc, _UpdateResult(
            upserted_id=(doc.get("key") if created else None),
            modified_count=int(modified and not created),
        )

    def update_one(
        self,
        query: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
    ) -> _UpdateResult:
        for key, doc in list(self._docs.items()):
            if self._matches(doc, query):
                updated, result = self._apply_update(doc, update, upsert=upsert)
                if updated is not None:
                    self._docs[key] = updated
                return result

        updated, result = self._apply_update(None, update, upsert=upsert)
        if updated is not None:
            self._docs[str(updated["key"])] = updated
        return result

    async def update_one_async(
        self,
        query: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
    ) -> _UpdateResult:
        async with self._lock:
            return self.update_one(query, update, upsert=upsert)

    def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        for doc in self._docs.values():
            if self._matches(doc, query):
                return dict(doc)
        return None

    async def find_one_async(self, query: dict[str, Any]) -> dict[str, Any] | None:
        async with self._lock:
            return self.find_one(query)

    def force_write(self, key: str, doc: dict[str, Any]) -> None:
        self._docs[key] = dict(doc)


class _AsyncCollectionAdapter:
    def __init__(self, collection: _InMemoryCollection) -> None:
        self._collection = collection

    async def create_index(self, *args: Any, **kwargs: Any) -> str:
        return self._collection.create_index(*args, **kwargs)

    async def update_one(self, *args: Any, **kwargs: Any) -> _UpdateResult:
        return await self._collection.update_one_async(*args, **kwargs)

    async def find_one(self, *args: Any, **kwargs: Any) -> dict[str, Any] | None:
        return await self._collection.find_one_async(*args, **kwargs)


class _CollectionRegistry:
    def __init__(self) -> None:
        self._collections: dict[str, _InMemoryCollection] = {}

    def get_sync(self, name: str) -> _InMemoryCollection:
        return self._collections.setdefault(name, _InMemoryCollection())

    def get_async(self, name: str) -> _AsyncCollectionAdapter:
        return _AsyncCollectionAdapter(self.get_sync(name))


@dataclass
class _RedisFault:
    when: str
    delay_ticks: int = 0
    message: str = "redis timeout"


class _ChaosRedis:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._expires_at: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._faults: dict[str, deque[_RedisFault]] = defaultdict(deque)
        self.history: list[dict[str, Any]] = []

    def queue_fault(self, op: str, *, when: str, count: int = 1, delay_ticks: int = 0) -> None:
        for _ in range(count):
            self._faults[op].append(
                _RedisFault(
                    when=when,
                    delay_ticks=delay_ticks,
                    message=f"{op}:{when}",
                )
            )

    def ttl_seconds(self, key: str) -> float | None:
        self._purge_expired()
        expires_at = self._expires_at.get(key)
        if expires_at is None:
            return None
        return max(expires_at - time.monotonic(), 0.0)

    async def get(self, key: str) -> str | None:
        async with self._lock:
            self._purge_expired()
            return self._values.get(key)

    async def set(
        self,
        key: str,
        value: str,
        *,
        nx: bool = False,
        px: int | None = None,
    ) -> bool:
        op = "wallet_acquire"
        fault = self._faults[op].popleft() if self._faults[op] else None
        if fault is not None and fault.when == "before":
            await _sleep_ticks(fault.delay_ticks)
            raise TimeoutError(fault.message)

        async with self._lock:
            self._purge_expired()
            if nx and key in self._values:
                return False
            self._values[key] = str(value)
            if px is not None:
                self._expires_at[key] = time.monotonic() + (px / 1000)
            self.history.append(
                {"op": op, "key": key, "value": str(value), "ttl_ms": px}
            )

        if fault is not None and fault.when == "after":
            await _sleep_ticks(fault.delay_ticks)
            raise TimeoutError(fault.message)
        return True

    async def eval(self, script: str, *, keys: list[str], args: list[object]) -> int:
        op = self._classify_script(script)
        fault = self._faults[op].popleft() if self._faults[op] else None
        if fault is not None and fault.when == "before":
            await _sleep_ticks(fault.delay_ticks)
            raise TimeoutError(fault.message)

        async with self._lock:
            self._purge_expired()
            if fault is not None and fault.delay_ticks:
                await _sleep_ticks(fault.delay_ticks)
            result = self._execute(script=script, op=op, keys=keys, args=args)

        if fault is not None and fault.when == "after":
            raise TimeoutError(fault.message)
        return int(result)

    def _classify_script(self, script: str) -> str:
        if 'redis.call("DEL", key)' in script:
            return "wallet_release"
        if 'redis.call("PEXPIRE", key, ttl_ms)' in script:
            return "wallet_refresh"
        if "failed_nonce" in script:
            return "nonce_rollback"
        if 'redis.call("SET", key, pending, "EX", ttl)' in script:
            return "nonce_reset"
        if "next_nonce + 1" in script:
            return "nonce_allocate"
        return "nonce_peek"

    def _execute(
        self,
        *,
        script: str,
        op: str,
        keys: list[str],
        args: list[object],
    ) -> int:
        if op.startswith("wallet_"):
            key = str(keys[0])
            owner = str(args[0])
            if op == "wallet_release":
                if self._values.get(key) == owner:
                    self._values.pop(key, None)
                    self._expires_at.pop(key, None)
                    self.history.append({"op": op, "key": key, "owner": owner})
                    return 1
                return 0
            ttl_ms = int(args[1])
            if self._values.get(key) == owner:
                self._expires_at[key] = time.monotonic() + (ttl_ms / 1000)
                self.history.append(
                    {"op": op, "key": key, "owner": owner, "ttl_ms": ttl_ms}
                )
                return 1
            return 0

        key = str(keys[0])
        sync_key = str(keys[1])
        op_key = str(keys[2])
        prior = self._as_int(self._values.get(op_key))
        if prior is not None:
            op_ttl_seconds = int(args[-1])
            self._set_with_ttl(op_key, prior, ex_seconds=op_ttl_seconds)
            self.history.append({"op": op, "key": key, "op_key": op_key, "replay": True})
            return prior

        current = self._as_int(self._values.get(key))
        if op == "nonce_allocate":
            if "last_sync" in script:
                now = int(args[0])
                ttl_seconds = int(args[1])
                op_ttl_seconds = int(args[2])
                reconcile_after = int(args[3])
                last_sync = self._as_int(self._values.get(sync_key)) or 0
                if current is None or last_sync == 0 or ((now - last_sync) >= reconcile_after):
                    self.history.append(
                        {
                            "op": op,
                            "key": key,
                            "op_key": op_key,
                            "sync_key": sync_key,
                            "result": -1,
                        }
                    )
                    return -1
                next_nonce = current
                self._set_with_ttl(key, next_nonce + 1, ex_seconds=ttl_seconds)
                self._set_with_ttl(sync_key, now, ex_seconds=ttl_seconds)
                self._set_with_ttl(op_key, next_nonce, ex_seconds=op_ttl_seconds)
                self.history.append(
                    {
                        "op": op,
                        "key": key,
                        "op_key": op_key,
                        "sync_key": sync_key,
                        "result": next_nonce,
                    }
                )
                return next_nonce

            rpc_pending = int(args[0])
            now = int(args[1])
            ttl_seconds = int(args[2])
            op_ttl_seconds = int(args[3])
            next_nonce = rpc_pending if current is None else max(current, rpc_pending)
            self._set_with_ttl(key, next_nonce + 1, ex_seconds=ttl_seconds)
            self._set_with_ttl(sync_key, now, ex_seconds=ttl_seconds)
            self._set_with_ttl(op_key, next_nonce, ex_seconds=op_ttl_seconds)
            self.history.append(
                {
                    "op": op,
                    "key": key,
                    "op_key": op_key,
                    "sync_key": sync_key,
                    "rpc_pending": rpc_pending,
                    "result": next_nonce,
                }
            )
            return next_nonce

        if op == "nonce_peek":
            if "last_sync" in script:
                now = int(args[0])
                ttl_seconds = int(args[1])
                op_ttl_seconds = int(args[2])
                reconcile_after = int(args[3])
                last_sync = self._as_int(self._values.get(sync_key)) or 0
                if current is None or last_sync == 0 or ((now - last_sync) >= reconcile_after):
                    self.history.append(
                        {
                            "op": op,
                            "key": key,
                            "op_key": op_key,
                            "sync_key": sync_key,
                            "result": -1,
                        }
                    )
                    return -1
                self._set_with_ttl(key, current, ex_seconds=ttl_seconds)
                self._set_with_ttl(sync_key, now, ex_seconds=ttl_seconds)
                self._set_with_ttl(op_key, current, ex_seconds=op_ttl_seconds)
                self.history.append(
                    {
                        "op": op,
                        "key": key,
                        "op_key": op_key,
                        "sync_key": sync_key,
                        "result": current,
                    }
                )
                return current

            rpc_pending = int(args[0])
            now = int(args[1])
            ttl_seconds = int(args[2])
            op_ttl_seconds = int(args[3])
            next_nonce = rpc_pending if current is None else max(current, rpc_pending)
            self._set_with_ttl(key, next_nonce, ex_seconds=ttl_seconds)
            self._set_with_ttl(sync_key, now, ex_seconds=ttl_seconds)
            self._set_with_ttl(op_key, next_nonce, ex_seconds=op_ttl_seconds)
            self.history.append(
                {
                    "op": op,
                    "key": key,
                    "op_key": op_key,
                    "sync_key": sync_key,
                    "result": next_nonce,
                }
            )
            return next_nonce

        if op == "nonce_reset":
            pending = int(args[0])
            now = int(args[1])
            ttl_seconds = int(args[2])
            op_ttl_seconds = int(args[3])
            self._set_with_ttl(key, pending, ex_seconds=ttl_seconds)
            self._set_with_ttl(sync_key, now, ex_seconds=ttl_seconds)
            self._set_with_ttl(op_key, pending, ex_seconds=op_ttl_seconds)
            self.history.append(
                {
                    "op": op,
                    "key": key,
                    "op_key": op_key,
                    "sync_key": sync_key,
                    "result": pending,
                }
            )
            return pending

        failed_nonce = int(args[0])
        rpc_pending = int(args[1])
        now = int(args[2])
        ttl_seconds = int(args[3])
        op_ttl_seconds = int(args[4])
        if current is None:
            result = rpc_pending
        elif current == failed_nonce + 1 and failed_nonce >= rpc_pending:
            result = failed_nonce
        else:
            result = max(current, rpc_pending)
        self._set_with_ttl(key, result, ex_seconds=ttl_seconds)
        self._set_with_ttl(sync_key, now, ex_seconds=ttl_seconds)
        self._set_with_ttl(op_key, result, ex_seconds=op_ttl_seconds)
        self.history.append(
            {
                "op": op,
                "key": key,
                "op_key": op_key,
                "sync_key": sync_key,
                "result": result,
            }
        )
        return result

    def _set_with_ttl(self, key: str, value: int, *, ex_seconds: int) -> None:
        self._values[key] = str(int(value))
        self._expires_at[key] = time.monotonic() + max(int(ex_seconds), 0)

    def _purge_expired(self) -> None:
        now = time.monotonic()
        expired_keys = [key for key, expires_at in self._expires_at.items() if expires_at <= now]
        for key in expired_keys:
            self._values.pop(key, None)
            self._expires_at.pop(key, None)

    @staticmethod
    def _as_int(value: object) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None


@pytest.fixture
def idempotency_registry(monkeypatch: pytest.MonkeyPatch) -> _CollectionRegistry:
    registry = _CollectionRegistry()
    monkeypatch.setattr(
        MongoDB,
        "get_collection",
        classmethod(lambda cls, name, db_name="auraagent": registry.get_sync(name)),
    )
    monkeypatch.setattr(
        AsyncMongoDB,
        "get_collection",
        classmethod(lambda cls, name, db_name="auraagent": registry.get_async(name)),
    )
    return registry


@pytest.mark.asyncio
async def test_nonce_manager_retries_after_post_commit_timeout_without_skipping_nonces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This attacks the Redis retry path where EVAL commits but the response times out.
    # Without an operation key, the internal retry burns a second nonce.
    monkeypatch.setenv("NONCE_MANAGER_TTL_SECONDS", "2")
    redis = _ChaosRedis()
    redis.queue_fault("nonce_allocate", when="after", count=18, delay_ticks=1)
    managers = [AsyncUpstashNonceManager(redis) for _ in range(18)]
    w3 = _StaticWeb3(700)
    start = asyncio.Event()

    async def worker(index: int) -> int:
        await start.wait()
        await asyncio.sleep(0)
        return await managers[index % len(managers)].allocate_safe("0xabc", 1, w3)

    tasks = [asyncio.create_task(worker(index)) for index in range(40)]
    start.set()
    results = await asyncio.gather(*tasks)

    assert len(results) == len(set(results)), f"duplicate nonce detected: {results}"
    assert sorted(results) == list(range(700, 740)), f"nonce sequence skipped: {results}"
    assert redis.ttl_seconds("nonce:0xabc:1") is not None


@pytest.mark.asyncio
async def test_nonce_manager_refreshes_ttl_under_delayed_redis_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This keeps the nonce key near expiry and verifies a later allocation refreshes TTL.
    monkeypatch.setenv("NONCE_MANAGER_TTL_SECONDS", "2")
    redis = _ChaosRedis()
    redis.queue_fault("nonce_peek", when="before", count=1, delay_ticks=1)
    manager = AsyncUpstashNonceManager(redis)
    w3 = _StaticWeb3(55)

    first = await manager.allocate_safe("0xdef", 8453, w3)
    assert first == 55
    await asyncio.sleep(1.1)
    ttl_before = redis.ttl_seconds("nonce:0xdef:8453")
    assert ttl_before is not None and ttl_before < 1.2

    second = await manager.peek("0xdef", 8453, w3)
    ttl_after = redis.ttl_seconds("nonce:0xdef:8453")

    assert second == 56
    assert ttl_after is not None and ttl_after > ttl_before, (
        ttl_before,
        ttl_after,
        redis.history,
    )


@pytest.mark.asyncio
async def test_idempotency_reclaims_failed_attempt_without_tx_hash(
    idempotency_registry: _CollectionRegistry,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # This exposes the dead-end failure mode where pre-broadcast errors made the
    # key permanently unusable until TTL expiry.
    store = IdempotencyStore(collection_name="idempotency_chaos_failed")
    caplog.set_level("INFO")

    claim = await claim_transfer_idempotency(
        operation="dex_swap",
        idempotency_key="swap-1",
        request_fields={"wallet": "0xabc", "amount": "1"},
        store=store,
    )
    assert claim is not None and claim.reused is False

    await mark_transfer_failed(claim, error="signer crashed before broadcast", store=store)

    retried = await claim_transfer_idempotency(
        operation="dex_swap",
        idempotency_key="swap-1",
        request_fields={"wallet": "0xabc", "amount": "1"},
        store=store,
    )

    assert retried is not None and retried.reused is False
    assert "idempotency_mark_failed" in caplog.text
    assert "idempotency_reclaimed" in caplog.text


@pytest.mark.asyncio
async def test_idempotency_reclaims_stale_pending_claim_after_crash(
    monkeypatch: pytest.MonkeyPatch,
    idempotency_registry: _CollectionRegistry,
) -> None:
    # This simulates a worker dying after claim creation but before tx_hash persistence.
    # Retries must not stay blocked behind a stale pending record forever.
    monkeypatch.setattr("core.idempotency.store.PENDING_RECLAIM_SECONDS", 1)
    store = IdempotencyStore(collection_name="idempotency_chaos_stale")

    first = await claim_transfer_idempotency(
        operation="bridge",
        idempotency_key="bridge-1",
        request_fields={"wallet": "0xabc", "route": "base->arb"},
        store=store,
    )
    assert first is not None and first.reused is False

    collection = idempotency_registry.get_sync("idempotency_chaos_stale")
    existing = collection.find_one({"key": first.scoped_key})
    assert existing is not None
    existing["updated_at"] = _utcnow() - timedelta(seconds=10)
    collection.force_write(first.scoped_key, existing)

    retried = await claim_transfer_idempotency(
        operation="bridge",
        idempotency_key="bridge-1",
        request_fields={"wallet": "0xabc", "route": "base->arb"},
        store=store,
    )

    assert retried is not None and retried.reused is False


@pytest.mark.asyncio
async def test_idempotency_concurrent_retries_execute_once_and_replay_result(
    idempotency_registry: _CollectionRegistry,
) -> None:
    # This hammers the same payment intent concurrently and forces all losers to
    # retry until the winner persists a result.
    store = IdempotencyStore(collection_name="idempotency_chaos_concurrent")
    execution_count = 0
    busy_messages: list[str] = []

    async def submit_once(worker_id: int) -> str:
        nonlocal execution_count
        for _ in range(40):
            try:
                claim = await claim_transfer_idempotency(
                    operation="swap",
                    idempotency_key="same-key",
                    request_fields={"wallet": "0xabc", "amount": "2.5"},
                    store=store,
                )
            except RuntimeError as exc:
                busy_messages.append(str(exc))
                await asyncio.sleep(0.005)
                continue

            assert claim is not None
            if claim.reused:
                replay = await load_transfer_idempotency_claim(claim, store=store)
                assert replay is not None and replay.tx_hash is not None
                return replay.tx_hash

            execution_count += 1
            tx_hash = f"0xsingle-{worker_id}"
            await asyncio.sleep(0.02)
            await mark_transfer_inflight(
                claim,
                tx_hash=tx_hash,
                result={"status": "pending", "tx_hash": tx_hash},
                store=store,
            )
            await asyncio.sleep(0.01)
            await mark_transfer_success(
                claim,
                tx_hash=tx_hash,
                result={"status": "success", "tx_hash": tx_hash},
                store=store,
            )
            return tx_hash
        raise AssertionError("worker never observed the stored idempotent result")

    results = await asyncio.gather(*[submit_once(index) for index in range(25)])

    assert execution_count == 1, f"multiple executions leaked through: {results}"
    assert len(set(results)) == 1, results
    assert any("already in progress" in message for message in busy_messages), busy_messages


@pytest.mark.asyncio
async def test_wallet_lock_recovers_from_post_commit_timeout_and_serializes_flows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This attacks SET NX PX after Redis accepted the owner but before the client
    # received the response. The lock must detect that it already owns the key.
    redis = _ChaosRedis()
    redis.queue_fault("wallet_acquire", when="after", count=1)
    monkeypatch.setattr(wallet_lock_module, "upstash_configured", lambda: True)
    monkeypatch.setattr(wallet_lock_module, "get_async_redis", lambda: asyncio.sleep(0, result=redis))

    active = 0
    max_active = 0
    order: list[str] = []

    async def flow(name: str) -> None:
        nonlocal active, max_active
        async with WalletLock(sender="0xlock", chain_id=1, ttl_ms=120, acquire_timeout_ms=600) as lock:
            active += 1
            max_active = max(max_active, active)
            order.append(f"enter:{name}")
            await asyncio.sleep(0.06)
            await lock.ensure_held()
            order.append(f"exit:{name}")
            active -= 1

    await asyncio.gather(flow("a"), flow("b"))

    assert max_active == 1, f"wallet flows interleaved: {order}"
    assert order == ["enter:a", "exit:a", "enter:b", "exit:b"], order


@pytest.mark.asyncio
async def test_wallet_lock_clamps_refresh_interval_to_prevent_mid_flow_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This misconfigures the refresh interval to exceed the TTL. The lock must
    # clamp it or the holder loses the lock mid-flow.
    redis = _ChaosRedis()
    monkeypatch.setattr(wallet_lock_module, "upstash_configured", lambda: True)
    monkeypatch.setattr(wallet_lock_module, "get_async_redis", lambda: asyncio.sleep(0, result=redis))
    monkeypatch.setenv("WALLET_LOCK_REFRESH_INTERVAL_MS", "5000")

    lock = WalletLock(sender="0xrefresh", chain_id=10, ttl_ms=90, acquire_timeout_ms=300)
    async with lock:
        await asyncio.sleep(0.18)
        await lock.ensure_held()
        ttl = redis.ttl_seconds("lock:10:0xrefresh")
        assert ttl is not None and ttl > 0.02, redis.history

    assert any(entry["op"] == "wallet_refresh" for entry in redis.history), redis.history


@pytest.mark.asyncio
async def test_multi_step_chaos_keeps_nonce_and_idempotency_consistent_across_wallets(
    monkeypatch: pytest.MonkeyPatch,
    idempotency_registry: _CollectionRegistry,
) -> None:
    # This mixes EVM, DEX, bridge, and Solana-style flows with crashes before
    # and after broadcast. Completed flows must replay; pre-broadcast failures
    # must become safely retryable; separate wallets must not interfere.
    redis = _ChaosRedis()
    nonce_manager = AsyncUpstashNonceManager(redis)
    store = IdempotencyStore(collection_name="idempotency_chaos_multi")
    monkeypatch.setattr(wallet_lock_module, "upstash_configured", lambda: True)
    monkeypatch.setattr(wallet_lock_module, "get_async_redis", lambda: asyncio.sleep(0, result=redis))
    monkeypatch.setattr("core.idempotency.store.PENDING_RECLAIM_SECONDS", 1)
    w3 = _StaticWeb3(1_000)

    active: dict[str, int] = defaultdict(int)
    max_active: dict[str, int] = defaultdict(int)

    async def run_flow(
        *,
        wallet: str,
        chain_id: int,
        operation: str,
        idem_key: str,
        crash_mode: str | None,
    ) -> dict[str, Any]:
        for attempt in range(3):
            try:
                claim = await claim_transfer_idempotency(
                    operation=operation,
                    idempotency_key=idem_key,
                    request_fields={"wallet": wallet, "operation": operation},
                    store=store,
                )
            except RuntimeError as exc:
                if "already in progress" not in str(exc):
                    raise
                await asyncio.sleep(0.01)
                continue

            assert claim is not None
            if claim.reused:
                replay = await load_transfer_idempotency_claim(claim, store=store)
                return {
                    "wallet": wallet,
                    "operation": operation,
                    "tx_hash": replay.tx_hash if replay is not None else claim.tx_hash,
                    "status": replay.status if replay is not None else claim.status,
                    "reused": True,
                }

            async with WalletLock(
                sender=wallet,
                chain_id=chain_id,
                ttl_ms=120,
                acquire_timeout_ms=800,
            ) as lock:
                active[wallet] += 1
                max_active[wallet] = max(max_active[wallet], active[wallet])
                try:
                    if operation == "solana":
                        nonce = None
                    else:
                        nonce = await nonce_manager.allocate_safe(wallet, chain_id, w3)
                    await asyncio.sleep(0.01)
                    await lock.ensure_held()
                    if crash_mode == "before_broadcast" and attempt == 0:
                        await mark_transfer_failed(
                            claim,
                            error=f"{operation} crashed before broadcast",
                            store=store,
                        )
                        raise RuntimeError("crash-before-broadcast")

                    tx_hash = (
                        f"0x{operation}-{wallet[-4:]}-{nonce}"
                        if nonce is not None
                        else f"sol-{wallet[-4:]}-{attempt}"
                    )
                    await mark_transfer_inflight(
                        claim,
                        tx_hash=tx_hash,
                        result={"status": "pending", "tx_hash": tx_hash},
                        store=store,
                    )
                    if crash_mode == "after_broadcast" and attempt == 0:
                        raise RuntimeError("crash-after-broadcast")

                    await mark_transfer_success(
                        claim,
                        tx_hash=tx_hash,
                        result={"status": "success", "tx_hash": tx_hash},
                        store=store,
                    )
                    return {
                        "wallet": wallet,
                        "operation": operation,
                        "tx_hash": tx_hash,
                        "status": "success",
                        "reused": False,
                    }
                finally:
                    active[wallet] -= 1

        raise AssertionError(f"{operation} flow for {wallet} exhausted retries")

    plans = [
        ("0xaaaa", 1, "evm", "flow-a", None),
        ("0xaaaa", 1, "dex", "flow-b", "after_broadcast"),
        ("0xaaaa", 1, "bridge", "flow-c", "before_broadcast"),
        ("sol-wallet-a", 101, "solana", "flow-d", None),
        ("0xbbbb", 1, "evm", "flow-e", None),
        ("0xbbbb", 1, "dex", "flow-f", "after_broadcast"),
        ("0xbbbb", 1, "bridge", "flow-g", "before_broadcast"),
        ("sol-wallet-b", 101, "solana", "flow-h", None),
    ]

    first_wave = await asyncio.gather(
        *[
            run_flow(
                wallet=wallet,
                chain_id=chain_id,
                operation=operation,
                idem_key=idem_key,
                crash_mode=crash_mode,
            )
            for wallet, chain_id, operation, idem_key, crash_mode in plans
        ],
        return_exceptions=True,
    )

    second_wave = await asyncio.gather(
        *[
            run_flow(
                wallet=wallet,
                chain_id=chain_id,
                operation=operation,
                idem_key=idem_key,
                crash_mode=None,
            )
            for wallet, chain_id, operation, idem_key, _crash_mode in plans
        ]
    )

    failures = [result for result in first_wave if isinstance(result, Exception)]
    assert any("crash-after-broadcast" in str(result) for result in failures), failures
    assert any("crash-before-broadcast" in str(result) for result in failures), failures

    replayed = {
        (entry["wallet"], entry["operation"]): entry
        for entry in second_wave
    }
    assert replayed[("0xaaaa", "dex")]["tx_hash"] is not None
    assert replayed[("0xbbbb", "dex")]["tx_hash"] is not None
    assert replayed[("0xaaaa", "bridge")]["status"] == "success"
    assert replayed[("0xbbbb", "bridge")]["status"] == "success"
    assert max_active["0xaaaa"] == 1, max_active
    assert max_active["0xbbbb"] == 1, max_active

    for wallet_key in ("nonce:0xaaaa:1", "nonce:0xbbbb:1"):
        evm_nonces = sorted(
            int(entry["result"])
            for entry in redis.history
            if entry["op"] == "nonce_allocate" and entry["key"] == wallet_key
            and int(entry["result"]) >= 0
        )
        assert evm_nonces == sorted(set(evm_nonces)), (wallet_key, evm_nonces)
        assert evm_nonces == list(range(min(evm_nonces), max(evm_nonces) + 1)), (
            wallet_key,
            evm_nonces,
        )
