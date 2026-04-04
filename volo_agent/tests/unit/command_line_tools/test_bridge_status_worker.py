import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from pymongo.errors import DuplicateKeyError

from core.utils.bridge_status_registry import BridgeStatusResult

from command_line_tools import bridge_status_worker as mod
from core import bridge_status_worker_locks as lock_mod
from core.planning.execution_plan import ExecutionPlan, PlanNode


def test_should_resume_for_transition_terminal():
    before = {"status": "PENDING"}
    after_success = {"status": "SUCCESS"}
    after_failed = {"status": "FAILED"}

    assert mod._should_resume_for_transition(before, after_success) is True
    assert mod._should_resume_for_transition(before, after_failed) is True

    already_terminal = {"status": "SUCCESS"}
    assert mod._should_resume_for_transition(already_terminal, after_success) is False


def test_process_pending_records_no_resume_on_pending():
    record = {
        "type": "bridge",
        "status": "PENDING",
        "tx_hash": "0xabc",
        "protocol": "across",
        "node_id": "step_0",
        "is_testnet": True,
    }

    def _fake_fetch(*_args, **_kwargs):
        return BridgeStatusResult(raw_status=None, normalized_status=None)

    original = mod.fetch_bridge_status_async
    try:
        mod.fetch_bridge_status_async = AsyncMock(side_effect=_fake_fetch)
        updated, delta, resume, _events = asyncio.run(
            mod._process_pending_records(
                [record], now_iso="2026-03-16T00:00:00+00:00", debug=False
            )
        )
    finally:
        mod.fetch_bridge_status_async = original

    assert resume is False
    assert len(updated) == 1
    assert updated[0]["status"] == "PENDING"
    assert delta.node_states == {}


def test_process_pending_records_socket_normalizes_chain_ids():
    record = {
        "type": "bridge",
        "status": "PENDING",
        "tx_hash": "0xabc",
        "protocol": "socket",
        "node_id": "step_0",
        "is_testnet": True,
        "source_chain_id": 1,
        "dest_chain_id": 8453,
        "meta": {},
    }
    captured: dict = {}

    def _fake_fetch(_protocol, _tx_hash, *, is_testnet, meta):
        captured["is_testnet"] = is_testnet
        captured["meta"] = meta
        return BridgeStatusResult(raw_status=None, normalized_status=None)

    original = mod.fetch_bridge_status_async
    try:
        mod.fetch_bridge_status_async = AsyncMock(side_effect=_fake_fetch)
        updated, _delta, _resume, _events = asyncio.run(
            mod._process_pending_records(
                [record], now_iso="2026-03-16T00:00:00+00:00", debug=False
            )
        )
    finally:
        mod.fetch_bridge_status_async = original

    assert captured["is_testnet"] is True
    assert captured["meta"]["fromChainId"] == 1
    assert captured["meta"]["toChainId"] == 8453
    assert updated[0]["meta"]["fromChainId"] == 1
    assert updated[0]["meta"]["toChainId"] == 8453


def test_process_pending_records_socket_skips_without_chain_ids():
    record = {
        "type": "bridge",
        "status": "PENDING",
        "tx_hash": "0xabc",
        "protocol": "socket",
        "node_id": "step_0",
        "is_testnet": True,
        "meta": {},
    }
    called = {"fetch": False}

    def _fake_fetch(*_args, **_kwargs):
        called["fetch"] = True
        return BridgeStatusResult(raw_status=None, normalized_status=None)

    original = mod.fetch_bridge_status_async
    try:
        mod.fetch_bridge_status_async = AsyncMock(side_effect=_fake_fetch)
        updated, _delta, _resume, _events = asyncio.run(
            mod._process_pending_records(
                [record], now_iso="2026-03-16T00:00:00+00:00", debug=False
            )
        )
    finally:
        mod.fetch_bridge_status_async = original

    assert called["fetch"] is False
    assert len(updated) == 1
    assert updated[0]["status"] == "PENDING"


def test_process_pending_records_lifi_adds_chain_ids_when_present():
    record = {
        "type": "bridge",
        "status": "PENDING",
        "tx_hash": "0xabc",
        "protocol": "lifi",
        "node_id": "step_0",
        "is_testnet": True,
        "source_chain_id": 1,
        "dest_chain_id": 137,
        "meta": {},
    }
    captured: dict = {}

    def _fake_fetch(_protocol, _tx_hash, *, is_testnet, meta):
        captured["is_testnet"] = is_testnet
        captured["meta"] = meta
        return BridgeStatusResult(raw_status=None, normalized_status=None)

    original = mod.fetch_bridge_status_async
    try:
        mod.fetch_bridge_status_async = AsyncMock(side_effect=_fake_fetch)
        updated, _delta, _resume, _events = asyncio.run(
            mod._process_pending_records(
                [record], now_iso="2026-03-16T00:00:00+00:00", debug=False
            )
        )
    finally:
        mod.fetch_bridge_status_async = original

    assert captured["is_testnet"] is True
    assert captured["meta"]["fromChain"] == 1
    assert captured["meta"]["toChain"] == 137
    assert updated[0]["meta"]["fromChain"] == 1
    assert updated[0]["meta"]["toChain"] == 137




def test_sync_completed_conversation_task_updates_registry_for_terminal_bridge():
    recorded: dict[str, object] = {}

    class _TaskRegistry:
        async def upsert_execution_task(self, **kwargs):
            recorded.update(kwargs)
            return kwargs

    plan = ExecutionPlan(
        goal="Bridge USDC to Base",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="bridge",
                args={},
                depends_on=[],
                approval_required=False,
            )
        },
    )

    values = {
        "provider": "discord",
        "user_id": "user-1",
        "user_info": {"volo_user_id": "volo-1"},
        "execution_id": "exec-1",
        "context": {"conversation_id": "discord:user-1"},
        "plan_history": [plan],
    }
    original = mod.ConversationTaskRegistry
    try:
        mod.ConversationTaskRegistry = _TaskRegistry
        asyncio.run(
            mod._sync_completed_conversation_task(
                values=values,
                thread_id="thread-1",
                terminal_events=[
                    {
                        "summary": "Bridge ethereum → base via across success.",
                        "tx_hash": "0xabc",
                    }
                ],
            )
        )
    finally:
        mod.ConversationTaskRegistry = original

    assert recorded["conversation_id"] == "discord:user-1"
    assert recorded["execution_id"] == "exec-1"
    assert recorded["thread_id"] == "thread-1"
    assert recorded["status"] == "COMPLETED"
    assert recorded["latest_summary"] == "Bridge ethereum → base via across success."
    assert recorded["tx_hash"] == "0xabc"


def test_distinct_thread_pairs_uses_async_collection_scan():
    class _Cursor:
        def __init__(self, docs):
            self._docs = docs

        def __aiter__(self):
            self._iter = iter(self._docs)
            return self

        async def __anext__(self):
            try:
                return next(self._iter)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    collection = MagicMock()
    collection.aggregate.return_value = _Cursor(
        [
            {"_id": {"thread_id": "thread-1", "checkpoint_ns": ""}},
            {"_id": {"thread_id": "thread-2", "checkpoint_ns": "ns-1"}},
        ]
    )
    original = mod.AsyncMongoDB.get_collection
    try:
        mod.AsyncMongoDB.get_collection = MagicMock(return_value=collection)
        pairs = asyncio.run(mod._distinct_thread_pairs())
    finally:
        mod.AsyncMongoDB.get_collection = original

    assert pairs == [("thread-1", ""), ("thread-2", "ns-1")]


def test_acquire_lock_uses_async_collection_update():
    collection = MagicMock()
    collection.find_one_and_update = AsyncMock(
        return_value={
            "owner": "worker-1",
            "expires_at": datetime.now(timezone.utc),
        }
    )
    original = lock_mod.AsyncMongoDB.get_collection
    try:
        lock_mod.AsyncMongoDB.get_collection = MagicMock(return_value=collection)
        acquired = asyncio.run(mod._acquire_lock("lock-1", "worker-1", 30))
    finally:
        lock_mod.AsyncMongoDB.get_collection = original

    assert acquired is True
    collection.find_one_and_update.assert_awaited_once()
    update_doc = collection.find_one_and_update.await_args.args[1]["$set"]
    assert update_doc["owner"] == "worker-1"
    assert isinstance(update_doc["expires_at"], datetime)
    assert update_doc["expires_at"].tzinfo is not None


def test_acquire_lock_accepts_legacy_numeric_expiry_docs():
    collection = MagicMock()
    collection.find_one_and_update = AsyncMock(
        return_value={"owner": "worker-1", "expires_at": 0}
    )
    original = lock_mod.AsyncMongoDB.get_collection
    try:
        lock_mod.AsyncMongoDB.get_collection = MagicMock(return_value=collection)
        acquired = asyncio.run(mod._acquire_lock("lock-1", "worker-1", 30))
    finally:
        lock_mod.AsyncMongoDB.get_collection = original

    assert acquired is True
    query = collection.find_one_and_update.await_args.args[0]
    assert {"expires_at": {"$type": "number", "$lte": query["$or"][1]["expires_at"]["$lte"]}} == query["$or"][1]


def test_acquire_lock_returns_false_on_duplicate_key_race():
    collection = MagicMock()
    collection.find_one_and_update = AsyncMock(side_effect=DuplicateKeyError("dup"))
    original = lock_mod.AsyncMongoDB.get_collection
    try:
        lock_mod.AsyncMongoDB.get_collection = MagicMock(return_value=collection)
        acquired = asyncio.run(mod._acquire_lock("lock-1", "worker-1", 30))
    finally:
        lock_mod.AsyncMongoDB.get_collection = original

    assert acquired is False
    collection.find_one_and_update.assert_awaited_once()


def test_ensure_lock_indexes_creates_ttl_index():
    collection = MagicMock()
    collection.create_indexes = AsyncMock()
    original = lock_mod.AsyncMongoDB.get_collection
    try:
        lock_mod.AsyncMongoDB.get_collection = MagicMock(return_value=collection)
        asyncio.run(mod._ensure_lock_indexes())
    finally:
        lock_mod.AsyncMongoDB.get_collection = original

    collection.create_indexes.assert_awaited_once()
    models = collection.create_indexes.await_args.args[0]
    assert len(models) == 1
    assert models[0].document["name"] == "ttl_bridge_status_worker_lock_expires_at"
    assert models[0].document["expireAfterSeconds"] == 0
