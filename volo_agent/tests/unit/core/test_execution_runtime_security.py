import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import HumanMessage

from core.execution.runtime import ExecutionRuntime, ExecutionRuntimeDeps
from core.idempotency.store import (
    IdempotencyRecord,
    compute_args_hash,
    compute_idempotency_key,
)
from core.memory.ledger import ErrorCategory
from core.planning.execution_plan import (
    ExecutionPlan,
    ExecutionState,
    NodeState,
    PlanNode,
    StepStatus,
)
from core.reservations.models import ReservationClaimResult
from core.reservations.service import WalletReservationService
from core.reservations.store import InMemoryReservationStore
from core.tools.base import Registry, Tool


class _NoopLedger:
    def record_execution(self, *args, **kwargs):
        return None

    def record_fee(self, *args, **kwargs):
        return None


class _NoopTaskHistoryRegistry:
    async def record_event(self, *args, **kwargs):
        return None


class _NoopTaskRegistry:
    async def upsert_execution_task(self, **kwargs):
        return None


class _MemoryIdempotencyStore:
    def __init__(self) -> None:
        self.records: dict[str, IdempotencyRecord] = {}

    def preload(self, key: str, record: IdempotencyRecord) -> None:
        self.records[key] = record

    def claim(self, *, key: str, metadata: dict):
        record = self.records.get(key)
        if record is not None:
            return record, False
        now = datetime.now(timezone.utc)
        record = IdempotencyRecord(
            key=key,
            status="pending",
            created_at=now,
            expires_at=now + timedelta(hours=1),
            metadata=dict(metadata),
        )
        self.records[key] = record
        return record, True

    def mark_success(self, *, key: str, result: dict, ttl_seconds: int = 0) -> None:
        current = self.records[key]
        self.records[key] = IdempotencyRecord(
            key=key,
            status="success",
            created_at=current.created_at,
            expires_at=current.expires_at,
            result=dict(result),
            error=None,
            metadata=current.metadata,
            tx_hash=result.get("tx_hash"),
        )

    def mark_failed(self, *, key: str, error: str, ttl_seconds: int = 0) -> None:
        current = self.records[key]
        self.records[key] = IdempotencyRecord(
            key=key,
            status="failed",
            created_at=current.created_at,
            expires_at=current.expires_at,
            result=current.result,
            error=str(error),
            metadata=current.metadata,
            tx_hash=current.tx_hash,
        )

    def mark_inflight(self, *, key: str, tx_hash: str) -> None:
        current = self.records[key]
        self.records[key] = IdempotencyRecord(
            key=key,
            status=current.status,
            created_at=current.created_at,
            expires_at=current.expires_at,
            result=current.result,
            error=current.error,
            metadata=current.metadata,
            tx_hash=str(tx_hash),
        )


@pytest.fixture(autouse=True)
def _stub_noncritical_side_effects(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("core.execution.runtime.get_ledger", lambda: _NoopLedger())
    monkeypatch.setattr(
        "core.execution.runtime.track_execution_volume",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "core.execution.runtime.resolve_conversation_id",
        lambda **kwargs: None,
    )


def _make_swap_args() -> dict[str, object]:
    return {
        "sender": "0xabc",
        "chain": "ethereum",
        "token_in_address": "0xusdc",
        "token_in_symbol": "USDC",
        "token_out_symbol": "ETH",
        "amount_in": "10",
    }


def _resource_key() -> str:
    return "0xabc|ethereum|0xusdc"


def _resource_snapshot(available_base_units: int = 10_000_000) -> dict[str, dict[str, str]]:
    return {
        _resource_key(): {
            "resource_key": _resource_key(),
            "wallet_scope": "sender:0xabc",
            "sender": "0xabc",
            "chain": "ethereum",
            "token_ref": "0xusdc",
            "symbol": "USDC",
            "decimals": 6,
            "available": "10",
            "available_base_units": str(available_base_units),
            "observed_at": "1",
            "chain_family": "evm",
        }
    }


def _reservation_requirements(required_base_units: int = 10_000_000) -> dict[str, list[dict[str, str]]]:
    return {
        "step_0": [
            {
                "resource_key": _resource_key(),
                "wallet_scope": "sender:0xabc",
                "sender": "0xabc",
                "chain": "ethereum",
                "token_ref": "0xusdc",
                "symbol": "USDC",
                "decimals": 6,
                "required": "10",
                "required_base_units": str(required_base_units),
                "kind": "token_spend",
            }
        ]
    }


def _make_plan() -> ExecutionPlan:
    return ExecutionPlan(
        goal="Swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args=_make_swap_args(),
                depends_on=[],
                approval_required=False,
            )
        },
    )


def _make_state(
    *,
    execution_id: str,
    messages=None,
    reservation_requirements=None,
    resource_snapshots=None,
    pending_transactions=None,
):
    plan = _make_plan()
    return plan, {
        "user_id": "user-1",
        "provider": "discord",
        "username": "alice",
        "user_info": {"volo_user_id": "user-1"},
        "intents": [{"intent_type": "swap", "slots": {"amount": "10", "chain": "ethereum"}}],
        "plans": [],
        "goal_parameters": {"intent_type": "swap", "amount": "10"},
        "plan_history": [plan],
        "execution_state": ExecutionState(
            node_states={"step_0": NodeState(node_id="step_0")}
        ),
        "artifacts": {},
        "context": {},
        "route_decision": "execute",
        "confirmation_status": None,
        "pending_transactions": list(pending_transactions or []),
        "reasoning_logs": [],
        "messages": list(messages or []),
        "fee_quotes": [],
        "balance_snapshot": {_resource_key(): "10"},
        "resource_snapshots": resource_snapshots or _resource_snapshot(),
        "native_requirements": {},
        "reservation_requirements": reservation_requirements or {},
        "trigger_id": None,
        "is_triggered_execution": None,
        "execution_id": execution_id,
    }


def _make_runtime(
    *,
    tool_result_factory,
    reservation_service_getter,
    idempotency_store=None,
    tx_receipt_status=lambda chain, tx_hash: "pending",
):
    registry = Registry()
    tool_calls = []

    async def _tool_func(args):
        tool_calls.append(dict(args))
        return await tool_result_factory(args)

    registry.register(
        Tool(
            name="swap",
            description="swap",
            func=_tool_func,
        )
    )

    async def _run_with_timing(tool_obj, args):
        return await tool_obj.run(args), 0.01

    async def _run_blocking(func, *args, **kwargs):
        return func(*args, **kwargs)

    runtime = ExecutionRuntime(
        ExecutionRuntimeDeps(
            task_history_registry_cls=_NoopTaskHistoryRegistry,
            task_registry_cls=_NoopTaskRegistry,
            idempotency_store_cls=(lambda: idempotency_store)
            if idempotency_store is not None
            else (lambda: (_ for _ in ()).throw(RuntimeError("disabled"))),
            reservation_service_getter=reservation_service_getter,
            run_with_timing=_run_with_timing,
            run_blocking=_run_blocking,
            tools_registry=registry,
            normalize_output=lambda node, result: None,
            swap_failure_message=lambda chain, has_suggestion: "swap failed",
            bridge_failure_message=lambda chain, has_suggestion: "bridge failed",
            tx_receipt_status=tx_receipt_status,
            publish_event=lambda payload: None,
            publish_event_async=None,
            task_history_write_timeout_seconds=0.01,
            native_marker="0x0000000000000000000000000000000000000000",
        )
    )
    return runtime, tool_calls


def _idempotency_key_for(plan: ExecutionPlan, scope_id: str) -> str:
    node = plan.nodes["step_0"]
    return compute_idempotency_key(
        scope_id=scope_id,
        node_id=node.id,
        tool=node.tool,
        args_hash=compute_args_hash(node.args),
    )


class _FakeAsyncWeb3:
    def __init__(self, *, pending_nonce=0, latest_nonce=0, receipt=None):
        self.eth = SimpleNamespace(
            get_transaction_count=AsyncMock(
                side_effect=lambda _sender, block: pending_nonce
                if block == "pending"
                else latest_nonce
            ),
            get_transaction_receipt=AsyncMock(return_value=receipt),
        )


def test_parallel_runtime_uses_global_reservations_to_block_overspend():
    async def _run():
        service = WalletReservationService(store=InMemoryReservationStore())

        async def _reservation_service_getter():
            return service

        async def _tool_result(_args):
            await asyncio.sleep(0.05)
            return {
                "message": "submitted",
                "tx_hash": "0xsubmitted",
                "chain": "ethereum",
                "nonce": 7,
            }

        runtime, tool_calls = _make_runtime(
            tool_result_factory=_tool_result,
            reservation_service_getter=_reservation_service_getter,
        )
        from unittest.mock import patch

        token_balance_patch = patch(
            "core.execution.runtime.get_token_balance_async",
            AsyncMock(return_value=Decimal("10")),
        )

        plan_a, state_a = _make_state(
            execution_id="exec-a",
            reservation_requirements=_reservation_requirements(),
        )
        plan_b, state_b = _make_state(
            execution_id="exec-b",
            reservation_requirements=_reservation_requirements(),
        )

        with token_balance_patch:
            results = await asyncio.gather(
                runtime.run(plan=plan_a, execution_state=state_a["execution_state"], state=state_a),
                runtime.run(plan=plan_b, execution_state=state_b["execution_state"], state=state_b),
            )

        success_results = [
            result
            for result in results
            if result["execution_state"].node_states["step_0"].status == StepStatus.SUCCESS
        ]
        deferred_results = [
            result
            for result in results
            if result["waiting_for_funds"] is not None
        ]

        assert len(tool_calls) == 1, "only one concurrent execution may reach the signing/broadcast path"
        assert len(success_results) == 1, "exactly one runtime should hold the wallet reservation"
        assert len(deferred_results) == 1, "conflicting executions must defer instead of overspending"
        assert deferred_results[0]["execution_state"].node_states["step_0"].status == StepStatus.PENDING, (
            "the losing execution should stay pending and wait for funds rather than racing ahead"
        )

    asyncio.run(_run())


def test_runtime_aborts_fail_closed_when_reservation_service_is_unavailable():
    async def _reservation_service_getter():
        return None

    async def _tool_result(_args):
        return {"message": "should not run", "tx_hash": "0xnope"}

    runtime, tool_calls = _make_runtime(
        tool_result_factory=_tool_result,
        reservation_service_getter=_reservation_service_getter,
    )
    plan, state = _make_state(
        execution_id="exec-no-service",
        reservation_requirements=_reservation_requirements(),
    )

    result = asyncio.run(
        runtime.run(plan=plan, execution_state=state["execution_state"], state=state)
    )

    node_state = result["execution_state"].node_states["step_0"]
    assert node_state.status == StepStatus.FAILED, "execution must fail closed if the reservation service is unavailable"
    assert node_state.error_category == ErrorCategory.SECURITY.value, "reservation outages should be treated as security failures"
    assert not tool_calls, "no transaction tool may run without a global reservation claim"


def test_runtime_blocks_reclaim_during_cooldown_even_when_wallet_looks_clean(monkeypatch):
    store = _MemoryIdempotencyStore()
    plan, state = _make_state(execution_id="exec-cooldown")
    key = _idempotency_key_for(plan, "exec-cooldown")
    store.preload(
        key,
        IdempotencyRecord(
            key=key,
            status="pending",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            metadata={"scope_id": "exec-cooldown", "sender": "0xabc", "chain": "ethereum"},
            tx_hash=None,
        ),
    )
    monkeypatch.setattr(
        "core.execution.runtime.make_async_web3",
        lambda _rpc_url: _FakeAsyncWeb3(pending_nonce=4, latest_nonce=4),
    )

    async def _reservation_service_getter():
        return None

    async def _tool_result(_args):
        return {"message": "should not execute", "tx_hash": "0xlate"}

    runtime, tool_calls = _make_runtime(
        tool_result_factory=_tool_result,
        reservation_service_getter=_reservation_service_getter,
        idempotency_store=store,
    )

    result = asyncio.run(
        runtime.run(plan=plan, execution_state=state["execution_state"], state=state)
    )

    node_state = result["execution_state"].node_states["step_0"]
    assert node_state.status == StepStatus.RUNNING, "pending records inside the cooldown window must not be reclaimed"
    assert "not reclaiming pending execution" in result["messages"][0].content.lower()
    assert not tool_calls, "cooldown-protected idempotency records must not execute a second transaction"


def test_runtime_blocks_reclaim_when_pending_tx_has_no_hash_and_nonce_is_uncertain(monkeypatch):
    store = _MemoryIdempotencyStore()
    plan, state = _make_state(
        execution_id="exec-uncertain",
        pending_transactions=[{"sender": "0xabc", "chain": "ethereum", "status": "pending"}],
    )
    key = _idempotency_key_for(plan, "exec-uncertain")
    store.preload(
        key,
        IdempotencyRecord(
            key=key,
            status="pending",
            created_at=datetime.now(timezone.utc) - timedelta(minutes=10),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            metadata={"scope_id": "exec-uncertain", "sender": "0xabc", "chain": "ethereum"},
            tx_hash=None,
        ),
    )
    monkeypatch.setattr(
        "core.execution.runtime.make_async_web3",
        lambda _rpc_url: _FakeAsyncWeb3(pending_nonce=9, latest_nonce=8),
    )

    async def _reservation_service_getter():
        return None

    async def _tool_result(_args):
        return {"message": "should not execute", "tx_hash": "0xreplayed"}

    runtime, tool_calls = _make_runtime(
        tool_result_factory=_tool_result,
        reservation_service_getter=_reservation_service_getter,
        idempotency_store=store,
    )

    result = asyncio.run(
        runtime.run(plan=plan, execution_state=state["execution_state"], state=state)
    )

    assert result["execution_state"].node_states["step_0"].status == StepStatus.RUNNING
    assert "uncertain broadcast state" in result["messages"][0].content.lower(), (
        "reclaim must be blocked when nonce/history checks suggest a broadcast may already exist"
    )
    assert not tool_calls, "uncertain reclaim conditions must not submit a second transaction"


def test_runtime_allows_reclaim_only_after_cooldown_when_wallet_is_clean(monkeypatch):
    store = _MemoryIdempotencyStore()
    plan, state = _make_state(execution_id="exec-clean")
    key = _idempotency_key_for(plan, "exec-clean")
    store.preload(
        key,
        IdempotencyRecord(
            key=key,
            status="pending",
            created_at=datetime.now(timezone.utc) - timedelta(minutes=10),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            metadata={"scope_id": "exec-clean", "sender": "0xabc", "chain": "ethereum"},
            tx_hash=None,
        ),
    )
    monkeypatch.setattr(
        "core.execution.runtime.make_async_web3",
        lambda _rpc_url: _FakeAsyncWeb3(pending_nonce=8, latest_nonce=8),
    )

    async def _reservation_service_getter():
        return None

    async def _tool_result(_args):
        return {
            "message": "submitted after cooldown",
            "tx_hash": "0xclean",
            "chain": "ethereum",
            "nonce": 3,
        }

    runtime, tool_calls = _make_runtime(
        tool_result_factory=_tool_result,
        reservation_service_getter=_reservation_service_getter,
        idempotency_store=store,
    )

    result = asyncio.run(
        runtime.run(plan=plan, execution_state=state["execution_state"], state=state)
    )

    assert result["execution_state"].node_states["step_0"].status == StepStatus.SUCCESS, (
        "clean wallet state after the reclaim cooldown should allow the original execution to proceed"
    )
    assert len(tool_calls) == 1, "the tool should execute once when reclaim is explicitly allowed"


def test_runtime_does_not_treat_single_rpc_failed_receipt_as_confirmed_revert(monkeypatch):
    store = _MemoryIdempotencyStore()
    plan, state = _make_state(execution_id="exec-rpc-ambiguous")
    key = _idempotency_key_for(plan, "exec-rpc-ambiguous")
    store.preload(
        key,
        IdempotencyRecord(
            key=key,
            status="pending",
            created_at=datetime.now(timezone.utc) - timedelta(minutes=10),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            metadata={"scope_id": "exec-rpc-ambiguous", "sender": "0xabc", "chain": "ethereum"},
            tx_hash="0xdeadbeef",
        ),
    )
    monkeypatch.setattr(
        "core.execution.runtime.make_async_web3",
        lambda _rpc_url: _FakeAsyncWeb3(receipt=None),
    )

    async def _reservation_service_getter():
        return None

    async def _tool_result(_args):
        return {"message": "should not execute", "tx_hash": "0xsecond"}

    runtime, tool_calls = _make_runtime(
        tool_result_factory=_tool_result,
        reservation_service_getter=_reservation_service_getter,
        idempotency_store=store,
        tx_receipt_status=lambda _chain, _tx_hash: "failed",
    )

    result = asyncio.run(
        runtime.run(plan=plan, execution_state=state["execution_state"], state=state)
    )

    assert result["execution_state"].node_states["step_0"].status == StepStatus.RUNNING, (
        "ambiguous RPC failures must leave the execution waiting instead of assuming a revert"
    )
    assert "not retrying while confirmation is uncertain" in result["messages"][0].content.lower()
    assert not tool_calls, "ambiguous receipt state must not trigger a duplicate retry"


def test_runtime_marks_pending_tx_failed_only_when_revert_is_confirmed(monkeypatch):
    store = _MemoryIdempotencyStore()
    plan, state = _make_state(execution_id="exec-rpc-revert")
    key = _idempotency_key_for(plan, "exec-rpc-revert")
    store.preload(
        key,
        IdempotencyRecord(
            key=key,
            status="pending",
            created_at=datetime.now(timezone.utc) - timedelta(minutes=10),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            metadata={"scope_id": "exec-rpc-revert", "sender": "0xabc", "chain": "ethereum"},
            tx_hash="0xdeadbeef",
        ),
    )
    monkeypatch.setattr(
        "core.execution.runtime.make_async_web3",
        lambda _rpc_url: _FakeAsyncWeb3(receipt=SimpleNamespace(status=0)),
    )

    async def _reservation_service_getter():
        return None

    async def _tool_result(_args):
        return {"message": "should not execute", "tx_hash": "0xsecond"}

    runtime, tool_calls = _make_runtime(
        tool_result_factory=_tool_result,
        reservation_service_getter=_reservation_service_getter,
        idempotency_store=store,
        tx_receipt_status=lambda _chain, _tx_hash: "failed",
    )

    result = asyncio.run(
        runtime.run(plan=plan, execution_state=state["execution_state"], state=state)
    )

    assert result["execution_state"].node_states["step_0"].status == StepStatus.FAILED, (
        "confirmed reverts should move the node into a terminal failure state"
    )
    assert not tool_calls, "the executor must not resubmit while handling a prior confirmed revert"


def test_runtime_deduplicates_replayed_intent_across_different_execution_ids():
    store = _MemoryIdempotencyStore()
    tool_results = []

    async def _reservation_service_getter():
        return None

    async def _tool_result(args):
        tool_results.append(dict(args))
        return {
            "message": "submitted once",
            "tx_hash": "0xonce",
            "chain": "ethereum",
            "nonce": 1,
        }

    runtime, tool_calls = _make_runtime(
        tool_result_factory=_tool_result,
        reservation_service_getter=_reservation_service_getter,
        idempotency_store=store,
    )
    replayed_message = HumanMessage(
        content="swap 10 usdc to eth",
        additional_kwargs={"message_id": "msg-123", "nonce": "nonce-123"},
    )

    plan_a, state_a = _make_state(
        execution_id="exec-original",
        messages=[replayed_message],
    )
    first = asyncio.run(
        runtime.run(plan=plan_a, execution_state=state_a["execution_state"], state=state_a)
    )

    plan_b, state_b = _make_state(
        execution_id="exec-replayed",
        messages=[replayed_message],
    )
    second = asyncio.run(
        runtime.run(plan=plan_b, execution_state=state_b["execution_state"], state=state_b)
    )

    assert first["execution_state"].node_states["step_0"].status == StepStatus.SUCCESS
    assert second["execution_state"].node_states["step_0"].status == StepStatus.SUCCESS
    assert len(tool_calls) == 1, "replaying the same intent with a new execution_id must not bypass idempotency"
    assert second["pending_transactions"] == [], (
        "a deduplicated replay must not create a second pending transaction record"
    )


def test_runtime_jit_balance_shortfall_blocks_submission_and_never_calls_tool(monkeypatch):
    reservation_service = MagicMock()
    reservation_service.claim = AsyncMock(
        return_value=ReservationClaimResult(acquired=True, reservation_id="res-1")
    )
    reservation_service.get_reserved_totals = AsyncMock(
        return_value={_resource_key(): 10_000_000}
    )
    reservation_service.release = AsyncMock()
    reservation_service.mark_broadcast = AsyncMock()

    async def _reservation_service_getter():
        return reservation_service

    async def _tool_result(_args):
        return {"message": "should not execute", "tx_hash": "0xjit"}

    monkeypatch.setattr(
        "core.execution.runtime.get_token_balance_async",
        AsyncMock(return_value=Decimal("5")),
    )

    runtime, tool_calls = _make_runtime(
        tool_result_factory=_tool_result,
        reservation_service_getter=_reservation_service_getter,
    )
    plan, state = _make_state(
        execution_id="exec-jit",
        reservation_requirements=_reservation_requirements(),
        resource_snapshots=_resource_snapshot(available_base_units=20_000_000),
    )

    result = asyncio.run(
        runtime.run(plan=plan, execution_state=state["execution_state"], state=state)
    )

    node_state = result["execution_state"].node_states["step_0"]
    assert node_state.status == StepStatus.FAILED, "JIT shortfalls must fail the node before signing/broadcast"
    assert "balance changed before signing" in result["messages"][0].content.lower()
    assert not tool_calls, "the tool must never be called after a just-in-time balance shortfall"
    reservation_service.release.assert_awaited_once()
    assert not result["pending_transactions"], "no transaction record should exist when JIT validation aborts pre-signing"


def test_runtime_allows_dynamic_routing_when_route_planner_reports_unrouted_nodes():
    async def _reservation_service_getter():
        return None

    async def _tool_result(_args):
        return {
            "message": "submitted",
            "tx_hash": "0xabc123",
            "chain": "ethereum",
            "nonce": 9,
        }

    runtime, tool_calls = _make_runtime(
        tool_result_factory=_tool_result,
        reservation_service_getter=_reservation_service_getter,
    )
    plan, state = _make_state(execution_id="exec-route-unrouted")
    plan.metadata = {
        "route_planner": {
            "applied": True,
            "routable_nodes": 1,
            "routed_nodes": 0,
            "unrouted_nodes": 1,
        }
    }

    result = asyncio.run(
        runtime.run(plan=plan, execution_state=state["execution_state"], state=state)
    )

    assert result["execution_state"].node_states["step_0"].status == StepStatus.SUCCESS
    assert len(tool_calls) == 1
    assert all(
        "route validation failed" not in log.lower()
        for log in result.get("reasoning_logs", [])
    )


def test_runtime_route_validation_failure_does_not_emit_action_starting_log():
    async def _reservation_service_getter():
        return None

    async def _tool_result(_args):
        return {"message": "should not execute", "tx_hash": "0xnope"}

    runtime, tool_calls = _make_runtime(
        tool_result_factory=_tool_result,
        reservation_service_getter=_reservation_service_getter,
    )
    plan, state = _make_state(execution_id="exec-route-fail-log")
    plan.metadata = {"route_planner": {"applied": True}}

    result = asyncio.run(
        runtime.run(plan=plan, execution_state=state["execution_state"], state=state)
    )

    assert result["execution_state"].node_states["step_0"].status == StepStatus.FAILED
    assert len(tool_calls) == 0
    assert not any(
        log.startswith("[ACTION] Starting")
        for log in result.get("reasoning_logs", [])
    )
