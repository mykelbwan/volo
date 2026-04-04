import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from core.reservations.models import (
    FundsWaitRecord,
    ReservationRecord,
    ReservationRequirement,
    iso_utc,
    utc_now,
)
from core.reservations.service import (
    _DEFAULT_RESERVATION_DELETE_AFTER_SECONDS,
    _DEFAULT_WAIT_DELETE_AFTER_SECONDS,
    WalletReservationService,
)
from core.reservations.store import InMemoryReservationStore


class _CountingReservationStore(InMemoryReservationStore):
    def __init__(self) -> None:
        super().__init__()
        self.list_records_calls = 0

    async def list_records(self, *, wallet_scope: str, statuses=None):
        self.list_records_calls += 1
        return await super().list_records(wallet_scope=wallet_scope, statuses=statuses)


class _LockAwareReservationStore(InMemoryReservationStore):
    def __init__(self) -> None:
        super().__init__()
        self.wait_update_lock_states: list[bool] = []

    async def update_wait(self, wait_id: str, *, updates: dict[str, object]) -> None:
        current = await self.get_wait(wait_id)
        if current is not None:
            self.wait_update_lock_states.append(bool(self._locks))
        await super().update_wait(wait_id, updates=updates)


def _snapshot(resource_key: str, *, available_units: int) -> dict[str, dict[str, str]]:
    return {
        resource_key: {
            "resource_key": resource_key,
            "wallet_scope": "sender:0xabc",
            "sender": "0xabc",
            "chain": "base",
            "token_ref": "usdc",
            "symbol": "USDC",
            "decimals": 6,
            "available": "100",
            "available_base_units": str(available_units),
            "observed_at": "1",
            "chain_family": "evm",
        }
    }


def _requirement(resource_key: str, *, required_units: int) -> list[dict[str, str]]:
    return [
        {
            "resource_key": resource_key,
            "wallet_scope": "sender:0xabc",
            "sender": "0xabc",
            "chain": "base",
            "token_ref": "usdc",
            "symbol": "USDC",
            "decimals": 6,
            "required": "100",
            "required_base_units": str(required_units),
            "kind": "token_spend",
        }
    ]


def test_reservation_service_blocks_conflicting_claim_and_recovers_after_release():
    service = WalletReservationService(store=InMemoryReservationStore())
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)

    first = asyncio.run(
        service.claim(
            execution_id="exec-1",
            thread_id="thread-1",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
            task_number=7,
        )
    )
    assert first.acquired is True
    assert first.reservation_id

    second = asyncio.run(
        service.claim(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
        )
    )
    assert second.acquired is False
    assert second.conflicts
    assert "Task 7" in (second.deferred_reason or "")

    asyncio.run(
        service.release(
            first.reservation_id,
            status="released",
            reason="test_release",
        )
    )

    third = asyncio.run(
        service.claim(
            execution_id="exec-3",
            thread_id="thread-3",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
        )
    )
    assert third.acquired is True


def test_reservation_service_uses_active_holder_hint_on_warm_conflict_path():
    store = _CountingReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)

    first = asyncio.run(
        service.claim(
            execution_id="exec-1",
            thread_id="thread-1",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
            task_number=7,
        )
    )
    assert first.acquired is True
    initial_scan_count = store.list_records_calls
    wallet_state = asyncio.run(store.get_wallet_state(wallet_scope="sender:0xabc"))
    assert wallet_state is not None
    assert wallet_state["active_holders"][resource_key]["reservation_id"] == first.reservation_id

    second = asyncio.run(
        service.claim(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
        )
    )

    assert second.acquired is False
    assert "Task 7" in (second.deferred_reason or "")
    assert store.list_records_calls == initial_scan_count + 1


def test_reservation_service_finalizes_bridge_reservation_from_pending_record():
    service = WalletReservationService(store=InMemoryReservationStore())
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)
    claimed = asyncio.run(
        service.claim(
            execution_id="exec-bridge",
            thread_id="thread-bridge",
            node_id="step_0",
            tool="bridge",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
        )
    )
    assert claimed.acquired is True
    assert claimed.reservation_id

    asyncio.run(
        service.mark_broadcast(
            claimed.reservation_id,
            tx_hash="0xbridge",
        )
    )
    asyncio.run(
        service.finalize_from_pending_record(
            {
                "reservation_id": claimed.reservation_id,
                "tx_hash": "0xbridge",
            },
            terminal_status="SUCCESS",
        )
    )

    next_claim = asyncio.run(
        service.claim(
            execution_id="exec-after",
            thread_id="thread-after",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
        )
    )
    assert next_claim.acquired is True


def test_reservation_service_enforces_fifo_wait_order_across_conflicting_claims():
    service = WalletReservationService(store=InMemoryReservationStore())
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)

    first = asyncio.run(
        service.claim(
            execution_id="exec-1",
            thread_id="thread-1",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
            task_number=7,
        )
    )
    assert first.acquired is True

    second_claim = asyncio.run(
        service.claim(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
            task_number=8,
        )
    )
    assert second_claim.acquired is False

    wait_record = asyncio.run(
        service.enqueue_wait(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
            conversation_id="discord:user-1",
            task_number=8,
            title="Buy DAI",
            deferred_reason=second_claim.deferred_reason,
            conflicts=second_claim.conflicts,
        )
    )
    assert wait_record is not None

    third_claim = asyncio.run(
        service.claim(
            execution_id="exec-3",
            thread_id="thread-3",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
        )
    )
    assert third_claim.acquired is False
    assert "queued behind Task 8" in (third_claim.deferred_reason or "")

    asyncio.run(
        service.release(
            first.reservation_id,
            status="released",
            reason="test_release",
        )
    )

    fourth_claim = asyncio.run(
        service.claim(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
        )
    )
    assert fourth_claim.acquired is True
    assert fourth_claim.wait_id == wait_record.wait_id


def test_reservation_service_updates_resumed_wait_after_lock_release():
    store = _LockAwareReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)

    first = asyncio.run(
        service.claim(
            execution_id="exec-1",
            thread_id="thread-1",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
            task_number=7,
        )
    )
    assert first.acquired is True

    second_claim = asyncio.run(
        service.claim(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
            task_number=8,
        )
    )
    wait_record = asyncio.run(
        service.enqueue_wait(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
            conversation_id="discord:user-1",
            task_number=8,
            title="Buy DAI",
            deferred_reason=second_claim.deferred_reason,
            conflicts=second_claim.conflicts,
        )
    )
    assert wait_record is not None

    asyncio.run(
        service.release(
            first.reservation_id,
            status="released",
            reason="test_release",
        )
    )

    resumed = asyncio.run(
        service.claim(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
        )
    )

    assert resumed.acquired is True
    assert resumed.wait_id == wait_record.wait_id
    assert store.wait_update_lock_states
    assert store.wait_update_lock_states[-1] is False


def test_reservation_service_lists_non_overlapping_resume_candidates():
    service = WalletReservationService(store=InMemoryReservationStore())
    usdc_key = "0xabc|base|usdc"
    dai_key = "0xabc|base|dai"
    snapshot = {
        **_snapshot(usdc_key, available_units=100_000_000),
        **{
            dai_key: {
                **_snapshot(dai_key, available_units=100_000_000)[dai_key],
                "token_ref": "dai",
                "symbol": "DAI",
            }
        },
    }

    first = asyncio.run(
        service.enqueue_wait(
            execution_id="exec-1",
            thread_id="thread-1",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(usdc_key, required_units=50_000_000),
            resource_snapshots=snapshot,
            conversation_id="discord:user-1",
            task_number=1,
            title="Buy USDC",
            deferred_reason="waiting",
        )
    )
    second = asyncio.run(
        service.enqueue_wait(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=[
                {
                    **_requirement(dai_key, required_units=50_000_000)[0],
                    "resource_key": dai_key,
                    "token_ref": "dai",
                    "symbol": "DAI",
                }
            ],
            resource_snapshots=snapshot,
            conversation_id="discord:user-1",
            task_number=2,
            title="Buy DAI",
            deferred_reason="waiting",
        )
    )

    candidates = asyncio.run(service.list_resume_candidates(limit=10))
    assert first is not None
    assert second is not None
    assert [item.wait_id for item in candidates] == [first.wait_id, second.wait_id]


def test_reservation_service_updates_resource_totals_on_claim_and_release():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)

    claimed = asyncio.run(
        service.claim(
            execution_id="exec-1",
            thread_id="thread-1",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
        )
    )
    assert claimed.acquired is True
    totals_after_claim = asyncio.run(
        store.get_resource_totals(
            wallet_scope="sender:0xabc",
            resource_keys=[resource_key],
        )
    )
    assert totals_after_claim[resource_key] == 50_000_000
    wallet_state_after_claim = asyncio.run(
        store.get_wallet_state(wallet_scope="sender:0xabc")
    )
    assert wallet_state_after_claim is not None
    assert wallet_state_after_claim["resource_totals"][resource_key] == 50_000_000

    asyncio.run(
        service.release(
            claimed.reservation_id,
            status="released",
            reason="done",
        )
    )
    totals_after_release = asyncio.run(
        store.get_resource_totals(
            wallet_scope="sender:0xabc",
            resource_keys=[resource_key],
        )
    )
    assert totals_after_release[resource_key] == 0
    wallet_state_after_release = asyncio.run(
        store.get_wallet_state(wallet_scope="sender:0xabc")
    )
    assert wallet_state_after_release is not None
    assert wallet_state_after_release["resource_totals"][resource_key] == 0
    assert wallet_state_after_release["active_holders"][resource_key] is None


def test_reservation_service_release_clamps_missing_wallet_state_totals():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    now = utc_now()
    record = ReservationRecord(
        reservation_id="manual-reservation",
        wallet_scope="sender:0xabc",
        execution_id="exec-existing",
        thread_id="thread-existing",
        conversation_id=None,
        task_number=5,
        title="Existing task",
        node_id="step_0",
        tool="swap",
        status="reserved",
        resources=[
            ReservationRequirement.from_dict(
                _requirement(resource_key, required_units=50_000_000)[0]
            )
        ],
        created_at=iso_utc(now),
        updated_at=iso_utc(now),
        expires_at=iso_utc(now + timedelta(minutes=10)),
    )
    asyncio.run(store.save_record(record))

    asyncio.run(
        service.release(
            record.reservation_id,
            status="released",
            reason="done",
        )
    )

    totals = asyncio.run(
        store.get_resource_totals(
            wallet_scope="sender:0xabc",
            resource_keys=[resource_key],
        )
    )
    wallet_state = asyncio.run(store.get_wallet_state(wallet_scope="sender:0xabc"))
    assert totals[resource_key] == 0
    assert wallet_state is not None
    assert wallet_state["resource_totals"][resource_key] == 0


def test_reservation_service_repairs_missing_resource_totals_from_active_records():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)
    now = utc_now()
    existing = ReservationRecord(
        reservation_id="existing-reservation",
        wallet_scope="sender:0xabc",
        execution_id="exec-existing",
        thread_id="thread-existing",
        conversation_id=None,
        task_number=7,
        title="Existing task",
        node_id="step_0",
        tool="swap",
        status="reserved",
        resources=[
            ReservationRequirement.from_dict(
                _requirement(resource_key, required_units=100_000_000)[0]
            )
        ],
        created_at=iso_utc(now),
        updated_at=iso_utc(now),
        expires_at=iso_utc(now + timedelta(minutes=10)),
    )
    asyncio.run(store.save_record(existing))

    claimed = asyncio.run(
        service.claim(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
        )
    )
    assert claimed.acquired is False
    assert claimed.conflicts
    assert "Task 7" in (claimed.deferred_reason or "")

    repaired_totals = asyncio.run(
        store.get_resource_totals(
            wallet_scope="sender:0xabc",
            resource_keys=[resource_key],
        )
    )
    assert repaired_totals[resource_key] == 100_000_000


def test_reservation_service_updates_wait_heads_on_enqueue_and_terminal():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)

    first = asyncio.run(
        service.enqueue_wait(
            execution_id="exec-1",
            thread_id="thread-1",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
            conversation_id="discord:user-1",
            task_number=1,
            title="First wait",
            deferred_reason="waiting",
        )
    )
    second = asyncio.run(
        service.enqueue_wait(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
            conversation_id="discord:user-1",
            task_number=2,
            title="Second wait",
            deferred_reason="waiting",
        )
    )

    heads = asyncio.run(
        store.get_wait_heads(
            wallet_scope="sender:0xabc",
            resource_keys=[resource_key],
        )
    )
    assert first is not None
    assert second is not None
    assert heads[resource_key]["head_wait_id"] == first.wait_id
    assert store._wait_heads == {}

    asyncio.run(service.mark_wait_terminal(first.wait_id, status="completed"))
    refreshed_heads = asyncio.run(
        store.get_wait_heads(
            wallet_scope="sender:0xabc",
            resource_keys=[resource_key],
        )
    )
    assert refreshed_heads[resource_key]["head_wait_id"] == second.wait_id
    assert store._wait_heads == {}


def test_reservation_service_repairs_missing_wait_heads_from_active_waits():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)
    now = utc_now()
    wait = FundsWaitRecord(
        wait_id="wait-manual",
        wallet_scope="sender:0xabc",
        conversation_id="discord:user-1",
        thread_id="thread-1",
        execution_id="exec-1",
        node_id="step_0",
        task_number=9,
        title="Manual wait",
        tool="swap",
        status="queued",
        resources=[
            ReservationRequirement.from_dict(
                _requirement(resource_key, required_units=50_000_000)[0]
            )
        ],
        resource_snapshots=snapshot,
        created_at=iso_utc(now),
        updated_at=iso_utc(now),
        meta={},
    )
    asyncio.run(store.save_wait(wait))

    claimed = asyncio.run(
        service.claim(
            execution_id="exec-2",
            thread_id="thread-2",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
        )
    )

    assert claimed.acquired is False
    assert "queued behind Task 9" in (claimed.deferred_reason or "")
    repaired_heads = asyncio.run(
        store.get_wait_heads(
            wallet_scope="sender:0xabc",
            resource_keys=[resource_key],
        )
    )
    assert repaired_heads[resource_key]["head_wait_id"] == wait.wait_id
    wallet_state = asyncio.run(store.get_wallet_state(wallet_scope="sender:0xabc"))
    assert wallet_state is not None
    assert wallet_state["wait_heads"][resource_key]["head_wait_id"] == wait.wait_id


def test_reservation_service_reconciles_expired_wallet_state_before_conflict_check():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)

    first = asyncio.run(
        service.claim(
            execution_id="exec-expiring",
            thread_id="thread-expiring",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
            task_number=3,
        )
    )
    assert first.acquired is True

    asyncio.run(
        store.update_record(
            first.reservation_id,
            updates={
                "expires_at": iso_utc(utc_now() - timedelta(seconds=1)),
            },
        )
    )

    second = asyncio.run(
        service.claim(
            execution_id="exec-after-expiry",
            thread_id="thread-after-expiry",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
        )
    )

    assert second.acquired is True


def test_reservation_service_sets_delete_after_on_terminal_release():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)

    claimed = asyncio.run(
        service.claim(
            execution_id="exec-terminal",
            thread_id="thread-terminal",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
        )
    )
    assert claimed.acquired is True

    fixed_now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
    with patch("core.reservations.service.utc_now", return_value=fixed_now):
        asyncio.run(
            service.release(
                claimed.reservation_id,
                status="released",
                reason="done",
            )
        )

    record = asyncio.run(store.get_record(claimed.reservation_id))
    assert record is not None
    assert record.status == "released"
    assert record.delete_after == iso_utc(
        fixed_now + timedelta(seconds=_DEFAULT_RESERVATION_DELETE_AFTER_SECONDS)
    )


def test_reservation_service_clears_delete_after_when_active_reservation_is_reused():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)

    first = asyncio.run(
        service.claim(
            execution_id="exec-reused",
            thread_id="thread-reused",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
        )
    )
    assert first.acquired is True

    asyncio.run(
        store.update_record(
            first.reservation_id,
            updates={
                "delete_after": iso_utc(utc_now() + timedelta(days=3)),
            },
        )
    )

    reused = asyncio.run(
        service.claim(
            execution_id="exec-reused",
            thread_id="thread-reused",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
        )
    )

    assert reused.acquired is True
    assert reused.reused_existing is True
    record = asyncio.run(store.get_record(first.reservation_id))
    assert record is not None
    assert record.delete_after is None


def test_mark_wait_terminal_sets_delete_after_for_terminal_wait():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)
    wait = asyncio.run(
        service.enqueue_wait(
            execution_id="exec-wait-terminal",
            thread_id="thread-wait-terminal",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
            conversation_id="discord:user-1",
            task_number=1,
            title="Wait terminal",
            deferred_reason="waiting",
        )
    )

    assert wait is not None
    fixed_now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
    with patch("core.reservations.service.utc_now", return_value=fixed_now):
        asyncio.run(service.mark_wait_terminal(wait.wait_id, status="completed"))

    refreshed = asyncio.run(store.get_wait(wait.wait_id))
    assert refreshed is not None
    assert refreshed.status == "completed"
    assert refreshed.delete_after == iso_utc(
        fixed_now + timedelta(seconds=_DEFAULT_WAIT_DELETE_AFTER_SECONDS)
    )


def test_mark_wait_queued_and_resuming_clear_delete_after():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)
    wait = asyncio.run(
        service.enqueue_wait(
            execution_id="exec-wait-active",
            thread_id="thread-wait-active",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
            conversation_id="discord:user-1",
            task_number=1,
            title="Wait active",
            deferred_reason="waiting",
        )
    )

    assert wait is not None
    delete_after = iso_utc(utc_now() + timedelta(days=3))
    asyncio.run(
        store.update_wait(
            wait.wait_id,
            updates={"delete_after": delete_after},
        )
    )

    resuming = asyncio.run(service.mark_wait_resuming(wait.wait_id))
    assert resuming is not None
    assert resuming.status == "resuming"
    assert resuming.delete_after is None

    asyncio.run(
        store.update_wait(
            wait.wait_id,
            updates={
                "status": "resuming",
                "delete_after": delete_after,
            },
        )
    )
    asyncio.run(service.mark_wait_queued(wait.wait_id, last_error="retry"))

    refreshed = asyncio.run(store.get_wait(wait.wait_id))
    assert refreshed is not None
    assert refreshed.status == "queued"
    assert refreshed.delete_after is None


def test_reconcile_wallet_locked_repairs_stale_wallet_state_after_expiry():
    store = InMemoryReservationStore()
    service = WalletReservationService(store=store)
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)

    first = asyncio.run(
        service.claim(
            execution_id="exec-expired-wallet-state",
            thread_id="thread-expired-wallet-state",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=100_000_000),
            resource_snapshots=snapshot,
            task_number=3,
        )
    )
    assert first.acquired is True

    asyncio.run(
        service.release(
            first.reservation_id,
            status="expired",
            reason="expired_for_test",
        )
    )

    asyncio.run(service._reconcile_wallet_locked("sender:0xabc"))

    expired = asyncio.run(store.get_record(first.reservation_id))
    totals = asyncio.run(
        store.get_resource_totals(
            wallet_scope="sender:0xabc",
            resource_keys=[resource_key],
        )
    )
    wallet_state = asyncio.run(store.get_wallet_state(wallet_scope="sender:0xabc"))

    assert expired is not None
    assert expired.status == "expired"
    assert expired.delete_after is not None
    assert totals[resource_key] == 0
    assert wallet_state is not None
    assert wallet_state["resource_totals"][resource_key] == 0
    assert wallet_state["active_holders"][resource_key] is None


def test_legacy_reservation_and_wait_payloads_default_delete_after_to_none():
    now_iso = "2026-03-31T12:00:00+00:00"
    reservation = ReservationRecord.from_dict(
        {
            "reservation_id": "legacy-reservation",
            "wallet_scope": "sender:0xabc",
            "execution_id": "exec-1",
            "thread_id": "thread-1",
            "conversation_id": "discord:user-1",
            "task_number": 1,
            "title": "Legacy reservation",
            "node_id": "step_0",
            "tool": "swap",
            "status": "reserved",
            "resources": _requirement("0xabc|base|usdc", required_units=50_000_000),
            "created_at": now_iso,
            "updated_at": now_iso,
            "expires_at": now_iso,
        }
    )
    wait = FundsWaitRecord.from_dict(
        {
            "wait_id": "legacy-wait",
            "wallet_scope": "sender:0xabc",
            "conversation_id": "discord:user-1",
            "thread_id": "thread-1",
            "execution_id": "exec-1",
            "node_id": "step_0",
            "task_number": 1,
            "title": "Legacy wait",
            "tool": "swap",
            "status": "queued",
            "resources": _requirement("0xabc|base|usdc", required_units=50_000_000),
            "resource_snapshots": _snapshot("0xabc|base|usdc", available_units=100_000_000),
            "created_at": now_iso,
            "updated_at": now_iso,
        }
    )

    assert reservation.delete_after is None
    assert wait.delete_after is None


def test_mark_wait_resuming_returns_none_when_wait_is_no_longer_queued():
    service = WalletReservationService(store=InMemoryReservationStore())
    resource_key = "0xabc|base|usdc"
    snapshot = _snapshot(resource_key, available_units=100_000_000)
    wait = asyncio.run(
        service.enqueue_wait(
            execution_id="exec-1",
            thread_id="thread-1",
            node_id="step_0",
            tool="swap",
            requirements=_requirement(resource_key, required_units=50_000_000),
            resource_snapshots=snapshot,
            conversation_id="discord:user-1",
            task_number=1,
            title="Wait once",
            deferred_reason="waiting",
        )
    )

    assert wait is not None
    first = asyncio.run(service.mark_wait_resuming(wait.wait_id))
    second = asyncio.run(service.mark_wait_resuming(wait.wait_id))

    assert first is not None
    assert first.status == "resuming"
    assert second is None
