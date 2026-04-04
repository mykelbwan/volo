from __future__ import annotations

import asyncio
import os
import secrets
import uuid
from collections import defaultdict
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from typing import Any, Awaitable, Dict, Iterable, List

from core.reservations.common import reservation_id_for
from core.reservations.models import (
    FundsWaitRecord,
    ReservationClaimResult,
    ReservationConflict,
    ReservationRecord,
    ReservationRequirement,
    ResourceSnapshot,
    iso_utc,
    parse_iso,
    utc_now,
)
from core.reservations.store import (
    MongoReservationStore,
    ReservationStore,
)

_ACTIVE_STATUSES = frozenset({"reserved", "broadcast"})
_WAIT_ACTIVE_STATUSES = frozenset({"queued", "resuming"})
_TERMINAL_RESERVATION_STATUSES = frozenset({"released", "failed", "expired"})
_TERMINAL_WAIT_STATUSES = frozenset({"completed", "failed", "resumed"})
_DEFAULT_LOCK_TTL_SECONDS = int(os.getenv("VOLO_RESERVATION_LOCK_TTL_SECONDS", "5"))
_DEFAULT_CLAIM_TTL_SECONDS = int(os.getenv("VOLO_RESERVATION_CLAIM_TTL_SECONDS", "900"))
_DEFAULT_BROADCAST_TTL_SECONDS = int(
    os.getenv("VOLO_RESERVATION_BROADCAST_TTL_SECONDS", "3600")
)
_DEFAULT_RESERVATION_DELETE_AFTER_SECONDS = int(
    os.getenv("VOLO_RESERVATION_DELETE_AFTER_SECONDS", str(3 * 24 * 60 * 60))
)
_DEFAULT_WAIT_DELETE_AFTER_SECONDS = int(
    os.getenv("VOLO_WAIT_DELETE_AFTER_SECONDS", str(3 * 24 * 60 * 60))
)
_DEFAULT_WAIT_RESUME_COOLDOWN_SECONDS = int(
    os.getenv("VOLO_RESERVATION_WAIT_RESUME_COOLDOWN_SECONDS", "15")
)
_RESUME_TOKEN_MIN_LENGTH = 32

_reservation_service = None
_reservation_service_lock: asyncio.Lock | None = None


def _coerce_requirement_list(
    items: Iterable[Dict[str, Any]],
) -> List[ReservationRequirement]:
    result: List[ReservationRequirement] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        requirement = ReservationRequirement.from_dict(item)
        if (
            not requirement.resource_key
            or not requirement.wallet_scope
            or requirement.required_base_units <= 0
        ):
            continue
        result.append(requirement)
    return result


def _coerce_snapshot_map(
    payload: Dict[str, Dict[str, Any]],
) -> Dict[str, ResourceSnapshot]:
    snapshots: Dict[str, ResourceSnapshot] = {}
    for key, item in (payload or {}).items():
        if not isinstance(item, dict):
            continue
        snapshot = ResourceSnapshot.from_dict({**item, "resource_key": key})
        if not snapshot.resource_key or not snapshot.wallet_scope:
            continue
        snapshots[snapshot.resource_key] = snapshot
    return snapshots


def _wait_id_for(*, execution_id: str, node_id: str) -> str:
    return f"wait:{reservation_id_for(execution_id=str(execution_id), node_id=str(node_id))}"


def _sort_waits(waits: Iterable[FundsWaitRecord]) -> List[FundsWaitRecord]:
    return sorted(
        waits,
        key=lambda item: (
            parse_iso(item.created_at) or utc_now(),
            item.wait_id,
        ),
    )


def _wait_resource_keys(wait: FundsWaitRecord) -> set[str]:
    return {
        item.resource_key
        for item in wait.resources
        if isinstance(item.resource_key, str) and item.resource_key
    }


def _default_wait_id(*, execution_id: str, node_id: str) -> str:
    return _wait_id_for(execution_id=str(execution_id), node_id=str(node_id))


def _generate_resume_token() -> str:
    return secrets.token_urlsafe(_RESUME_TOKEN_MIN_LENGTH)


def _is_valid_resume_token(token: str | None) -> bool:
    normalized = str(token or "").strip()
    return len(normalized) >= _RESUME_TOKEN_MIN_LENGTH


def _delete_after_iso(*, now: Any, retention_seconds: int) -> str:
    return iso_utc(now + timedelta(seconds=max(1, int(retention_seconds))))


def _reservation_delete_after(status: str, *, now: Any) -> str | None:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in _TERMINAL_RESERVATION_STATUSES:
        return None
    return _delete_after_iso(
        now=now,
        retention_seconds=_DEFAULT_RESERVATION_DELETE_AFTER_SECONDS,
    )


def _wait_delete_after(status: str, *, now: Any) -> str | None:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in _TERMINAL_WAIT_STATUSES:
        return None
    return _delete_after_iso(
        now=now,
        retention_seconds=_DEFAULT_WAIT_DELETE_AFTER_SECONDS,
    )


async def _load_current_wait(
    store: ReservationStore,
    *,
    execution_id: str,
    node_id: str,
    requested_wait_id: str | None,
) -> FundsWaitRecord | None:
    wait_id = str(requested_wait_id).strip() if requested_wait_id else ""
    default_wait_id = _default_wait_id(execution_id=execution_id, node_id=node_id)
    if wait_id:
        current = await store.get_wait(wait_id)
        if current is not None:
            return current
        if wait_id == default_wait_id:
            return None
    return await store.get_wait(default_wait_id)


def _task_label(*, task_number: int | None, title: str | None) -> str:
    if task_number is not None:
        return f"Task {task_number}"
    if title:
        return str(title)
    return "another active task"


def _resource_deltas(
    requirements: Iterable[ReservationRequirement],
    *,
    multiplier: int = 1,
) -> Dict[str, int]:
    deltas: Dict[str, int] = defaultdict(int)
    # Ensure factor correctly reflects the sign of the multiplier
    factor = 1 if multiplier > 0 else -1 if multiplier < 0 else 0
    for requirement in requirements:
        if not requirement.resource_key:
            continue
        deltas[requirement.resource_key] += factor * max(
            0, int(requirement.required_base_units)
        )
    return dict(deltas)


def _wait_head_payload(wait: FundsWaitRecord) -> Dict[str, object]:
    return {
        "head_wait_id": wait.wait_id,
        "task_number": wait.task_number,
        "title": wait.title,
        "head_created_at": wait.created_at,
        "status": wait.status,
    }


def _wait_sorts_before(
    *,
    created_at: str | None,
    wait_id: str | None,
    other_created_at: str | None,
    other_wait_id: str | None,
) -> bool:
    created = parse_iso(created_at)
    other_created = parse_iso(other_created_at)
    if created is None and other_created is None:
        return str(wait_id or "") < str(other_wait_id or "")
    if created is None:
        return False
    if other_created is None:
        return True
    if created != other_created:
        return created < other_created
    return str(wait_id or "") < str(other_wait_id or "")


def _sort_records(records: Iterable[ReservationRecord]) -> List[ReservationRecord]:
    return sorted(
        records,
        key=lambda item: (
            parse_iso(item.created_at) or utc_now(),
            item.reservation_id,
        ),
    )


def _summarize_wait_heads(
    waits: Iterable[FundsWaitRecord],
    *,
    resource_keys: Iterable[str],
    exclude_wait_ids: set[str] | None = None,
) -> Dict[str, Dict[str, object]]:
    requested_keys = {
        str(resource_key).strip().lower()
        for resource_key in resource_keys
        if str(resource_key).strip()
    }
    excluded = exclude_wait_ids or set()
    heads: Dict[str, Dict[str, object]] = {}
    for wait in _sort_waits(waits):
        if wait.wait_id in excluded:
            continue
        payload = _wait_head_payload(wait)
        for resource_key in _wait_resource_keys(wait):
            if resource_key not in requested_keys or resource_key in heads:
                continue
            heads[resource_key] = payload
    return heads


def _active_holder_payload(
    record: ReservationRecord,
    requirement: ReservationRequirement,
) -> Dict[str, object]:
    return {
        "reservation_id": record.reservation_id,
        "execution_id": record.execution_id,
        "thread_id": record.thread_id,
        "task_number": record.task_number,
        "title": record.title,
        "node_id": record.node_id,
        "status": record.status,
        "required_base_units": str(requirement.required_base_units),
    }


def _wallet_state_resource_totals(
    wallet_state: Dict[str, object] | None,
) -> Dict[str, int]:
    raw = wallet_state.get("resource_totals") if isinstance(wallet_state, dict) else {}
    if not isinstance(raw, dict):
        return {}
    totals: Dict[str, int] = {}
    for resource_key, total in raw.items():
        normalized_key = str(resource_key).strip().lower()
        if not normalized_key:
            continue
        try:
            total_units = int(total)
        except Exception:
            total_units = 0
        totals[normalized_key] = max(0, total_units)
    return totals


def _wallet_state_wait_heads(
    wallet_state: Dict[str, object] | None,
) -> Dict[str, Dict[str, object]]:
    raw = wallet_state.get("wait_heads") if isinstance(wallet_state, dict) else {}
    if not isinstance(raw, dict):
        return {}
    heads: Dict[str, Dict[str, object]] = {}
    for resource_key, head in raw.items():
        normalized_key = str(resource_key).strip().lower()
        if not normalized_key:
            continue
        if isinstance(head, dict):
            heads[normalized_key] = dict(head)
    return heads


def _wallet_state_active_holders(
    wallet_state: Dict[str, object] | None,
) -> Dict[str, Dict[str, object]]:
    raw = wallet_state.get("active_holders") if isinstance(wallet_state, dict) else {}
    if not isinstance(raw, dict):
        return {}
    holders: Dict[str, Dict[str, object]] = {}
    for resource_key, holder in raw.items():
        normalized_key = str(resource_key).strip().lower()
        if not normalized_key:
            continue
        if isinstance(holder, dict):
            holders[normalized_key] = dict(holder)
    return holders


def _summarize_active_records(
    records: Iterable[ReservationRecord],
    *,
    exclude_reservation_id: str | None = None,
) -> tuple[Dict[str, int], Dict[str, List[Dict[str, Any]]]]:
    totals_by_key: Dict[str, int] = defaultdict(int)
    holders_by_key: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        if exclude_reservation_id and record.reservation_id == exclude_reservation_id:
            continue
        for resource in record.resources:
            totals_by_key[resource.resource_key] += resource.required_base_units
            holders_by_key[resource.resource_key].append(
                _active_holder_payload(record, resource)
            )
    return dict(totals_by_key), dict(holders_by_key)


def _summarize_active_holder_heads(
    records: Iterable[ReservationRecord],
    *,
    resource_keys: Iterable[str],
    exclude_reservation_id: str | None = None,
) -> Dict[str, Dict[str, object]]:
    requested_keys = {
        str(resource_key).strip().lower()
        for resource_key in resource_keys
        if str(resource_key).strip()
    }
    holders: Dict[str, Dict[str, object]] = {}
    if not requested_keys:
        return holders
    for record in records:
        if exclude_reservation_id and record.reservation_id == exclude_reservation_id:
            continue
        for resource in record.resources:
            resource_key = resource.resource_key
            if resource_key not in requested_keys or resource_key in holders:
                continue
            holders[resource_key] = _active_holder_payload(record, resource)
    return holders


class WalletReservationService:
    def __init__(self, store: ReservationStore | None = None) -> None:
        self._store = store or self._build_default_store()

    @staticmethod
    def _build_default_store() -> ReservationStore:
        return MongoReservationStore()

    async def _refresh_wait_heads_locked(
        self,
        *,
        wallet_scope: str,
        resource_keys: Iterable[str],
        exclude_wait_ids: set[str] | None = None,
    ) -> Dict[str, Dict[str, object]]:
        normalized_keys = tuple(
            sorted(
                {
                    str(resource_key).strip().lower()
                    for resource_key in resource_keys
                    if str(resource_key).strip()
                }
            )
        )
        if not normalized_keys:
            return {}
        active_waits = await self._store.list_waits(
            wallet_scope=wallet_scope,
            statuses=_WAIT_ACTIVE_STATUSES,
        )
        heads_by_key = _summarize_wait_heads(
            active_waits,
            resource_keys=normalized_keys,
            exclude_wait_ids=exclude_wait_ids,
        )
        now_iso = iso_utc()
        await self._store.update_wallet_state(
            wallet_scope=wallet_scope,
            wait_heads={
                resource_key: heads_by_key.get(resource_key)
                for resource_key in normalized_keys
            },
            updated_at=now_iso,
        )
        return heads_by_key

    async def _upsert_wait_heads_from_wait_locked(
        self,
        *,
        wait: FundsWaitRecord,
    ) -> None:
        resource_keys = tuple(sorted(_wait_resource_keys(wait)))
        if not resource_keys:
            return
        existing_heads = await self._store.get_wait_heads(
            wallet_scope=wait.wallet_scope,
            resource_keys=resource_keys,
        )
        updates: Dict[str, Dict[str, object] | None] = {}
        for resource_key in resource_keys:
            existing = existing_heads.get(resource_key)
            if existing is None or not existing.get("head_wait_id"):
                updates[resource_key] = _wait_head_payload(wait)
                continue
            if _wait_sorts_before(
                created_at=wait.created_at,
                wait_id=wait.wait_id,
                other_created_at=str(existing.get("head_created_at") or ""),
                other_wait_id=str(existing.get("head_wait_id") or ""),
            ):
                updates[resource_key] = _wait_head_payload(wait)
        if not updates:
            return
        now_iso = iso_utc()
        await self._store.update_wallet_state(
            wallet_scope=wait.wallet_scope,
            wait_heads=updates,
            updated_at=now_iso,
        )

    async def get_reserved_totals(
        self,
        *,
        wallet_scope: str,
        resource_keys: Iterable[str],
    ) -> Dict[str, int]:
        normalized_keys = tuple(
            sorted(
                {
                    str(resource_key).strip().lower()
                    for resource_key in resource_keys
                    if str(resource_key).strip()
                }
            )
        )
        if not normalized_keys:
            return {}

        try:
            totals = await self._store.get_resource_totals(
                wallet_scope=str(wallet_scope),
                resource_keys=normalized_keys,
            )
        except Exception:
            totals = {}

        missing_keys = [key for key in normalized_keys if key not in totals]
        if not missing_keys:
            return {key: max(0, int(totals.get(key, 0))) for key in normalized_keys}

        try:
            active_records = await self._store.list_records(
                wallet_scope=str(wallet_scope),
                statuses=_ACTIVE_STATUSES,
            )
            sorted_records = _sort_records(active_records)
            repaired_totals, _holders = _summarize_active_records(sorted_records)
        except Exception:
            repaired_totals = {}

        return {
            key: max(0, int(totals.get(key, repaired_totals.get(key, 0))))
            for key in normalized_keys
        }

    async def claim(
        self,
        *,
        execution_id: str,
        thread_id: str,
        node_id: str,
        tool: str,
        requirements: Iterable[Dict[str, Any]],
        resource_snapshots: Dict[str, Dict[str, Any]],
        conversation_id: str | None = None,
        task_number: int | None = None,
        title: str | None = None,
        wait_id: str | None = None,
    ) -> ReservationClaimResult:
        normalized_requirements = _coerce_requirement_list(requirements)
        if not normalized_requirements:
            return ReservationClaimResult(acquired=True)

        wallet_scopes = {
            item.wallet_scope for item in normalized_requirements if item.wallet_scope
        }
        if len(wallet_scopes) != 1:
            return ReservationClaimResult(
                acquired=False,
                deferred_reason=(
                    "Reservation requirements used inconsistent wallet scopes. "
                    "Recovery path: rebuild the plan and retry."
                ),
            )
        wallet_scope = next(iter(wallet_scopes))
        snapshots = _coerce_snapshot_map(resource_snapshots)
        reservation_id = reservation_id_for(
            execution_id=str(execution_id),
            node_id=str(node_id),
        )
        requested_wait_id = str(wait_id).strip() if wait_id else None
        owner = f"{thread_id}:{node_id}:{uuid.uuid4().hex[:8]}"
        successful_claim: ReservationClaimResult | None = None
        post_lock_updates: list[Awaitable[Any]] = []

        try:
            if not await self._store.acquire_wallet_lock(
                wallet_scope=wallet_scope,
                owner=owner,
                ttl_seconds=_DEFAULT_LOCK_TTL_SECONDS,
            ):
                return ReservationClaimResult(
                    acquired=False,
                    deferred_reason=(
                        f"Reservation lock busy for {wallet_scope}. "
                        "Recovery path: retry shortly."
                    ),
                )

            await self._reconcile_wallet_locked(wallet_scope)
            merged_requirements: Dict[str, ReservationRequirement] = {}
            for requirement in normalized_requirements:
                existing_req = merged_requirements.get(requirement.resource_key)
                if existing_req is None:
                    merged_requirements[requirement.resource_key] = requirement
                    continue
                try:
                    merged_required = Decimal(existing_req.required) + Decimal(
                        requirement.required
                    )
                except Exception:
                    merged_required = Decimal("0")
                merged_requirements[requirement.resource_key] = replace(
                    existing_req,
                    required=str(merged_required),
                    required_base_units=(
                        existing_req.required_base_units
                        + requirement.required_base_units
                    ),
                    kind=f"{existing_req.kind}+{requirement.kind}",
                )

            requested_resource_keys = tuple(merged_requirements.keys())
            current_wait, existing, wallet_state = await asyncio.gather(
                _load_current_wait(
                    self._store,
                    execution_id=str(execution_id),
                    node_id=str(node_id),
                    requested_wait_id=requested_wait_id,
                ),
                self._store.get_record(reservation_id),
                self._store.get_wallet_state(wallet_scope=wallet_scope),
            )
            aggregate_totals = _wallet_state_resource_totals(wallet_state)
            aggregate_wait_heads = _wallet_state_wait_heads(wallet_state)
            aggregate_active_holders = _wallet_state_active_holders(wallet_state)
            if existing is not None and existing.status in _ACTIVE_STATUSES:
                await self._store.update_record(
                    reservation_id,
                    updates={
                        "updated_at": iso_utc(),
                        "delete_after": None,
                        "expires_at": iso_utc(
                            utc_now()
                            + timedelta(seconds=max(1, _DEFAULT_CLAIM_TTL_SECONDS))
                        ),
                    },
                )
                return ReservationClaimResult(
                    acquired=True,
                    reservation_id=reservation_id,
                    reused_existing=True,
                )

            conflicts: List[ReservationConflict] = []
            current_wait_id = current_wait.wait_id if current_wait is not None else None
            excluded_wait_ids = {current_wait_id} if current_wait_id else set()
            wait_heads_complete = all(
                resource_key in aggregate_wait_heads
                for resource_key in requested_resource_keys
            )
            if not wait_heads_complete:
                aggregate_wait_heads = await self._refresh_wait_heads_locked(
                    wallet_scope=wallet_scope,
                    resource_keys=requested_resource_keys,
                    exclude_wait_ids=excluded_wait_ids,
                )
            blocking_head = next(
                (
                    aggregate_wait_heads.get(resource_key)
                    for resource_key in requested_resource_keys
                    if isinstance(aggregate_wait_heads.get(resource_key), dict)
                    and aggregate_wait_heads.get(resource_key, {}).get("head_wait_id")
                    and aggregate_wait_heads.get(resource_key, {}).get("head_wait_id")
                    != current_wait_id
                ),
                None,
            )
            if isinstance(blocking_head, dict):
                task_number_raw = blocking_head.get("task_number")
                blocking_task_number: int | None
                if task_number_raw is None:
                    blocking_task_number = None
                else:
                    try:
                        blocking_task_number = int(str(task_number_raw))
                    except Exception:
                        blocking_task_number = None
                title_raw = blocking_head.get("title")
                blocking_title = str(title_raw) if title_raw is not None else None
                return ReservationClaimResult(
                    acquired=False,
                    reservation_id=reservation_id,
                    deferred_reason=(
                        f"Funds are queued behind {_task_label(task_number=blocking_task_number, title=blocking_title)}. "
                        "Recovery path: wait for the earlier task to finish, or cancel it to free funds sooner."
                    ),
                    wait_id=current_wait_id
                    or str(blocking_head.get("head_wait_id") or ""),
                )

            totals_by_key: Dict[str, int] = {}
            holders_by_key: Dict[str, List[Dict[str, Any]]] = {}
            conflicting_requirements: Dict[str, ReservationRequirement] = {}
            aggregate_complete = all(
                resource_key in aggregate_totals
                for resource_key in requested_resource_keys
            )
            needs_record_fallback = not aggregate_complete
            for resource_key, requirement in merged_requirements.items():
                snapshot = snapshots.get(resource_key)
                if snapshot is None:
                    return ReservationClaimResult(
                        acquired=False,
                        deferred_reason=(
                            f"Reservation snapshot missing for {resource_key}. "
                            "Recovery path: refresh balances and retry."
                        ),
                    )
                reserved = max(0, int(aggregate_totals.get(resource_key, 0)))
                available = snapshot.available_base_units
                if reserved + requirement.required_base_units > available:
                    conflicting_requirements[resource_key] = requirement

            stale_active_holder_hint = False
            if conflicting_requirements and all(
                isinstance(aggregate_active_holders.get(resource_key), dict)
                and aggregate_active_holders.get(resource_key, {}).get("reservation_id")
                for resource_key in conflicting_requirements
            ):
                hinted_ids = {
                    str(
                        aggregate_active_holders[resource_key].get("reservation_id")
                        or ""
                    )
                    for resource_key in conflicting_requirements
                }
                for hinted_id in hinted_ids:
                    if not hinted_id:
                        stale_active_holder_hint = True
                        break
                    hinted_record = await self._store.get_record(hinted_id)
                    if (
                        hinted_record is None
                        or hinted_record.status not in _ACTIVE_STATUSES
                    ):
                        stale_active_holder_hint = True
                        break
            if (
                conflicting_requirements
                and not stale_active_holder_hint
                and all(
                    isinstance(aggregate_active_holders.get(resource_key), dict)
                    and aggregate_active_holders.get(resource_key, {}).get(
                        "reservation_id"
                    )
                    for resource_key in conflicting_requirements
                )
            ):
                for resource_key, requirement in conflicting_requirements.items():
                    snapshot = snapshots.get(resource_key)
                    if snapshot is None:
                        continue
                    conflicts.append(
                        ReservationConflict(
                            resource_key=resource_key,
                            required_base_units=requirement.required_base_units,
                            available_base_units=snapshot.available_base_units,
                            reserved_base_units=max(
                                0, int(aggregate_totals.get(resource_key, 0))
                            ),
                            holders=[aggregate_active_holders[resource_key]],
                        )
                    )
            elif conflicting_requirements:
                needs_record_fallback = True

            if needs_record_fallback:
                active_records = await self._store.list_records(
                    wallet_scope=wallet_scope,
                    statuses=_ACTIVE_STATUSES,
                )
                sorted_records = _sort_records(active_records)
                totals_by_key, holders_by_key = _summarize_active_records(
                    sorted_records,
                    exclude_reservation_id=reservation_id,
                )
                active_holder_heads = _summarize_active_holder_heads(
                    sorted_records,
                    resource_keys=requested_resource_keys,
                    exclude_reservation_id=reservation_id,
                )
                repaired_totals = {
                    resource_key: totals_by_key.get(resource_key, 0)
                    for resource_key in requested_resource_keys
                }
                repaired_active_holders = {
                    resource_key: active_holder_heads.get(resource_key)
                    for resource_key in requested_resource_keys
                }
                repair_now_iso = iso_utc()
                await self._store.update_wallet_state(
                    wallet_scope=wallet_scope,
                    resource_totals=repaired_totals,
                    active_holders=repaired_active_holders,
                    updated_at=repair_now_iso,
                )
                conflicts = []
                for resource_key, requirement in merged_requirements.items():
                    snapshot = snapshots.get(resource_key)
                    if snapshot is None:
                        continue
                    reserved = totals_by_key.get(resource_key, 0)
                    available = snapshot.available_base_units
                    if reserved + requirement.required_base_units > available:
                        conflicts.append(
                            ReservationConflict(
                                resource_key=resource_key,
                                required_base_units=requirement.required_base_units,
                                available_base_units=available,
                                reserved_base_units=reserved,
                                holders=holders_by_key.get(resource_key, [])[:3],
                            )
                        )

            if conflicts:
                conflict = conflicts[0]
                holder = conflict.holders[0] if conflict.holders else {}
                holder_label = (
                    f"Task {holder.get('task_number')}"
                    if holder.get("task_number") is not None
                    else "another active task"
                )
                return ReservationClaimResult(
                    acquired=False,
                    reservation_id=reservation_id,
                    deferred_reason=(
                        f"Funds for {conflict.resource_key} are already reserved by "
                        f"{holder_label}. Recovery path: wait, inspect the active task, "
                        "or cancel it before retrying."
                    ),
                    conflicts=conflicts,
                    wait_id=current_wait_id,
                )

            now = utc_now()
            record = ReservationRecord(
                reservation_id=reservation_id,
                wallet_scope=wallet_scope,
                execution_id=str(execution_id),
                thread_id=str(thread_id),
                conversation_id=str(conversation_id) if conversation_id else None,
                task_number=int(task_number) if task_number is not None else None,
                title=str(title) if title else None,
                node_id=str(node_id),
                tool=str(tool).lower(),
                status="reserved",
                resources=list(merged_requirements.values()),
                created_at=iso_utc(now),
                updated_at=iso_utc(now),
                expires_at=iso_utc(
                    now + timedelta(seconds=max(1, _DEFAULT_CLAIM_TTL_SECONDS))
                ),
            )
            await self._store.save_record(record)
            now_iso = iso_utc(now)
            await self._store.adjust_wallet_state_resource_totals(
                wallet_scope=wallet_scope,
                deltas_by_key=_resource_deltas(merged_requirements.values()),
                updated_at=now_iso,
            )
            missing_active_holders: Dict[str, Dict[str, object] | None] = {
                resource_key: _active_holder_payload(record, requirement)
                for resource_key, requirement in merged_requirements.items()
                if not isinstance(aggregate_active_holders.get(resource_key), dict)
                or not aggregate_active_holders.get(resource_key, {}).get(
                    "reservation_id"
                )
            }
            if missing_active_holders:
                await self._store.update_wallet_state(
                    wallet_scope=wallet_scope,
                    active_holders=missing_active_holders,
                    updated_at=now_iso,
                )

            if current_wait_id:
                post_lock_updates.extend(
                    [
                        self._store.update_wait(
                            current_wait_id,
                            updates={
                                "status": "resumed",
                                "updated_at": now_iso,
                                "resume_after": None,
                                "delete_after": _wait_delete_after(
                                    "resumed",
                                    now=now,
                                ),
                                "last_error": None,
                                "meta": {
                                    **(
                                        current_wait.meta
                                        if current_wait is not None
                                        else {}
                                    ),
                                    "reservation_id": reservation_id,
                                },
                            },
                        ),
                        self._refresh_wait_heads_locked(
                            wallet_scope=wallet_scope,
                            resource_keys=requested_resource_keys,
                            exclude_wait_ids={current_wait_id},
                        ),
                    ]
                )
            successful_claim = ReservationClaimResult(
                acquired=True,
                reservation_id=reservation_id,
                wait_id=current_wait_id,
            )
        except Exception:
            return ReservationClaimResult(
                acquired=False,
                reservation_id=reservation_id,
                store_unavailable=True,
                deferred_reason=(
                    "Reservation store unavailable. Recovery path: retry shortly; "
                    "the executor will fall back to local safeguards for now."
                ),
            )
        finally:
            try:
                await self._store.release_wallet_lock(
                    wallet_scope=wallet_scope, owner=owner
                )
            except Exception:
                pass
        if successful_claim is not None:
            if post_lock_updates:
                await asyncio.gather(*post_lock_updates, return_exceptions=True)
            return successful_claim

    async def enqueue_wait(
        self,
        *,
        execution_id: str,
        thread_id: str,
        node_id: str,
        tool: str,
        requirements: Iterable[Dict[str, Any]],
        resource_snapshots: Dict[str, Dict[str, Any]],
        conversation_id: str,
        task_number: int | None = None,
        title: str | None = None,
        deferred_reason: str | None = None,
        conflicts: Iterable[ReservationConflict] | None = None,
    ) -> FundsWaitRecord | None:
        normalized_requirements = _coerce_requirement_list(requirements)
        if not normalized_requirements:
            return None

        wallet_scopes = {
            item.wallet_scope for item in normalized_requirements if item.wallet_scope
        }
        if len(wallet_scopes) != 1:
            return None
        wallet_scope = next(iter(wallet_scopes))
        owner = f"{thread_id}:{node_id}:{uuid.uuid4().hex[:8]}"

        try:
            if not await self._store.acquire_wallet_lock(
                wallet_scope=wallet_scope,
                owner=owner,
                ttl_seconds=_DEFAULT_LOCK_TTL_SECONDS,
            ):
                return None
            await self._reconcile_wallet_locked(wallet_scope)
            wait_id_value = _default_wait_id(
                execution_id=str(execution_id),
                node_id=str(node_id),
            )
            existing = await self._store.get_wait(wait_id_value)
            now_iso = iso_utc()
            resume_token = (
                existing.resume_token
                if existing is not None
                and _is_valid_resume_token(existing.resume_token)
                else _generate_resume_token()
            )
            conflict_payload = [item.to_dict() for item in (conflicts or [])]
            wait_meta = {
                "deferred_reason": deferred_reason,
                "resource_keys": sorted(
                    {
                        item.resource_key
                        for item in normalized_requirements
                        if item.resource_key
                    }
                ),
                "conflicts": conflict_payload,
            }
            if existing is not None:
                merged_meta = {**(existing.meta or {}), **wait_meta}
                await self._store.update_wait(
                    existing.wait_id,
                    updates={
                        "status": "queued",
                        "updated_at": now_iso,
                        "resume_token": resume_token,
                        "resume_after": None,
                        "delete_after": None,
                        "last_error": deferred_reason,
                        "meta": merged_meta,
                    },
                )
                refreshed = await self._store.get_wait(existing.wait_id)
                if refreshed is not None and refreshed.status == "queued":
                    await self._upsert_wait_heads_from_wait_locked(wait=refreshed)
                return refreshed or existing

            wait = FundsWaitRecord(
                wait_id=wait_id_value,
                resume_token=resume_token,
                wallet_scope=wallet_scope,
                conversation_id=str(conversation_id),
                thread_id=str(thread_id),
                execution_id=str(execution_id),
                node_id=str(node_id),
                task_number=int(task_number) if task_number is not None else None,
                title=str(title) if title else None,
                tool=str(tool).lower(),
                status="queued",
                resources=list(normalized_requirements),
                resource_snapshots=dict(resource_snapshots or {}),
                created_at=now_iso,
                updated_at=now_iso,
                resume_after=None,
                last_error=deferred_reason,
                attempts=0,
                meta=wait_meta,
            )
            await self._store.save_wait(wait)
            await self._upsert_wait_heads_from_wait_locked(wait=wait)
            return wait
        finally:
            try:
                await self._store.release_wallet_lock(
                    wallet_scope=wallet_scope, owner=owner
                )
            except Exception:
                pass

    async def get_wait(self, wait_id: str) -> FundsWaitRecord | None:
        return await self._store.get_wait(str(wait_id))

    async def list_resume_candidates(
        self, *, limit: int = 100
    ) -> List[FundsWaitRecord]:
        waits = _sort_waits(
            await self._store.list_waits(
                statuses=_WAIT_ACTIVE_STATUSES,
                limit=max(1, int(limit) * 4),
            )
        )
        eligible: List[FundsWaitRecord] = []
        now = utc_now()
        blocked_keys_by_wallet: Dict[str, set[str]] = defaultdict(set)
        for wait in waits:
            if wait.status == "resuming":
                resume_after = parse_iso(wait.resume_after)
                if resume_after is not None and resume_after > now:
                    blocked_keys_by_wallet[wait.wallet_scope].update(
                        _wait_resource_keys(wait)
                    )
                    continue
                await self._store.update_wait(
                    wait.wait_id,
                    updates={
                        "status": "queued",
                        "updated_at": iso_utc(now),
                        "delete_after": None,
                    },
                )
                wait = (await self._store.get_wait(wait.wait_id)) or wait
            resume_after = parse_iso(wait.resume_after)
            if resume_after is not None and resume_after > now:
                continue
            wait_keys = _wait_resource_keys(wait)
            if blocked_keys_by_wallet[wait.wallet_scope].intersection(wait_keys):
                blocked_keys_by_wallet[wait.wallet_scope].update(wait_keys)
                continue
            eligible.append(wait)
            blocked_keys_by_wallet[wait.wallet_scope].update(wait_keys)
            if len(eligible) >= max(1, int(limit)):
                break
        return eligible

    async def mark_wait_resuming(self, wait_id: str) -> FundsWaitRecord | None:
        now = utc_now()
        resume_after = iso_utc(
            now + timedelta(seconds=max(1, _DEFAULT_WAIT_RESUME_COOLDOWN_SECONDS))
        )
        existing = await self._store.get_wait(str(wait_id))
        resume_token = (
            existing.resume_token
            if existing is not None and _is_valid_resume_token(existing.resume_token)
            else _generate_resume_token()
        )
        attempts = (existing.attempts + 1) if existing is not None else 1
        wait = await self._store.transition_wait(
            str(wait_id),
            from_statuses={"queued"},
            updates={
                "status": "resuming",
                "updated_at": iso_utc(now),
                "resume_token": resume_token,
                "resume_after": resume_after,
                "delete_after": None,
                "attempts": attempts,
            },
        )
        if wait is not None:
            return wait
        return None

    async def mark_wait_queued(
        self,
        wait_id: str,
        *,
        last_error: str | None = None,
        resume_after: str | None = None,
    ) -> None:
        current = await self._store.get_wait(str(wait_id))
        await self._store.update_wait(
            str(wait_id),
            updates={
                "status": "queued",
                "updated_at": iso_utc(),
                "resume_after": resume_after,
                "delete_after": None,
                "last_error": last_error,
            },
        )
        if current is not None:
            refreshed = await self._store.get_wait(str(wait_id))
            if refreshed is not None and refreshed.status == "queued":
                owner = f"wait-queued:{wait_id}:{uuid.uuid4().hex[:8]}"
                if await self._store.acquire_wallet_lock(
                    wallet_scope=refreshed.wallet_scope,
                    owner=owner,
                    ttl_seconds=_DEFAULT_LOCK_TTL_SECONDS,
                ):
                    try:
                        await self._upsert_wait_heads_from_wait_locked(wait=refreshed)
                    finally:
                        await self._store.release_wallet_lock(
                            wallet_scope=refreshed.wallet_scope,
                            owner=owner,
                        )

    async def mark_wait_terminal(
        self,
        wait_id: str,
        *,
        status: str,
        last_error: str | None = None,
    ) -> None:
        now = utc_now()
        current = await self._store.get_wait(str(wait_id))
        await self._store.update_wait(
            str(wait_id),
            updates={
                "status": str(status).strip().lower(),
                "updated_at": iso_utc(now),
                "resume_after": None,
                "delete_after": _wait_delete_after(status, now=now),
                "last_error": last_error,
            },
        )
        if current is not None:
            owner = f"wait-terminal:{wait_id}:{uuid.uuid4().hex[:8]}"
            if await self._store.acquire_wallet_lock(
                wallet_scope=current.wallet_scope,
                owner=owner,
                ttl_seconds=_DEFAULT_LOCK_TTL_SECONDS,
            ):
                try:
                    await self._refresh_wait_heads_locked(
                        wallet_scope=current.wallet_scope,
                        resource_keys=_wait_resource_keys(current),
                    )
                finally:
                    await self._store.release_wallet_lock(
                        wallet_scope=current.wallet_scope,
                        owner=owner,
                    )

    async def mark_broadcast(
        self, reservation_id: str, *, tx_hash: str | None = None
    ) -> None:
        now = utc_now()
        await self._store.update_record(
            str(reservation_id),
            updates={
                "status": "broadcast",
                "tx_hash": str(tx_hash) if tx_hash else None,
                "updated_at": iso_utc(now),
                "delete_after": None,
                "expires_at": iso_utc(
                    now + timedelta(seconds=max(1, _DEFAULT_BROADCAST_TTL_SECONDS))
                ),
            },
        )

    async def touch(
        self, reservation_id: str, *, ttl_seconds: int | None = None
    ) -> None:
        now = utc_now()
        await self._store.update_record(
            str(reservation_id),
            updates={
                "updated_at": iso_utc(now),
                "delete_after": None,
                "expires_at": iso_utc(
                    now
                    + timedelta(
                        seconds=max(
                            1,
                            ttl_seconds
                            if ttl_seconds is not None
                            else _DEFAULT_CLAIM_TTL_SECONDS,
                        )
                    )
                ),
            },
        )

    async def release(
        self,
        reservation_id: str,
        *,
        status: str = "released",
        reason: str | None = None,
        tx_hash: str | None = None,
    ) -> None:
        now = utc_now()
        now_iso = iso_utc(now)
        record = await self._store.get_record(str(reservation_id))
        if record is None:
            return

        target_status = str(status).strip().lower()
        wallet_scope = record.wallet_scope
        owner = f"release:{reservation_id}:{uuid.uuid4().hex[:8]}"

        # Lock the wallet scope before modifying state to prevent race conditions
        if await self._store.acquire_wallet_lock(
            wallet_scope=wallet_scope,
            owner=owner,
            ttl_seconds=_DEFAULT_LOCK_TTL_SECONDS,
        ):
            try:
                # Phase 1: Update the record status (Truth)
                await self._store.update_record(
                    str(reservation_id),
                    updates={
                        "status": target_status,
                        "reason": reason,
                        "tx_hash": str(tx_hash) if tx_hash else None,
                        "updated_at": now_iso,
                        "expires_at": now_iso,
                        "delete_after": _reservation_delete_after(
                            target_status, now=now
                        ),
                    },
                )

                # Phase 2: If we transitioned from active to terminal, update denormalized state
                if (
                    record.status in _ACTIVE_STATUSES
                    and target_status not in _ACTIVE_STATUSES
                ):
                    await self._store.adjust_wallet_state_resource_totals(
                        wallet_scope=wallet_scope,
                        deltas_by_key=_resource_deltas(record.resources, multiplier=-1),
                        updated_at=now_iso,
                    )
                    await self._store.update_wallet_state(
                        wallet_scope=wallet_scope,
                        active_holders={
                            resource.resource_key: None
                            for resource in record.resources
                            if resource.resource_key
                        },
                        updated_at=now_iso,
                    )
            finally:
                await self._store.release_wallet_lock(
                    wallet_scope=wallet_scope,
                    owner=owner,
                )
        else:
            # Fallback/retry if lock is unavailable
            raise RuntimeError(
                f"Could not acquire lock to release reservation {reservation_id}"
            )

        # Phase 3: Update associated wait record
        wait_status = "completed"
        if target_status == "failed":
            wait_status = "failed"
        wait_id = _wait_id_for(
            execution_id=str(record.execution_id),
            node_id=str(record.node_id),
        )
        wait_record = await self._store.get_wait(wait_id)
        if wait_record is not None:
            await self._store.update_wait(
                wait_id,
                updates={
                    "status": wait_status,
                    "updated_at": now_iso,
                    "resume_after": None,
                    "delete_after": _wait_delete_after(wait_status, now=now),
                    "last_error": reason,
                },
            )
            # Refresh wait heads to unblock next in queue
            owner = f"release-wait:{wait_id}:{uuid.uuid4().hex[:8]}"
            if await self._store.acquire_wallet_lock(
                wallet_scope=wait_record.wallet_scope,
                owner=owner,
                ttl_seconds=_DEFAULT_LOCK_TTL_SECONDS,
            ):
                try:
                    await self._refresh_wait_heads_locked(
                        wallet_scope=wait_record.wallet_scope,
                        resource_keys=_wait_resource_keys(wait_record),
                    )
                finally:
                    await self._store.release_wallet_lock(
                        wallet_scope=wait_record.wallet_scope,
                        owner=owner,
                    )

    async def finalize_from_pending_record(
        self, pending_record: Dict[str, Any], *, terminal_status: str
    ) -> None:
        reservation_id = str(pending_record.get("reservation_id") or "").strip()
        if not reservation_id:
            return
        status = str(terminal_status or "").strip().upper()
        final_status = "released"
        if status == "FAILED":
            final_status = "failed"
        elif status == "SUCCESS":
            final_status = "released"
        else:
            final_status = "released"
        await self.release(
            reservation_id,
            status=final_status,
            reason=f"bridge_terminal:{status.lower()}" if status else None,
            tx_hash=str(pending_record.get("tx_hash") or "") or None,
        )

    async def _reconcile_wallet_locked(self, wallet_scope: str) -> None:
        now = utc_now()
        now_iso = iso_utc(now)
        await self._store.expire_records(
            wallet_scope=wallet_scope,
            statuses=_ACTIVE_STATUSES,
            expires_before=now,
            terminal_status="expired",
            delete_after=_reservation_delete_after("expired", now=now),
            reason="lease_expired",
            updated_at=now_iso,
        )

        # Refresh list after expiration
        active_records = await self._store.list_records(
            wallet_scope=wallet_scope,
            statuses=_ACTIVE_STATUSES,
        )
        sorted_records = _sort_records(active_records)
        affected_resource_keys = {
            resource.resource_key
            for record in sorted_records
            for resource in record.resources
            if resource.resource_key
        }
        if not affected_resource_keys:
            # If no active records, ensure totals are zeroed out for previously tracked keys
            # or skip if we want to be more conservative.
            # Better to repair based on actual records found.
            pass

        repaired_totals, _holders = _summarize_active_records(sorted_records)
        repaired_active_holders = _summarize_active_holder_heads(
            sorted_records,
            resource_keys=affected_resource_keys,
        )
        repaired_resource_totals = {
            resource_key: repaired_totals.get(resource_key, 0)
            for resource_key in affected_resource_keys
        }
        await asyncio.gather(
            self._store.replace_resource_totals(
                wallet_scope=wallet_scope,
                totals_by_key=repaired_resource_totals,
                updated_at=now_iso,
            ),
            self._store.update_wallet_state(
                wallet_scope=wallet_scope,
                resource_totals=repaired_resource_totals,
                active_holders={
                    resource_key: repaired_active_holders.get(resource_key)
                    for resource_key in affected_resource_keys
                },
                updated_at=now_iso,
            ),
        )


async def get_reservation_service() -> WalletReservationService:
    global _reservation_service, _reservation_service_lock
    if _reservation_service is not None:
        return _reservation_service
    if _reservation_service_lock is None:
        _reservation_service_lock = asyncio.Lock()
    async with _reservation_service_lock:
        if _reservation_service is None:
            _reservation_service = WalletReservationService()
        return _reservation_service
