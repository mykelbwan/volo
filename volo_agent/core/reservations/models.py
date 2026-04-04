from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List

from core.reservations.common import safe_int


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | None = None) -> str:
    return (value or utc_now()).astimezone(timezone.utc).isoformat()


def parse_iso(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def decimal_str(value: Any) -> str:
    try:
        return str(Decimal(str(value)))
    except Exception:
        return "0"


def _coerce_meta(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _coerce_resource_snapshots(value: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(value, dict):
        return {}
    snapshots: Dict[str, Dict[str, Any]] = {}
    for key, snapshot in value.items():
        if isinstance(snapshot, dict):
            snapshots[str(key)] = dict(snapshot)
    return snapshots


@dataclass(frozen=True)
class ResourceSnapshot:
    resource_key: str
    wallet_scope: str
    sender: str
    chain: str
    token_ref: str
    symbol: str
    decimals: int
    available: str
    available_base_units: int
    observed_at: str
    reserved: str = "0"
    reserved_base_units: int = 0
    net_available: str | None = None
    net_available_base_units: int | None = None
    chain_family: str | None = None

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "resource_key": self.resource_key,
            "wallet_scope": self.wallet_scope,
            "sender": self.sender,
            "chain": self.chain,
            "token_ref": self.token_ref,
            "symbol": self.symbol,
            "decimals": int(self.decimals),
            "available": str(self.available),
            "available_base_units": str(int(self.available_base_units)),
            "reserved": str(self.reserved),
            "reserved_base_units": str(int(self.reserved_base_units)),
            "net_available": str(
                self.net_available if self.net_available is not None else self.available
            ),
            "net_available_base_units": str(
                int(
                    self.net_available_base_units
                    if self.net_available_base_units is not None
                    else self.available_base_units
                )
            ),
            "observed_at": self.observed_at,
        }
        if self.chain_family:
            payload["chain_family"] = self.chain_family
        return payload

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ResourceSnapshot":
        return cls(
            resource_key=str(payload.get("resource_key") or "").strip().lower(),
            wallet_scope=str(payload.get("wallet_scope") or "").strip().lower(),
            sender=str(payload.get("sender") or "").strip().lower(),
            chain=str(payload.get("chain") or "").strip().lower(),
            token_ref=str(payload.get("token_ref") or "").strip().lower(),
            symbol=str(payload.get("symbol") or "").strip().upper(),
            decimals=max(0, safe_int(payload.get("decimals"), default=0)),
            available=decimal_str(payload.get("available")),
            available_base_units=max(
                0, safe_int(payload.get("available_base_units"), default=0)
            ),
            reserved=decimal_str(payload.get("reserved")),
            reserved_base_units=max(
                0, safe_int(payload.get("reserved_base_units"), default=0)
            ),
            net_available=decimal_str(
                payload.get("net_available")
                if payload.get("net_available") is not None
                else payload.get("available")
            ),
            net_available_base_units=max(
                0,
                safe_int(
                    payload.get("net_available_base_units")
                    if payload.get("net_available_base_units") is not None
                    else payload.get("available_base_units"),
                    default=0,
                ),
            ),
            observed_at=str(payload.get("observed_at") or iso_utc()),
            chain_family=(
                str(payload.get("chain_family")).strip().lower()
                if payload.get("chain_family")
                else None
            ),
        )


@dataclass(frozen=True)
class ReservationRequirement:
    resource_key: str
    wallet_scope: str
    sender: str
    chain: str
    token_ref: str
    symbol: str
    decimals: int
    required: str
    required_base_units: int
    kind: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resource_key": self.resource_key,
            "wallet_scope": self.wallet_scope,
            "sender": self.sender,
            "chain": self.chain,
            "token_ref": self.token_ref,
            "symbol": self.symbol,
            "decimals": int(self.decimals),
            "required": str(self.required),
            "required_base_units": str(int(self.required_base_units)),
            "kind": self.kind,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ReservationRequirement":
        return cls(
            resource_key=str(payload.get("resource_key") or "").strip().lower(),
            wallet_scope=str(payload.get("wallet_scope") or "").strip().lower(),
            sender=str(payload.get("sender") or "").strip().lower(),
            chain=str(payload.get("chain") or "").strip().lower(),
            token_ref=str(payload.get("token_ref") or "").strip().lower(),
            symbol=str(payload.get("symbol") or "").strip().upper(),
            decimals=max(0, safe_int(payload.get("decimals"), default=0)),
            required=decimal_str(payload.get("required")),
            required_base_units=max(
                0, safe_int(payload.get("required_base_units"), default=0)
            ),
            kind=str(payload.get("kind") or "spend").strip().lower(),
        )


@dataclass
class ReservationRecord:
    reservation_id: str
    wallet_scope: str
    execution_id: str
    thread_id: str
    conversation_id: str | None
    task_number: int | None
    title: str | None
    node_id: str
    tool: str
    status: str
    resources: List[ReservationRequirement]
    created_at: str
    updated_at: str
    expires_at: str
    delete_after: str | None = None
    tx_hash: str | None = None
    reason: str | None = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "reservation_id": self.reservation_id,
            "wallet_scope": self.wallet_scope,
            "execution_id": self.execution_id,
            "thread_id": self.thread_id,
            "conversation_id": self.conversation_id,
            "task_number": self.task_number,
            "title": self.title,
            "node_id": self.node_id,
            "tool": self.tool,
            "status": self.status,
            "resources": [item.to_dict() for item in self.resources],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "tx_hash": self.tx_hash,
            "reason": self.reason,
            "meta": dict(self.meta or {}),
        }
        if self.delete_after is not None:
            payload["delete_after"] = self.delete_after
        return payload

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ReservationRecord":
        resources_raw = payload.get("resources") or []
        resources = []
        if isinstance(resources_raw, Iterable):
            for item in resources_raw:
                if isinstance(item, dict):
                    resources.append(ReservationRequirement.from_dict(item))
        return cls(
            reservation_id=str(payload.get("reservation_id") or "").strip(),
            wallet_scope=str(payload.get("wallet_scope") or "").strip().lower(),
            execution_id=str(payload.get("execution_id") or "").strip(),
            thread_id=str(payload.get("thread_id") or "").strip(),
            conversation_id=(
                str(payload.get("conversation_id")).strip()
                if payload.get("conversation_id") is not None
                else None
            ),
            task_number=(
                safe_int(payload.get("task_number"))
                if payload.get("task_number") is not None
                else None
            ),
            title=(
                str(payload.get("title")).strip()
                if payload.get("title") is not None
                else None
            ),
            node_id=str(payload.get("node_id") or "").strip(),
            tool=str(payload.get("tool") or "").strip().lower(),
            status=str(payload.get("status") or "").strip().lower(),
            resources=resources,
            created_at=str(payload.get("created_at") or iso_utc()),
            updated_at=str(payload.get("updated_at") or iso_utc()),
            expires_at=str(payload.get("expires_at") or iso_utc()),
            delete_after=(
                iso_utc(parsed)
                if (parsed := parse_iso(payload.get("delete_after"))) is not None
                else (
                    str(payload.get("delete_after")).strip()
                    if payload.get("delete_after") is not None
                    else None
                )
            ),
            tx_hash=(
                str(payload.get("tx_hash")).strip()
                if payload.get("tx_hash") is not None
                else None
            ),
            reason=(
                str(payload.get("reason")).strip()
                if payload.get("reason") is not None
                else None
            ),
            meta=_coerce_meta(payload.get("meta")),
        )


@dataclass
class FundsWaitRecord:
    wait_id: str
    wallet_scope: str
    conversation_id: str
    thread_id: str
    execution_id: str
    node_id: str
    task_number: int | None
    title: str | None
    tool: str
    status: str
    resources: List[ReservationRequirement]
    resource_snapshots: Dict[str, Dict[str, Any]]
    created_at: str
    updated_at: str
    resume_token: str | None = None
    resume_after: str | None = None
    delete_after: str | None = None
    last_error: str | None = None
    attempts: int = 0
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "wait_id": self.wait_id,
            "resume_token": self.resume_token,
            "wallet_scope": self.wallet_scope,
            "conversation_id": self.conversation_id,
            "thread_id": self.thread_id,
            "execution_id": self.execution_id,
            "node_id": self.node_id,
            "task_number": self.task_number,
            "title": self.title,
            "tool": self.tool,
            "status": self.status,
            "resources": [item.to_dict() for item in self.resources],
            "resource_snapshots": dict(self.resource_snapshots or {}),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "resume_after": self.resume_after,
            "last_error": self.last_error,
            "attempts": int(self.attempts),
            "meta": dict(self.meta or {}),
        }
        if self.delete_after is not None:
            payload["delete_after"] = self.delete_after
        return payload

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "FundsWaitRecord":
        resources_raw = payload.get("resources") or []
        resources = []
        if isinstance(resources_raw, Iterable):
            for item in resources_raw:
                if isinstance(item, dict):
                    resources.append(ReservationRequirement.from_dict(item))
        snapshots = _coerce_resource_snapshots(payload.get("resource_snapshots"))
        return cls(
            wait_id=str(payload.get("wait_id") or "").strip(),
            resume_token=(
                str(payload.get("resume_token")).strip()
                if payload.get("resume_token") is not None
                else None
            ),
            wallet_scope=str(payload.get("wallet_scope") or "").strip().lower(),
            conversation_id=str(payload.get("conversation_id") or "").strip(),
            thread_id=str(payload.get("thread_id") or "").strip(),
            execution_id=str(payload.get("execution_id") or "").strip(),
            node_id=str(payload.get("node_id") or "").strip(),
            task_number=(
                safe_int(payload.get("task_number"))
                if payload.get("task_number") is not None
                else None
            ),
            title=(
                str(payload.get("title")).strip()
                if payload.get("title") is not None
                else None
            ),
            tool=str(payload.get("tool") or "").strip().lower(),
            status=str(payload.get("status") or "").strip().lower(),
            resources=resources,
            resource_snapshots=snapshots,
            created_at=str(payload.get("created_at") or iso_utc()),
            updated_at=str(payload.get("updated_at") or iso_utc()),
            resume_after=(
                str(payload.get("resume_after")).strip()
                if payload.get("resume_after") is not None
                else None
            ),
            delete_after=(
                iso_utc(parsed)
                if (parsed := parse_iso(payload.get("delete_after"))) is not None
                else (
                    str(payload.get("delete_after")).strip()
                    if payload.get("delete_after") is not None
                    else None
                )
            ),
            last_error=(
                str(payload.get("last_error")).strip()
                if payload.get("last_error") is not None
                else None
            ),
            attempts=max(0, safe_int(payload.get("attempts"), default=0)),
            meta=_coerce_meta(payload.get("meta")),
        )


@dataclass(frozen=True)
class ReservationConflict:
    resource_key: str
    required_base_units: int
    available_base_units: int
    reserved_base_units: int
    holders: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resource_key": self.resource_key,
            "required_base_units": str(int(self.required_base_units)),
            "available_base_units": str(int(self.available_base_units)),
            "reserved_base_units": str(int(self.reserved_base_units)),
            "holders": list(self.holders),
        }


@dataclass(frozen=True)
class ReservationClaimResult:
    acquired: bool
    reservation_id: str | None = None
    reused_existing: bool = False
    deferred_reason: str | None = None
    conflicts: List[ReservationConflict] = field(default_factory=list)
    store_unavailable: bool = False
    wait_id: str | None = None
