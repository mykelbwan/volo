from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
from uuid import uuid4

from core.idempotency.store import IdempotencyStore, compute_args_hash
from wallet_service.common.messages import format_with_recovery, require_non_empty_str

logger = logging.getLogger("volo.transfer_idempotency")


@dataclass(frozen=True)
class TransferIdempotencyClaim:
    """
    Result of claiming an idempotency key for a transfer submission.
    """

    raw_key: str
    scoped_key: str
    request_hash: str
    reused: bool
    tx_hash: str | None = None
    status: str | None = None
    result: dict[str, Any] | None = None


def build_transfer_request_id(request_id: Any | None = None) -> str:
    raw = str(request_id).strip() if request_id is not None else ""
    if raw:
        return raw
    return uuid4().hex


def canonicalize_decimal_idempotency_value(value: Any) -> str:
    try:
        decimal_value = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError, AttributeError) as exc:
        raise ValueError(f"invalid decimal value for idempotency: {value!r}") from exc
    if not decimal_value.is_finite():
        raise ValueError(f"invalid decimal value for idempotency: {value!r}")
    text = format(decimal_value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _scoped_idempotency_key(operation: str, raw_key: str) -> str:
    digest = hashlib.sha256(
        f"wallet-transfer:{operation}:{raw_key}".encode("utf-8")
    ).hexdigest()
    return digest


def build_deterministic_transfer_key(
    *,
    tool_name: str,
    request_fields: Mapping[str, Any],
) -> str:
    """
    Build a deterministic idempotency key for state-changing tool invocations.

    The caller decides the scope fields (wallet, chain, token pair, amount,
    destination, etc.). We hash the canonical payload so retries across
    processes/instances converge on the same key.
    """
    payload = {"tool_name": str(tool_name), **dict(request_fields)}
    return compute_args_hash(payload)


def resolve_transfer_idempotency(
    *,
    tool_name: str,
    request_fields: Mapping[str, Any],
    external_key: Any | None = None,
    request_id: Any | None = None,
) -> tuple[str | None, dict[str, Any], str | None]:
    fields = dict(request_fields)
    if external_key is not None and str(external_key).strip():
        return (
            require_non_empty_str(external_key, field="idempotency_key"),
            fields,
            None,
        )

    resolved_request_id = build_transfer_request_id(request_id)
    return (
        build_deterministic_transfer_key(
            tool_name=tool_name,
            request_fields=fields,
        ),
        fields,
        resolved_request_id,
    )


def resume_transfer_idempotency_claim(
    claim: TransferIdempotencyClaim | None,
) -> TransferIdempotencyClaim | None:
    if claim is None or not claim.reused:
        return claim
    return TransferIdempotencyClaim(
        raw_key=claim.raw_key,
        scoped_key=claim.scoped_key,
        request_hash=claim.request_hash,
        reused=False,
        tx_hash=claim.tx_hash,
        status=claim.status,
        result=dict(claim.result or {}),
    )


async def claim_transfer_idempotency(
    *,
    operation: str,
    idempotency_key: str | None,
    request_fields: Mapping[str, Any],
    store: IdempotencyStore | None = None,
) -> TransferIdempotencyClaim | None:
    if idempotency_key is None or not str(idempotency_key).strip():
        logger.info(
            "idempotency_skip operation=%s reason=no_key",
            operation,
        )
        return None

    raw_key = require_non_empty_str(idempotency_key, field="idempotency_key")
    request_hash = compute_args_hash(dict(request_fields))
    scoped_key = _scoped_idempotency_key(operation, raw_key)

    try:
        idempotency_store = store or IdempotencyStore()
        record, created = await idempotency_store.aclaim(
            key=scoped_key,
            metadata={
                "raw_key": raw_key,
                "operation": operation,
                "request_hash": request_hash,
            },
        )
    except Exception as exc:
        raise RuntimeError(
            format_with_recovery(
                "Idempotency storage is unavailable",
                "retry after restoring idempotency storage health to avoid duplicate transfers",
            )
        ) from exc

    existing_hash = None
    if isinstance(record.metadata, dict):
        existing_hash = record.metadata.get("request_hash")
    if existing_hash and str(existing_hash) != request_hash:
        logger.warning(
            "idempotency_conflict operation=%s key=%s request_hash=%s existing_hash=%s",
            operation,
            scoped_key,
            request_hash,
            existing_hash,
        )
        raise ValueError(
            format_with_recovery(
                "Idempotency key was already used for a different transfer payload",
                "retry with a new idempotency_key for the changed transfer request",
            )
        )

    if created:
        logger.info(
            "idempotency_miss operation=%s key=%s request_hash=%s",
            operation,
            scoped_key,
            request_hash,
        )
        return TransferIdempotencyClaim(
            raw_key=raw_key,
            scoped_key=scoped_key,
            request_hash=request_hash,
            reused=False,
        )

    if record.status == "success" and record.tx_hash:
        logger.info(
            "idempotency_hit operation=%s key=%s status=%s tx_hash=%s",
            operation,
            scoped_key,
            record.status,
            record.tx_hash,
        )
        return TransferIdempotencyClaim(
            raw_key=raw_key,
            scoped_key=scoped_key,
            request_hash=request_hash,
            reused=True,
            tx_hash=record.tx_hash,
            status=record.status,
            result=dict(record.result or {}),
        )

    if record.status == "pending" and record.tx_hash:
        logger.info(
            "idempotency_inflight operation=%s key=%s status=%s tx_hash=%s",
            operation,
            scoped_key,
            record.status,
            record.tx_hash,
        )
        return TransferIdempotencyClaim(
            raw_key=raw_key,
            scoped_key=scoped_key,
            request_hash=request_hash,
            reused=True,
            tx_hash=record.tx_hash,
            status=record.status,
            result=dict(record.result or {}),
        )

    if record.status == "pending":
        logger.info(
            "idempotency_busy operation=%s key=%s status=%s",
            operation,
            scoped_key,
            record.status,
        )
        raise RuntimeError(
            format_with_recovery(
                "A transfer with this idempotency_key is already in progress",
                "wait for the earlier submission to settle or retry with a new idempotency_key",
            )
        )

    logger.info(
        "idempotency_failed_prior operation=%s key=%s status=%s",
        operation,
        scoped_key,
        record.status,
    )
    raise RuntimeError(
        format_with_recovery(
            "A previous transfer attempt with this idempotency_key failed",
            "inspect the earlier failure before retrying with the same payment intent",
        )
    )


async def mark_transfer_inflight(
    claim: TransferIdempotencyClaim | None,
    *,
    tx_hash: str,
    result: Mapping[str, Any] | None = None,
    store: IdempotencyStore | None = None,
) -> None:
    if claim is None or claim.reused:
        return
    idempotency_store = store or IdempotencyStore()
    payload = dict(result or {})
    payload.setdefault("tx_hash", str(tx_hash))
    payload.setdefault("status", "pending")
    logger.info(
        "idempotency_mark_inflight key=%s tx_hash=%s",
        claim.scoped_key,
        tx_hash,
    )
    await idempotency_store.amark_inflight(
        key=claim.scoped_key,
        tx_hash=str(tx_hash),
        result=payload,
    )


async def mark_transfer_success(
    claim: TransferIdempotencyClaim | None,
    *,
    tx_hash: str,
    result: Mapping[str, Any] | None = None,
    store: IdempotencyStore | None = None,
) -> None:
    if claim is None or claim.reused:
        return
    payload = dict(result or {})
    payload.setdefault("tx_hash", str(tx_hash))
    payload.setdefault("status", "success")
    idempotency_store = store or IdempotencyStore()
    logger.info(
        "idempotency_mark_success key=%s tx_hash=%s",
        claim.scoped_key,
        tx_hash,
    )
    await idempotency_store.amark_success(key=claim.scoped_key, result=payload)


async def mark_transfer_failed(
    claim: TransferIdempotencyClaim | None,
    *,
    error: str,
    store: IdempotencyStore | None = None,
) -> None:
    if claim is None or claim.reused:
        return
    idempotency_store = store or IdempotencyStore()
    logger.info(
        "idempotency_mark_failed key=%s error=%s",
        claim.scoped_key,
        str(error),
    )
    await idempotency_store.amark_failed(key=claim.scoped_key, error=str(error))


async def load_transfer_idempotency_claim(
    claim: TransferIdempotencyClaim | None,
    *,
    store: IdempotencyStore | None = None,
) -> TransferIdempotencyClaim | None:
    if claim is None:
        return None
    idempotency_store = store or IdempotencyStore()
    record = await idempotency_store.aget(key=claim.scoped_key)
    if record is None:
        return None
    return TransferIdempotencyClaim(
        raw_key=claim.raw_key,
        scoped_key=claim.scoped_key,
        request_hash=claim.request_hash,
        reused=claim.reused,
        tx_hash=record.tx_hash,
        status=record.status,
        result=dict(record.result or {}),
    )
