from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

from hexbytes import HexBytes

from core.memory.ledger import ErrorCategory
from core.planning.execution_plan import ExecutionState
from core.utils.user_feedback import execution_failed


@dataclass(frozen=True)
class BridgeStatusRuntimeDeps:
    fetch_bridge_status_async: Callable[..., Awaitable[Any]]
    infer_across_deposit_meta: Callable[[str, str], Awaitable[Optional[Dict[str, Any]]]]
    infer_is_testnet: Callable[[Dict[str, Any]], bool]
    normalize_lifi_meta: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]
    normalize_socket_meta: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]
    normalize_pending: Callable[[Dict[str, Any]], Dict[str, Any]]
    normalize_tx_hash: Callable[[str], str]
    apply_status_update: Callable[..., Dict[str, Any]]
    status_changed: Callable[[Dict[str, Any], Dict[str, Any]], bool]
    should_resume_for_transition: Callable[[Dict[str, Any], Dict[str, Any]], bool]
    build_bridge_event: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]]
    parse_iso: Callable[[Optional[str]], Optional[datetime]]
    classify_missing_tx: Callable[..., str]
    resend_min_age_seconds: Callable[[], int]
    should_update_resend_meta: Callable[[Optional[dict], dict], bool]
    should_resend: Callable[..., bool]
    get_resend_attempts: Callable[[Optional[dict]], int]
    broadcast_resend: Callable[..., Awaitable[Any]]
    normalize_signed_tx: Callable[[object], Optional[str]]
    async_broadcast_evm: Callable[..., Awaitable[Any]]
    update_resend_meta: Callable[..., dict]
    update_task_history_tx_hash: Callable[..., Awaitable[bool]]
    get_task_history_collection: Callable[[], Any]
    get_chain_by_name: Callable[[str], Any]
    make_async_web3: Callable[[str], Any]
    create_node_success_state: Callable[[str, Dict[str, Any]], ExecutionState]
    create_node_failure_state: Callable[..., ExecutionState]
    terminal_statuses: set[str]
    finalize_reservation: Optional[Callable[[Dict[str, Any], str], Awaitable[None]]] = (
        None
    )


async def process_pending_records(
    pending_transactions: Iterable[Dict[str, Any]],
    *,
    now_iso: str,
    deps: BridgeStatusRuntimeDeps,
    debug: bool = False,
) -> Tuple[List[Dict[str, Any]], ExecutionState, bool, List[Dict[str, Any]]]:
    updated_pending: List[Dict[str, Any]] = []
    execution_delta = ExecutionState(node_states={})
    resume_needed = False
    terminal_events: List[Dict[str, Any]] = []

    for record in pending_transactions:
        if not isinstance(record, dict):
            continue
        normalized = deps.normalize_pending(record)
        if normalized.get("type") != "bridge":
            updated_pending.append(normalized)
            continue

        status = str(normalized.get("status", "")).upper()
        if status in deps.terminal_statuses:
            if debug:
                print(
                    f"[bridge-status] drop terminal tx={normalized.get('tx_hash')} "
                    f"status={status}"
                )
            continue

        tx_hash = normalized.get("tx_hash")
        protocol = normalized.get("protocol")
        if not tx_hash or not protocol:
            if debug:
                print(
                    f"[bridge-status] skip: missing tx_hash/protocol "
                    f"(tx={tx_hash}, protocol={protocol})"
                )
            updated_pending.append(normalized)
            continue

        meta = normalized.get("meta") or {}
        tx_hash = deps.normalize_tx_hash(str(tx_hash))
        protocol_lower = str(protocol).lower()

        if protocol_lower == "across":
            needs_meta = (
                meta.get("deposit_id") is None or meta.get("origin_chain_id") is None
            )
            source_chain = normalized.get("source_chain") or normalized.get("chain")
            if needs_meta and source_chain:
                inferred = await deps.infer_across_deposit_meta(
                    str(tx_hash), str(source_chain)
                )
                if inferred:
                    meta = {**meta, **inferred}
                    if debug:
                        print(
                            f"[bridge-status] inferred deposit meta tx={tx_hash} "
                            f"origin_chain_id={meta.get('origin_chain_id')} "
                            f"deposit_id={meta.get('deposit_id')}"
                        )
                elif debug:
                    print(
                        f"[bridge-status] could not infer deposit meta tx={tx_hash} "
                        f"source_chain={source_chain}"
                    )
            elif debug and not source_chain:
                print(f"[bridge-status] across missing source_chain for tx={tx_hash}")

        if protocol_lower == "lifi":
            meta = deps.normalize_lifi_meta(
                meta if isinstance(meta, dict) else {}, normalized
            )
        elif protocol_lower == "socket":
            meta = deps.normalize_socket_meta(
                meta if isinstance(meta, dict) else {}, normalized
            )
            if meta.get("fromChainId") is None or meta.get("toChainId") is None:
                if debug:
                    print(
                        "[bridge-status] socket missing chain ids "
                        f"tx={tx_hash} meta={meta}"
                    )
                normalized = {**normalized, "meta": meta}
                updated_pending.append(normalized)
                continue

        if meta != (normalized.get("meta") or {}):
            normalized = {**normalized, "meta": meta}

        is_testnet = deps.infer_is_testnet(normalized)
        if debug:
            print(
                f"[bridge-status] check protocol={protocol} tx={tx_hash} "
                f"is_testnet={is_testnet}"
            )
        result = await deps.fetch_bridge_status_async(
            str(protocol),
            str(tx_hash),
            is_testnet=is_testnet,
            meta=meta,
        )
        if debug:
            print(
                f"[bridge-status] status protocol={protocol} tx={tx_hash} "
                f"raw={result.raw_status} normalized={result.normalized_status}"
            )
        updated = deps.apply_status_update(
            normalized,
            raw_status=result.raw_status,
            normalized_status=result.normalized_status,
            now_iso=now_iso,
        )

        if protocol_lower == "across" and result.raw_status == "not_found":
            updated, meta = await _handle_across_missing_tx(
                normalized=normalized,
                updated=updated,
                meta=meta if isinstance(meta, dict) else {},
                tx_hash=tx_hash,
                now_iso=now_iso,
                deps=deps,
                debug=debug,
            )

        if meta:
            updated["meta"] = meta
        if deps.status_changed(normalized, updated):
            updated_pending.append(updated)
        else:
            updated_pending.append(normalized)

        node_id = updated.get("node_id")
        if node_id and deps.should_resume_for_transition(normalized, updated):
            resume_needed = True
            event = deps.build_bridge_event(updated, now_iso)
            if event:
                terminal_events.append(event)
        if node_id and result.normalized_status == "success":
            if deps.finalize_reservation is not None:
                try:
                    await deps.finalize_reservation(updated, "SUCCESS")
                except Exception:
                    pass
            delta = deps.create_node_success_state(
                node_id,
                {
                    "status": "success",
                    "tx_hash": tx_hash,
                    "message": (
                        f"Bridge finalized (protocol={protocol}, tx={tx_hash})."
                    ),
                },
            )
            execution_delta = execution_delta.merge(delta)
        elif node_id and result.normalized_status == "failed":
            if deps.finalize_reservation is not None:
                try:
                    await deps.finalize_reservation(updated, "FAILED")
                except Exception:
                    pass
            source_chain = normalized.get("source_chain") or normalized.get("chain")
            feedback = execution_failed(
                ErrorCategory.UNKNOWN,
                "bridge",
                str(source_chain) if source_chain else None,
            )
            delta = deps.create_node_failure_state(
                node_id,
                (
                    f"Bridge failed (protocol={protocol}, tx={tx_hash}, "
                    f"status={result.raw_status})."
                ),
                category=ErrorCategory.UNKNOWN.value,
                user_message=feedback.render(),
            )
            execution_delta = execution_delta.merge(delta)

    return updated_pending, execution_delta, resume_needed, terminal_events


async def _handle_across_missing_tx(
    *,
    normalized: Dict[str, Any],
    updated: Dict[str, Any],
    meta: Dict[str, Any],
    tx_hash: str,
    now_iso: str,
    deps: BridgeStatusRuntimeDeps,
    debug: bool,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    source_chain = normalized.get("source_chain") or normalized.get("chain")
    if not source_chain:
        return updated, meta
    try:
        chain = deps.get_chain_by_name(str(source_chain))
        w3 = deps.make_async_web3(chain.rpc_url)
        tx = await w3.eth.get_transaction(HexBytes(str(tx_hash)))
        if tx is None:
            return await _handle_missing_mempool_tx(
                normalized=normalized,
                meta=meta,
                tx_hash=tx_hash,
                now_iso=now_iso,
                w3=w3,
                source_chain=str(source_chain),
                deps=deps,
                debug=debug,
            )
        block_number = getattr(tx, "blockNumber", None)
        if debug:
            print(
                f"[bridge-status] across tx_seen tx={tx_hash} "
                f"chain={source_chain} block={block_number}"
            )
        new_check = {
            "state": "mined" if block_number else "mempool",
            "checked_at": now_iso,
            "block_number": block_number,
        }
        existing_check = (meta or {}).get("resend_check")
        if deps.should_update_resend_meta(existing_check, new_check):
            meta = {**(meta or {}), "resend_check": new_check}
    except Exception as exc:
        if debug:
            print(
                f"[bridge-status] across tx_check_error tx={tx_hash} "
                f"chain={source_chain} err={exc}"
            )
    return updated, meta


async def _handle_missing_mempool_tx(
    *,
    normalized: Dict[str, Any],
    meta: Dict[str, Any],
    tx_hash: str,
    now_iso: str,
    w3: Any,
    source_chain: str,
    deps: BridgeStatusRuntimeDeps,
    debug: bool,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    submitted_at = normalized.get("submitted_at")
    nonce = normalized.get("nonce")
    age_note = ""
    if submitted_at:
        age_note = f" submitted_at={submitted_at}"
    if nonce is not None:
        age_note += f" nonce={nonce}"
    parsed = deps.parse_iso(submitted_at)
    age_seconds = None
    if parsed:
        age_seconds = (datetime.now(timezone.utc) - parsed).total_seconds()

    pending_nonce = None
    latest_nonce = None
    sender = normalized.get("sender")
    if sender:
        try:
            pending_nonce = int(await w3.eth.get_transaction_count(sender, "pending"))
            latest_nonce = int(await w3.eth.get_transaction_count(sender, "latest"))
        except Exception:
            pending_nonce = None
            latest_nonce = None

    classification = deps.classify_missing_tx(
        age_seconds=age_seconds,
        tx_nonce=nonce if isinstance(nonce, int) else None,
        pending_nonce=pending_nonce,
        latest_nonce=latest_nonce,
        min_age_seconds=deps.resend_min_age_seconds(),
    )
    if debug:
        print(
            f"[bridge-status] across tx_not_found tx={tx_hash} "
            f"chain={source_chain}{age_note} age_s={age_seconds} "
            f"pending_nonce={pending_nonce} latest_nonce={latest_nonce} "
            f"classify={classification}"
        )

    new_check = {
        "state": classification,
        "checked_at": now_iso,
        "age_seconds": age_seconds,
        "pending_nonce": pending_nonce,
        "latest_nonce": latest_nonce,
    }
    existing_check = (meta or {}).get("resend_check")
    if deps.should_update_resend_meta(existing_check, new_check):
        meta = {**(meta or {}), "resend_check": new_check}

    raw_tx = normalized.get("raw_tx")
    if not deps.should_resend(
        classification=classification,
        raw_tx=raw_tx,
        meta=meta,
    ):
        return normalized, meta

    try:
        resend_hash = await _resend_bridge_transaction(
            normalized=normalized,
            meta=meta,
            w3=w3,
            deps=deps,
        )
        if debug:
            print(f"[bridge-status] resend broadcast tx={resend_hash} orig={tx_hash}")
        meta = deps.update_resend_meta(
            meta,
            tx_hash=resend_hash,
            error=None,
            now_iso=now_iso,
        )
        if resend_hash:
            normalized = dict(normalized)
            normalized["tx_hash"] = resend_hash
            history_updated = await deps.update_task_history_tx_hash(
                collection=deps.get_task_history_collection(),
                execution_id=normalized.get("execution_id"),
                node_id=normalized.get("node_id"),
                tx_hash=resend_hash,
                now_iso=now_iso,
                status="RESUBMITTED",
            )
            if debug:
                print(
                    f"[bridge-status] history_tx_update tx={resend_hash} "
                    f"updated={history_updated}"
                )
    except Exception as exc:
        if debug:
            print(f"[bridge-status] resend error tx={tx_hash} err={exc}")
        meta = deps.update_resend_meta(
            meta, tx_hash=None, error=str(exc), now_iso=now_iso
        )

    return normalized, meta


async def _resend_bridge_transaction(
    *,
    normalized: Dict[str, Any],
    meta: Dict[str, Any],
    w3: Any,
    deps: BridgeStatusRuntimeDeps,
) -> str:
    sub_org_id = normalized.get("sub_org_id")
    sender_addr = normalized.get("sender")
    tx_payload = normalized.get("tx_payload")
    attempts = deps.get_resend_attempts(meta)
    if tx_payload and sub_org_id and sender_addr:
        return await deps.broadcast_resend(
            w3=w3,
            tx_payload=tx_payload,
            sub_org_id=str(sub_org_id),
            sender=str(sender_addr),
            attempts=attempts + 1,
        )

    raw_signed = deps.normalize_signed_tx(normalized.get("raw_tx"))
    if not raw_signed:
        raise ValueError("Signed transaction payload is missing or invalid.")
    return await deps.async_broadcast_evm(w3, raw_signed)
