"""
Bridge Status Worker
--------------------
Polls bridge transactions recorded in LangGraph state and updates their status
per tick. Intended to run as a separate process.
this tool is used in development only.

Usage:
  uv run command_line_tools/bridge_status_worker.py --interval 15
  uv run command_line_tools/bridge_status_worker.py --once
  BRIDGE_STATUS_WORKER_DEBUG=1 uv run command_line_tools/bridge_status_worker.py --interval 15
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

try:
    from dotenv import find_dotenv, load_dotenv
except Exception:  # pragma: no cover - optional dependency
    load_dotenv = None
    find_dotenv = None


def _load_local_env() -> None:
    if load_dotenv is None:
        return
    try:
        env_path = find_dotenv(usecwd=True) if find_dotenv else ""
        load_dotenv(env_path or None)
    except Exception:
        pass


# Avoid noisy background analytics tasks when resuming async execution.
os.environ.setdefault("DISABLE_CDP_USAGE_TRACKING", "true")
os.environ.setdefault("DISABLE_CDP_ERROR_REPORTING", "true")

from eth_typing import ABIEvent  # noqa: E402
from hexbytes import HexBytes  # noqa: E402
from langgraph.types import Command  # noqa: E402
from web3._utils.events import (  # noqa: E402
    event_abi_to_log_topic,  # type: ignore[attr-defined]
    get_event_data,  # type: ignore[attr-defined]
)

from config.abi import ACROSS_SPOKE_POOL_ABI  # noqa: E402
from config.chains import get_chain_by_name  # noqa: E402
from core.bridge_status_runtime import (  # noqa: E402
    BridgeStatusRuntimeDeps,
    process_pending_records,
)
from core.bridge_status_worker_locks import (  # noqa: E402
    acquire_bridge_status_worker_lock as _acquire_lock,
)
from core.bridge_status_worker_locks import (  # noqa: E402
    ensure_bridge_status_worker_lock_indexes as _ensure_lock_indexes,
)
from core.bridge_status_worker_runtime import (  # noqa: E402
    BridgeStatusWorkerRuntimeDeps,
    run_bridge_status_tick,
)
from core.bridge_status_worker_service import (  # noqa: E402
    run_bridge_status_worker_loop,
)
from core.database.mongodb_async import AsyncMongoDB  # noqa: E402
from core.history.task_history import (  # noqa: E402
    update_task_history_terminal_status as _update_task_history_terminal_status,
)
from core.history.task_history import (  # noqa: E402
    update_task_history_tx_hash as _update_task_history_tx_hash,
)
from core.planning.execution_plan import (  # noqa: E402
    ExecutionState,
    check_plan_complete,
    create_node_failure_state,
    create_node_success_state,
)
from core.reservations.service import get_reservation_service  # noqa: E402
from core.tasks.registry import (  # noqa: E402
    ConversationTaskRegistry,
    resolve_conversation_id,
)
from core.utils.bridge_status_registry import fetch_bridge_status_async  # noqa: E402
from core.utils.event_stream import publish_event  # noqa: E402
from core.utils.evm_async import async_broadcast_evm, make_async_web3  # noqa: E402
from wallet_service.evm.sign_tx import sign_transaction_async  # noqa: E402

_CHECKPOINT_COLLECTION = "lg_checkpoints"

_TERMINAL_STATUSES = {"SUCCESS", "FAILED"}

_ACROSS_V3_EVENT_ABI = next(
    (
        item
        for item in ACROSS_SPOKE_POOL_ABI
        if item.get("type") == "event" and item.get("name") == "V3FundsDeposited"
    ),
    None,
)
_ACROSS_V3_EVENT_TOPIC = (
    event_abi_to_log_topic(cast(ABIEvent, _ACROSS_V3_EVENT_ABI))
    if _ACROSS_V3_EVENT_ABI
    else None
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _finalize_reservation(
    pending_record: Dict[str, Any], terminal_status: str
) -> None:
    service = await get_reservation_service()
    await service.finalize_from_pending_record(
        pending_record,
        terminal_status=str(terminal_status or "").strip().upper(),
    )


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _resend_min_age_seconds() -> int:
    raw = os.getenv("BRIDGE_RESEND_MIN_AGE_SECONDS", "").strip()
    if not raw:
        return 360
    try:
        value = int(raw)
    except ValueError:
        return 360
    return value if value > 0 else 360


def _resend_retry_interval_seconds() -> int:
    raw = os.getenv("BRIDGE_RESEND_RETRY_INTERVAL_SECONDS", "").strip()
    if not raw:
        return 180
    try:
        value = int(raw)
    except ValueError:
        return 180
    return value if value > 0 else 180


def _resend_max_attempts() -> int:
    raw = os.getenv("BRIDGE_RESEND_MAX_ATTEMPTS", "").strip()
    if not raw:
        return 2
    try:
        value = int(raw)
    except ValueError:
        return 2
    return value if value > 0 else 2


def _resend_bump_pct() -> float:
    raw = os.getenv("BRIDGE_RESEND_BUMP_PCT", "").strip()
    if not raw:
        return 20.0
    try:
        value = float(raw)
    except ValueError:
        return 20.0
    return value if value > 0 else 20.0


def _resend_max_multiplier() -> float:
    raw = os.getenv("BRIDGE_RESEND_MAX_MULTIPLIER", "").strip()
    if not raw:
        return 2.0
    try:
        value = float(raw)
    except ValueError:
        return 2.0
    return value if value > 1.0 else 2.0


def _resend_enabled() -> bool:
    return os.getenv("BRIDGE_RESEND_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _get_resend_attempts(meta: Optional[dict]) -> int:
    if not isinstance(meta, dict):
        return 0
    resend = meta.get("resend")
    if not isinstance(resend, dict):
        return 0
    return int(resend.get("attempts") or 0)


def _last_resend_at(meta: Optional[dict]) -> Optional[str]:
    if not isinstance(meta, dict):
        return None
    resend = meta.get("resend")
    if not isinstance(resend, dict):
        return None
    return resend.get("last_attempt_at")


def _should_resend(
    *,
    classification: str,
    raw_tx: Optional[str],
    meta: Optional[dict],
) -> bool:
    if not _resend_enabled():
        return False
    if not raw_tx:
        return False
    if classification != "safe_to_resend":
        return False
    attempts = _get_resend_attempts(meta)
    if attempts >= _resend_max_attempts():
        return False
    last_attempt = _parse_iso(_last_resend_at(meta))
    if last_attempt:
        elapsed = (datetime.now(timezone.utc) - last_attempt).total_seconds()
        if elapsed < _resend_retry_interval_seconds():
            return False
    return True


def _update_resend_meta(
    meta: Optional[dict],
    *,
    tx_hash: Optional[str],
    error: Optional[str],
    now_iso: str,
) -> dict:
    base = meta if isinstance(meta, dict) else {}
    attempts = _get_resend_attempts(base) + 1
    payload = {
        "attempts": attempts,
        "last_attempt_at": now_iso,
    }
    if tx_hash:
        payload["last_tx_hash"] = tx_hash
    if error:
        payload["last_error"] = error
    return {**base, "resend": payload}


def _apply_fee_bump(tx_payload: dict, *, attempts: int) -> dict:
    if not isinstance(tx_payload, dict):
        return tx_payload
    bump_pct = _resend_bump_pct()
    max_mult = _resend_max_multiplier()
    factor = 1.0 + (bump_pct / 100.0) * max(attempts, 1)
    if factor > max_mult:
        factor = max_mult

    updated = dict(tx_payload)

    def _bump(value: int) -> int:
        bumped = int(value * factor)
        return max(bumped, value + 1)

    if "maxFeePerGas" in updated and "maxPriorityFeePerGas" in updated:
        updated["maxFeePerGas"] = _bump(int(updated["maxFeePerGas"]))
        updated["maxPriorityFeePerGas"] = _bump(int(updated["maxPriorityFeePerGas"]))
    elif "gasPrice" in updated:
        updated["gasPrice"] = _bump(int(updated["gasPrice"]))

    return updated


async def _broadcast_resend(
    *,
    w3: Any,
    tx_payload: dict,
    sub_org_id: str,
    sender: str,
    attempts: int,
) -> str:
    bumped = _apply_fee_bump(tx_payload, attempts=attempts)
    signed = await sign_transaction_async(sub_org_id, bumped, sender)
    return await async_broadcast_evm(w3, signed)


def _classify_missing_tx(
    *,
    age_seconds: Optional[float],
    tx_nonce: Optional[int],
    pending_nonce: Optional[int],
    latest_nonce: Optional[int],
    min_age_seconds: int,
) -> str:
    if age_seconds is None:
        return "unknown_age"
    if age_seconds < min_age_seconds:
        return "too_soon"
    if tx_nonce is None or pending_nonce is None or latest_nonce is None:
        return "missing_nonce"
    if pending_nonce > tx_nonce or latest_nonce > tx_nonce:
        return "replaced"
    if pending_nonce == tx_nonce and latest_nonce == tx_nonce:
        return "safe_to_resend"
    return "nonce_unknown"


def _should_update_resend_meta(existing: Optional[dict], new_payload: dict) -> bool:
    if not existing:
        return True
    for key in ("state", "pending_nonce", "latest_nonce", "block_number"):
        if existing.get(key) != new_payload.get(key):
            return True
    return False


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


async def _distinct_thread_pairs() -> List[Tuple[str, str]]:
    collection = AsyncMongoDB.get_collection(_CHECKPOINT_COLLECTION)
    pairs: List[Tuple[str, str]] = []
    async for doc in collection.aggregate(
        [
            {
                "$group": {
                    "_id": {
                        "thread_id": "$thread_id",
                        "checkpoint_ns": "$checkpoint_ns",
                    }
                }
            }
        ]
    ):
        key = doc.get("_id") or {}
        thread_id = key.get("thread_id")
        checkpoint_ns = key.get("checkpoint_ns") or ""
        if thread_id:
            pairs.append((str(thread_id), str(checkpoint_ns)))
    return pairs


def _infer_is_testnet(pending: Dict[str, Any]) -> bool:
    raw = pending.get("is_testnet")
    if isinstance(raw, bool):
        return raw
    for key in ("source_chain", "dest_chain", "chain"):
        name = pending.get(key)
        if not name:
            continue
        try:
            chain = get_chain_by_name(str(name))
            return bool(chain.is_testnet)
        except Exception:
            continue
    return False


def _infer_chain_id(chain_name: Optional[str]) -> Optional[int]:
    if not chain_name:
        return None
    try:
        chain = get_chain_by_name(str(chain_name))
        return int(chain.chain_id)
    except Exception:
        return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _normalize_lifi_meta(
    meta: Dict[str, Any], pending: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Ensure Li.Fi status polling has fromChain/toChain when available.
    Li.Fi only needs txHash; chains are optional but speed up lookups.
    """
    updated = dict(meta)

    if updated.get("fromChain") is None:
        if updated.get("fromChainId") is not None:
            updated["fromChain"] = _coerce_int(updated.get("fromChainId"))
        elif pending.get("source_chain_id") is not None:
            updated["fromChain"] = _coerce_int(pending.get("source_chain_id"))
        else:
            src = pending.get("source_chain") or pending.get("chain")
            cid = _infer_chain_id(src)
            if cid is not None:
                updated["fromChain"] = cid

    if updated.get("toChain") is None:
        if updated.get("toChainId") is not None:
            updated["toChain"] = _coerce_int(updated.get("toChainId"))
        elif pending.get("dest_chain_id") is not None:
            updated["toChain"] = _coerce_int(pending.get("dest_chain_id"))
        else:
            dst = pending.get("dest_chain")
            cid = _infer_chain_id(dst)
            if cid is not None:
                updated["toChain"] = cid

    return updated


def _normalize_tx_hash(tx_hash: str) -> str:
    h = (tx_hash or "").strip()
    if not h:
        return h
    if h.startswith("0x"):
        return h
    if len(h) == 64:
        return f"0x{h}"
    return h


def _normalize_signed_tx(raw_tx: object) -> Optional[str]:
    if raw_tx is None:
        return None
    if isinstance(raw_tx, bytes):
        return "0x" + raw_tx.hex()
    s = str(raw_tx).strip()
    if not s:
        return None
    if s.startswith("0x"):
        return s
    if all(c in "0123456789abcdefABCDEF" for c in s):
        return f"0x{s}"
    return None


def _normalize_pending(pending: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(pending)
    if "status" not in normalized:
        normalized["status"] = "PENDING"
    if "last_checked_at" not in normalized:
        normalized["last_checked_at"] = None
    if "last_status" not in normalized:
        normalized["last_status"] = None
    return normalized


def _apply_status_update(
    pending: Dict[str, Any],
    *,
    raw_status: Optional[str],
    normalized_status: Optional[str],
    now_iso: str,
) -> Dict[str, Any]:
    updated = dict(pending)
    updated["last_status"] = normalized_status
    updated["last_status_raw"] = raw_status
    updated["last_checked_at"] = now_iso
    if normalized_status == "success":
        updated["status"] = "SUCCESS"
        updated["finalized_at"] = now_iso
    elif normalized_status == "failed":
        updated["status"] = "FAILED"
        updated["finalized_at"] = now_iso
    else:
        updated["status"] = updated.get("status") or "PENDING"
    return updated


def _status_changed(before: Dict[str, Any], after: Dict[str, Any]) -> bool:
    keys = ("status", "last_status", "last_status_raw", "finalized_at")
    for key in keys:
        if before.get(key) != after.get(key):
            return True
    if (before.get("meta") or {}) != (after.get("meta") or {}):
        return True
    return False


def _is_terminal_status(status: Optional[str]) -> bool:
    if not status:
        return False
    return str(status).upper() in _TERMINAL_STATUSES


def _should_resume_for_transition(
    before: Dict[str, Any], after: Dict[str, Any]
) -> bool:
    before_status = str(before.get("status", "")).upper()
    after_status = str(after.get("status", "")).upper()
    if _is_terminal_status(before_status):
        return False
    return _is_terminal_status(after_status)


def _latest_plan(values: Dict[str, Any]) -> Any:
    history = values.get("plan_history") or []
    if isinstance(history, list) and history:
        return history[-1]
    return None


def _task_goal(plan: Any) -> str:
    goal = getattr(plan, "goal", None)
    if goal:
        return str(goal)
    if isinstance(plan, dict):
        return str(plan.get("goal") or "Task")
    return "Task"


async def _sync_completed_conversation_task(
    *,
    values: Dict[str, Any],
    thread_id: str,
    terminal_events: List[Dict[str, Any]],
) -> None:
    conversation_id = resolve_conversation_id(
        provider=values.get("provider"),
        provider_user_id=values.get("user_id"),
        context=values.get("context"),
    )
    execution_id = values.get("execution_id")
    if not conversation_id or not execution_id:
        return

    plan = _latest_plan(values)
    if plan is None:
        return

    summary = ""
    for event in terminal_events:
        candidate = str(event.get("summary") or "").strip()
        if candidate:
            summary = candidate
            break
    if not summary:
        summary = _task_goal(plan)

    tx_hash = None
    for event in terminal_events:
        candidate = str(event.get("tx_hash") or "").strip()
        if candidate:
            tx_hash = candidate
            break

    user_info = values.get("user_info") or {}
    if isinstance(user_info, dict) and user_info.get("volo_user_id"):
        resolved_user_id = str(user_info.get("volo_user_id"))
    else:
        resolved_user_id = str(values.get("user_id") or "unknown")

    first_tool = None
    nodes = getattr(plan, "nodes", None)
    if isinstance(nodes, dict) and nodes:
        first_tool = getattr(next(iter(nodes.values())), "tool", None)

    try:
        task_registry = ConversationTaskRegistry()
        await task_registry.upsert_execution_task(
            conversation_id=str(conversation_id),
            execution_id=str(execution_id),
            thread_id=str(thread_id),
            provider=str(values.get("provider") or ""),
            provider_user_id=str(values.get("user_id") or ""),
            user_id=resolved_user_id,
            title=_task_goal(plan),
            status="COMPLETED",
            latest_summary=summary,
            tool=str(first_tool) if first_tool else None,
            tx_hash=tx_hash,
        )
    except Exception:
        return


def _status_value(status: Any) -> str:
    value = getattr(status, "value", status)
    return str(value).strip().lower().replace("stepstatus.", "")


def _is_plan_complete(plan: Any, execution_state: Any) -> bool:
    if plan is None or execution_state is None:
        return False
    try:
        if check_plan_complete(plan, execution_state):
            return True
    except Exception:
        pass

    if isinstance(plan, dict):
        nodes = plan.get("nodes")
    else:
        nodes = getattr(plan, "nodes", None)
    node_ids = list(nodes.keys()) if isinstance(nodes, dict) else []
    if not node_ids:
        return False

    node_states = getattr(execution_state, "node_states", None)
    if not isinstance(node_states, dict):
        return False

    terminal = {"success", "skipped", "failed"}
    for node_id in node_ids:
        node_state = node_states.get(node_id)
        if node_state is None:
            return False
        if _status_value(getattr(node_state, "status", None)) not in terminal:
            return False
    return True


def _resume_execution_engine(app, config) -> None:
    async def _run() -> None:
        try:
            async for _ in app.astream(Command(goto="execution_engine"), config):
                pass
        except Exception as exc:
            print(
                f"[bridge-status] resume stream error: {exc}",
                file=sys.stderr,
            )

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():

        def _on_done(t: "asyncio.Task[None]") -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc:
                print(
                    f"[bridge-status] resume task failed: {exc}",
                    file=sys.stderr,
                )

        task = loop.create_task(_run())
        task.add_done_callback(_on_done)
    else:
        asyncio.run(_run())


async def _infer_across_deposit_meta(
    tx_hash: str, source_chain_name: str
) -> Optional[Dict[str, Any]]:
    if not _ACROSS_V3_EVENT_ABI or not _ACROSS_V3_EVENT_TOPIC:
        return None
    try:
        chain = get_chain_by_name(str(source_chain_name))
        w3 = make_async_web3(chain.rpc_url)
        receipt = await w3.eth.get_transaction_receipt(
            HexBytes(_normalize_tx_hash(tx_hash))
        )
        if receipt is None:
            return None
        for log in receipt.get("logs", []):
            topics = log.get("topics") or []
            if not topics or topics[0] != _ACROSS_V3_EVENT_TOPIC:
                continue
            decoded = get_event_data(
                w3.codec, cast(ABIEvent, _ACROSS_V3_EVENT_ABI), log
            )
            args = decoded.get("args") or {}
            deposit_id = args.get("depositId")
            if deposit_id is None:
                continue
            return {
                "deposit_id": int(deposit_id),
                "origin_chain_id": int(chain.chain_id),
            }
    except Exception:
        return None
    return None


def _build_bridge_event(
    record: Dict[str, Any], now_iso: str
) -> Optional[Dict[str, Any]]:
    status = str(record.get("status", "")).upper()
    if not _is_terminal_status(status):
        return None
    protocol = record.get("protocol")
    source_chain = record.get("source_chain") or record.get("chain")
    dest_chain = record.get("dest_chain")
    summary = "Bridge completed."
    if source_chain and dest_chain:
        summary = f"Bridge {source_chain} → {dest_chain} {status.lower()}."
        if protocol:
            summary = (
                f"Bridge {source_chain} → {dest_chain} via {protocol} {status.lower()}."
            )
    elif protocol:
        summary = f"Bridge via {protocol} {status.lower()}."
    return {
        "event": "node_completed" if status == "SUCCESS" else "node_failed",
        "execution_id": record.get("execution_id"),
        "node_id": record.get("node_id"),
        "tool": "bridge",
        "status": status,
        "protocol": protocol,
        "tx_hash": record.get("tx_hash"),
        "summary": summary,
        "timestamp": now_iso,
    }


async def _process_pending_records(
    pending_transactions: Iterable[Dict[str, Any]],
    *,
    now_iso: str,
    debug: bool = False,
) -> Tuple[List[Dict[str, Any]], ExecutionState, bool, List[Dict[str, Any]]]:
    return await process_pending_records(
        pending_transactions,
        now_iso=now_iso,
        deps=BridgeStatusRuntimeDeps(
            fetch_bridge_status_async=fetch_bridge_status_async,
            infer_across_deposit_meta=_infer_across_deposit_meta,
            infer_is_testnet=_infer_is_testnet,
            normalize_lifi_meta=_normalize_lifi_meta,
            normalize_pending=_normalize_pending,
            normalize_tx_hash=_normalize_tx_hash,
            apply_status_update=_apply_status_update,
            status_changed=_status_changed,
            should_resume_for_transition=_should_resume_for_transition,
            build_bridge_event=_build_bridge_event,
            parse_iso=_parse_iso,
            classify_missing_tx=_classify_missing_tx,
            resend_min_age_seconds=_resend_min_age_seconds,
            should_update_resend_meta=_should_update_resend_meta,
            should_resend=_should_resend,
            get_resend_attempts=_get_resend_attempts,
            broadcast_resend=_broadcast_resend,
            normalize_signed_tx=_normalize_signed_tx,
            async_broadcast_evm=async_broadcast_evm,
            update_resend_meta=_update_resend_meta,
            update_task_history_tx_hash=_update_task_history_tx_hash,
            get_task_history_collection=lambda: AsyncMongoDB.get_collection(
                "task_history"
            ),
            get_chain_by_name=get_chain_by_name,
            make_async_web3=make_async_web3,
            create_node_success_state=create_node_success_state,
            create_node_failure_state=create_node_failure_state,
            terminal_statuses=_TERMINAL_STATUSES,
            finalize_reservation=_finalize_reservation,
        ),
        debug=debug,
    )


async def _handle_terminal_events(
    *,
    terminal_events: list[dict[str, Any]],
    values: dict[str, Any],
    thread_id: str,
    now_iso: str,
    debug: bool,
) -> None:
    task_history_collection = None
    try:
        task_history_collection = AsyncMongoDB.get_collection("task_history")
    except Exception:
        task_history_collection = None
    for event in terminal_events:
        if not event.get("thread_id"):
            event["thread_id"] = thread_id
        if not event.get("execution_id"):
            event["execution_id"] = values.get("execution_id")
        if task_history_collection is not None:
            await _update_task_history_terminal_status(
                collection=task_history_collection,
                execution_id=event.get("execution_id"),
                node_id=event.get("node_id"),
                status=str(event.get("status") or ""),
                now_iso=now_iso,
                tx_hash=event.get("tx_hash"),
                summary=event.get("summary"),
            )
        summary = event.get("summary") or "Execution event."
        status = event.get("status") or ""
        tx_hash = event.get("tx_hash") or ""
        node_id = event.get("node_id") or ""
        prefix = f"[bridge-status] ({node_id}) " if node_id else "[bridge-status] "
        if tx_hash:
            print(f"{prefix}{summary} status={status} tx={tx_hash}")
        else:
            print(f"{prefix}{summary} status={status}")
        published = publish_event(event)
        if not published:
            print(
                f"[bridge-status] event_publish failed thread={thread_id} "
                f"event={event.get('event')} (Upstash configured?)",
                file=sys.stderr,
            )
        if debug:
            print(
                f"[bridge-status] event_publish thread={thread_id} "
                f"event={event.get('event')} ok={published}"
            )


def _bridge_status_worker_runtime_deps() -> BridgeStatusWorkerRuntimeDeps:
    from graph.graph import app

    return BridgeStatusWorkerRuntimeDeps(
        get_app=lambda: app,
        distinct_thread_pairs=_distinct_thread_pairs,
        acquire_lock=_acquire_lock,
        process_pending_records=_process_pending_records,
        latest_plan=_latest_plan,
        is_plan_complete=_is_plan_complete,
        sync_completed_conversation_task=_sync_completed_conversation_task,
        handle_terminal_events=_handle_terminal_events,
        resume_execution_engine=_resume_execution_engine,
        debug_enabled=lambda: _bool_env("BRIDGE_STATUS_WORKER_DEBUG", False),
        utc_now_iso=_utc_now_iso,
    )


async def _run_once(interval_seconds: int, lock_ttl_seconds: int, owner: str) -> None:
    await run_bridge_status_tick(
        deps=_bridge_status_worker_runtime_deps(),
        lock_ttl_seconds=lock_ttl_seconds,
        owner=owner,
    )
    if interval_seconds <= 0:
        return


async def _run_loop(
    interval_seconds: int, lock_ttl_seconds: int, owner: str, *, once: bool
) -> None:
    await run_bridge_status_worker_loop(
        interval_seconds=interval_seconds,
        lock_ttl_seconds=lock_ttl_seconds,
        owner=owner,
        once=once,
    )


async def _main_async(args: argparse.Namespace) -> int:
    if not args.skip_mongodb_healthcheck:
        if not await AsyncMongoDB.ping():
            print("MongoDB ping failed. Aborting worker startup.")
            return 1

    try:
        await _ensure_lock_indexes()
    except Exception as exc:
        print(f"Failed to ensure bridge worker lock indexes: {exc}")
        return 1

    if _bool_env("SKIP_MONGODB_HEALTHCHECK", False):
        print(
            "SKIP_MONGODB_HEALTHCHECK is set; the graph may be using MemorySaver. "
            "Unset it for worker updates to persist."
        )

    owner = f"bridge-worker-{uuid.uuid4().hex[:8]}"

    await _run_loop(
        args.interval,
        args.lock_ttl_seconds,
        owner,
        once=args.once,
    )
    return 0


def main() -> int:
    _load_local_env()
    parser = argparse.ArgumentParser(description="Bridge status polling worker.")
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("BRIDGE_STATUS_WORKER_INTERVAL_SECONDS", "15")),
        help="Polling interval in seconds.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single polling iteration and exit.",
    )
    parser.add_argument(
        "--lock-ttl-seconds",
        type=int,
        default=int(os.getenv("BRIDGE_STATUS_WORKER_LOCK_TTL_SECONDS", "30")),
        help="Per-thread lock TTL in seconds (0 disables locking).",
    )
    parser.add_argument(
        "--skip-mongodb-healthcheck",
        action="store_true",
        help="Skip MongoDB ping check (not recommended).",
    )
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
