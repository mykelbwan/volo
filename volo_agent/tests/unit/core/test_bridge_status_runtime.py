import asyncio

from core.bridge_status_runtime import BridgeStatusRuntimeDeps, process_pending_records
from core.planning.execution_plan import (
    create_node_failure_state,
    create_node_success_state,
)
from core.utils.bridge_status_registry import BridgeStatusResult


def test_process_pending_records_failed_bridge_sets_user_facing_failure_message():
    record = {
        "type": "bridge",
        "status": "PENDING",
        "tx_hash": "0xabc",
        "protocol": "across",
        "node_id": "step_0",
        "source_chain": "ethereum",
    }

    async def _fetch(*_args, **_kwargs):
        return BridgeStatusResult(raw_status="failed", normalized_status="failed")

    async def _infer(*_args, **_kwargs):
        return None

    async def _update_task_history_tx_hash(**_kwargs):
        return False

    deps = BridgeStatusRuntimeDeps(
        fetch_bridge_status_async=_fetch,
        infer_across_deposit_meta=_infer,
        infer_is_testnet=lambda _record: True,
        normalize_lifi_meta=lambda meta, _record: meta,
        normalize_socket_meta=lambda meta, _record: meta,
        normalize_pending=lambda rec: dict(rec),
        normalize_tx_hash=lambda value: value,
        apply_status_update=lambda normalized, raw_status, normalized_status, now_iso: {
            **dict(normalized),
            "last_status": normalized_status,
            "last_status_raw": raw_status,
            "status": (
                "SUCCESS"
                if normalized_status == "success"
                else "FAILED"
                if normalized_status == "failed"
                else normalized.get("status") or "PENDING"
            ),
        },
        status_changed=lambda before, after: before != after,
        should_resume_for_transition=lambda _before, _after: True,
        build_bridge_event=lambda record, _now_iso: {
            "event": "node_failed",
            "status": "FAILED",
            "node_id": record.get("node_id"),
            "summary": "Bridge failed.",
        },
        parse_iso=lambda _value: None,
        classify_missing_tx=lambda **_kwargs: "unknown",
        resend_min_age_seconds=lambda: 360,
        should_update_resend_meta=lambda _existing, _new_payload: False,
        should_resend=lambda **_kwargs: False,
        get_resend_attempts=lambda _meta: 0,
        broadcast_resend=_fetch,
        normalize_signed_tx=lambda _signed: None,
        async_broadcast_evm=_fetch,
        update_resend_meta=lambda *_args, **_kwargs: {},
        update_task_history_tx_hash=_update_task_history_tx_hash,
        get_task_history_collection=lambda: object(),
        get_chain_by_name=lambda _name: object(),
        make_async_web3=lambda _rpc: object(),
        create_node_success_state=create_node_success_state,
        create_node_failure_state=create_node_failure_state,
        terminal_statuses={"SUCCESS", "FAILED"},
    )

    _updated, delta, resume, events = asyncio.run(
        process_pending_records(
            [record],
            now_iso="2026-03-16T00:00:00+00:00",
            deps=deps,
            debug=False,
        )
    )

    assert resume is True
    assert events and events[0]["event"] == "node_failed"
    node_state = delta.node_states["step_0"]
    assert node_state.error_category == "unknown"
    assert node_state.user_message is not None
    assert "bridge failed" in node_state.user_message.lower()
    assert "reply with: retry, edit, cancel" in node_state.user_message.lower()
