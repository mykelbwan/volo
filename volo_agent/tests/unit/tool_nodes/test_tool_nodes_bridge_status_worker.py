import asyncio
from unittest.mock import AsyncMock, patch

from command_line_tools.bridge_status_worker import (
    _classify_missing_tx,
    _broadcast_resend,
    _apply_fee_bump,
    _should_update_resend_meta,
    _should_resend,
    _update_task_history_terminal_status,
    _update_task_history_tx_hash,
)


def test_classify_missing_tx_unknown_age():
    assert (
        _classify_missing_tx(
            age_seconds=None,
            tx_nonce=1,
            pending_nonce=1,
            latest_nonce=1,
            min_age_seconds=360,
        )
        == "unknown_age"
    )


def test_classify_missing_tx_too_soon():
    assert (
        _classify_missing_tx(
            age_seconds=100,
            tx_nonce=1,
            pending_nonce=1,
            latest_nonce=1,
            min_age_seconds=360,
        )
        == "too_soon"
    )


def test_classify_missing_tx_missing_nonce():
    assert (
        _classify_missing_tx(
            age_seconds=400,
            tx_nonce=None,
            pending_nonce=2,
            latest_nonce=2,
            min_age_seconds=360,
        )
        == "missing_nonce"
    )


def test_classify_missing_tx_replaced_pending():
    assert (
        _classify_missing_tx(
            age_seconds=400,
            tx_nonce=1,
            pending_nonce=2,
            latest_nonce=1,
            min_age_seconds=360,
        )
        == "replaced"
    )


def test_classify_missing_tx_replaced_latest():
    assert (
        _classify_missing_tx(
            age_seconds=400,
            tx_nonce=1,
            pending_nonce=1,
            latest_nonce=2,
            min_age_seconds=360,
        )
        == "replaced"
    )


def test_classify_missing_tx_safe_to_resend():
    assert (
        _classify_missing_tx(
            age_seconds=400,
            tx_nonce=3,
            pending_nonce=3,
            latest_nonce=3,
            min_age_seconds=360,
        )
        == "safe_to_resend"
    )


def test_classify_missing_tx_nonce_unknown():
    assert (
        _classify_missing_tx(
            age_seconds=400,
            tx_nonce=5,
            pending_nonce=4,
            latest_nonce=4,
            min_age_seconds=360,
        )
        == "nonce_unknown"
    )


def test_should_update_resend_meta_only_on_changes():
    existing = {
        "state": "safe_to_resend",
        "pending_nonce": 5,
        "latest_nonce": 5,
        "block_number": None,
        "checked_at": "old",
    }
    new_payload = {
        "state": "safe_to_resend",
        "pending_nonce": 5,
        "latest_nonce": 5,
        "block_number": None,
        "checked_at": "new",
    }
    assert _should_update_resend_meta(existing, new_payload) is False

    new_payload["latest_nonce"] = 6
    assert _should_update_resend_meta(existing, new_payload) is True


def test_should_resend_guards():
    assert (
        _should_resend(
            classification="safe_to_resend",
            raw_tx=None,
            meta=None,
        )
        is False
    )
    assert (
        _should_resend(
            classification="too_soon",
            raw_tx="0xdead",
            meta=None,
        )
        is False
    )


def test_should_resend_when_enabled(monkeypatch):
    monkeypatch.setenv("BRIDGE_RESEND_ENABLED", "1")
    assert (
        _should_resend(
            classification="safe_to_resend",
            raw_tx="0xdead",
            meta={"resend": {"attempts": 0}},
        )
        is True
    )


def test_apply_fee_bump_eip1559(monkeypatch):
    monkeypatch.setenv("BRIDGE_RESEND_BUMP_PCT", "20")
    monkeypatch.setenv("BRIDGE_RESEND_MAX_MULTIPLIER", "2")
    payload = {"maxFeePerGas": 100, "maxPriorityFeePerGas": 10}
    bumped = _apply_fee_bump(payload, attempts=1)
    assert bumped["maxFeePerGas"] > payload["maxFeePerGas"]
    assert bumped["maxPriorityFeePerGas"] > payload["maxPriorityFeePerGas"]


def test_apply_fee_bump_legacy(monkeypatch):
    monkeypatch.setenv("BRIDGE_RESEND_BUMP_PCT", "50")
    monkeypatch.setenv("BRIDGE_RESEND_MAX_MULTIPLIER", "2")
    payload = {"gasPrice": 100}
    bumped = _apply_fee_bump(payload, attempts=2)
    assert bumped["gasPrice"] >= 100


def test_broadcast_resend_uses_signer(monkeypatch):
    def _sign(_sub_org_id, tx, sender):
        assert tx["maxFeePerGas"] > 100
        assert sender == "0xSender"
        return "0xdeadbeef"

    monkeypatch.setenv("BRIDGE_RESEND_BUMP_PCT", "20")
    monkeypatch.setenv("BRIDGE_RESEND_MAX_MULTIPLIER", "2")

    with patch(
        "command_line_tools.bridge_status_worker.sign_transaction_async",
        new=AsyncMock(side_effect=_sign),
    ):
        with patch(
            "command_line_tools.bridge_status_worker.async_broadcast_evm",
            new=AsyncMock(return_value="0xabc"),
        ):
            tx_hash = asyncio.run(
                _broadcast_resend(
                    w3=object(),
                    tx_payload={
                        "maxFeePerGas": 100,
                        "maxPriorityFeePerGas": 10,
                        "nonce": 1,
                    },
                    sub_org_id="sub",
                    sender="0xSender",
                    attempts=1,
                )
            )

    assert tx_hash == "0xabc"


def test_update_task_history_tx_hash():
    calls = {}

    class _Coll:
        async def find_one_and_update(self, query, update, sort=None):
            calls["query"] = query
            calls["update"] = update
            calls["sort"] = sort
            return {"_id": "x"}

    updated = asyncio.run(
        _update_task_history_tx_hash(
            collection=_Coll(),
            execution_id="exec-1",
            node_id="step_0",
            tx_hash="0xabc",
            now_iso="2026-01-01T00:00:00Z",
            status="RESUBMITTED",
        )
    )

    assert updated is True
    assert calls["query"]["execution_id"] == "exec-1"
    assert calls["query"]["node_id"] == "step_0"
    assert calls["update"]["$set"]["tx_hash"] == "0xabc"
    assert calls["update"]["$set"]["status"] == "RESUBMITTED"


def test_update_task_history_terminal_status():
    calls = {}

    class _Coll:
        async def find_one_and_update(self, query, update, sort=None):
            calls["query"] = query
            calls["update"] = update
            calls["sort"] = sort
            return {"_id": "x"}

    updated = asyncio.run(
        _update_task_history_terminal_status(
            collection=_Coll(),
            execution_id="exec-1",
            node_id="step_0",
            status="SUCCESS",
            now_iso="2026-01-01T00:00:00Z",
            tx_hash="0xabc",
            summary="Bridge completed.",
        )
    )

    assert updated is True
    assert calls["query"]["execution_id"] == "exec-1"
    assert calls["query"]["node_id"] == "step_0"
    assert "RESUBMITTED" in calls["query"]["status"]["$in"]
    assert calls["update"]["$set"]["status"] == "SUCCESS"
    assert calls["update"]["$set"]["tx_hash"] == "0xabc"
    assert calls["update"]["$set"]["summary"] == "Bridge completed."
