from __future__ import annotations

import asyncio
import logging
import os
from types import SimpleNamespace
from unittest.mock import patch

from command_line_tools import cli


def test_extract_state_counters_reads_snapshot_values():
    snapshot = SimpleNamespace(values={"messages": [1, 2, 3], "reasoning_logs": [1, 2]})
    assert cli._extract_state_counters(snapshot) == (3, 2)


def test_extract_state_counters_handles_missing_values():
    snapshot = SimpleNamespace(values=None)
    assert cli._extract_state_counters(snapshot) == (0, 0)


def test_extract_last_ai_signature_returns_latest_ai_message():
    snapshot = SimpleNamespace(
        values={
            "messages": [
                SimpleNamespace(type="human", id="h1", content="hello"),
                SimpleNamespace(type="ai", id="a1", content="first"),
                SimpleNamespace(type="ai", id="a2", content="second"),
            ]
        }
    )
    assert cli._extract_last_ai_signature(snapshot) == ("a2", "second")


def test_select_unprinted_latest_ai_message_detects_in_place_update():
    messages = [
        SimpleNamespace(type="human", id="h1", content="hi"),
        SimpleNamespace(type="ai", id="a1", content="updated receipt"),
    ]
    selected = cli._select_unprinted_latest_ai_message(messages, ("a1", "old content"))
    assert selected is not None
    assert selected.content == "updated receipt"


def test_select_unprinted_latest_ai_message_skips_already_printed_content():
    messages = [SimpleNamespace(type="ai", id="a1", content="same")]
    selected = cli._select_unprinted_latest_ai_message(messages, ("a1", "same"))
    assert selected is None


def test_load_state_counters_prefers_async_state_api():
    class _AsyncApp:
        async def aget_state(self, _config):
            return SimpleNamespace(values={"messages": [1], "reasoning_logs": [1, 2]})

    assert asyncio.run(cli._load_state_counters(_AsyncApp(), config={})) == (1, 2)


def test_load_state_counters_supports_sync_state_api():
    class _SyncApp:
        def get_state(self, _config):
            return SimpleNamespace(values={"messages": [1, 2], "reasoning_logs": [1]})

    assert asyncio.run(cli._load_state_counters(_SyncApp(), config={})) == (2, 1)


def test_load_state_counters_returns_zero_on_get_state_error():
    class _BrokenApp:
        def get_state(self, _config):
            raise RuntimeError("boom")

    assert asyncio.run(cli._load_state_counters(_BrokenApp(), config={})) == (0, 0)


def test_load_last_ai_signature_prefers_async_state_api():
    class _AsyncApp:
        async def aget_state(self, _config):
            return SimpleNamespace(
                values={"messages": [SimpleNamespace(type="ai", id="a1", content="hi")]}
            )

    assert asyncio.run(cli._load_last_ai_signature(_AsyncApp(), config={})) == (
        "a1",
        "hi",
    )


def test_is_confirm_like_recognizes_confirmation_words():
    assert cli._is_confirm_like("confirm") is True
    assert cli._is_confirm_like("yes") is True
    assert cli._is_confirm_like("proceed") is True
    assert cli._is_confirm_like("swap 1 eth to usdc") is False


def test_build_idle_feedback_is_specific_for_confirm_turns():
    assert cli._build_idle_feedback("confirm", 3.0) == "Confirmed. Sending the transaction."
    message = cli._build_idle_feedback("confirm", 12.9, progress_stage="submitted")
    assert message == "Transaction sent. Waiting for confirmation (12s)."


def test_build_idle_feedback_is_specific_for_action_turns():
    message = cli._build_idle_feedback("swap 0.2 stt for nia on somnia", 7.2)
    assert message == "Working on it."


def test_build_idle_feedback_stays_quiet_for_non_action_small_talk():
    assert cli._build_idle_feedback("hi", 3.0) is None


def test_build_idle_feedback_uses_finalizing_stage_for_confirm_turns():
    message = cli._build_idle_feedback("confirm", 22.4, progress_stage="finalizing")
    assert message == "Confirmed on-chain. Finalizing (22s)."


def test_next_idle_timeout_accelerates_first_confirm_updates():
    assert cli._next_idle_timeout_seconds("confirm", 3.0) == 5.0
    assert cli._next_idle_timeout_seconds("confirm", 10.0, progress_stage="submitted") == 12.0


def test_progress_update_from_reasoning_log_maps_transaction_stages():
    sending = cli._progress_update_from_reasoning_log("[ACTION] Starting 'swap' (step_0)")
    submitted = cli._progress_update_from_reasoning_log(
        "[SUCCESS] Swap submitted: 0.2 STT to NIA. Transaction ID: 0x123."
    )
    finalizing = cli._progress_update_from_reasoning_log("[FEE] Collecting fee for 'step_0' …")

    assert sending is not None
    assert sending.stage == "sending"
    assert sending.message == "Sending transaction..."
    assert submitted is not None
    assert submitted.stage == "submitted"
    assert submitted.message == "Transaction sent. Waiting for confirmation."
    assert finalizing is not None
    assert finalizing.stage == "finalizing"
    assert finalizing.message == "Confirmed on-chain. Finalizing..."


def test_status_table_from_values_returns_empty_without_history_or_state():
    assert cli._status_table_from_values({}) == ""
    assert cli._status_table_from_values({"plan_history": []}) == ""
    assert cli._status_table_from_values({"execution_state": object()}) == ""


def test_status_table_from_values_uses_latest_plan_and_state():
    values = {"plan_history": ["plan-1"], "execution_state": "state-1"}
    with patch.object(cli, "get_status_table", return_value="TABLE"):
        assert cli._status_table_from_values(values) == "TABLE"


def test_should_print_status_table_dedupes_unchanged_output():
    assert cli._should_print_status_table("", "") is False
    assert cli._should_print_status_table("A", "") is False
    assert cli._should_print_status_table("", "A") is True
    assert cli._should_print_status_table("A", "A") is False
    assert cli._should_print_status_table("A", "B") is True


def test_cli_is_quiet_by_default_for_internal_reasoning_logs():
    with patch.dict(os.environ, {}, clear=False):
        assert cli._should_print_reasoning_log("[ROUTE] picked route") is False
        assert cli._should_print_reasoning_log("[BALANCE_CHECK] ok") is False
        assert cli._should_print_reasoning_log("[SUCCESS] Swap submitted") is False
        assert cli._should_print_reasoning_log("[ERROR] Swap failed") is True


def test_cli_event_lines_are_hidden_by_default_unless_failed():
    with patch.dict(os.environ, {}, clear=False):
        assert (
            cli._should_print_event_line(
                {"event": "node_completed", "status": "SUCCESS"}
            )
            is False
        )
        assert (
            cli._should_print_event_line({"event": "node_completed", "status": "FAILED"})
            is True
        )


def test_cli_verbose_debug_restores_internal_logs_and_events():
    with patch.dict(os.environ, {"VOLO_CLI_VERBOSE_DEBUG": "1"}, clear=False):
        assert cli._should_print_reasoning_log("[ROUTE] picked route") is True
        assert (
            cli._should_print_event_line(
                {"event": "node_completed", "status": "SUCCESS"}
            )
            is True
        )


def test_cli_status_table_is_hidden_by_default_and_enabled_by_flags():
    with patch.dict(os.environ, {}, clear=False):
        assert cli._cli_show_status_table() is False
    with patch.dict(os.environ, {"VOLO_CLI_SHOW_STATUS_TABLE": "1"}, clear=False):
        assert cli._cli_show_status_table() is True
    with patch.dict(os.environ, {"VOLO_CLI_VERBOSE_DEBUG": "1"}, clear=False):
        assert cli._cli_show_status_table() is True


def test_configure_cli_logging_suppresses_chat_diagnostics_by_default():
    router_logger = logging.getLogger("intent_hub.parser.router")
    responder_logger = logging.getLogger("core.conversation.responder")
    old_router_level = router_logger.level
    old_responder_level = responder_logger.level
    try:
        router_logger.setLevel(logging.NOTSET)
        responder_logger.setLevel(logging.NOTSET)
        with patch.dict(os.environ, {}, clear=False):
            cli._configure_cli_logging()
        assert router_logger.level == logging.ERROR
        assert responder_logger.level == logging.ERROR
    finally:
        router_logger.setLevel(old_router_level)
        responder_logger.setLevel(old_responder_level)


def test_configure_cli_logging_keeps_debug_mode_unsuppressed():
    router_logger = logging.getLogger("intent_hub.parser.router")
    old_router_level = router_logger.level
    try:
        router_logger.setLevel(logging.NOTSET)
        with patch.dict(os.environ, {"VOLO_CLI_VERBOSE_DEBUG": "1"}, clear=False):
            cli._configure_cli_logging()
        assert router_logger.level == logging.NOTSET
    finally:
        router_logger.setLevel(old_router_level)
