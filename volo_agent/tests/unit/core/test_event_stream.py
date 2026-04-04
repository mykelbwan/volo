from core.utils import event_stream


class _DummyClient:
    def __init__(self):
        self.calls = []

    def xadd(self, stream, record_id, data, *args, **kwargs):
        self.calls.append((stream, record_id, data))


class _AsyncDummyClient:
    def __init__(self):
        self.calls = []

    async def xadd(self, stream, record_id, data, *args, **kwargs):
        self.calls.append((stream, record_id, data))


def test_publish_event_returns_false_without_client():
    original = event_stream.get_upstash_client
    try:
        event_stream.get_upstash_client = lambda: None
        assert event_stream.publish_event({"event": "test"}) is False
    finally:
        event_stream.get_upstash_client = original


def test_publish_event_sends_payload():
    dummy = _DummyClient()
    original = event_stream.get_upstash_client
    try:
        event_stream.get_upstash_client = lambda: dummy
        ok = event_stream.publish_event({"event": "node_completed", "node_id": "step_1"})
    finally:
        event_stream.get_upstash_client = original

    assert ok is True
    assert dummy.calls
    stream, record_id, data = dummy.calls[0]
    assert stream == event_stream.event_stream_name()
    assert record_id == "*"
    assert data["event"] == "node_completed"
    assert data["node_id"] == "step_1"


def test_publish_event_async_sends_payload():
    import asyncio

    dummy = _AsyncDummyClient()
    original = event_stream.get_async_redis

    async def _get_async_redis():
        return dummy

    try:
        event_stream.get_async_redis = _get_async_redis
        ok = asyncio.run(
            event_stream.publish_event_async(
                {"event": "node_completed", "node_id": "step_1"}
            )
        )
    finally:
        event_stream.get_async_redis = original

    assert ok is True
    assert dummy.calls
    stream, record_id, data = dummy.calls[0]
    assert stream == event_stream.event_stream_name()
    assert record_id == "*"
    assert data["event"] == "node_completed"
    assert data["node_id"] == "step_1"


def test_coerce_event_dict_from_list():
    data = ["event", "node_completed", "node_id", "step_0"]
    parsed = event_stream.coerce_event_dict(data)
    assert parsed["event"] == "node_completed"
    assert parsed["node_id"] == "step_0"


def test_format_event():
    payload = {"event": "node_completed", "status": "SUCCESS", "node_id": "step_1"}
    formatted = event_stream.format_event(payload)
    assert "node_completed" in formatted
    assert "status=SUCCESS" in formatted
    assert "node=step_1" in formatted


def test_format_event_compacts_multiline_summary():
    payload = {
        "event": "node_completed",
        "status": "SUCCESS",
        "node_id": "step_1",
        "summary": "Balances across supported chains:\n\nEthereum:\n  - No assets found.",
    }
    formatted = event_stream.format_event(payload)

    assert "\n" not in formatted
    assert "summary=Balances across supported chains:" in formatted


def test_progress_stage_message_formats_initial_and_elapsed_variants():
    assert event_stream.progress_stage_message("sending") == "Sending transaction..."
    assert (
        event_stream.progress_stage_message("submitted", 22.4)
        == "Transaction sent. Waiting for confirmation (22s)."
    )


def test_format_user_event_uses_rich_progress_and_completion_copy():
    progress = event_stream.format_user_event(
        {"event": "node_progress", "stage": "finalizing", "tool": "swap"}
    )
    completed = event_stream.format_user_event(
        {
            "event": "node_completed",
            "status": "SUCCESS",
            "tool": "swap",
            "summary": "Swap submitted: 0.2 STT to NIA.",
        }
    )
    failed = event_stream.format_user_event(
        {
            "event": "node_failed",
            "tool": "swap",
            "summary": "RPC timeout",
        }
    )

    assert progress == "Confirmed on-chain. Finalizing..."
    assert completed == "Done. Swap complete."
    assert failed == "Swap failed. RPC timeout"


def test_format_user_event_reports_unwrap_completion():
    completed = event_stream.format_user_event(
        {
            "event": "node_completed",
            "status": "SUCCESS",
            "tool": "unwrap",
            "summary": "Unwrap submitted",
        }
    )

    assert completed == "Done. Unwrap complete."
def test_format_user_event_appends_recovery_hint_for_failures():
    formatted = event_stream.format_user_event(
        {
            "event": "node_failed",
            "tool": "swap",
            "summary": "RPC timeout",
            "error_category": "network",
            "recovery_hint": (
                "Swap failed on Base. The network is congested or temporarily unavailable.\n\n"
                "Reply with: retry, edit, cancel"
            ),
        }
    )

    assert formatted == "Swap failed. RPC timeout"


def test_format_user_event_uses_edit_hint_for_liquidity_failures():
    formatted = event_stream.format_user_event(
        {
            "event": "node_failed",
            "tool": "swap",
            "summary": "No route found",
            "error_category": "liquidity",
            "recovery_hint": (
                "Swap failed on Base. There isn’t enough liquidity to complete this right now.\n\n"
                "Reply with: retry, edit, cancel"
            ),
        }
    )

    assert formatted == "Swap failed. No route found"


def test_format_user_event_preserves_explicit_suggestion_hint():
    formatted = event_stream.format_user_event(
        {
            "event": "node_failed",
            "tool": "swap",
            "summary": "Slippage too high",
            "error_category": "slippage",
            "recovery_hint": (
                "The swap didn't go through on Base. I can try a safer setting. Reply 'go ahead' to try that."
            ),
        }
    )

    assert formatted == "Swap failed. Slippage too high"
