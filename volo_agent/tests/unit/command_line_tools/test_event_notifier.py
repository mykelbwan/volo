from command_line_tools import event_notifier


def test_update_inflight_progress_tracks_and_clears_node_state():
    inflight: dict[str, event_notifier.InflightProgress] = {}

    event_notifier.update_inflight_progress(
        inflight,
        {"event": "node_progress", "thread_id": "t1", "node_id": "step_0", "stage": "submitted"},
        now=10.0,
    )

    assert inflight["t1:step_0"].stage == "submitted"

    event_notifier.update_inflight_progress(
        inflight,
        {"event": "node_completed", "thread_id": "t1", "node_id": "step_0"},
        now=20.0,
    )

    assert "t1:step_0" not in inflight


def test_collect_wait_updates_emits_elapsed_message_after_threshold():
    inflight = {
        "t1:step_0": event_notifier.InflightProgress(
            stage="submitted",
            started_at=0.0,
            last_notice_at=0.0,
        )
    }

    messages = event_notifier.collect_wait_updates(inflight, now=12.0)

    assert messages == ["Transaction sent. Waiting for confirmation (12s)."]
    assert inflight["t1:step_0"].last_notice_at == 12.0


def test_collect_wait_updates_does_not_repeat_before_interval():
    inflight = {
        "t1:step_0": event_notifier.InflightProgress(
            stage="submitted",
            started_at=0.0,
            last_notice_at=12.0,
        )
    }

    messages = event_notifier.collect_wait_updates(inflight, now=20.0)

    assert messages == []
    assert inflight["t1:step_0"].last_notice_at == 12.0


def test_collect_wait_updates_repeats_after_interval_for_long_waits():
    inflight = {
        "t1:step_0": event_notifier.InflightProgress(
            stage="submitted",
            started_at=0.0,
            last_notice_at=12.0,
        )
    }

    messages = event_notifier.collect_wait_updates(inflight, now=24.5)

    assert messages == ["Transaction sent. Waiting for confirmation (24s)."]
    assert inflight["t1:step_0"].last_notice_at == 24.5


def test_progress_stage_update_resets_wait_timer_after_new_stage_signal():
    inflight = {
        "t1:step_0": event_notifier.InflightProgress(
            stage="sending",
            started_at=0.0,
            last_notice_at=12.0,
        )
    }

    event_notifier.update_inflight_progress(
        inflight,
        {"event": "node_progress", "thread_id": "t1", "node_id": "step_0", "stage": "submitted"},
        now=30.0,
    )

    assert inflight["t1:step_0"].stage == "submitted"
    assert inflight["t1:step_0"].started_at == 30.0
    assert inflight["t1:step_0"].last_notice_at == 30.0
    assert event_notifier.collect_wait_updates(inflight, now=35.0) == []


def test_render_event_output_defaults_to_user_facing_message():
    event_data = {"event": "node_progress", "stage": "sending", "tool": "swap"}

    assert event_notifier.render_event_output(event_data, raw=False) == "Sending transaction..."
