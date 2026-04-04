from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from core.utils.event_stream import (
    coerce_event_dict,
    format_event,
    format_user_event,
    progress_stage_message,
)

WAIT_UPDATE_INITIAL_SECONDS = 8.0
WAIT_UPDATE_INTERVAL_SECONDS = 12.0


@dataclass
class InflightProgress:
    stage: str
    started_at: float
    last_notice_at: float


def progress_key(event_data: dict) -> str:
    thread_id = str(event_data.get("thread_id") or "unknown")
    node_id = str(event_data.get("node_id") or "unknown")
    return f"{thread_id}:{node_id}"


def render_event_output(event_data: dict, *, raw: bool) -> str:
    if raw:
        return format_event(event_data)
    return format_user_event(event_data) or format_event(event_data)


def update_inflight_progress(
    inflight: dict[str, InflightProgress],
    event_data: dict,
    *,
    now: float,
) -> None:
    event_name = str(event_data.get("event") or "").strip().lower()
    key = progress_key(event_data)
    if event_name == "node_progress":
        stage = str(event_data.get("stage") or "").strip().lower()
        if stage:
            inflight[key] = InflightProgress(
                stage=stage,
                started_at=now,
                last_notice_at=now,
            )
        return
    if event_name in {"node_completed", "node_failed"}:
        inflight.pop(key, None)


def collect_wait_updates(
    inflight: dict[str, InflightProgress],
    *,
    now: float,
) -> list[str]:
    messages: list[str] = []
    for state in inflight.values():
        elapsed = max(0.0, now - state.started_at)
        since_last = max(0.0, now - state.last_notice_at)
        if elapsed < WAIT_UPDATE_INITIAL_SECONDS:
            continue
        if since_last < WAIT_UPDATE_INTERVAL_SECONDS:
            continue
        reminder = progress_stage_message(state.stage, elapsed)
        if reminder:
            messages.append(reminder)
            state.last_notice_at = now
    return messages


def ensure_group(client: Any, stream: str, group: str) -> None:
    try:
        client.xgroup_create(stream, group, id="0-0", mkstream=True)
    except Exception as exc:
        message = str(exc)
        if "BUSYGROUP" in message or "Consumer Group name already exists" in message:
            return
        raise


def run_notifier(
    *,
    client: Any,
    stream: str,
    group: str,
    consumer: str,
    block_ms: int,
    count: int,
    once: bool,
    tail: int,
    raw: bool,
    stdout_write: Callable[[str], None],
    stderr_write: Callable[[str], None],
) -> int:
    inflight: dict[str, InflightProgress] = {}

    ensure_group(client, stream, group)

    if tail > 0:
        try:
            history = client.xrange(stream, "-", "+", count=tail)
        except Exception:
            history = []
        for _msg_id, data in history:
            event_data = coerce_event_dict(data)
            if not event_data:
                _log_malformed_event(payload=data, reason="unexpected payload", stderr_write=stderr_write)
                continue
            now = time.monotonic()
            update_inflight_progress(inflight, event_data, now=now)
            stdout_write(render_event_output(event_data, raw=raw))

    while True:
        response = client.xreadgroup(
            group,
            consumer,
            streams={stream: ">"},
            count=count,
        )
        if not response:
            if not raw:
                for line in collect_wait_updates(inflight, now=time.monotonic()):
                    stdout_write(line)
            if once:
                break
            time.sleep(max(0.1, block_ms / 1000.0))
            continue

        for _stream_name, messages in response:
            for msg_id, data in messages:
                event_data = coerce_event_dict(data)
                if not event_data:
                    _log_malformed_event(payload=data, reason="unexpected payload", stderr_write=stderr_write)
                    continue
                now = time.monotonic()
                update_inflight_progress(inflight, event_data, now=now)
                stdout_write(render_event_output(event_data, raw=raw))
                try:
                    client.xack(stream, group, msg_id)
                except Exception:
                    pass

        if once:
            break

    return 0


def _log_malformed_event(
    *,
    payload: object,
    reason: str,
    stderr_write: Callable[[str], None],
) -> None:
    preview = repr(payload)
    if len(preview) > 240:
        preview = preview[:240] + "…"
    stderr_write(f"[event_notifier] malformed event payload ({reason}): {preview}")
