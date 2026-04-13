from __future__ import annotations

import inspect
import os
from typing import Any, Dict, Optional

from core.utils.upstash_client import get_async_redis, get_upstash_client

_DEFAULT_STREAM = "volo:events"
_MAX_SUMMARY_CHARS = 120


def event_stream_name() -> str:
    value = os.getenv("VOLO_EVENT_STREAM", "").strip()
    return value or _DEFAULT_STREAM


def publish_event(payload: Dict[str, Any], *, stream: Optional[str] = None) -> bool:
    if not isinstance(payload, dict):
        return False
    client = get_upstash_client()
    if client is None:
        return False
    stream_name = stream or event_stream_name()
    data = {str(k): str(v) for k, v in payload.items() if v is not None}
    if not data:
        return False
    try:
        client.xadd(stream_name, "*", data)
        return True
    except Exception:
        return False


async def publish_event_async(
    payload: Dict[str, Any],
    *,
    stream: Optional[str] = None,
) -> bool:
    if not isinstance(payload, dict):
        return False
    client = await get_async_redis()
    if client is None:
        return False
    stream_name = stream or event_stream_name()
    data = {str(k): str(v) for k, v in payload.items() if v is not None}
    if not data:
        return False
    try:
        result = client.xadd(stream_name, "*", data)
        if inspect.isawaitable(result):
            await result
        return True
    except Exception:
        return False


def coerce_event_dict(data: object) -> dict:
    if isinstance(data, dict):
        return {str(k): v for k, v in data.items()}
    if isinstance(data, list):
        if len(data) % 2 != 0:
            return {}
        try:
            it = iter(data)
            return {str(k): v for k, v in zip(it, it)}
        except Exception:
            return {}
    if isinstance(data, tuple):
        return coerce_event_dict(list(data))
    return {}


def _single_line_summary(summary: object) -> str:
    text = str(summary or "").strip().replace("\r", "")
    if not text:
        return ""
    text = text.splitlines()[0].strip()
    if len(text) > _MAX_SUMMARY_CHARS:
        text = text[: _MAX_SUMMARY_CHARS - 3].rstrip() + "..."
    return text


def _reply_actions_hint(raw_actions: str) -> str:
    actions = [part.strip().lower() for part in str(raw_actions or "").split(",")]
    actions = [action for action in actions if action]
    if not actions:
        return ""
    if actions == ["retry", "edit", "cancel"]:
        return "Reply 'retry' to try again, 'edit' to change it, or 'cancel' to stop."
    if actions == ["edit", "cancel"]:
        return "Reply 'edit' to change it or 'cancel' to stop."
    if len(actions) == 1:
        action = actions[0]
        return f"Reply '{action}' to continue."
    quoted = [f"'{action}'" for action in actions]
    if len(quoted) == 2:
        return f"Reply {quoted[0]} or {quoted[1]}."
    return "Reply " + ", ".join(quoted[:-1]) + f", or {quoted[-1]}."


def _suggestion_hint_from_message(message: object) -> str:
    text = str(message or "").strip()
    if not text:
        return ""
    for marker in (
        " I can try ",
        " Reply 'go ahead'",
        " Reply 'retry'",
        " After funding, try again.",
    ):
        idx = text.find(marker)
        if idx >= 0:
            return text[idx + 1 :].strip()
    return ""
def progress_stage_message(
    stage: str | None, elapsed_seconds: float | None = None
) -> str | None:
    stage_name = str(stage or "").strip().lower()
    elapsed_label = None
    if elapsed_seconds is not None:
        elapsed_label = f"{max(0, int(elapsed_seconds))}s"

    if stage_name == "sending":
        if elapsed_label is None:
            return "Sending transaction..."
        return f"Still sending the transaction ({elapsed_label})."
    if stage_name == "submitted":
        if elapsed_label is None:
            return "Transaction sent. Waiting for confirmation."
        return f"Transaction sent. Waiting for confirmation ({elapsed_label})."
    if stage_name == "finalizing":
        if elapsed_label is None:
            return "Confirmed on-chain. Finalizing..."
        return f"Confirmed on-chain. Finalizing ({elapsed_label})."
    return None


def format_user_event(data: dict) -> str | None:
    event = str(data.get("event") or "").strip().lower()
    tool = str(data.get("tool") or "").strip().lower()
    status = str(data.get("status") or "").strip().upper()
    stage = str(data.get("stage") or "").strip().lower()
    summary = _single_line_summary(data.get("summary"))

    if event == "node_progress":
        return progress_stage_message(stage)

    if event == "node_completed" and status == "SUCCESS":
        if tool == "swap":
            return "Done. Swap complete."
        if tool == "transfer":
            return "Done. Transfer complete."
        if tool == "bridge":
            return summary or "Done. Bridge complete."
        if tool == "unwrap":
            return "Done. Unwrap complete."
        return summary or "Done."

    if event == "node_failed":
        if tool:
            prefix = f"{tool.capitalize()} failed."
        else:
            prefix = "Action failed."
        parts = [prefix]
        if summary:
            parts.append(summary)
        return " ".join(parts).strip()

    return None


def format_event(data: dict) -> str:
    event = data.get("event", "event")
    status = data.get("status", "")
    summary = _single_line_summary(data.get("summary", ""))
    node_id = data.get("node_id", "")
    tx_hash = data.get("tx_hash", "")
    parts = [str(event)]
    if status:
        parts.append(f"status={status}")
    if node_id:
        parts.append(f"node={node_id}")
    if tx_hash:
        parts.append(f"tx={tx_hash}")
    if summary:
        parts.append(f"summary={summary}")
    return " ".join(parts)
