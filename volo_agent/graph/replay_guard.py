from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Sequence, Tuple

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

_LOGGER = logging.getLogger("volo.replay_guard")
_LOCK = threading.Lock()

_COUNTERS: Dict[str, int] = {
    "parse_scope_total": 0,
    "parse_scope_full": 0,
    "parse_scope_last_user": 0,
    "parse_scope_recent_turn": 0,
    "parse_scope_recent_window": 0,
    "parse_messages_total": 0,
    "parse_messages_selected_total": 0,
    "parse_tokens_estimate_total": 0,
    "parse_tokens_estimate_selected_total": 0,
    "replay_prevented_total": 0,
    "history_trim_events_total": 0,
    "history_messages_dropped_total": 0,
}
_COUNTER_LOCKS: Dict[str, threading.Lock] = {
    name: threading.Lock() for name in _COUNTERS
}


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "")
    if raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def replay_guard_enabled() -> bool:
    return _bool_env("VOLO_ENABLE_REPLAY_GUARD", True)


def normalize_parse_scope(scope: str | None) -> str | None:
    text = str(scope or "").strip().lower()
    if not text:
        return None
    if text in {"full", "all", "none", "off", "disabled"}:
        return None
    if text in {"last_user", "recent_turn"}:
        return text
    if text.startswith("recent_window:"):
        _, _, raw = text.partition(":")
        try:
            value = int(raw.strip())
        except ValueError:
            return None
        if value <= 0:
            return None
        return f"recent_window:{value}"
    return None


def parse_scope_default() -> str | None:
    raw = os.getenv("VOLO_PARSE_SCOPE_DEFAULT", "").strip()
    if not raw:
        return None
    normalized = normalize_parse_scope(raw)
    if normalized is None and raw.lower() not in {
        "full",
        "all",
        "none",
        "off",
        "disabled",
    }:
        _LOGGER.warning(
            "invalid_parse_scope_default value=%s fallback=full_history",
            raw,
        )
    return normalized


def _content_str(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(str(part) for part in content)
    return str(content)


def _normalize_dedup_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _mapping_value(mapping: Mapping[str, Any] | None, *keys: str) -> str | None:
    if not isinstance(mapping, Mapping):
        return None
    for key in keys:
        if key not in mapping:
            continue
        normalized = _normalize_dedup_value(mapping.get(key))
        if normalized:
            return normalized
    return None


def extract_client_dedup_fields(
    message: Any | None,
    *,
    context: Mapping[str, Any] | None = None,
) -> Dict[str, str | None]:
    additional_kwargs = getattr(message, "additional_kwargs", None)
    response_metadata = getattr(message, "response_metadata", None)
    message_id = (
        _mapping_value(context, "client_message_id", "message_id")
        or _normalize_dedup_value(getattr(message, "id", None))
        or _mapping_value(
            additional_kwargs,
            "client_message_id",
            "message_id",
            "event_id",
            "update_id",
        )
        or _mapping_value(
            response_metadata,
            "client_message_id",
            "message_id",
            "event_id",
            "update_id",
        )
    )
    client_nonce = (
        _mapping_value(context, "client_nonce", "nonce")
        or _mapping_value(additional_kwargs, "client_nonce", "nonce", "request_nonce")
        or _mapping_value(response_metadata, "client_nonce", "nonce", "request_nonce")
    )
    return {
        "client_message_id": message_id,
        "client_nonce": client_nonce,
    }


def compute_execution_dedup_key(
    *,
    intent_payload: Any,
    client_message_id: str | None,
    client_nonce: str | None,
) -> str | None:
    message_id = _normalize_dedup_value(client_message_id)
    nonce = _normalize_dedup_value(client_nonce)
    if not message_id and not nonce:
        return None

    payload = {
        "intent_payload": intent_payload,
        "client_message_id": message_id,
        "client_nonce": nonce,
    }
    canonical = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def estimate_message_tokens(messages: Sequence[BaseMessage]) -> int:
    chars = 0
    for msg in messages:
        chars += len(_content_str(getattr(msg, "content", "")))
    if chars <= 0:
        return 0
    # Fast approximation suitable for telemetry and guardrail tuning.
    return max(1, chars // 4)


def select_messages_for_scope(
    messages: Sequence[BaseMessage],
    scope: str | None,
) -> Tuple[Sequence[BaseMessage], Sequence[BaseMessage], str | None]:
    normalized_scope = normalize_parse_scope(scope)
    if not normalized_scope:
        return messages, [], None

    if normalized_scope == "last_user":
        for idx in range(len(messages) - 1, -1, -1):
            if isinstance(messages[idx], HumanMessage):
                return [messages[idx]], messages[:idx], normalized_scope
        return messages, [], None

    if normalized_scope == "recent_turn":
        last_user_idx = None
        for idx in range(len(messages) - 1, -1, -1):
            if isinstance(messages[idx], HumanMessage):
                last_user_idx = idx
                break
        if last_user_idx is None:
            return messages, [], None

        selected = [messages[last_user_idx]]
        start_idx = last_user_idx
        if last_user_idx > 0 and isinstance(messages[last_user_idx - 1], AIMessage):
            selected.insert(0, messages[last_user_idx - 1])
            start_idx = last_user_idx - 1
        return selected, messages[:start_idx], normalized_scope

    if normalized_scope.startswith("recent_window:"):
        _, _, raw = normalized_scope.partition(":")
        try:
            window = int(raw)
        except ValueError:
            return messages, [], None
        if window <= 0:
            return messages, [], None
        if len(messages) <= window:
            return messages, [], normalized_scope
        return messages[-window:], messages[:-window], normalized_scope

    return messages, [], None


def _scope_counter_key(scope: str | None) -> str:
    if not scope:
        return "parse_scope_full"
    if scope.startswith("recent_window:"):
        return "parse_scope_recent_window"
    return f"parse_scope_{scope}"


def _counter_add(name: str, value: int = 1) -> None:
    if value == 0:
        return
    lock = _COUNTER_LOCKS[name]
    # Striping the locks by counter removes the single global bottleneck while
    # preserving thread-safe increments for telemetry updates.
    with lock:
        _COUNTERS[name] = int(_COUNTERS.get(name, 0)) + int(value)


def observe_parse_scope(
    *,
    scope: str | None,
    messages_total: int,
    messages_selected: int,
    token_estimate_total: int,
    token_estimate_selected: int,
) -> None:
    _counter_add("parse_scope_total", 1)
    _counter_add(_scope_counter_key(scope), 1)
    _counter_add("parse_messages_total", messages_total)
    _counter_add("parse_messages_selected_total", messages_selected)
    _counter_add("parse_tokens_estimate_total", token_estimate_total)
    _counter_add("parse_tokens_estimate_selected_total", token_estimate_selected)

    _LOGGER.info(
        "parse_scope scope=%s messages_total=%s messages_selected=%s token_estimate_total=%s token_estimate_selected=%s",
        scope or "full",
        messages_total,
        messages_selected,
        token_estimate_total,
        token_estimate_selected,
    )


def observe_replay_guard(
    *,
    replay_prevented: bool,
    parse_scope: str | None,
    messages: Sequence[BaseMessage],
    reason: str,
) -> None:
    if replay_prevented:
        _counter_add("replay_prevented_total", 1)
    _LOGGER.info(
        "replay_guard replay_prevented=%s reason=%s parse_scope=%s message_count=%s token_estimate=%s",
        replay_prevented,
        reason,
        parse_scope or "full",
        len(messages),
        estimate_message_tokens(messages),
    )


def observe_history_retention(*, before_count: int, after_count: int) -> None:
    dropped = max(0, int(before_count) - int(after_count))
    if dropped <= 0:
        return
    _counter_add("history_trim_events_total", 1)
    _counter_add("history_messages_dropped_total", dropped)
    _LOGGER.info(
        "message_history_retention before=%s after=%s dropped=%s",
        before_count,
        after_count,
        dropped,
    )


def build_rolling_summary_artifact(
    older_messages: Sequence[BaseMessage],
    *,
    scope: str | None,
    max_items: int = 6,
) -> Dict[str, Any] | None:
    if not older_messages:
        return None

    highlights = []
    for msg in older_messages:
        if not isinstance(msg, HumanMessage):
            continue
        text = " ".join(_content_str(msg.content).split())
        if not text:
            continue
        if len(text) > 140:
            text = f"{text[:137]}..."
        highlights.append(text)

    if not highlights:
        for msg in older_messages[-max_items:]:
            text = " ".join(_content_str(getattr(msg, "content", "")).split())
            if not text:
                continue
            if len(text) > 140:
                text = f"{text[:137]}..."
            highlights.append(text)

    if len(highlights) > max_items:
        highlights = highlights[-max_items:]

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": scope or "full",
        "summarized_message_count": len(older_messages),
        "highlights": highlights,
        "note": (
            "Older context was summarized to reduce parsing latency and "
            "prevent replay of stale intents."
        ),
    }
