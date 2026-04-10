from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List

from core.tasks.follow_up_classifier import classify_follow_up_reply
from core.tasks.presentation import format_task_line


def _normalized_status(task: Dict[str, Any]) -> str:
    return str(task.get("status") or "").strip().upper()


def task_reference(task: Dict[str, Any]) -> str:
    number = task.get("task_number")
    title = str(task.get("title") or task.get("latest_summary") or "Task").strip()
    if number is None:
        return title or "Task"
    return f"Task {number}: {title}" if title else f"Task {number}"


def looks_like_explicit_action(text: str | None) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    action_markers = (
        "swap",
        "convert",
        "exchange",
        "bridge",
        "transfer",
        "send",
        "buy",
        "sell",
        "unwrap",
        "check balance",
        "balance",
        "portfolio",
        "holdings",
    )
    return any(marker in normalized for marker in action_markers)


def is_global_conversation_request(text: str | None) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    exact_matches = {
        "hi",
        "hello",
        "hey",
        "help",
        "thanks",
        "thank you",
        "status",
        "show my tasks",
        "show tasks",
        "history",
        "what can you do",
        "what's happening",
        "whats happening",
    }
    if normalized in exact_matches:
        return True
    return normalized.startswith("show my tasks") or normalized.startswith("show tasks")


def parse_task_number(text: str) -> int | None:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return None
    match = re.search(r"\btask\s+(\d+)\b", normalized)
    if not match:
        return None
    try:
        value = int(match.group(1))
    except Exception:
        return None
    return value if value > 0 else None


def is_task_detail_request(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    if any(
        normalized.startswith(prefix)
        for prefix in ("retry ", "cancel ", "edit ", "change ", "confirm ")
    ):
        return False
    return (
        normalized.startswith("show task ")
        or normalized.startswith("task ")
        or "status of task " in normalized
        or normalized.startswith("task status ")
    )


def is_task_selection_request(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    return normalized.startswith("use task ") or normalized.startswith("select task ")


def is_task_cancel_request(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    return normalized.startswith("cancel task ")


def is_task_confirm_request(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    return normalized.startswith("confirm task ") or normalized.startswith("proceed task ")


def is_task_control_request(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    prefixes = (
        "retry task ",
        "cancel task ",
        "edit task ",
        "change task ",
        "confirm task ",
        "proceed task ",
    )
    return any(normalized.startswith(prefix) for prefix in prefixes)


def is_clear_task_selection_request(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    return normalized in {
        "clear task",
        "clear selection",
        "clear task selection",
        "stop using task",
        "stop using selected task",
    }


def is_generic_task_status_request(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    return normalized in {"task status", "show task", "show selected task", "current task"}


def looks_like_follow_up_message(text: str | None) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    if (
        looks_like_explicit_action(normalized)
        or is_global_conversation_request(normalized)
        or is_task_selection_request(normalized)
        or is_clear_task_selection_request(normalized)
        or is_task_detail_request(normalized)
        or is_generic_task_status_request(normalized)
        or is_task_control_request(normalized)
    ):
        return False

    control_phrases = {
        "yes",
        "no",
        "confirm",
        "proceed",
        "cancel",
        "retry",
        "edit",
        "change",
        "go ahead",
    }
    if normalized in control_phrases:
        return True

    return False


def is_selected_task_follow_up_candidate(text: str | None) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    return not (
        looks_like_explicit_action(normalized)
        or is_global_conversation_request(normalized)
        or is_task_selection_request(normalized)
        or is_clear_task_selection_request(normalized)
        or is_task_detail_request(normalized)
        or is_generic_task_status_request(normalized)
        or is_task_control_request(normalized)
    )


def is_classifier_positive_selected_task_follow_up(
    text: str | None,
    *,
    expected_slot: str | None = None,
) -> bool:
    normalized = str(text or "").strip().lower()
    if not is_selected_task_follow_up_candidate(normalized):
        return False
    result = classify_follow_up_reply(normalized, expected_slot=expected_slot)
    return result.kind in {"control", "valid_slot_value"}


def should_route_to_selected_task(
    text: str | None,
    *,
    expected_slot: str | None = None,
) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    if (
        is_task_selection_request(normalized)
        or is_clear_task_selection_request(normalized)
        or is_task_detail_request(normalized)
        or is_generic_task_status_request(normalized)
        or is_task_control_request(normalized)
    ):
        return True

    if is_global_conversation_request(normalized):
        return False
    if looks_like_explicit_action(normalized):
        return False
    return is_classifier_positive_selected_task_follow_up(
        normalized,
        expected_slot=expected_slot,
    )


def should_route_to_referenced_task(text: str | None) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized or parse_task_number(normalized) is None:
        return False
    return (
        is_task_selection_request(normalized)
        or is_task_detail_request(normalized)
        or is_generic_task_status_request(normalized)
        or is_task_control_request(normalized)
    )


def follow_up_candidate_tasks(tasks: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidate_statuses = {
        "FAILED",
        "RUNNING",
        "WAITING_EXTERNAL",
        "WAITING_INPUT",
        "WAITING_CONFIRMATION",
        "WAITING_FUNDS",
    }
    return [task for task in tasks if _normalized_status(task) in candidate_statuses]


def build_task_disambiguation_prompt(
    tasks: Iterable[Dict[str, Any]],
    *,
    user_message: str | None = None,
) -> str:
    task_list = list(tasks)
    if not task_list:
        return "Which task do you mean? Type 'show my tasks' to review them."
    lines = ["Which task do you mean?"]
    for task in task_list[:3]:
        lines.append(format_task_line(task))
    normalized = str(user_message or "").strip().lower()
    if "cancel" in normalized:
        lines.append("Say 'cancel task <number>'.")
    elif any(marker in normalized for marker in ("confirm", "proceed", "yes")):
        lines.append("Say 'confirm task <number>' or 'use task <number>'.")
    elif "retry" in normalized:
        lines.append("Say 'retry task <number>'.")
    else:
        lines.append("Say 'show task <number>' or 'use task <number>'.")
    return "\n".join(lines)


def find_task_by_number(
    tasks: Iterable[Dict[str, Any]], task_number: int | None
) -> Dict[str, Any] | None:
    if task_number is None:
        return None
    for task in tasks:
        raw_task_number = task.get("task_number")
        if raw_task_number is None:
            continue
        try:
            if int(str(raw_task_number)) == int(task_number):
                return task
        except Exception:
            continue
    return None


def find_current_task(
    tasks: Iterable[Dict[str, Any]],
    *,
    thread_id: str | None,
    execution_id: str | None,
) -> Dict[str, Any] | None:
    execution_key = str(execution_id or "").strip()
    thread_key = str(thread_id or "").strip()
    for task in tasks:
        if execution_key and str(task.get("execution_id") or "").strip() == execution_key:
            return task
    for task in tasks:
        if thread_key and str(task.get("thread_id") or "").strip() == thread_key:
            return task
    return None


def failed_tasks(tasks: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [task for task in tasks if _normalized_status(task) == "FAILED"]


def active_tasks(tasks: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    active_statuses = {
        "RUNNING",
        "WAITING_EXTERNAL",
        "WAITING_INPUT",
        "WAITING_CONFIRMATION",
        "WAITING_FUNDS",
    }
    return [task for task in tasks if _normalized_status(task) in active_statuses]
