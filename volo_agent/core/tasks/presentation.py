from __future__ import annotations

from typing import Any, Dict


def task_latest_update_text(task: Dict[str, Any]) -> str:
    if not isinstance(task, dict):
        return ""
    return str(task.get("latest_summary") or "").strip()


def task_latest_update_line(task: Dict[str, Any]) -> str:
    latest_update = task_latest_update_text(task)
    if not latest_update:
        return ""
    return f"Latest update: {latest_update}"


def user_facing_task_status(status: str | None) -> str:
    normalized = str(status or "").strip().upper()
    labels = {
        "WAITING_INPUT": "Needs your reply",
        "WAITING_CONFIRMATION": "Needs confirmation",
        "WAITING_EXTERNAL": "In progress",
        "WAITING_FUNDS": "Waiting for funds",
        "RUNNING": "In progress",
        "COMPLETED": "Done",
        "FAILED": "Failed",
        "CANCELLED": "Cancelled",
    }
    return labels.get(normalized, "Updated")


def format_task_detail(task: Dict[str, Any], *, task_label: str) -> str:
    lines = [str(task_label).strip() or "Task"]
    lines.append(f"Status: {user_facing_task_status(task.get('status'))}")
    latest_update_line = task_latest_update_line(task)
    if latest_update_line:
        lines.append(latest_update_line)
    return "\n".join(lines)


def format_task_line(task: Dict[str, Any]) -> str:
    task_number = task.get("task_number")
    title = task.get("title") or task_latest_update_text(task) or "Task"
    status_label = user_facing_task_status(task.get("status"))
    label = f"Task {task_number}" if task_number is not None else "Task"
    return f"- {label}: {title} ({status_label})"
