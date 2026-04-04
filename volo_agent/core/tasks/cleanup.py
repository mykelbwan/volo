from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from core.tasks.registry import ConversationTaskRegistry, normalize_task_status
from core.tasks.selection import ConversationTaskSelectionRegistry

_TERMINAL_TASK_STATUSES = {"FAILED", "COMPLETED", "CANCELLED"}
_LOGGER = logging.getLogger("volo.tasks.cleanup")


def _task_number(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


async def run_terminal_task_cleanup(
    *,
    task_record: Dict[str, Any] | None,
    task_registry_cls: Any = ConversationTaskRegistry,
    selection_registry_cls: Any = ConversationTaskSelectionRegistry,
) -> None:
    if not isinstance(task_record, dict):
        return
    conversation_id = str(task_record.get("conversation_id") or "").strip()
    status = normalize_task_status(task_record.get("status"))
    if not conversation_id or status not in _TERMINAL_TASK_STATUSES:
        return

    task_number = _task_number(task_record.get("task_number"))
    try:
        task_registry = task_registry_cls()
    except Exception as exc:
        _LOGGER.warning(
            "task_cleanup_registry_unavailable conversation_id=%s detail=%s",
            conversation_id,
            exc,
        )
        return

    selection_registry = None
    try:
        selection_registry = selection_registry_cls()
    except Exception as exc:
        _LOGGER.warning(
            "task_cleanup_selection_unavailable conversation_id=%s detail=%s",
            conversation_id,
            exc,
        )

    if selection_registry is not None and task_number is not None:
        try:
            await selection_registry.clear_selected_task_number_if_matches(
                conversation_id=conversation_id,
                task_number=task_number,
            )
        except Exception as exc:
            _LOGGER.warning(
                "task_cleanup_selection_clear_failed conversation_id=%s task_number=%s detail=%s",
                conversation_id,
                task_number,
                exc,
            )

    try:
        await task_registry.backfill_failed_task_expiry_fields(conversation_id)
    except Exception as exc:
        _LOGGER.warning(
            "task_cleanup_failed_ttl_backfill_failed conversation_id=%s detail=%s",
            conversation_id,
            exc,
        )

    if selection_registry is None:
        return

    try:
        selected_task_number = await selection_registry.get_selected_task_number(
            conversation_id
        )
    except Exception as exc:
        _LOGGER.warning(
            "task_cleanup_selection_lookup_failed conversation_id=%s detail=%s",
            conversation_id,
            exc,
        )
        return

    protected_numbers = (
        {int(selected_task_number)} if selected_task_number is not None else set()
    )
    try:
        await task_registry.prune_terminal_tasks(
            conversation_id,
            protected_task_numbers=protected_numbers,
        )
    except Exception as exc:
        _LOGGER.warning(
            "task_cleanup_prune_failed conversation_id=%s detail=%s",
            conversation_id,
            exc,
        )


def schedule_terminal_task_cleanup(
    *,
    task_record: Dict[str, Any] | None,
    task_registry_cls: Any = ConversationTaskRegistry,
    selection_registry_cls: Any = ConversationTaskSelectionRegistry,
) -> None:
    if not isinstance(task_record, dict):
        return
    if normalize_task_status(task_record.get("status")) not in _TERMINAL_TASK_STATUSES:
        return
    try:
        asyncio.create_task(
            run_terminal_task_cleanup(
                task_record=task_record,
                task_registry_cls=task_registry_cls,
                selection_registry_cls=selection_registry_cls,
            )
        )
    except Exception as exc:
        conversation_id = str(task_record.get("conversation_id") or "").strip()
        _LOGGER.warning(
            "task_cleanup_schedule_failed conversation_id=%s detail=%s",
            conversation_id,
            exc,
        )
