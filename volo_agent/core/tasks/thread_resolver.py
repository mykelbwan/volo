from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Dict

from core.tasks.registry import ConversationTaskRegistry, resolve_conversation_id
from core.tasks.router import (
    active_tasks,
    build_task_disambiguation_prompt,
    follow_up_candidate_tasks,
    looks_like_explicit_action,
    looks_like_follow_up_message,
    parse_task_number,
    should_route_to_referenced_task,
    should_route_to_selected_task,
)
from core.tasks.selection import ConversationTaskSelectionRegistry


def _selection_routing_mode(task: Dict[str, Any] | None) -> str:
    if not isinstance(task, dict):
        return "invalid"
    task_thread_id = str(task.get("thread_id") or "").strip()
    if not task_thread_id:
        return "invalid"
    status = str(task.get("status") or "").strip().upper()
    if status in {"COMPLETED", "CANCELLED"}:
        return "inspect_only"
    return "active"


@dataclass(frozen=True)
class TurnThreadResolution:
    thread_id: str
    selected_task_number: int | None
    allocated_new_thread: bool
    blocked_message: str | None = None


async def _load_candidate_tasks(
    *,
    task_registry_cls: Any,
    conversation_id: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    try:
        task_registry = task_registry_cls()
    except Exception:
        return []

    for method_name in ("list_recent", "list_active"):
        method = getattr(task_registry, method_name, None)
        if not callable(method):
            continue
        try:
            tasks = await method(str(conversation_id), limit=limit)
        except Exception:
            continue
        if isinstance(tasks, list):
            return tasks
    return []


async def persist_selected_task_number(
    *,
    provider: str,
    provider_user_id: str,
    selected_task_number: int | None,
    context: Dict[str, Any] | None = None,
    selection_registry_cls: Any = ConversationTaskSelectionRegistry,
) -> str | None:
    conversation_id = resolve_conversation_id(
        provider=provider,
        provider_user_id=provider_user_id,
        context=context,
    )
    if not conversation_id:
        return None
    try:
        selection_registry = selection_registry_cls()
    except Exception:
        return conversation_id
    try:
        await selection_registry.set_selected_task_number(
            conversation_id=str(conversation_id),
            task_number=selected_task_number,
        )
    except Exception:
        pass
    return str(conversation_id)


async def resolve_selected_thread_id(
    *,
    provider: str,
    provider_user_id: str,
    default_thread_id: str,
    user_message: str | None = None,
    context: Dict[str, Any] | None = None,
    task_registry_cls: Any = ConversationTaskRegistry,
    selection_registry_cls: Any = ConversationTaskSelectionRegistry,
) -> tuple[str, int | None]:
    conversation_id = resolve_conversation_id(
        provider=provider,
        provider_user_id=provider_user_id,
        context=context,
    )
    if not conversation_id:
        return str(default_thread_id), None

    ctx = dict(context or {})
    selected_task_number = ctx.get("selected_task_number")
    selection_registry = None
    if selected_task_number is None:
        try:
            selection_registry = selection_registry_cls()
            selected_task_number = await selection_registry.get_selected_task_number(
                str(conversation_id)
            )
        except Exception:
            selected_task_number = None
    if selected_task_number is None:
        return str(default_thread_id), None

    try:
        task_number = int(selected_task_number)
    except Exception:
        return str(default_thread_id), None

    if not should_route_to_selected_task(user_message):
        return str(default_thread_id), task_number

    try:
        task_registry = task_registry_cls()
        task = await task_registry.get_task_by_number(
            str(conversation_id), task_number=task_number
        )
    except Exception:
        task = None
    mode = _selection_routing_mode(task)
    if mode == "invalid":
        if selection_registry is None:
            try:
                selection_registry = selection_registry_cls()
            except Exception:
                selection_registry = None
        if selection_registry is not None:
            try:
                await selection_registry.set_selected_task_number(
                    conversation_id=str(conversation_id),
                    task_number=None,
                )
            except Exception:
                pass
        return str(default_thread_id), None
    if mode == "inspect_only":
        return str(default_thread_id), task_number

    task_thread_id = str(task.get("thread_id") or "").strip()
    return task_thread_id, task_number
async def resolve_turn_routing(
    *,
    provider: str,
    provider_user_id: str,
    default_thread_id: str,
    user_message: str | None = None,
    context: Dict[str, Any] | None = None,
    task_registry_cls: Any = ConversationTaskRegistry,
    selection_registry_cls: Any = ConversationTaskSelectionRegistry,
    thread_id_factory: Any | None = None,
) -> TurnThreadResolution:
    selected_thread_id, selected_task_number = await resolve_selected_thread_id(
        provider=provider,
        provider_user_id=provider_user_id,
        default_thread_id=default_thread_id,
        user_message=user_message,
        context=context,
        task_registry_cls=task_registry_cls,
        selection_registry_cls=selection_registry_cls,
    )
    if str(selected_thread_id) != str(default_thread_id):
        return TurnThreadResolution(
            thread_id=str(selected_thread_id),
            selected_task_number=selected_task_number,
            allocated_new_thread=False,
        )

    referenced_task_number = parse_task_number(user_message or "")
    conversation_id = resolve_conversation_id(
        provider=provider,
        provider_user_id=provider_user_id,
        context=context,
    )
    if should_route_to_referenced_task(user_message):
        if conversation_id and referenced_task_number is not None:
            try:
                task_registry = task_registry_cls()
                referenced_task = await task_registry.get_task_by_number(
                    str(conversation_id), task_number=int(referenced_task_number)
                )
            except Exception:
                referenced_task = None
            mode = _selection_routing_mode(referenced_task)
            if mode == "active":
                return TurnThreadResolution(
                    thread_id=str(referenced_task.get("thread_id")),
                    selected_task_number=int(referenced_task_number),
                    allocated_new_thread=False,
                )
            if mode == "inspect_only":
                return TurnThreadResolution(
                    thread_id=str(default_thread_id),
                    selected_task_number=int(referenced_task_number),
                    allocated_new_thread=False,
                )

    if not conversation_id:
        return TurnThreadResolution(
            thread_id=str(default_thread_id),
            selected_task_number=selected_task_number,
            allocated_new_thread=False,
        )

    tasks = await _load_candidate_tasks(
        task_registry_cls=task_registry_cls,
        conversation_id=str(conversation_id),
        limit=20,
    )

    if not looks_like_explicit_action(user_message):
        if looks_like_follow_up_message(user_message):
            candidates = [
                task
                for task in follow_up_candidate_tasks(tasks)
                if _selection_routing_mode(task) == "active"
            ]
            if len(candidates) == 1:
                task = candidates[0]
                try:
                    task_number = int(task.get("task_number"))
                except Exception:
                    task_number = selected_task_number
                return TurnThreadResolution(
                    thread_id=str(task.get("thread_id")),
                    selected_task_number=task_number,
                    allocated_new_thread=False,
                )
            if len(candidates) > 1:
                return TurnThreadResolution(
                    thread_id=str(default_thread_id),
                    selected_task_number=selected_task_number,
                    allocated_new_thread=False,
                    blocked_message=build_task_disambiguation_prompt(
                        candidates,
                        user_message=user_message,
                    ),
                )
        return TurnThreadResolution(
            thread_id=str(default_thread_id),
            selected_task_number=selected_task_number,
            allocated_new_thread=False,
        )

    if not active_tasks(tasks):
        return TurnThreadResolution(
            thread_id=str(default_thread_id),
            selected_task_number=selected_task_number,
            allocated_new_thread=False,
        )

    factory = thread_id_factory or (lambda: str(uuid.uuid4()))
    try:
        new_thread_id = str(factory())
    except Exception:
        new_thread_id = str(uuid.uuid4())
    return TurnThreadResolution(
        thread_id=new_thread_id,
        selected_task_number=selected_task_number,
        allocated_new_thread=True,
    )


async def persist_selection_for_thread(
    *,
    provider: str,
    provider_user_id: str,
    thread_id: str,
    context: Dict[str, Any] | None = None,
    task_registry_cls: Any = ConversationTaskRegistry,
    selection_registry_cls: Any = ConversationTaskSelectionRegistry,
) -> int | None:
    conversation_id = resolve_conversation_id(
        provider=provider,
        provider_user_id=provider_user_id,
        context=context,
    )
    if not conversation_id:
        return None
    try:
        task_registry = task_registry_cls()
        task = await task_registry.get_latest_task_for_thread(
            str(conversation_id), thread_id=str(thread_id)
        )
    except Exception:
        task = None
    if not isinstance(task, dict):
        return None
    task_number = task.get("task_number")
    try:
        selected_task_number = int(task_number)
    except Exception:
        return None
    await persist_selected_task_number(
        provider=provider,
        provider_user_id=provider_user_id,
        selected_task_number=selected_task_number,
        context=context,
        selection_registry_cls=selection_registry_cls,
    )
    return selected_task_number
