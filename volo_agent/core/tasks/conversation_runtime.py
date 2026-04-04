from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from core.tasks.registry import resolve_conversation_id
from core.tasks.selection import ConversationTaskSelectionRegistry
from core.tasks.thread_resolver import (
    TurnThreadResolution,
    persist_selected_task_number,
    persist_selection_for_thread,
    resolve_turn_routing,
)

_UNSET = object()


@dataclass(frozen=True)
class PreparedConversationTurn:
    conversation_id: str
    thread_id: str
    selected_task_number: int | None
    allocated_new_thread: bool
    blocked_message: str | None
    context: Dict[str, Any]


async def prepare_conversation_turn(
    *,
    provider: str,
    provider_user_id: str,
    default_thread_id: str,
    user_message: str | None = None,
    conversation_id: str | None = None,
    selected_task_number: int | None = None,
    task_registry_cls: Any = None,
    selection_registry_cls: Any = ConversationTaskSelectionRegistry,
    thread_id_factory: Any | None = None,
) -> PreparedConversationTurn:
    resolved_conversation_id = (
        str(conversation_id).strip()
        if str(conversation_id or "").strip()
        else (
            resolve_conversation_id(
                provider=provider,
                provider_user_id=provider_user_id,
                context=None,
            )
            or f"{provider}:{provider_user_id}"
        )
    )
    context: Dict[str, Any] = {"conversation_id": resolved_conversation_id}
    if selected_task_number is not None:
        context["selected_task_number"] = int(selected_task_number)

    routing_kwargs: Dict[str, Any] = {
        "provider": provider,
        "provider_user_id": provider_user_id,
        "default_thread_id": default_thread_id,
        "user_message": user_message,
        "context": context,
        "selection_registry_cls": selection_registry_cls,
        "thread_id_factory": thread_id_factory,
    }
    if task_registry_cls is not None:
        routing_kwargs["task_registry_cls"] = task_registry_cls

    resolution: TurnThreadResolution = await resolve_turn_routing(**routing_kwargs)
    return PreparedConversationTurn(
        conversation_id=resolved_conversation_id,
        thread_id=str(resolution.thread_id),
        selected_task_number=resolution.selected_task_number,
        allocated_new_thread=resolution.allocated_new_thread,
        blocked_message=resolution.blocked_message,
        context=context,
    )


async def finalize_conversation_turn(
    *,
    provider: str,
    provider_user_id: str,
    conversation_id: str,
    thread_id: str,
    event_selected_task_number: Any = _UNSET,
    current_selected_task_number: int | None = None,
    task_registry_cls: Any = None,
    selection_registry_cls: Any = ConversationTaskSelectionRegistry,
) -> int | None:
    context = {"conversation_id": str(conversation_id)}
    if event_selected_task_number is not _UNSET:
        value = event_selected_task_number
        try:
            selected_task_number = int(value) if value is not None else None
        except Exception:
            selected_task_number = None
        await persist_selected_task_number(
            provider=provider,
            provider_user_id=provider_user_id,
            selected_task_number=selected_task_number,
            context=context,
            selection_registry_cls=selection_registry_cls,
        )
        return selected_task_number

    persist_kwargs: Dict[str, Any] = {
        "provider": provider,
        "provider_user_id": provider_user_id,
        "thread_id": str(thread_id),
        "context": context,
        "selection_registry_cls": selection_registry_cls,
    }
    if task_registry_cls is not None:
        persist_kwargs["task_registry_cls"] = task_registry_cls
    return await persist_selection_for_thread(**persist_kwargs)
