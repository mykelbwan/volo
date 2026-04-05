from __future__ import annotations

import contextlib
import inspect
from typing import Any, Sequence

from langchain_core.messages import AIMessage, BaseMessage

from api.schemas.agent import AgentTurnRequest, AgentTurnResponse
from core.tasks import finalize_conversation_turn, prepare_conversation_turn
from core.utils.async_resources import async_resource_scope
from graph.runtime_io import build_thread_config, build_turn_input


def _load_graph_app() -> Any:
    from graph.graph import app as graph_app

    return graph_app


def _latest_ai_text(messages: Sequence[BaseMessage] | None) -> str | None:
    if not messages:
        return None
    for message in reversed(messages):
        if not isinstance(message, AIMessage):
            continue
        content = message.content
        if content is None:
            return None
        return content if isinstance(content, str) else str(content)
    return None


async def run_turn(payload: AgentTurnRequest) -> AgentTurnResponse:
    prepared_turn = await prepare_conversation_turn(
        provider=payload.provider,
        provider_user_id=payload.user_id,
        default_thread_id=payload.thread_id,
        user_message=payload.message,
        conversation_id=payload.conversation_id,
        selected_task_number=payload.selected_task_number,
    )

    if prepared_turn.blocked_message is not None:
        return AgentTurnResponse(
            assistant_message=prepared_turn.blocked_message,
            conversation_id=prepared_turn.conversation_id,
            thread_id=prepared_turn.thread_id,
            selected_task_number=prepared_turn.selected_task_number,
            allocated_new_thread=prepared_turn.allocated_new_thread,
            blocked=True,
            blocked_message=prepared_turn.blocked_message,
        )

    graph_app = _load_graph_app()
    config = build_thread_config(thread_id=prepared_turn.thread_id)
    context: dict[str, Any] = {"conversation_id": prepared_turn.conversation_id}
    if payload.client_message_id:
        context["client_message_id"] = payload.client_message_id
    if payload.client_nonce:
        context["client_nonce"] = payload.client_nonce

    turn_input = build_turn_input(
        user_message=payload.message,
        user_id=payload.user_id,
        provider=payload.provider,
        username=payload.username,
        thread_id=prepared_turn.thread_id,
        selected_task_number=prepared_turn.selected_task_number,
        context=context,
    )

    final_event: dict[str, Any] | None = None
    assistant_message: str | None = None

    async with async_resource_scope():
        events = graph_app.astream(
            turn_input,
            config,
            stream_mode="values",
        )
        try:
            async for event in events:
                if not isinstance(event, dict):
                    continue
                final_event = event
                assistant_message = _latest_ai_text(event.get("messages"))
        finally:
            aclose = getattr(events, "aclose", None)
            if callable(aclose):
                with contextlib.suppress(Exception):
                    close_result = aclose()
                    if inspect.isawaitable(close_result):
                        await close_result

    selected_task_number = prepared_turn.selected_task_number
    if final_event is not None and "selected_task_number" in final_event:
        selected_task_number = await finalize_conversation_turn(
            provider=payload.provider,
            provider_user_id=payload.user_id,
            conversation_id=prepared_turn.conversation_id,
            thread_id=prepared_turn.thread_id,
            event_selected_task_number=final_event.get("selected_task_number"),
            current_selected_task_number=selected_task_number,
        )
    elif prepared_turn.thread_id != payload.thread_id:
        persisted = await finalize_conversation_turn(
            provider=payload.provider,
            provider_user_id=payload.user_id,
            conversation_id=prepared_turn.conversation_id,
            thread_id=prepared_turn.thread_id,
        )
        if persisted is not None:
            selected_task_number = persisted

    return AgentTurnResponse(
        assistant_message=assistant_message,
        conversation_id=prepared_turn.conversation_id,
        thread_id=prepared_turn.thread_id,
        selected_task_number=selected_task_number,
        allocated_new_thread=prepared_turn.allocated_new_thread,
        blocked=False,
        blocked_message=None,
    )
