from __future__ import annotations

import asyncio
from typing import Any, Dict

from core.tasks.cleanup import schedule_terminal_task_cleanup
from core.tasks.registry import (
    ConversationTaskRegistry,
    draft_execution_id,
    resolve_conversation_id,
)


def _state_thread_id(state: Dict[str, Any]) -> str | None:
    context = state.get("context") or {}
    user_info = state.get("user_info") or {}
    thread_id = context.get("thread_id") or user_info.get("thread_id")
    if not thread_id:
        return None
    return str(thread_id)


def task_title_from_intent(intent_data: Dict[str, Any] | Any) -> str:
    if isinstance(intent_data, dict):
        intent_type = str(intent_data.get("intent_type") or "").strip().lower()
        slots = intent_data.get("slots") or {}
        raw_input = str(intent_data.get("raw_input") or "").strip()
    else:
        intent_type = str(getattr(intent_data, "intent_type", "") or "").strip().lower()
        slots = getattr(intent_data, "slots", {}) or {}
        raw_input = str(getattr(intent_data, "raw_input", "") or "").strip()

    if intent_type == "swap":
        amount = slots.get("amount")
        token_in = slots.get("token_in") or {}
        token_out = slots.get("token_out") or {}
        token_in_symbol = (
            token_in.get("symbol") if isinstance(token_in, dict) else token_in
        )
        token_out_symbol = (
            token_out.get("symbol") if isinstance(token_out, dict) else token_out
        )
        if amount and token_in_symbol and token_out_symbol:
            return f"Swap {amount} {token_in_symbol} to {token_out_symbol}"
        return "Swap tokens"

    if intent_type == "bridge":
        amount = slots.get("amount")
        token = slots.get("token_in") or slots.get("token_symbol")
        token_symbol = token.get("symbol") if isinstance(token, dict) else token
        target_chain = slots.get("target_chain")
        if amount and token_symbol and target_chain:
            return f"Bridge {amount} {token_symbol} to {target_chain}"
        return "Bridge tokens"

    if intent_type == "transfer":
        amount = slots.get("amount")
        token = slots.get("token") or slots.get("token_symbol")
        token_symbol = token.get("symbol") if isinstance(token, dict) else token
        recipient = slots.get("recipient")
        if amount and token_symbol and recipient:
            return f"Send {amount} {token_symbol} to {recipient}"
        return "Send tokens"

    if intent_type == "unwrap":
        amount = slots.get("amount")
        token = slots.get("token") or slots.get("token_symbol")
        token_symbol = token.get("symbol") if isinstance(token, dict) else token
        chain = slots.get("chain")
        if token_symbol and chain:
            if amount:
                return f"Unwrap {amount} {token_symbol} on {chain}"
            return f"Unwrap {token_symbol} on {chain}"
        return "Unwrap wrapped tokens"

    if raw_input:
        return raw_input[:120]
    return "Task"


async def upsert_task_from_state(
    state: Dict[str, Any],
    *,
    title: str,
    status: str,
    latest_summary: str | None = None,
    tool: str | None = None,
    tx_hash: str | None = None,
    error_category: str | None = None,
    execution_id: str | None = None,
    registry_cls: Any = ConversationTaskRegistry,
    timeout_seconds: float = 0.2,
) -> Dict[str, Any] | None:
    provider = str(state.get("provider") or "")
    provider_user_id = str(state.get("user_id") or "")
    conversation_id = resolve_conversation_id(
        provider=provider,
        provider_user_id=provider_user_id,
        context=state.get("context"),
    )
    if not conversation_id:
        return None

    thread_id = _state_thread_id(state)
    if not thread_id:
        return None

    user_info = state.get("user_info") or {}
    user_id = (
        user_info.get("volo_user_id")
        if isinstance(user_info, dict) and user_info.get("volo_user_id")
        else state.get("user_id")
    )
    if not user_id:
        return None

    resolved_execution_id = (
        str(execution_id)
        if execution_id
        else str(state.get("execution_id") or draft_execution_id(thread_id))
    )
    try:
        registry = registry_cls()
    except Exception:
        return None

    try:
        record = await asyncio.wait_for(
            registry.upsert_execution_task(
                conversation_id=str(conversation_id),
                execution_id=resolved_execution_id,
                thread_id=str(thread_id),
                provider=provider,
                provider_user_id=provider_user_id,
                user_id=str(user_id),
                title=str(title),
                status=str(status),
                latest_summary=latest_summary,
                tool=tool,
                tx_hash=tx_hash,
                error_category=error_category,
            ),
            timeout=timeout_seconds,
        )
        try:
            schedule_terminal_task_cleanup(
                task_record=record,
                task_registry_cls=registry_cls,
            )
        except Exception:
            pass
        return record
    except Exception:
        return None
