from __future__ import annotations

from typing import Any, Dict

from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig


def build_turn_input(
    *,
    user_message: str,
    user_id: str,
    provider: str,
    username: str | None = None,
    thread_id: str | None = None,
    selected_task_number: int | None = None,
    context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    ctx: Dict[str, Any] = dict(context or {})
    if thread_id and not ctx.get("thread_id"):
        ctx["thread_id"] = thread_id

    payload: Dict[str, Any] = {
        "messages": [HumanMessage(content=user_message)],
        "user_id": str(user_id),
        "provider": str(provider),
        "username": username,
    }
    if selected_task_number is not None:
        payload["selected_task_number"] = int(selected_task_number)
    if ctx:
        payload["context"] = ctx
    return payload


def build_thread_config(
    *,
    thread_id: str,
    checkpoint_ns: str | None = None,
) -> RunnableConfig:
    configurable: Dict[str, Any] = {"thread_id": str(thread_id)}
    if checkpoint_ns:
        configurable["checkpoint_ns"] = str(checkpoint_ns)
    return {"configurable": configurable}
