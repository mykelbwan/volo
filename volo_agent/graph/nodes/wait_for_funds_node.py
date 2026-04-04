from __future__ import annotations

import secrets
from typing import Any, Dict

from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from langgraph.types import interrupt

from graph.agent_state import AgentState

_RESUME_TOKEN_MIN_LENGTH = 32


async def wait_for_funds_node(
    state: AgentState, config: RunnableConfig
) -> Dict[str, Any]:
    wait_info = state.get("waiting_for_funds") or {}
    wait_id = str(wait_info.get("wait_id") or "").strip()
    resume_token = str(wait_info.get("resume_token") or "").strip()
    thread_id = str((config.get("configurable") or {}).get("thread_id") or "")

    if not wait_id or len(resume_token) < _RESUME_TOKEN_MIN_LENGTH:
        return {
            "route_decision": "end",
            "waiting_for_funds": None,
            "messages": [
                AIMessage(
                    content=(
                        "The queued funds wait could not be resumed because the wait "
                        "record or resume token is missing. Recovery path: run the task again."
                    )
                )
            ],
        }

    interrupt_value = {
        "status": "waiting_funds",
        "thread_id": thread_id,
        "wait_id": wait_id,
        "resume_token": resume_token,
        "node_id": wait_info.get("node_id"),
        "message": (
            wait_info.get("message")
            or "This task is waiting for funds held by another active task."
        ),
    }
    resume_payload: Dict[str, Any] = interrupt(interrupt_value)
    resumed_wait_id = str(resume_payload.get("wait_id") or wait_id).strip()
    resumed_token = str(resume_payload.get("resume_token") or "").strip()
    if resumed_wait_id != wait_id or not secrets.compare_digest(
        resumed_token,
        resume_token,
    ):
        return {
            "route_decision": "end",
            "waiting_for_funds": None,
            "messages": [
                AIMessage(
                    content=(
                        "The queued funds wait resumed with an unexpected token. "
                        "Recovery path: check task status and retry if needed."
                    )
                )
            ],
        }

    return {
        "route_decision": "resume",
        "waiting_for_funds": None,
        "auto_resume_execution": True,
    }
