from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, MutableMapping

from langchain_core.runnables import RunnableConfig
from langgraph.types import Command

from core.observer.trigger_matcher import MatchResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResumeRuntimeDeps:
    app: Any
    mark_triggered_or_reschedule: Callable[[str, Any], Awaitable[bool]]
    mark_failed: Callable[[str, str], Awaitable[None]]
    timeout_seconds: float


async def resume_thread(
    match: MatchResult,
    *,
    deps: ResumeRuntimeDeps,
    stats: MutableMapping[str, int],
) -> None:
    trigger_id = match.trigger_id
    thread_id = match.thread_id
    user_id = match.user_id
    resume_payload = match.resume_payload

    logger.info(
        "ObserverWatcher: attempting to resume trigger %s (thread=%s, user=%s).",
        trigger_id[:8],
        thread_id[:8],
        user_id,
    )

    marked = await deps.mark_triggered_or_reschedule(
        trigger_id, match.next_execute_at
    )
    if not marked:
        logger.info(
            "ObserverWatcher: trigger %s already processed — skipping resume.",
            trigger_id[:8],
        )
        return

    config: RunnableConfig = {
        "configurable": {
            "thread_id": thread_id,
        }
    }

    try:
        if "trigger_fire_id" not in resume_payload:
            resume_payload = {
                **resume_payload,
                "trigger_fire_id": str(uuid.uuid4()),
            }
        logger.info(
            "ObserverWatcher: resuming LangGraph thread %s with payload: %s",
            thread_id[:8],
            {k: v for k, v in resume_payload.items() if k != "trigger_condition"},
        )

        events = deps.app.astream(
            Command(resume=resume_payload),
            config,
            stream_mode="values",
        )
        await asyncio.wait_for(
            _drain_resume_stream(events, thread_id=thread_id),
            timeout=deps.timeout_seconds,
        )

        stats["successful_resumes"] += 1
        logger.info(
            "ObserverWatcher: trigger %s successfully executed (thread=%s).",
            trigger_id[:8],
            thread_id[:8],
        )

    except asyncio.TimeoutError:
        error_msg = (
            f"Graph resume timed out after {deps.timeout_seconds}s "
            f"(thread={thread_id})."
        )
        logger.error("ObserverWatcher: %s", error_msg)
        stats["failed_resumes"] += 1
        await deps.mark_failed(trigger_id, error_msg)

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.error(
            "ObserverWatcher: failed to resume trigger %s (thread=%s): %s",
            trigger_id[:8],
            thread_id[:8],
            error_msg,
            exc_info=True,
        )
        stats["failed_resumes"] += 1
        try:
            await deps.mark_failed(trigger_id, error_msg)
        except Exception as mark_exc:
            logger.error(
                "ObserverWatcher: could not mark trigger %s as failed: %s",
                trigger_id[:8],
                mark_exc,
            )


async def _drain_resume_stream(events: Any, *, thread_id: str) -> None:
    last_message = None
    async for event in events:
        messages = event.get("messages", [])
        if messages:
            last_msg = messages[-1]
            if hasattr(last_msg, "type") and last_msg.type == "ai":
                last_message = last_msg

    if last_message:
        logger.info(
            "ObserverWatcher: thread %s completed. Final message: %s",
            thread_id[:8],
            str(last_message.content)[:200],
        )
