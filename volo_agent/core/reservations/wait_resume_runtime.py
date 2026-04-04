from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Awaitable, Callable, MutableMapping

from langgraph.types import Command

from core.reservations.models import FundsWaitRecord, iso_utc, utc_now

logger = logging.getLogger(__name__)
_RESUME_TOKEN_MIN_LENGTH = 32
_MAX_BACKOFF_SECONDS = 3600


@dataclass(frozen=True)
class FundsWaitResumeDeps:
    app: Any
    timeout_seconds: float
    get_wait: Callable[[str], Awaitable[FundsWaitRecord | None]]
    mark_wait_queued: Callable[..., Awaitable[None]]


def _calculate_backoff(attempts: int) -> str:
    delay = min(_MAX_BACKOFF_SECONDS, 2**attempts)
    return iso_utc(utc_now() + timedelta(seconds=delay))


async def resume_wait(
    wait: FundsWaitRecord,
    *,
    deps: FundsWaitResumeDeps,
    stats: MutableMapping[str, int],
) -> None:
    resume_token = str(wait.resume_token or "").strip()
    if len(resume_token) < _RESUME_TOKEN_MIN_LENGTH:
        logger.error("Funds wait %s is missing a valid resume token.", wait.wait_id)
        stats["failed_resumes"] = stats.get("failed_resumes", 0) + 1
        await deps.mark_wait_queued(
            wait.wait_id,
            last_error=(
                "Funds wait resume token missing or invalid. "
                "Recovery path: retry so a fresh secure resume token can be issued."
            ),
            resume_after=_calculate_backoff(wait.attempts),
        )
        return

    config = {"configurable": {"thread_id": str(wait.thread_id)}}
    payload = {
        "wait_id": wait.wait_id,
        "resume_token": resume_token,
        "wallet_scope": wait.wallet_scope,
        "node_id": wait.node_id,
    }
    try:
        events = deps.app.astream(
            Command(resume=payload),
            config,
            stream_mode="values",
        )
        await asyncio.wait_for(
            _drain_resume_stream(events),
            timeout=deps.timeout_seconds,
        )
        refreshed = await deps.get_wait(wait.wait_id)
        if refreshed is not None and refreshed.status == "resuming":
            await deps.mark_wait_queued(
                wait.wait_id,
                last_error=None,
                resume_after=None,
            )
        stats["successful_resumes"] = stats.get("successful_resumes", 0) + 1
    except asyncio.TimeoutError:
        stats["failed_resumes"] = stats.get("failed_resumes", 0) + 1
        await deps.mark_wait_queued(
            wait.wait_id,
            last_error=(
                f"Funds wait resume timed out after {deps.timeout_seconds}s."
            ),
            resume_after=_calculate_backoff(wait.attempts),
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error(
            "Funds wait resume failed for %s: %s",
            wait.wait_id,
            exc,
            exc_info=True,
        )
        stats["failed_resumes"] = stats.get("failed_resumes", 0) + 1
        await deps.mark_wait_queued(
            wait.wait_id,
            last_error=f"{type(exc).__name__}: {exc}",
            resume_after=_calculate_backoff(wait.attempts),
        )


async def _drain_resume_stream(events: Any) -> None:
    async for _event in events:
        pass
