from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, MutableMapping

from core.reservations.wait_resume_runtime import FundsWaitResumeDeps, resume_wait


@dataclass(frozen=True)
class FundsWaitPollingDeps:
    app: Any
    list_resume_candidates: Callable[..., Awaitable[list[Any]]]
    mark_wait_resuming: Callable[[str], Awaitable[Any]]
    get_wait: Callable[[str], Awaitable[Any]]
    mark_wait_queued: Callable[..., Awaitable[None]]
    timeout_seconds: float


async def process_wait_batch(
    *,
    deps: FundsWaitPollingDeps,
    batch_size: int,
    max_concurrent: int,
    stats: MutableMapping[str, int],
) -> int:
    waits = await deps.list_resume_candidates(limit=max(1, int(batch_size)))
    if not waits:
        return 0

    semaphore = asyncio.Semaphore(max(1, int(max_concurrent)))
    resumed_count = 0

    async def _resume_one(wait: Any) -> None:
        nonlocal resumed_count
        async with semaphore:
            claimed = await deps.mark_wait_resuming(wait.wait_id)
            if claimed is None or str(getattr(claimed, "status", "")) != "resuming":
                return
            resumed_count += 1
            await resume_wait(
                claimed,
                deps=FundsWaitResumeDeps(
                    app=deps.app,
                    timeout_seconds=deps.timeout_seconds,
                    get_wait=deps.get_wait,
                    mark_wait_queued=deps.mark_wait_queued,
                ),
                stats=stats,
            )

    await asyncio.gather(*[_resume_one(wait) for wait in waits])
    return resumed_count


async def run_wait_poll_loop(
    *,
    deps: FundsWaitPollingDeps,
    poll_interval: float,
    batch_size: int,
    max_concurrent: int,
    stop_event: asyncio.Event,
    stats: MutableMapping[str, int],
) -> None:
    while not stop_event.is_set():
        processed = await process_wait_batch(
            deps=deps,
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            stats=stats,
        )
        if processed > 0:
            await asyncio.sleep(0)
            continue
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=max(0.1, float(poll_interval)),
            )
        except asyncio.TimeoutError:
            pass
