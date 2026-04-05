from __future__ import annotations

import asyncio
import logging

from core.reservations.funds_wait_runtime import FundsWaitPollingDeps, run_wait_poll_loop
from core.reservations.service import get_reservation_service

logger = logging.getLogger("volo.funds_wait_worker")


async def start_funds_wait_worker() -> asyncio.Task[None]:
    from graph.graph import app

    service = await get_reservation_service()
    stats = {"successful_resumes": 0, "failed_resumes": 0}

    async def _run() -> None:
        try:
            await run_wait_poll_loop(
                deps=FundsWaitPollingDeps(
                    app=app,
                    list_resume_candidates=service.list_resume_candidates,
                    mark_wait_resuming=service.mark_wait_resuming,
                    get_wait=service.get_wait,
                    mark_wait_queued=service.mark_wait_queued,
                    timeout_seconds=20.0,
                ),
                poll_interval=5.0,
                batch_size=8,
                max_concurrent=4,
                stop_event=asyncio.Event(),
                stats=stats,
            )
        finally:
            logger.info(
                "funds-wait-worker stopped successful=%s failed=%s",
                stats["successful_resumes"],
                stats["failed_resumes"],
            )

    return asyncio.create_task(_run(), name="volo-funds-wait-worker")

