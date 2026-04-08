from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

try:
    from dotenv import find_dotenv, load_dotenv
except Exception:  # pragma: no cover - optional dependency
    find_dotenv = None
    load_dotenv = None

from core.reservations.service import get_reservation_service
from core.reservations.funds_wait_runtime import (
    FundsWaitPollingDeps,
    run_wait_poll_loop,
)

logger = logging.getLogger("volo.funds_wait_worker")


def _load_project_env() -> None:
    if load_dotenv is None:
        return
    env_path = find_dotenv(usecwd=True) if find_dotenv is not None else ""
    load_dotenv(env_path or None)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="funds_wait_worker",
        description="Resume queued wallet-funds waits when funds become available.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-concurrent", type=int, default=4)
    parser.add_argument("--resume-timeout", type=float, default=20.0)
    return parser.parse_args()


async def _run_worker(args: argparse.Namespace) -> int:
    from graph.graph import app

    service = await get_reservation_service()
    stats = {"successful_resumes": 0, "failed_resumes": 0}
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        signum = getattr(signal, signame, None)
        if signum is None:
            continue
        try:
            loop.add_signal_handler(signum, stop.set)
        except NotImplementedError:
            pass

    await run_wait_poll_loop(
        deps=FundsWaitPollingDeps(
            app=app,
            list_resume_candidates=service.list_resume_candidates,
            mark_wait_resuming=service.mark_wait_resuming,
            get_wait=service.get_wait,
            mark_wait_queued=service.mark_wait_queued,
            timeout_seconds=args.resume_timeout,
        ),
        poll_interval=args.poll_interval,
        batch_size=args.batch_size,
        max_concurrent=args.max_concurrent,
        stop_event=stop,
        stats=stats,
    )
    logger.info(
        "funds-wait-worker stopped successful=%s failed=%s",
        stats["successful_resumes"],
        stats["failed_resumes"],
    )
    return 0


def main() -> int:
    _load_project_env()
    logging.basicConfig(
        level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        stream=sys.stdout,
    )
    args = _parse_args()
    return asyncio.run(_run_worker(args))


if __name__ == "__main__":
    raise SystemExit(main())
