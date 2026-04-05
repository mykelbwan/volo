"""
will resume working on this later
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

# ── Logging setup (before any project imports so config is in place) ──────────

_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)

# Quieten noisy third-party loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("langchain").setLevel(logging.WARNING)
logging.getLogger("langgraph").setLevel(logging.WARNING)

logger = logging.getLogger("volo.observer")


# ── Argument parsing ──────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="observer_service",
        description="Volo Observer Watcher – event-driven trigger execution service.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=60.0,
        metavar="SECONDS",
        help="Interval between CoinGecko REST price polls.",
    )
    parser.add_argument(
        "--eval-interval",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help=(
            "Maximum interval between trigger evaluation cycles. "
            "The loop also runs immediately on each price update."
        ),
    )
    parser.add_argument(
        "--time-trigger-interval",
        type=float,
        default=60.0,
        metavar="SECONDS",
        help="Interval between time-based trigger evaluation cycles.",
    )
    parser.add_argument(
        "--stale-price-threshold",
        type=float,
        default=300.0,
        metavar="SECONDS",
        help=(
            "How old a cached price can be (in seconds) before triggers "
            "watching that asset are skipped for safety."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help=(
            "Evaluate triggers and log matches without actually resuming "
            "LangGraph threads.  Useful for testing the observer logic."
        ),
    )
    return parser.parse_args()


# ── Health-check ticker ───────────────────────────────────────────────────────


async def _health_tick(watcher: "ObserverWatcher", interval: float = 300.0) -> None:  # type: ignore[name-defined]  # noqa: F821
    """
    Log watcher statistics every ``interval`` seconds so operators can
    confirm the service is alive and healthy without external monitoring.
    """
    while True:
        try:
            await asyncio.sleep(interval)
            await watcher.log_stats()
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Health tick error: %s", exc)


# ── Dry-run watcher ───────────────────────────────────────────────────────────


class _DryRunWatcher:
    """
    A minimal watcher that evaluates triggers and logs matches but never
    resumes any LangGraph threads.  Used with --dry-run.
    """

    def __init__(
        self,
        poll_interval: float,
        eval_interval: float,
        time_trigger_interval: float,
        stale_price_threshold: float,
    ) -> None:
        from core.observer.price_observer import PriceCache, PriceObserver
        from core.observer.trigger_matcher import TriggerMatcher
        from core.observer.trigger_registry import TriggerRegistry

        self._stop = asyncio.Event()
        self._eval_trigger = asyncio.Event()

        self._cache = PriceCache(
            on_update=lambda asset, price: self._eval_trigger.set()
        )
        self._observer = PriceObserver(cache=self._cache, symbols=None)
        self._registry = TriggerRegistry()
        self._matcher = TriggerMatcher(
            registry=self._registry,
            cache=self._cache,
            max_price_age_seconds=stale_price_threshold,
        )
        self._poll_interval = poll_interval
        self._eval_interval = eval_interval
        self._time_trigger_interval = time_trigger_interval
        self._match_count = 0

    async def run(self) -> None:
        logger.info("[DRY-RUN] Dry-run watcher starting …")
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self._observer.run_rest_polling(self._poll_interval),
                    name="dry_rest_polling",
                )
                tg.create_task(
                    self._observer.run_websocket(),
                    name="dry_websocket",
                )
                tg.create_task(self._eval_loop(), name="dry_eval_loop")
        except* asyncio.CancelledError:
            pass
        logger.info(
            "[DRY-RUN] Dry-run watcher stopped. Total matches logged: %d",
            self._match_count,
        )

    def stop(self) -> None:
        self._stop.set()
        self._observer.stop()
        self._eval_trigger.set()

    async def _eval_loop(self) -> None:
        import time as _time

        last_time_check = 0.0

        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._eval_trigger.wait(), timeout=self._eval_interval
                )
                self._eval_trigger.clear()
            except asyncio.TimeoutError:
                pass

            if self._stop.is_set():
                break

            prices = await self._cache.snapshot()
            report = await self._matcher.evaluate(prices)

            for match in report.matches:
                self._match_count += 1
                logger.info(
                    "[DRY-RUN] MATCH — trigger_id=%s  thread=%s  user=%s  payload=%s",
                    match.trigger_id[:8],
                    match.thread_id[:8],
                    match.user_id,
                    {
                        k: v
                        for k, v in match.resume_payload.items()
                        if k != "trigger_condition"
                    },
                )

            now = _time.monotonic()
            if now - last_time_check >= self._time_trigger_interval:
                last_time_check = now
                time_report = await self._matcher.evaluate_time_triggers()
                for match in time_report.matches:
                    self._match_count += 1
                    logger.info(
                        "[DRY-RUN] TIME MATCH — trigger_id=%s  thread=%s",
                        match.trigger_id[:8],
                        match.thread_id[:8],
                    )


# ── Main entry point ──────────────────────────────────────────────────────────


async def _main(args: argparse.Namespace) -> None:
    """
    Async main: build the watcher, register signal handlers, and run until
    a stop signal is received.
    """
    loop = asyncio.get_running_loop()
    from core.health import run_startup_checks_async

    await run_startup_checks_async()

    if args.dry_run:
        logger.info("=" * 60)
        logger.info("Volo Observer Watcher  [DRY-RUN MODE]")
        logger.info("Triggers will be LOGGED but NOT executed.")
        logger.info("=" * 60)

        watcher = _DryRunWatcher(
            poll_interval=args.poll_interval,
            eval_interval=args.eval_interval,
            time_trigger_interval=args.time_trigger_interval,
            stale_price_threshold=args.stale_price_threshold,
        )

        def _shutdown_dry() -> None:
            logger.info("Shutdown signal received.")
            watcher.stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown_dry)

        await watcher.run()
        return

    # ── Production run ────────────────────────────────────────────────────────

    # Import here (after logging is set up) so any startup errors are
    # reported with the correct log format.
    from core.volume.flusher import start_volume_flusher

    logger.info("Importing LangGraph application …")
    try:
        from graph.graph import app  # noqa: F401 – compiled with MongoDBSaver
    except Exception as exc:
        logger.critical("Failed to import LangGraph app: %s", exc, exc_info=True)
        sys.exit(1)

    import os as _os

    from core.observer.watcher import ObserverWatcher

    _flush_interval = int(_os.getenv("VOLUME_FLUSH_INTERVAL", str(5 * 60)))

    logger.info("=" * 60)
    logger.info("Volo Observer Watcher  [PRODUCTION MODE]")
    logger.info("Symbols      : DB-driven (pending triggers + volume data)")
    logger.info("REST poll    : %.0fs", args.poll_interval)
    logger.info("Eval tick    : %.0fs", args.eval_interval)
    logger.info("Volume flush : %ds", _flush_interval)
    logger.info("=" * 60)

    watcher = ObserverWatcher(
        app=app,
        symbols=None,
        rest_poll_interval=args.poll_interval,
        eval_interval=args.eval_interval,
        time_trigger_check_interval=args.time_trigger_interval,
        max_price_age_seconds=args.stale_price_threshold,
    )

    # Register OS signal handlers for graceful shutdown
    def _shutdown() -> None:
        logger.info("Shutdown signal received — stopping watcher …")
        watcher.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    # Run the watcher, the health-check ticker, and the volume flusher
    # concurrently.  All three are cancelled on graceful shutdown.
    health_task = asyncio.create_task(
        _health_tick(watcher, interval=300.0), name="health_ticker"
    )
    volume_task = start_volume_flusher(
        watcher=watcher,
        interval_seconds=_flush_interval,
    )

    try:
        await watcher.run()
    finally:
        health_task.cancel()
        volume_task.cancel()
        for task in (health_task, volume_task):
            try:
                await task
            except asyncio.CancelledError:
                pass

    logger.info("Observer service exited cleanly.")


def main() -> None:
    args = _parse_args()
    try:
        asyncio.run(_main(args))
    except KeyboardInterrupt:
        # Already handled by the signal handler; suppress the traceback.
        pass


if __name__ == "__main__":
    main()
