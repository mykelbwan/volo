"""
Observer Watcher Service – the heartbeat of Volo's event-driven execution.

This is the top-level async service that ties together the PriceObserver,
TriggerMatcher, and TriggerRegistry to form a self-contained background
process.  It runs alongside (but independently of) the main LangGraph
app and is responsible for waking sleeping threads when their trigger
conditions are satisfied.

Architecture
------------

                 ┌─────────────────────────────────────┐
                 │         ObserverWatcher              │
                 │                                      │
  CoinGecko ────►│  PriceObserver ──► PriceCache        │
  REST           │                        │             │
                 │                        ▼             │
                 │              TriggerMatcher          │
                 │              (evaluate on tick)      │
                 │                        │             │
                 │              MatchResult list        │
                 │                        │             │
                 │                        ▼             │
                 │  TriggerRegistry.mark_triggered()    │
                 │                        │             │
                 │  app.stream(Command(resume=...))     │──► LangGraph thread
                 │        (MongoDBSaver loads state)    │    resumes execution
                 └─────────────────────────────────────┘

Concurrency model
-----------------
The watcher runs three concurrent asyncio tasks:

  Task A – REST polling loop (PriceObserver.run_rest_polling)
           Polls CoinGecko every ``rest_poll_interval`` seconds as the
           primary feed for CoinGecko-listed assets.

  Task B – WebSocket streaming loop (PriceObserver.run_websocket)
           No-op for CoinGecko (kept for interface parity).

  Task C – Evaluation loop (ObserverWatcher._evaluation_loop)
           Evaluates all pending triggers against the price cache every
           ``eval_interval`` seconds.  Also immediately re-evaluates when
           the PriceObserver notifies a price update via the on_update hook.

The evaluation loop also handles time-based (TIME_AT) triggers, checking
them every ``time_trigger_check_interval`` seconds.

Resuming a LangGraph thread
---------------------------
When a match is found the watcher:

  1. Calls ``TriggerRegistry.mark_triggered(trigger_id)`` — atomic CAS
     update that prevents double-firing.
  2. Constructs the LangGraph config:
         {"configurable": {"thread_id": "<thread_id>"}}
  3. Calls ``app.astream(Command(resume=resume_payload), config)`` using
     the compiled graph with the MongoDBSaver checkpointer.
  4. Drains the async stream to completion (all remaining graph nodes execute).
  5. On any exception, calls ``TriggerRegistry.mark_failed(trigger_id, error)``
     so operators can audit failures without data loss.

Anti-fragility
--------------
- Idempotency: mark_triggered() uses a conditional update; a second call for
  the same trigger is a safe no-op.
- Isolation: a failure in one thread resume does NOT crash the watcher; it is
  caught, logged, and marked as failed while other triggers continue normally.
- Graceful shutdown: ``stop()`` sets a stop event; all loops exit cleanly on
  the next tick without data loss.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from core.observer.price_observer import (
    PriceCache,
    PriceObserver,
    price_cache,
)
from core.observer.resume_runtime import ResumeRuntimeDeps, resume_thread
from core.observer.watchlist import WatchlistRefreshDeps, refresh_price_watchlist
from core.observer.trigger_matcher import EvaluationReport, MatchResult, TriggerMatcher
from core.observer.trigger_registry import TriggerRegistry

logger = logging.getLogger(__name__)

# ── Default tuning constants ──────────────────────────────────────────────────

_DEFAULT_REST_POLL_INTERVAL = 60.0  # seconds between REST price polls
_DEFAULT_EVAL_INTERVAL = 30.0  # seconds between evaluation ticks
_DEFAULT_TIME_TRIGGER_INTERVAL = 60.0  # seconds between time trigger checks
_DEFAULT_STALE_PRICE_THRESHOLD = 300.0  # seconds before a cached price is stale
_RESUME_STREAM_TIMEOUT = 300.0  # max seconds to wait for a resumed graph


class ObserverWatcher:
    """
    Top-level event-driven execution orchestrator.

    Parameters
    ----------
    app:
        The compiled LangGraph application (``workflow.compile(checkpointer=...)``).
        Must be compiled with a ``MongoDBSaver`` checkpointer so that interrupted
        thread state can be retrieved and the resumed graph can write new
        checkpoints.
    symbols:
        List of uppercase asset symbols to monitor (e.g. ``["ETH", "BTC"]``).
        If None, watchlist will be populated from pending triggers.
    rest_poll_interval:
        How often (in seconds) the REST polling fallback fetches fresh prices.
    eval_interval:
        How often (in seconds) the main evaluation loop checks all pending
        price triggers.  The loop also evaluates immediately when the
        PriceObserver fires the ``on_update`` callback.
    time_trigger_check_interval:
        How often (in seconds) the evaluation loop checks for time-based
        triggers whose ``execute_at`` has passed.
    max_price_age_seconds:
        Price staleness threshold passed to the TriggerMatcher.
    """

    def __init__(
        self,
        app: Any,
        symbols: Optional[list[str]] = None,
        rest_poll_interval: float = _DEFAULT_REST_POLL_INTERVAL,
        eval_interval: float = _DEFAULT_EVAL_INTERVAL,
        time_trigger_check_interval: float = _DEFAULT_TIME_TRIGGER_INTERVAL,
        max_price_age_seconds: float = _DEFAULT_STALE_PRICE_THRESHOLD,
    ) -> None:
        self._app = app
        self._rest_poll_interval = rest_poll_interval
        self._eval_interval = eval_interval
        self._time_trigger_check_interval = time_trigger_check_interval

        # Shared state
        self._stop_event = asyncio.Event()
        self._eval_trigger = asyncio.Event()  # notified on every price update

        # Build the price cache with an on_update hook that immediately
        # signals the evaluation loop.
        self._cache = price_cache
        self._cache.on_update = self._on_price_update

        # Components
        self._static_symbols = [s.upper() for s in symbols] if symbols else []
        self._price_observer = PriceObserver(
            cache=self._cache,
            symbols=self._static_symbols if symbols is not None else [],
        )
        self._registry = TriggerRegistry()
        self._matcher = TriggerMatcher(
            registry=self._registry,
            cache=self._cache,
            max_price_age_seconds=max_price_age_seconds,
        )

        # Tokens discovered from real swap/bridge volume — populated by the
        # volume flusher so that the price observer keeps them warm in the
        # cache after the first cold fetch.
        self._volume_symbols: set[str] = set()

        # Runtime stats (for operator visibility)
        self._stats: dict[str, int] = {
            "total_evaluations": 0,
            "total_matches": 0,
            "successful_resumes": 0,
            "failed_resumes": 0,
        }
        self._dex_address_cache: dict[tuple[str, str], str] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Start all background tasks and block until ``stop()`` is called or
        all tasks complete.

        This is the main entry point — call it from the observer service
        entry point::

            watcher = ObserverWatcher(app=graph_app, symbols=["ETH", "BTC"])
            await watcher.run()
        """
        logger.info("ObserverWatcher starting up …")

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self._price_observer.run_rest_polling(self._rest_poll_interval),
                    name="rest_polling",
                )
                tg.create_task(
                    self._price_observer.run_websocket(),
                    name="websocket_streaming",
                )
                tg.create_task(
                    self._evaluation_loop(),
                    name="evaluation_loop",
                )
        except* asyncio.CancelledError:
            logger.info("ObserverWatcher: tasks cancelled during shutdown.")
        except* Exception as eg:
            for exc in eg.exceptions:
                logger.error(
                    "ObserverWatcher task raised an unexpected error: %s",
                    exc,
                    exc_info=exc,
                )

        logger.info("ObserverWatcher stopped. Final stats: %s", self._stats)

    def stop(self) -> None:
        """
        Signal all background loops to exit on their next tick.
        Safe to call from any thread or coroutine.
        """
        logger.info("ObserverWatcher: stop requested.")
        self._stop_event.set()
        self._price_observer.stop()
        self._eval_trigger.set()  # wake evaluation loop so it can exit promptly

    # ── Volume symbol registration (called by volume flusher) ─────────────────

    def register_volume_symbols(self, symbols: list[str]) -> None:
        """
        Register token symbols that have appeared in real swap/bridge volume.

        The symbols are merged into ``_volume_symbols`` and included in every
        subsequent ``_refresh_price_watchlist`` call, ensuring the
        ``PriceObserver`` keeps their prices warm in the cache.

        This is intentionally synchronous and lock-free — it only writes to a
        plain Python set, which is safe from a single asyncio task (the flusher).

        Parameters
        ----------
        symbols:
            Uppercase token symbols, e.g. ``["ETH", "USDC", "LINK"]``.
        """
        new = {s.strip().upper() for s in symbols if s}
        if not new:
            return
        added = new - self._volume_symbols
        if not added:
            return
        self._volume_symbols.update(added)
        logger.debug(
            "ObserverWatcher: registered %d new volume symbol(s): %s",
            len(added),
            sorted(added),
        )

    # ── Price cache accessor (used by volume flusher) ─────────────────────────

    @property
    def price_cache(self) -> PriceCache:
        """
        Expose the shared ``PriceCache`` for read access by the volume flusher.

        The flusher calls ``price_cache.get_sync(symbol)`` to serve prices from
        the warm in-memory cache before falling back to live API calls.
        """
        return self._cache

    # ── Price update hook (called from PriceCache.set) ────────────────────────

    def _on_price_update(self, asset: str, price: float) -> None:
        """
        Called synchronously by PriceCache whenever a price changes.
        Signals the evaluation loop to run immediately without waiting
        for the next scheduled tick.
        """
        logger.debug("Price update hook: %s = $%.4f", asset, price)
        # asyncio.Event.set() is thread-safe when called from a different
        # coroutine context (same event loop), but NOT from a different
        # OS thread.  Since PriceCache.set() is always called from our
        # async tasks on the same event loop, this is safe.
        self._eval_trigger.set()

    # ── Evaluation loop ───────────────────────────────────────────────────────

    async def _evaluation_loop(self) -> None:
        """
        Main trigger evaluation loop.

        Runs every ``eval_interval`` seconds OR immediately when the price
        observer fires the ``_eval_trigger`` event (whichever comes first).

        Also runs the time-trigger check every ``time_trigger_check_interval``
        seconds as a secondary cadence.
        """
        logger.info(
            "ObserverWatcher evaluation loop started "
            "(eval_interval=%.0fs, time_trigger_check_interval=%.0fs).",
            self._eval_interval,
            self._time_trigger_check_interval,
        )

        last_time_check: float = 0.0

        while not self._stop_event.is_set():
            # ── Wait for the next tick or an immediate price-update signal ────
            try:
                await asyncio.wait_for(
                    self._eval_trigger.wait(),
                    timeout=self._eval_interval,
                )
                self._eval_trigger.clear()
            except asyncio.TimeoutError:
                pass  # scheduled tick
            except asyncio.CancelledError:
                break

            if self._stop_event.is_set():
                break

            # ── Price trigger evaluation ──────────────────────────────────────
            try:
                await self._refresh_price_watchlist()
                price_snapshot = await self._cache.snapshot()
                report: EvaluationReport = await self._matcher.evaluate(price_snapshot)
                self._stats["total_evaluations"] += 1

                if report.total_evaluated > 0 or report.expired_count > 0:
                    logger.info(
                        "Evaluation cycle %d: %s",
                        self._stats["total_evaluations"],
                        report.summary(),
                    )

                if report.matches:
                    self._stats["total_matches"] += len(report.matches)
                    await self._process_matches(report.matches)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "ObserverWatcher: evaluation cycle error: %s",
                    exc,
                    exc_info=True,
                )

            # ── Time trigger evaluation (lower-frequency cadence) ─────────────
            import time as _time  # avoid shadowing module name at top level

            now = _time.monotonic()
            if now - last_time_check >= self._time_trigger_check_interval:
                last_time_check = now
                try:
                    time_report: EvaluationReport = (
                        await self._matcher.evaluate_time_triggers()
                    )
                    if time_report.matches:
                        logger.info(
                            "Time trigger evaluation: %d match(es) found.",
                            len(time_report.matches),
                        )
                        self._stats["total_matches"] += len(time_report.matches)
                        await self._process_matches(time_report.matches)
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.error(
                        "ObserverWatcher: time trigger evaluation error: %s",
                        exc,
                        exc_info=True,
                    )

        logger.info("ObserverWatcher evaluation loop exited.")

    async def _refresh_price_watchlist(self) -> None:
        """
        Update the price watchlist based on pending triggers.

        This keeps CoinGecko/Dexscreener polling aligned with the DB and
        enables chain-aware, address-scoped price updates for tokens.
        """
        await refresh_price_watchlist(
            WatchlistRefreshDeps(
                get_pending_price_triggers=self._registry.get_pending_price_triggers,
                set_symbols=self._price_observer.set_symbols,
                set_dex_tokens=self._price_observer.set_dex_tokens,
                dex_address_cache=self._dex_address_cache,
                static_symbols=self._static_symbols,
                volume_symbols=self._volume_symbols,
            )
        )

    # ── Match processing ──────────────────────────────────────────────────────

    async def _process_matches(self, matches: list[MatchResult]) -> None:
        """
        Process a list of matched triggers concurrently.

        Each match is handled in its own asyncio task so that a slow or
        failing resume does not block other matches from firing.

        Parameters
        ----------
        matches:
            List of ``MatchResult`` objects from the TriggerMatcher.
        """
        tasks = [
            asyncio.create_task(
                self._resume_thread(match),
                name=f"resume_{match.trigger_id[:8]}",
            )
            for match in matches
        ]
        if tasks:
            # Gather with return_exceptions=True so one failure doesn't
            # cancel the others.
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for match, result in zip(matches, results):
                if isinstance(result, Exception):
                    logger.error(
                        "ObserverWatcher: unhandled exception resuming trigger %s: %s",
                        match.trigger_id[:8],
                        result,
                        exc_info=result,
                    )

    # ── Thread resume ─────────────────────────────────────────────────────────

    async def _resume_thread(self, match: MatchResult) -> None:
        """
        Atomically mark a trigger as triggered and resume the LangGraph thread.

        This method guarantees exactly-once semantics:
          1. ``mark_triggered_or_reschedule()`` uses a conditional update — if the trigger
             is already in a non-pending state (e.g. the Observer fired twice
             due to a race), the mark is a no-op and we abort early.
          2. The LangGraph resume is only called after the mark succeeds.
          3. On any resume exception, the trigger is marked as ``failed``
             with the error message for operator audit.

        Parameters
        ----------
        match:
            The ``MatchResult`` describing which trigger fired.
        """
        await resume_thread(
            match,
            deps=ResumeRuntimeDeps(
                app=self._app,
                mark_triggered_or_reschedule=self._registry.mark_triggered_or_reschedule,
                mark_failed=self._registry.mark_failed,
                timeout_seconds=_RESUME_STREAM_TIMEOUT,
            ),
            stats=self._stats,
        )

    # ── Diagnostics ───────────────────────────────────────────────────────────

    async def get_stats(self) -> dict[str, Any]:
        """
        Return a snapshot of runtime statistics.

        Includes watcher counters, the current price cache snapshot (sync
        read, no lock), and the TriggerRegistry status summary.

        Useful for health-check endpoints or operator CLI commands.
        """
        return {
            "watcher": dict(self._stats),
            "prices": {
                asset: self._cache.get_sync(asset) for asset in self._cache.all_assets()
            },
            "triggers": await self._registry.summary(),
        }

    async def log_stats(self) -> None:
        """Log current stats at INFO level — useful for periodic health dumps."""
        stats = await self.get_stats()
        logger.info(
            "ObserverWatcher stats | watcher=%s | prices=%d asset(s) cached | "
            "triggers=%s",
            stats["watcher"],
            len(stats["prices"]),
            stats["triggers"],
        )
