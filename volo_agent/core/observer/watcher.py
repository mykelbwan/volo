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
_DEFAULT_REST_POLL_INTERVAL = 60.0  # seconds between REST price polls
_DEFAULT_EVAL_INTERVAL = 30.0  # seconds between evaluation ticks
_DEFAULT_TIME_TRIGGER_INTERVAL = 60.0  # seconds between time trigger checks
_DEFAULT_STALE_PRICE_THRESHOLD = 300.0  # seconds before a cached price is stale
_RESUME_STREAM_TIMEOUT = 300.0  # max seconds to wait for a resumed graph


class ObserverWatcher:
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


    async def run(self) -> None:
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
        logger.info("ObserverWatcher: stop requested.")
        self._stop_event.set()
        self._price_observer.stop()
        self._eval_trigger.set()  # wake evaluation loop so it can exit promptly


    def register_volume_symbols(self, symbols: list[str]) -> None:
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

    # Price cache accessor (used by volume flusher) 

    @property
    def price_cache(self) -> PriceCache:
        return self._cache

    # Price update hook (called from PriceCache.set) 

    def _on_price_update(self, asset: str, price: float) -> None:
        logger.debug("Price update hook: %s = $%.4f", asset, price)
        self._eval_trigger.set()

    async def _evaluation_loop(self) -> None:
        logger.info(
            "ObserverWatcher evaluation loop started "
            "(eval_interval=%.0fs, time_trigger_check_interval=%.0fs).",
            self._eval_interval,
            self._time_trigger_check_interval,
        )

        last_time_check: float = 0.0

        while not self._stop_event.is_set():
            # Wait for the next tick or an immediate price-update signal 
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

            # Price trigger evaluation 
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

            # Time trigger evaluation (lower-frequency cadence) 
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

    # Match processing 

    async def _process_matches(self, matches: list[MatchResult]) -> None:
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

    # Thread resume 

    async def _resume_thread(self, match: MatchResult) -> None:
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

    async def get_stats(self) -> dict[str, Any]:
        return {
            "watcher": dict(self._stats),
            "prices": {
                asset: self._cache.get_sync(asset) for asset in self._cache.all_assets()
            },
            "triggers": await self._registry.summary(),
        }

    async def log_stats(self) -> None:
        stats = await self.get_stats()
        logger.info(
            "ObserverWatcher stats | watcher=%s | prices=%d asset(s) cached | "
            "triggers=%s",
            stats["watcher"],
            len(stats["prices"]),
            stats["triggers"],
        )
