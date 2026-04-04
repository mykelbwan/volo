from __future__ import annotations

import gc
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Tuple

_LOGGER = logging.getLogger("volo.gc")
_LOCK = threading.RLock()

_CONFIGURED = False
_MODE = "off"
_INITIAL_THRESHOLDS: Tuple[int, int, int] | None = None
_ACTIVE_THRESHOLDS: Tuple[int, int, int] | None = None
_MONITOR: "_GCPauseMonitor | None" = None
_CALLBACK: Any = None


def _get_bool_env(key: str, default: bool = False) -> bool:
    raw = os.getenv(key, "")
    if raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_float_env(key: str, default: float) -> float:
    raw = os.getenv(key, "")
    if raw == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value


def _get_int_env(key: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(key, "")
    if raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= minimum else default


def _parse_thresholds(raw: str) -> Tuple[int, int, int] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    parts = [p.strip() for p in text.split(",")]
    if len(parts) != 3:
        return None
    try:
        parsed = tuple(int(p) for p in parts)
    except ValueError:
        return None
    if any(v <= 0 for v in parsed):
        return None
    return parsed[0], parsed[1], parsed[2]


def _resolve_mode(raw_mode: str) -> str:
    mode = str(raw_mode or "").strip().lower() or "off"
    if mode in {"off", "safe"}:
        return mode
    _LOGGER.warning("gc_tuning_invalid_mode mode=%s defaulting=off", mode)
    return "off"


def _resolve_safe_thresholds(current: Tuple[int, int, int]) -> Tuple[int, int, int]:
    # Conservative tuning that reduces frequency of expensive full collections
    # without disabling GC or using highly aggressive values.
    return (
        max(int(current[0] * 2), 2000),
        max(int(current[1] * 3), 30),
        max(int(current[2] * 8), 80),
    )


@dataclass
class _PauseStats:
    events_total: int = 0
    pause_total_ms: float = 0.0
    pause_max_ms: float = 0.0
    generation_events: Dict[int, int] = field(default_factory=lambda: {0: 0, 1: 0, 2: 0})
    generation_pause_total_ms: Dict[int, float] = field(
        default_factory=lambda: {0: 0.0, 1: 0.0, 2: 0.0}
    )
    generation_pause_max_ms: Dict[int, float] = field(
        default_factory=lambda: {0: 0.0, 1: 0.0, 2: 0.0}
    )


class _GCPauseMonitor:
    def __init__(self, *, warn_ms: float, summary_every: int) -> None:
        self._warn_ms = max(0.0, float(warn_ms))
        self._summary_every = max(0, int(summary_every))
        self._starts: Dict[int, float] = {}
        self._stats = _PauseStats()

    def __call__(self, phase: str, info: Dict[str, Any]) -> None:
        generation = int(info.get("generation", -1))
        if phase == "start":
            self._starts[generation] = time.perf_counter()
            return
        if phase != "stop":
            return
        start = self._starts.pop(generation, None)
        if start is None:
            return

        pause_ms = (time.perf_counter() - start) * 1000.0
        with _LOCK:
            stats = self._stats
            stats.events_total += 1
            stats.pause_total_ms += pause_ms
            stats.pause_max_ms = max(stats.pause_max_ms, pause_ms)
            if generation in stats.generation_events:
                stats.generation_events[generation] += 1
                stats.generation_pause_total_ms[generation] += pause_ms
                stats.generation_pause_max_ms[generation] = max(
                    stats.generation_pause_max_ms[generation], pause_ms
                )
            events_total = stats.events_total
            total_ms = stats.pause_total_ms
            max_ms = stats.pause_max_ms

        collected = info.get("collected")
        uncollectable = info.get("uncollectable")
        if pause_ms >= self._warn_ms:
            _LOGGER.warning(
                "gc_pause gen=%s pause_ms=%.2f collected=%s uncollectable=%s",
                generation,
                pause_ms,
                collected,
                uncollectable,
            )
            return
        if self._summary_every > 0 and events_total % self._summary_every == 0:
            _LOGGER.info(
                "gc_pause_summary events=%s total_pause_ms=%.2f max_pause_ms=%.2f",
                events_total,
                total_ms,
                max_ms,
            )

    def snapshot(self) -> Dict[str, Any]:
        with _LOCK:
            stats = self._stats
            return {
                "events_total": int(stats.events_total),
                "pause_total_ms": float(stats.pause_total_ms),
                "pause_max_ms": float(stats.pause_max_ms),
                "generation_events": dict(stats.generation_events),
                "generation_pause_total_ms": dict(stats.generation_pause_total_ms),
                "generation_pause_max_ms": dict(stats.generation_pause_max_ms),
            }


def configure_gc_runtime() -> Dict[str, Any]:
    global _CONFIGURED, _MODE, _INITIAL_THRESHOLDS, _ACTIVE_THRESHOLDS, _MONITOR, _CALLBACK
    with _LOCK:
        if _CONFIGURED:
            # Return a snapshot without re-entering status helpers that also
            # read pause stats under the same lock.
            return {
                "configured": bool(_CONFIGURED),
                "mode": str(_MODE),
                "initial_thresholds": _INITIAL_THRESHOLDS,
                "active_thresholds": _ACTIVE_THRESHOLDS or gc.get_threshold(),
                "instrumentation_enabled": _MONITOR is not None,
                "pause_stats": (
                    _MONITOR.snapshot()
                    if _MONITOR is not None
                    else {
                        "events_total": 0,
                        "pause_total_ms": 0.0,
                        "pause_max_ms": 0.0,
                        "generation_events": {0: 0, 1: 0, 2: 0},
                        "generation_pause_total_ms": {0: 0.0, 1: 0.0, 2: 0.0},
                        "generation_pause_max_ms": {0: 0.0, 1: 0.0, 2: 0.0},
                    }
                ),
            }

        current = gc.get_threshold()
        _INITIAL_THRESHOLDS = (int(current[0]), int(current[1]), int(current[2]))

        _MODE = _resolve_mode(os.getenv("VOLO_GC_TUNING_MODE", "off"))
        explicit = _parse_thresholds(os.getenv("VOLO_GC_THRESHOLDS", ""))

        if explicit is not None:
            _ACTIVE_THRESHOLDS = explicit
        elif _MODE == "safe":
            _ACTIVE_THRESHOLDS = _resolve_safe_thresholds(_INITIAL_THRESHOLDS)
        else:
            _ACTIVE_THRESHOLDS = _INITIAL_THRESHOLDS

        if _ACTIVE_THRESHOLDS != _INITIAL_THRESHOLDS:
            gc.set_threshold(*_ACTIVE_THRESHOLDS)
            _LOGGER.info(
                "gc_tuning_enabled mode=%s initial=%s active=%s",
                _MODE,
                _INITIAL_THRESHOLDS,
                _ACTIVE_THRESHOLDS,
            )
        else:
            _LOGGER.info(
                "gc_tuning_disabled mode=%s thresholds=%s",
                _MODE,
                _ACTIVE_THRESHOLDS,
            )

        instrument_enabled = _get_bool_env("VOLO_GC_PAUSE_INSTRUMENTATION", default=False)
        if instrument_enabled:
            warn_ms = _get_float_env("VOLO_GC_PAUSE_WARN_MS", default=25.0)
            summary_every = _get_int_env(
                "VOLO_GC_PAUSE_SUMMARY_EVERY",
                default=250,
                minimum=0,
            )
            _MONITOR = _GCPauseMonitor(warn_ms=warn_ms, summary_every=summary_every)
            _CALLBACK = _MONITOR
            gc.callbacks.append(_CALLBACK)
            _LOGGER.info(
                "gc_pause_instrumentation_enabled warn_ms=%.2f summary_every=%s",
                warn_ms,
                summary_every,
            )

        _CONFIGURED = True
    return get_gc_runtime_status()


def get_gc_pause_stats() -> Dict[str, Any]:
    monitor = _MONITOR
    if monitor is None:
        return {
            "events_total": 0,
            "pause_total_ms": 0.0,
            "pause_max_ms": 0.0,
            "generation_events": {0: 0, 1: 0, 2: 0},
            "generation_pause_total_ms": {0: 0.0, 1: 0.0, 2: 0.0},
            "generation_pause_max_ms": {0: 0.0, 1: 0.0, 2: 0.0},
        }
    return monitor.snapshot()


def get_gc_runtime_status() -> Dict[str, Any]:
    return {
        "configured": bool(_CONFIGURED),
        "mode": str(_MODE),
        "initial_thresholds": _INITIAL_THRESHOLDS,
        "active_thresholds": _ACTIVE_THRESHOLDS or gc.get_threshold(),
        "instrumentation_enabled": _MONITOR is not None,
        "pause_stats": get_gc_pause_stats(),
    }
