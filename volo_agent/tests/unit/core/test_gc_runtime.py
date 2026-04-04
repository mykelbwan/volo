import importlib
import types

import pytest

from core.utils import gc_runtime


def _fake_gc(initial_threshold=(700, 10, 10)):
    state = {
        "threshold": tuple(initial_threshold),
        "callbacks": [],
    }

    def get_threshold():
        return state["threshold"]

    def set_threshold(a, b, c):
        state["threshold"] = (int(a), int(b), int(c))

    return state, types.SimpleNamespace(
        get_threshold=get_threshold,
        set_threshold=set_threshold,
        callbacks=state["callbacks"],
    )


@pytest.fixture(autouse=True)
def _reset_gc_runtime_state():
    importlib.reload(gc_runtime)
    yield
    importlib.reload(gc_runtime)


def test_configure_gc_runtime_safe_mode(monkeypatch):
    state, fake_gc = _fake_gc((700, 10, 10))
    monkeypatch.setattr(gc_runtime, "gc", fake_gc)
    monkeypatch.setenv("VOLO_GC_TUNING_MODE", "safe")
    monkeypatch.delenv("VOLO_GC_THRESHOLDS", raising=False)
    monkeypatch.setenv("VOLO_GC_PAUSE_INSTRUMENTATION", "0")

    status = gc_runtime.configure_gc_runtime()

    assert state["threshold"] == (2000, 30, 80)
    assert status["mode"] == "safe"
    assert status["active_thresholds"] == (2000, 30, 80)
    assert status["instrumentation_enabled"] is False


def test_configure_gc_runtime_explicit_thresholds_override(monkeypatch):
    state, fake_gc = _fake_gc((700, 10, 10))
    monkeypatch.setattr(gc_runtime, "gc", fake_gc)
    monkeypatch.setenv("VOLO_GC_TUNING_MODE", "safe")
    monkeypatch.setenv("VOLO_GC_THRESHOLDS", "3200,45,95")
    monkeypatch.setenv("VOLO_GC_PAUSE_INSTRUMENTATION", "0")

    status = gc_runtime.configure_gc_runtime()

    assert state["threshold"] == (3200, 45, 95)
    assert status["active_thresholds"] == (3200, 45, 95)


def test_gc_pause_instrumentation_records_pause(monkeypatch):
    state, fake_gc = _fake_gc((700, 10, 10))
    monkeypatch.setattr(gc_runtime, "gc", fake_gc)
    monkeypatch.setenv("VOLO_GC_TUNING_MODE", "off")
    monkeypatch.setenv("VOLO_GC_PAUSE_INSTRUMENTATION", "1")
    monkeypatch.setenv("VOLO_GC_PAUSE_WARN_MS", "1000")
    monkeypatch.setenv("VOLO_GC_PAUSE_SUMMARY_EVERY", "0")

    values = iter([1.000, 1.025])  # 25ms pause
    monkeypatch.setattr(gc_runtime.time, "perf_counter", lambda: next(values))

    status = gc_runtime.configure_gc_runtime()
    assert status["instrumentation_enabled"] is True
    assert len(state["callbacks"]) == 1

    cb = state["callbacks"][0]
    cb("start", {"generation": 2})
    cb("stop", {"generation": 2, "collected": 0, "uncollectable": 0})

    pause_stats = gc_runtime.get_gc_pause_stats()
    assert pause_stats["events_total"] == 1
    assert pause_stats["generation_events"][2] == 1
    assert pause_stats["pause_max_ms"] >= 24.9


def test_configure_gc_runtime_is_idempotent_without_deadlock(monkeypatch):
    state, fake_gc = _fake_gc((700, 10, 10))
    monkeypatch.setattr(gc_runtime, "gc", fake_gc)
    monkeypatch.setenv("VOLO_GC_TUNING_MODE", "safe")
    monkeypatch.setenv("VOLO_GC_PAUSE_INSTRUMENTATION", "1")
    monkeypatch.setenv("VOLO_GC_PAUSE_WARN_MS", "1000")
    monkeypatch.setenv("VOLO_GC_PAUSE_SUMMARY_EVERY", "0")

    first = gc_runtime.configure_gc_runtime()
    second = gc_runtime.configure_gc_runtime()

    assert first["configured"] is True
    assert second["configured"] is True
    assert second["active_thresholds"] == first["active_thresholds"]
    assert len(state["callbacks"]) == 1
