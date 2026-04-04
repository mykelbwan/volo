import asyncio
from unittest.mock import AsyncMock

from core.observer.trigger_matcher import MatchResult
from core.observer.watcher import ObserverWatcher


class _DummyApp:
    def __init__(self, events=None, raises=False):
        self._events = events or []
        self._raises = raises
        self.calls = 0

    def astream(self, _command, _config, stream_mode=None):
        self.calls += 1
        if self._raises:
            async def _gen():
                raise RuntimeError("boom")
                yield  # pragma: no cover
            return _gen()

        async def _gen():
            for event in self._events:
                yield event
        return _gen()


def _make_watcher(app, registry):
    watcher = ObserverWatcher.__new__(ObserverWatcher)
    watcher._app = app
    watcher._registry = registry
    watcher._stats = {
        "total_evaluations": 0,
        "total_matches": 0,
        "successful_resumes": 0,
        "failed_resumes": 0,
    }
    return watcher


def _make_match(trigger_id="t-1"):
    return MatchResult(
        trigger_id=trigger_id,
        thread_id="thread-1",
        user_id="user-1",
        resume_payload={"condition_met": True, "trigger_id": trigger_id},
        trigger_doc={},
    )


def test_resume_thread_success_path():
    registry = AsyncMock()
    registry.mark_triggered_or_reschedule.return_value = True
    registry.mark_failed.return_value = None

    app = _DummyApp(events=[{"messages": []}])
    watcher = _make_watcher(app, registry)

    asyncio.run(watcher._resume_thread(_make_match()))

    assert app.calls == 1
    registry.mark_triggered_or_reschedule.assert_awaited_once()
    registry.mark_failed.assert_not_awaited()
    assert watcher._stats["successful_resumes"] == 1


def test_resume_thread_skips_when_already_triggered():
    registry = AsyncMock()
    registry.mark_triggered_or_reschedule.return_value = False

    app = _DummyApp()
    watcher = _make_watcher(app, registry)

    asyncio.run(watcher._resume_thread(_make_match()))

    assert app.calls == 0
    registry.mark_failed.assert_not_awaited()
    assert watcher._stats["successful_resumes"] == 0


def test_resume_thread_marks_failed_on_error():
    registry = AsyncMock()
    registry.mark_triggered_or_reschedule.return_value = True
    registry.mark_failed.return_value = None

    app = _DummyApp(raises=True)
    watcher = _make_watcher(app, registry)

    asyncio.run(watcher._resume_thread(_make_match("t-err")))

    assert app.calls == 1
    registry.mark_failed.assert_awaited_once()
    error_arg = registry.mark_failed.call_args[0][1]
    assert "RuntimeError" in error_arg
    assert watcher._stats["failed_resumes"] == 1
