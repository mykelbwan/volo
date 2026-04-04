from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from core.observer.resume_runtime import ResumeRuntimeDeps, resume_thread
from core.observer.trigger_matcher import MatchResult


class _HangingApp:
    def __init__(self) -> None:
        self.calls = 0

    def astream(self, _command, _config, stream_mode=None):
        self.calls += 1

        async def _gen():
            await asyncio.sleep(3600)
            yield {}

        return _gen()


def _make_match(trigger_id: str = "t-timeout") -> MatchResult:
    return MatchResult(
        trigger_id=trigger_id,
        thread_id="thread-1",
        user_id="user-1",
        resume_payload={"condition_met": True, "trigger_id": trigger_id},
        trigger_doc={},
    )


def test_resume_runtime_marks_failed_on_timeout():
    app = _HangingApp()
    mark_triggered = AsyncMock(return_value=True)
    mark_failed = AsyncMock(return_value=None)
    stats = {
        "total_evaluations": 0,
        "total_matches": 0,
        "successful_resumes": 0,
        "failed_resumes": 0,
    }

    asyncio.run(
        resume_thread(
            _make_match(),
            deps=ResumeRuntimeDeps(
                app=app,
                mark_triggered_or_reschedule=mark_triggered,
                mark_failed=mark_failed,
                timeout_seconds=0.01,
            ),
            stats=stats,
        )
    )

    assert app.calls == 1
    mark_triggered.assert_awaited_once()
    mark_failed.assert_awaited_once()
    error_arg = mark_failed.call_args[0][1]
    assert "timed out after 0.01s" in error_arg
    assert stats["successful_resumes"] == 0
    assert stats["failed_resumes"] == 1
