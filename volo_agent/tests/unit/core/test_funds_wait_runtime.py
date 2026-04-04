from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from core.reservations.funds_wait_runtime import (
    FundsWaitPollingDeps,
    process_wait_batch,
)
from core.reservations.models import FundsWaitRecord, ReservationRequirement
from core.reservations.wait_resume_runtime import FundsWaitResumeDeps, resume_wait


class _App:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.calls = 0

    def astream(self, _command, _config, stream_mode=None):
        self.calls += 1

        async def _gen():
            if self.should_fail:
                raise RuntimeError("resume boom")
            yield {"messages": []}

        return _gen()


def _wait(status: str = "resuming") -> FundsWaitRecord:
    return FundsWaitRecord(
        wait_id="wait-1",
        wallet_scope="sender:0xabc",
        conversation_id="discord:user-1",
        thread_id="thread-1",
        execution_id="exec-1",
        node_id="step_0",
        task_number=1,
        title="Buy USDC",
        tool="swap",
        status=status,
        resources=[
            ReservationRequirement(
                resource_key="0xabc|base|usdc",
                wallet_scope="sender:0xabc",
                sender="0xabc",
                chain="base",
                token_ref="usdc",
                symbol="USDC",
                decimals=6,
                required="100",
                required_base_units=100_000_000,
                kind="token_spend",
            )
        ],
        resource_snapshots={},
        created_at="2026-03-26T00:00:00+00:00",
        updated_at="2026-03-26T00:00:00+00:00",
        resume_token="a" * 32,
        attempts=1,
        meta={},
    )


def test_resume_wait_requeues_if_resume_finished_without_claim():
    app = _App()
    get_wait = AsyncMock(return_value=_wait(status="resuming"))
    mark_wait_queued = AsyncMock(return_value=None)
    stats = {"successful_resumes": 0, "failed_resumes": 0}

    asyncio.run(
        resume_wait(
            _wait(),
            deps=FundsWaitResumeDeps(
                app=app,
                timeout_seconds=0.1,
                get_wait=get_wait,
                mark_wait_queued=mark_wait_queued,
            ),
            stats=stats,
        )
    )

    assert app.calls == 1
    mark_wait_queued.assert_awaited_once_with(
        "wait-1",
        last_error=None,
        resume_after=None,
    )
    assert stats["successful_resumes"] == 1
    assert stats["failed_resumes"] == 0


def test_resume_wait_requeues_on_failure():
    app = _App(should_fail=True)
    mark_wait_queued = AsyncMock(return_value=None)
    stats = {"successful_resumes": 0, "failed_resumes": 0}

    asyncio.run(
        resume_wait(
            _wait(),
            deps=FundsWaitResumeDeps(
                app=app,
                timeout_seconds=0.1,
                get_wait=AsyncMock(return_value=_wait(status="queued")),
                mark_wait_queued=mark_wait_queued,
            ),
            stats=stats,
        )
    )

    assert app.calls == 1
    mark_wait_queued.assert_awaited_once()
    assert stats["successful_resumes"] == 0
    assert stats["failed_resumes"] == 1


def test_process_wait_batch_marks_and_resumes_candidates():
    app = _App()
    stats = {"successful_resumes": 0, "failed_resumes": 0}
    wait = _wait(status="queued")
    mark_wait_resuming = AsyncMock(return_value=_wait(status="resuming"))

    processed = asyncio.run(
        process_wait_batch(
            deps=FundsWaitPollingDeps(
                app=app,
                list_resume_candidates=AsyncMock(return_value=[wait]),
                mark_wait_resuming=mark_wait_resuming,
                get_wait=AsyncMock(return_value=_wait(status="queued")),
                mark_wait_queued=AsyncMock(return_value=None),
                timeout_seconds=0.1,
            ),
            batch_size=4,
            max_concurrent=2,
            stats=stats,
        )
    )

    assert processed == 1
    mark_wait_resuming.assert_awaited_once_with("wait-1")
    assert app.calls == 1


def test_process_wait_batch_skips_unclaimed_waits_without_hot_loop_count():
    app = _App()
    stats = {"successful_resumes": 0, "failed_resumes": 0}
    wait = _wait(status="queued")

    processed = asyncio.run(
        process_wait_batch(
            deps=FundsWaitPollingDeps(
                app=app,
                list_resume_candidates=AsyncMock(return_value=[wait]),
                mark_wait_resuming=AsyncMock(return_value=None),
                get_wait=AsyncMock(return_value=_wait(status="queued")),
                mark_wait_queued=AsyncMock(return_value=None),
                timeout_seconds=0.1,
            ),
            batch_size=4,
            max_concurrent=2,
            stats=stats,
        )
    )

    assert processed == 0
    assert app.calls == 0


def test_resume_wait_requeues_when_resume_token_is_missing():
    app = _App()
    wait = _wait()
    wait.resume_token = None
    mark_wait_queued = AsyncMock(return_value=None)
    stats = {"successful_resumes": 0, "failed_resumes": 0}

    asyncio.run(
        resume_wait(
            wait,
            deps=FundsWaitResumeDeps(
                app=app,
                timeout_seconds=0.1,
                get_wait=AsyncMock(return_value=_wait(status="queued")),
                mark_wait_queued=mark_wait_queued,
            ),
            stats=stats,
        )
    )

    assert app.calls == 0
    mark_wait_queued.assert_awaited_once()
    assert stats["successful_resumes"] == 0
    assert stats["failed_resumes"] == 1
