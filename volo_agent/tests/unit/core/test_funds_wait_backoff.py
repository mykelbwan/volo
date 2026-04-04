from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

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


def _wait(status: str = "resuming", attempts: int = 1) -> FundsWaitRecord:
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
        attempts=attempts,
        meta={},
    )


def test_resume_wait_backoff_on_failure():
    app = _App(should_fail=True)
    mark_wait_queued = AsyncMock(return_value=None)
    stats = {"successful_resumes": 0, "failed_resumes": 0}
    wait = _wait(attempts=1)

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

    assert app.calls == 1
    # Verify mark_wait_queued was called with a resume_after
    args, kwargs = mark_wait_queued.await_args
    assert "resume_after" in kwargs
    assert kwargs["resume_after"] is not None
    assert stats["failed_resumes"] == 1
