from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from core.bridge_status_worker_runtime import (
    BridgeStatusWorkerRuntimeDeps,
    run_bridge_status_tick,
)
from core.planning.execution_plan import ExecutionState


class _Snapshot:
    def __init__(self, values):
        self.values = values


class _App:
    def __init__(self) -> None:
        self.updated = []
        self.resumed = []

    def get_state(self, _config):
        return _Snapshot(
            {
                "pending_transactions": [{"type": "bridge", "status": "PENDING"}],
                "execution_state": ExecutionState(node_states={}),
                "execution_id": "exec-1",
            }
        )

    def update_state(self, config, payload, as_node=None):
        self.updated.append((config, payload, as_node))
        return config


def test_run_bridge_status_tick_updates_state_and_resumes():
    app = _App()
    handle_terminal_events = AsyncMock(return_value=None)
    sync_completed = AsyncMock(return_value=None)
    execution_delta = ExecutionState(node_states={})

    asyncio.run(
        run_bridge_status_tick(
            deps=BridgeStatusWorkerRuntimeDeps(
                get_app=lambda: app,
                distinct_thread_pairs=AsyncMock(return_value=[("thread-1", "")]),
                acquire_lock=AsyncMock(return_value=True),
                process_pending_records=AsyncMock(
                    return_value=(
                        [{"type": "bridge", "status": "SUCCESS"}],
                        execution_delta,
                        True,
                        [],
                    )
                ),
                latest_plan=lambda _values: None,
                is_plan_complete=lambda _plan, _state: False,
                sync_completed_conversation_task=sync_completed,
                handle_terminal_events=handle_terminal_events,
                resume_execution_engine=lambda app_obj, config: app_obj.resumed.append(config),
                debug_enabled=lambda: False,
                utc_now_iso=lambda: "2026-03-26T00:00:00+00:00",
            ),
            lock_ttl_seconds=30,
            owner="worker-1",
        )
    )

    assert len(app.updated) == 1
    assert app.updated[0][2] == "execution_engine"
    assert app.resumed == [{"configurable": {"thread_id": "thread-1"}}]
    handle_terminal_events.assert_not_awaited()
    sync_completed.assert_not_awaited()
