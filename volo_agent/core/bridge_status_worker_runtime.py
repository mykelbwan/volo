from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass(frozen=True)
class BridgeStatusWorkerRuntimeDeps:
    get_app: Callable[[], Any]
    distinct_thread_pairs: Callable[[], Awaitable[list[tuple[str, str]]]]
    acquire_lock: Callable[[str, str, int], Awaitable[bool]]
    process_pending_records: Callable[
        ..., Awaitable[tuple[list[dict], Any, bool, list[dict]]]
    ]
    latest_plan: Callable[[dict[str, Any]], Any]
    is_plan_complete: Callable[[Any, Any], bool]
    sync_completed_conversation_task: Callable[..., Awaitable[None]]
    handle_terminal_events: Callable[..., Awaitable[None]]
    resume_execution_engine: Callable[[Any, dict[str, Any]], None]
    debug_enabled: Callable[[], bool]
    utc_now_iso: Callable[[], str]


async def run_bridge_status_tick(
    *,
    deps: BridgeStatusWorkerRuntimeDeps,
    lock_ttl_seconds: int,
    owner: str,
) -> None:
    debug = deps.debug_enabled()
    now_iso = deps.utc_now_iso()
    app = deps.get_app()
    pairs = await deps.distinct_thread_pairs()
    if debug:
        print(f"[bridge-status] tick={now_iso} threads={len(pairs)} owner={owner}")

    for thread_id, checkpoint_ns in pairs:
        lock_id = f"{thread_id}:{checkpoint_ns}"
        if lock_ttl_seconds > 0 and not await deps.acquire_lock(
            lock_id, owner, lock_ttl_seconds
        ):
            if debug:
                print(
                    f"[bridge-status] lock-skip thread={thread_id} ns={checkpoint_ns}"
                )
            continue

        configurable: dict[str, str] = {"thread_id": thread_id}
        if checkpoint_ns:
            configurable["checkpoint_ns"] = checkpoint_ns
        config = {"configurable": configurable}

        try:
            snapshot = app.get_state(config)
        except Exception:
            continue

        values = getattr(snapshot, "values", {}) or {}
        pending = values.get("pending_transactions") or []
        if not isinstance(pending, list) or not pending:
            continue

        (
            updated_pending,
            execution_delta,
            resume_needed,
            terminal_events,
        ) = await deps.process_pending_records(
            pending,
            now_iso=now_iso,
            debug=debug,
        )

        if (
            updated_pending == pending
            and not execution_delta.node_states
            and not execution_delta.artifacts
        ):
            if debug:
                print(
                    f"[bridge-status] no-change thread={thread_id} ns={checkpoint_ns}"
                )
            continue

        execution_completed = False
        existing_execution = values.get("execution_state")
        if existing_execution is not None:
            try:
                execution_completed = bool(existing_execution.completed)
            except Exception:
                execution_completed = bool(
                    getattr(existing_execution, "completed", False)
                )

        if not execution_completed and existing_execution is not None:
            plan = deps.latest_plan(values)
            if plan is not None:
                try:
                    merged_state = existing_execution.merge(execution_delta)
                    if deps.is_plan_complete(plan, merged_state):
                        execution_delta.completed = True
                        execution_completed = True
                except Exception:
                    pass

        new_config = app.update_state(
            config,
            {
                "pending_transactions": updated_pending,
                "execution_state": execution_delta,
            },
            as_node="execution_engine",
        )
        if debug:
            print(
                f"[bridge-status] updated thread={thread_id} ns={checkpoint_ns} "
                f"nodes={len(execution_delta.node_states)}"
            )

        if terminal_events:
            await deps.handle_terminal_events(
                terminal_events=terminal_events,
                values=values,
                thread_id=thread_id,
                now_iso=now_iso,
                debug=debug,
            )
        if execution_completed:
            await deps.sync_completed_conversation_task(
                values=values,
                thread_id=thread_id,
                terminal_events=terminal_events,
            )
        if resume_needed and not execution_completed:
            if debug:
                print(
                    f"[bridge-status] resume thread={thread_id} ns={checkpoint_ns} "
                    "goto=execution_engine"
                )
            try:
                deps.resume_execution_engine(app, new_config)
            except Exception as exc:
                print(
                    f"[bridge-status] resume error thread={thread_id} "
                    f"ns={checkpoint_ns} err={exc}"
                )


async def run_bridge_status_loop(
    *,
    deps: BridgeStatusWorkerRuntimeDeps,
    interval_seconds: int,
    lock_ttl_seconds: int,
    owner: str,
    once: bool,
) -> None:
    while True:
        await run_bridge_status_tick(
            deps=deps,
            lock_ttl_seconds=lock_ttl_seconds,
            owner=owner,
        )
        if once:
            break
        await asyncio.sleep(max(1, int(interval_seconds)))
