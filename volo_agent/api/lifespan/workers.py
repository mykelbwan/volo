from __future__ import annotations

import asyncio
from dataclasses import dataclass

from api.lifespan.bridge_status_worker import start_bridge_status_worker
from api.lifespan.event_notifier_worker import start_event_notifier_worker
from api.lifespan.funds_wait_worker import start_funds_wait_worker


@dataclass(frozen=True)
class WorkerHandles:
    bridge_status_worker: asyncio.Task[None]
    event_notifier_worker: asyncio.Task[None]
    funds_wait_worker: asyncio.Task[None]


async def start_workers() -> WorkerHandles:
    bridge_status_worker = await start_bridge_status_worker()
    try:
        event_notifier_worker = await start_event_notifier_worker()
    except Exception:
        bridge_status_worker.cancel()
        try:
            await bridge_status_worker
        except asyncio.CancelledError:
            pass
        raise
    try:
        funds_wait_worker = await start_funds_wait_worker()
    except Exception:
        event_notifier_worker.cancel()
        bridge_status_worker.cancel()
        for task in (event_notifier_worker, bridge_status_worker):
            try:
                await task
            except asyncio.CancelledError:
                pass
        raise
    return WorkerHandles(
        bridge_status_worker=bridge_status_worker,
        event_notifier_worker=event_notifier_worker,
        funds_wait_worker=funds_wait_worker,
    )


async def stop_workers(handles: WorkerHandles) -> None:
    handles.funds_wait_worker.cancel()
    handles.event_notifier_worker.cancel()
    handles.bridge_status_worker.cancel()
    for task in (
        handles.funds_wait_worker,
        handles.event_notifier_worker,
        handles.bridge_status_worker,
    ):
        try:
            await task
        except asyncio.CancelledError:
            pass
