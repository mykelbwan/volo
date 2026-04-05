from __future__ import annotations

import asyncio
import os
import uuid

from core.bridge_status_worker_locks import ensure_bridge_status_worker_lock_indexes
from core.bridge_status_worker_service import run_bridge_status_worker_loop
from core.database.mongodb_async import AsyncMongoDB


async def start_bridge_status_worker() -> asyncio.Task[None]:
    if not await AsyncMongoDB.ping():
        raise RuntimeError("MongoDB ping failed. Bridge status worker cannot start.")
    await ensure_bridge_status_worker_lock_indexes()

    interval_seconds = int(os.getenv("BRIDGE_STATUS_WORKER_INTERVAL_SECONDS", "15"))
    lock_ttl_seconds = int(os.getenv("BRIDGE_STATUS_WORKER_LOCK_TTL_SECONDS", "30"))
    owner = f"bridge-worker-{uuid.uuid4().hex[:8]}"

    return asyncio.create_task(
        run_bridge_status_worker_loop(
            interval_seconds=interval_seconds,
            lock_ttl_seconds=lock_ttl_seconds,
            owner=owner,
            once=False,
        ),
        name="volo-bridge-status-worker",
    )

