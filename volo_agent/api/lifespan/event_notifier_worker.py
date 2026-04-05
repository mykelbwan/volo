from __future__ import annotations

import asyncio
import os
import sys
import uuid
from typing import Any

from core.event_notifier_runtime import run_notifier
from core.utils.event_stream import event_stream_name
from core.utils.upstash_client import get_upstash_client, upstash_configured


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


async def _run_event_notifier_loop(
    *,
    client: Any,
    stream: str,
    group: str,
    consumer: str,
    block_ms: int,
    count: int,
    tail: int,
    raw: bool,
) -> None:
    next_tail = tail
    while True:
        await asyncio.to_thread(
            run_notifier,
            client=client,
            stream=stream,
            group=group,
            consumer=consumer,
            block_ms=block_ms,
            count=count,
            once=True,
            tail=next_tail,
            raw=raw,
            stdout_write=print,
            stderr_write=lambda line: print(line, file=sys.stderr),
        )
        next_tail = 0
        await asyncio.sleep(max(0.1, block_ms / 1000.0))


async def start_event_notifier_worker() -> asyncio.Task[None]:
    if not upstash_configured():
        raise RuntimeError(
            "Upstash not configured. Set UPSTASH_REDIS_REST_URL and "
            "UPSTASH_REDIS_REST_TOKEN."
        )
    client = get_upstash_client()
    if client is None:
        raise RuntimeError("Upstash client unavailable.")

    stream = event_stream_name()
    group = os.getenv("VOLO_EVENT_GROUP", "volo-notify")
    consumer = os.getenv("VOLO_EVENT_CONSUMER", "").strip()
    if not consumer:
        consumer = f"consumer-{uuid.uuid4().hex[:6]}"

    block_ms = int(os.getenv("VOLO_EVENT_BLOCK_MS", "5000"))
    count = int(os.getenv("VOLO_EVENT_COUNT", "50"))
    tail = int(os.getenv("VOLO_EVENT_TAIL", "0"))
    raw = _bool_env("VOLO_EVENT_RAW", False)

    print(
        f"event_notifier running stream={stream} group={group} consumer={consumer}",
        file=sys.stdout,
    )

    return asyncio.create_task(
        _run_event_notifier_loop(
            client=client,
            stream=stream,
            group=group,
            consumer=consumer,
            block_ms=block_ms,
            count=count,
            tail=tail,
            raw=raw,
        ),
        name="volo-event-notifier",
    )

