"""
Event Notifier
--------------
Consumes Volo execution events from Redis Streams and notifies the client.
intended to run as a separate process.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

try:
    from dotenv import find_dotenv, load_dotenv
except Exception:  # pragma: no cover - optional dependency
    load_dotenv = None
    find_dotenv = None


def _load_local_env() -> None:
    if load_dotenv is None:
        return
    try:
        env_path = find_dotenv(usecwd=True) if find_dotenv else ""
        load_dotenv(env_path or None)
    except Exception:
        pass


from core.event_notifier_runtime import ( # noqa: E402
    run_notifier,
)
from core.utils.event_stream import event_stream_name  # noqa: E402
from core.utils.upstash_client import get_upstash_client, upstash_configured  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="event_notifier",
        description="Consume Volo execution events from Redis Streams.",
    )
    parser.add_argument(
        "--group",
        type=str,
        default=os.getenv("VOLO_EVENT_GROUP", "volo-notify"),
        help="Redis consumer group name.",
    )
    parser.add_argument(
        "--consumer",
        type=str,
        default=os.getenv("VOLO_EVENT_CONSUMER", ""),
        help="Redis consumer name (defaults to random).",
    )
    parser.add_argument(
        "--block-ms",
        type=int,
        default=int(os.getenv("VOLO_EVENT_BLOCK_MS", "5000")),
        help="XREADGROUP block time in ms.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=int(os.getenv("VOLO_EVENT_COUNT", "50")),
        help="Max events per read.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Consume at most one batch and exit.",
    )
    parser.add_argument(
        "--tail",
        type=int,
        default=int(os.getenv("VOLO_EVENT_TAIL", "0")),
        help="Print the last N events on startup (0 disables).",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw event lines instead of user-facing progress messages.",
    )
    return parser.parse_args()


def main() -> int:
    _load_local_env()
    args = _parse_args()
    if not upstash_configured():
        print("Upstash not configured. Set UPSTASH_REDIS_REST_URL/TOKEN.")
        return 1
    client = get_upstash_client()
    if client is None:
        print("Upstash client unavailable.")
        return 1

    stream = event_stream_name()
    group = args.group
    consumer = args.consumer or f"consumer-{uuid.uuid4().hex[:6]}"

    print(
        f"event_notifier running stream={stream} group={group} consumer={consumer}",
        file=sys.stdout,
    )
    return run_notifier(
        client=client,
        stream=stream,
        group=group,
        consumer=consumer,
        block_ms=args.block_ms,
        count=args.count,
        once=args.once,
        tail=args.tail,
        raw=args.raw,
        stdout_write=print,
        stderr_write=lambda line: print(line, file=sys.stderr),
    )


if __name__ == "__main__":
    raise SystemExit(main())
