"""
Usage:
  uv run command_line_tools/cli.py
  uv run command_line_tools/cli.py --skip-mongodb

Notes:
- Set CLI_SENDER_ADDRESS and CLI_SUB_ORG_ID (e.g. in .env) to bypass MongoDB
  user lookup when running with --skip-mongodb.
"""

import argparse
import asyncio
import contextlib
from dataclasses import dataclass
import logging
import os
import re
import sys
import traceback
import uuid
from typing import Any, cast

from dotenv import load_dotenv
from langchain_core.runnables import RunnableConfig

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


from config.bridge_registry import (
    MAINNET_RELAY_API_BASE_URL,
    RELAY,
    TESTNET_RELAY_API_BASE_URL,
)
from core.health import run_startup_checks
from core.tasks import (
    finalize_conversation_turn,
    prepare_conversation_turn,
    resolve_conversation_id,
)
from core.utils.async_resources import async_resource_scope
from core.utils.event_stream import (
    coerce_event_dict,
    event_stream_name,
    format_event,
    progress_stage_message,
)
from core.utils.http import raise_for_status, request_json
from core.utils.upstash_client import get_upstash_client, upstash_configured
from core.utils.viz import get_status_table
from graph.agent_state import AgentState
from graph.runtime_io import build_thread_config, build_turn_input

DEFAULT_CLI_PROVIDER = "cli"
DEFAULT_CLI_USER_ID = "cli_demo_user"
DEFAULT_CLI_USERNAME = "cli_demo_username"
DEFAULT_CLI_THREAD_ID = "cli_demo_thread"
_CLI_INITIAL_IDLE_NOTICE_SECONDS = 3.0
_CLI_IDLE_NOTICE_INTERVAL_SECONDS = 20.0


@dataclass(frozen=True)
class _CliProgressUpdate:
    stage: str
    message: str


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _cli_verbose_debug() -> bool:
    return _env_flag("VOLO_CLI_VERBOSE_DEBUG", False)


def _cli_show_status_table() -> bool:
    return _env_flag("VOLO_CLI_SHOW_STATUS_TABLE", _cli_verbose_debug())


def _configure_cli_logging() -> None:
    if _cli_verbose_debug():
        return
    for logger_name in (
        "intent_hub.parser.router",
        "core.conversation.responder",
    ):
        logging.getLogger(logger_name).setLevel(logging.ERROR)


def _should_print_reasoning_log(log: Any) -> bool:
    if _cli_verbose_debug():
        return True
    text = str(log or "").strip().upper()
    if not text:
        return False
    important_prefixes = (
        "[ERROR]",
        "[FAIL]",
        "[FAILED]",
        "[WARNING]",
        "[WARN]",
    )
    return text.startswith(important_prefixes)


def _should_print_event_line(event_data: dict[str, Any]) -> bool:
    if _cli_verbose_debug():
        return True
    status = str(event_data.get("status") or "").strip().lower()
    return status in {"failed", "error"}


def _status_table_from_values(values: Any) -> str:
    if not isinstance(values, dict):
        return ""
    history = values.get("plan_history")
    state = values.get("execution_state")
    if not history or not state:
        return ""
    try:
        latest_plan = history[-1]
        return str(get_status_table(latest_plan, state) or "")
    except Exception:
        return ""


def _extract_status_table(snapshot: object) -> str:
    values = getattr(snapshot, "values", {}) or {}
    return _status_table_from_values(values)


def _should_print_status_table(previous_table: str, current_table: str) -> bool:
    current = str(current_table or "").strip()
    if not current:
        return False
    previous = str(previous_table or "").strip()
    if not previous:
        return True
    return current != previous


def _extract_state_counters(snapshot: object) -> tuple[int, int]:
    values = getattr(snapshot, "values", {}) or {}
    messages = values.get("messages") or []
    logs = values.get("reasoning_logs") or []
    return len(messages), len(logs)


def _ai_signature(message: Any) -> tuple[str, str]:
    message_id = getattr(message, "id", None)
    content = getattr(message, "content", "")
    if not isinstance(content, str):
        content = str(content)
    return str(message_id or ""), content


def _extract_last_ai_signature(snapshot: object) -> tuple[str, str] | None:
    values = getattr(snapshot, "values", {}) or {}
    messages = values.get("messages") or []
    for message in reversed(messages):
        if getattr(message, "type", None) == "ai":
            return _ai_signature(message)
    return None


def _latest_ai_message(messages: list[Any] | None) -> Any | None:
    if not messages:
        return None
    for message in reversed(messages):
        if getattr(message, "type", None) == "ai":
            return message
    return None


def _select_unprinted_latest_ai_message(
    messages: list[Any] | None,
    last_printed_signature: tuple[str, str] | None,
) -> Any | None:
    latest = _latest_ai_message(messages)
    if latest is None:
        return None
    latest_signature = _ai_signature(latest)
    if latest_signature == last_printed_signature:
        return None
    return latest


async def _load_state_counters(app: object, config: RunnableConfig) -> tuple[int, int]:
    try:
        aget_state = getattr(app, "aget_state", None)
        if callable(aget_state):
            snapshot = await aget_state(config)
        else:
            get_state = getattr(app, "get_state", None)
            if not callable(get_state):
                return 0, 0
            snapshot = get_state(config)
    except Exception:
        return 0, 0
    try:
        return _extract_state_counters(snapshot)
    except Exception:
        return 0, 0


async def _load_last_ai_signature(
    app: object, config: RunnableConfig
) -> tuple[str, str] | None:
    try:
        aget_state = getattr(app, "aget_state", None)
        if callable(aget_state):
            snapshot = await aget_state(config)
        else:
            get_state = getattr(app, "get_state", None)
            if not callable(get_state):
                return None
            snapshot = get_state(config)
    except Exception:
        return None
    try:
        return _extract_last_ai_signature(snapshot)
    except Exception:
        return None


async def _load_status_table(app: object, config: RunnableConfig) -> str:
    try:
        aget_state = getattr(app, "aget_state", None)
        if callable(aget_state):
            snapshot = await aget_state(config)
        else:
            get_state = getattr(app, "get_state", None)
            if not callable(get_state):
                return ""
            snapshot = get_state(config)
    except Exception:
        return ""
    try:
        return _extract_status_table(snapshot)
    except Exception:
        return ""


def _normalize_user_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def _is_confirm_like(text: str) -> bool:
    normalized = _normalize_user_text(text)
    if not normalized:
        return False
    hay = f" {normalized} "
    return any(token in hay for token in (" confirm ", " yes ", " proceed "))


def _is_action_like(text: str) -> bool:
    normalized = _normalize_user_text(text)
    if not normalized:
        return False
    hay = f" {normalized} "
    action_tokens = (
        " swap ",
        " bridge ",
        " transfer ",
        " send ",
        " convert ",
        " exchange ",
        " buy ",
        " sell ",
    )
    return any(token in hay for token in action_tokens)


def _next_idle_timeout_seconds(
    user_input: str,
    elapsed_seconds: float,
    progress_stage: str | None = None,
) -> float:
    if _is_confirm_like(user_input):
        if progress_stage in {"submitted", "finalizing"}:
            return 12.0
        if elapsed_seconds < 8.0:
            return max(1.0, 8.0 - elapsed_seconds)
        return 12.0
    if _is_action_like(user_input):
        if elapsed_seconds < 8.0:
            return max(1.0, 8.0 - elapsed_seconds)
        return 15.0
    return _CLI_IDLE_NOTICE_INTERVAL_SECONDS


def _build_idle_feedback(
    user_input: str,
    elapsed_seconds: float,
    progress_stage: str | None = None,
) -> str | None:
    elapsed_label = f"{int(elapsed_seconds)}s"
    if _is_confirm_like(user_input):
        if progress_stage == "submitted":
            return progress_stage_message("submitted", elapsed_seconds) or (
                f"Transaction sent. Waiting for confirmation ({elapsed_label})."
            )
        if progress_stage == "finalizing":
            return progress_stage_message("finalizing", elapsed_seconds) or (
                f"Confirmed on-chain. Finalizing ({elapsed_label})."
            )
        if elapsed_seconds < 8.0:
            return "Confirmed. Sending the transaction."
        return progress_stage_message("sending", elapsed_seconds) or (
            f"Still sending the transaction ({elapsed_label})."
        )
    if _is_action_like(user_input):
        if elapsed_seconds < 8.0:
            return "Working on it."
        return f"Still working ({elapsed_label})."
    if progress_stage is not None:
        return f"Still working ({elapsed_label})."
    return None


def _progress_update_from_reasoning_log(log: Any) -> _CliProgressUpdate | None:
    text = str(log or "").strip()
    if not text:
        return None
    upper = text.upper()
    if upper.startswith("[ACTION] STARTING"):
        # Performance/Milestone 3 Optimization:
        # Avoid misleading "Sending transaction..." for read-only tools like balance.
        tool_match = re.search(r"STARTING '([^']+)'", upper)
        if tool_match:
            tool = tool_match.group(1).lower()
            if tool in {"swap", "bridge", "transfer"}:
                return _CliProgressUpdate(
                    "sending", progress_stage_message("sending") or "Sending transaction..."
                )
        return None
    if upper.startswith("[SUCCESS]") and "SUBMITTED" in upper:
        return _CliProgressUpdate(
            "submitted",
            progress_stage_message("submitted")
            or "Transaction sent. Waiting for confirmation.",
        )
    if upper.startswith("[FEE] COLLECTING"):
        return _CliProgressUpdate(
            "finalizing",
            progress_stage_message("finalizing")
            or "Confirmed on-chain. Finalizing...",
        )
    return None


async def _stream_turn_events(
    events: Any,
    queue: "asyncio.Queue[tuple[str, Any]]",
) -> None:
    try:
        async for event in events:
            await queue.put(("event", event))
    except Exception as exc:
        await queue.put(("error", (exc, traceback.format_exc().rstrip())))
    finally:
        aclose = getattr(events, "aclose", None)
        if callable(aclose):
            with contextlib.suppress(Exception):
                await aclose()
        await queue.put(("done", None))


async def _event_listener(
    app, config: RunnableConfig, thread_id: str, stop_event: asyncio.Event
) -> None:
    client = get_upstash_client()
    if client is None:
        return
    stream = event_stream_name()
    group = os.getenv("VOLO_EVENT_CLI_GROUP", "volo-cli")
    consumer = f"cli-{uuid.uuid4().hex[:6]}"
    poll_s = float(os.getenv("VOLO_EVENT_CLI_POLL_SECONDS", "2"))
    last_printed_table = ""

    try:
        client.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as exc:
        message = str(exc)
        if (
            "BUSYGROUP" not in message
            and "Consumer Group name already exists" not in message
        ):
            return

    while not stop_event.is_set():
        try:
            response = await asyncio.to_thread(
                client.xreadgroup,
                group,
                consumer,
                streams={stream: ">"},
                count=10,
            )
        except Exception:
            await asyncio.sleep(poll_s)
            continue

        if not response:
            await asyncio.sleep(poll_s)
            continue

        for _stream_name, messages in response:
            for msg_id, data in messages:
                event_data = coerce_event_dict(data)
                if not event_data:
                    continue
                event_thread = event_data.get("thread_id")
                if event_thread and event_thread != thread_id:
                    continue

                if _should_print_event_line(event_data):
                    print(f"\n[Event] {format_event(event_data)}")

                try:
                    aget_state = getattr(app, "aget_state", None)
                    if callable(aget_state):
                        snapshot = await aget_state(config)
                    else:
                        snapshot = app.get_state(config)
                    values = getattr(snapshot, "values", {}) or {}
                    latest_table = (
                        _status_table_from_values(values)
                        if _cli_show_status_table()
                        else ""
                    )
                    if _cli_show_status_table() and _should_print_status_table(
                        last_printed_table, latest_table
                    ):
                        print("\n" + latest_table + "\n")
                        last_printed_table = latest_table
                except Exception:
                    pass

                print("\nUser: ", end="", flush=True)
                try:
                    client.xack(stream, group, msg_id)
                except Exception:
                    pass


def relay_discover(
    *,
    base_url: str,
    chain_ids: list[int] | None,
    term: str | None,
    limit: int,
) -> None:
    """
    Query Relay /chains and /currencies to discover supported chains and tokens.

    Usage examples:
      uv run command_line_tools/cli.py --relay-discover
      uv run command_line_tools/cli.py --relay-discover --relay-chain-ids 11155111,50312
      uv run command_line_tools/cli.py --relay-discover --relay-chain-ids 11155111 --relay-term USDC

    Notes:
    - Defaults to testnet Relay base URL unless --relay-mainnet is passed.
    - Provide --relay-chain-ids and --relay-term to list matching currencies.
    """
    headers = {"Content-Type": "application/json"}
    if RELAY.api_key:
        headers["x-api-key"] = RELAY.api_key

    print(f"Relay discovery using base URL: {base_url}")

    try:
        response = request_json(
            "GET",
            f"{base_url}/chains",
            headers=headers,
            service="relay",
        )
        raise_for_status(response, "relay")
        data = response.json()
    except Exception as exc:
        print(f"Failed to fetch /chains: {exc}")
        return

    if isinstance(data, dict) and "chains" in data:
        chains = data.get("chains")
    else:
        chains = data.get("data") if isinstance(data, dict) else data
        if isinstance(chains, dict):
            chains = chains.get("chains") or chains.get("data") or []
    if not isinstance(chains, list):
        chains = []

    if chain_ids:
        chain_set = set(chain_ids)
        chains = [c for c in chains if int(c.get("id", -1)) in chain_set]

    if chains:
        print("\nSupported chains:")
        for chain in chains:
            chain_id = chain.get("id")
            name = chain.get("name") or chain.get("displayName") or "unknown"
            deposit_enabled = chain.get("depositEnabled")
            withdraw_enabled = chain.get("withdrawEnabled") or chain.get(
                "withdrawalEnabled"
            )
            disabled = chain.get("disabled")
            print(
                f"- {chain_id}: {name} "
                f"(deposit={deposit_enabled}, withdraw={withdraw_enabled}, disabled={disabled})"
            )
    else:
        print("\nNo chains returned.")

    if not (term and chain_ids):
        return

    payload = {
        "chainIds": chain_ids,
        "term": term,
        "limit": limit,
        "depositAddressOnly": True,
        "useExternalSearch": True,
        "defaultList": True,
    }

    try:
        response = request_json(
            "POST",
            f"{base_url}/currencies/v2",
            headers=headers,
            json=payload,
            service="relay",
        )
        raise_for_status(response, "relay")
        data = response.json()
    except Exception as exc:
        print(f"\nFailed to fetch /currencies/v2: {exc}")
        return

    currencies = data.get("data") if isinstance(data, dict) else data
    if isinstance(currencies, dict):
        currencies = currencies.get("currencies") or currencies.get("data") or []
    if not isinstance(currencies, list):
        currencies = []

    print("\nMatching currencies:")
    if not currencies:
        print("- (none)")
        return

    for cur in currencies:
        chain_id = cur.get("chainId")
        symbol = cur.get("symbol")
        name = cur.get("name") or cur.get("displayName") or ""
        address = (
            cur.get("address")
            or cur.get("currencyAddress")
            or cur.get("tokenAddress")
            or cur.get("contractAddress")
            or ""
        )
        print(f"- {symbol} {name} (chain {chain_id}) {address}")


async def run_cli(
    *,
    thread_id: str,
    user_id: str,
    username: str,
    provider: str,
):
    run_startup_checks()
    from graph.graph import app

    # Reuse a stable thread by default for predictable local testing sessions.
    base_thread_id = thread_id
    conversation_id = resolve_conversation_id(
        provider=provider,
        provider_user_id=user_id,
        context=None,
    ) or f"{provider}:{user_id}"
    current_thread_id = base_thread_id
    current_selected_task_number: int | None = None
    config = cast(RunnableConfig, build_thread_config(thread_id=current_thread_id))

    print("--- Volo ReAct Agent CLI ---")
    print("Type 'exit' or 'quit' to stop.")
    stop_event_listener = asyncio.Event()
    event_task = None
    message_count = 0
    log_count = 0
    last_ai_signature: tuple[str | None, str | None] | None = None

    async def _switch_cli_thread_if_needed(
        *, force: bool = False, user_input: str | None = None
    ) -> str | None:
        nonlocal current_thread_id
        nonlocal current_selected_task_number
        nonlocal config
        nonlocal stop_event_listener
        nonlocal event_task
        nonlocal message_count
        nonlocal log_count
        nonlocal last_ai_signature

        prepared_turn = await prepare_conversation_turn(
            provider=provider,
            provider_user_id=user_id,
            default_thread_id=base_thread_id,
            user_message=user_input,
            conversation_id=conversation_id,
            selected_task_number=current_selected_task_number,
        )
        resolved_thread_id = prepared_turn.thread_id
        current_selected_task_number = prepared_turn.selected_task_number
        if prepared_turn.blocked_message is not None:
            return prepared_turn.blocked_message
        if not force and resolved_thread_id == current_thread_id:
            return None

        if event_task is not None:
            stop_event_listener.set()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(event_task, timeout=2.0)
            event_task = None

        current_thread_id = str(resolved_thread_id)
        config = cast(RunnableConfig, build_thread_config(thread_id=current_thread_id))
        message_count, log_count = await _load_state_counters(app, config)
        last_ai_signature = await _load_last_ai_signature(app, config)
        stop_event_listener = asyncio.Event()
        if upstash_configured():
            event_task = asyncio.create_task(
                _event_listener(app, config, current_thread_id, stop_event_listener)
            )
        return None

    await _switch_cli_thread_if_needed(force=True)
    print(
        f"Session: provider={provider} user_id={user_id} username={username} "
        f"thread_id={current_thread_id}"
    )

    while True:
        # Using a simple input() is fine for a CLI, but we need to run it in a way
        # that doesn't block if we had background tasks. For now, simple is best.
        user_input = await asyncio.get_event_loop().run_in_executor(
            None, input, "\nUser: "
        )

        if user_input.lower() in ["exit", "quit"]:
            break

        blocked_message = await _switch_cli_thread_if_needed(user_input=user_input)
        if blocked_message is not None:
            print(f"Assistant: {blocked_message}")
            continue
        pre_turn_table = await _load_status_table(app, config)
        new_ai_messages = []
        # Track what we've already printed IN THIS TURN
        printed_log_count = log_count
        printed_message_count = message_count
        latest_table = ""
        final_messages = None
        final_logs = None
        final_event = None
        turn_failed = False
        progress_stage: str | None = None
        last_progress_message: str | None = None

        try:
            async with async_resource_scope():
                events = app.astream(
                    cast(
                        AgentState,
                        build_turn_input(
                            user_message=user_input,
                            user_id=user_id,
                            provider=provider,
                            username=username,
                            thread_id=current_thread_id,
                            selected_task_number=current_selected_task_number,
                            context={"conversation_id": conversation_id},
                        ),
                    ),
                    config,
                    stream_mode="values",
                )
                event_queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
                stream_task = asyncio.create_task(
                    _stream_turn_events(events, event_queue)
                )
                turn_started_at = asyncio.get_running_loop().time()
                idle_timeout = _CLI_INITIAL_IDLE_NOTICE_SECONDS

                try:
                    while True:
                        try:
                            kind, payload = await asyncio.wait_for(
                                event_queue.get(),
                                timeout=idle_timeout,
                            )
                        except asyncio.TimeoutError:
                            elapsed = asyncio.get_running_loop().time() - turn_started_at
                            idle_message = _build_idle_feedback(
                                user_input,
                                elapsed,
                                progress_stage=progress_stage,
                            )
                            if idle_message is not None and idle_message != last_progress_message:
                                print(f"Assistant: {idle_message}")
                                last_progress_message = idle_message
                            idle_timeout = _next_idle_timeout_seconds(
                                user_input,
                                elapsed,
                                progress_stage=progress_stage,
                            )
                            continue

                        if kind == "done":
                            break

                        if kind == "error":
                            exc, tb = payload
                            raise RuntimeError(tb) from exc

                        event = payload
                        final_event = event
                        idle_timeout = _CLI_IDLE_NOTICE_INTERVAL_SECONDS

                        # Update the latest status table
                        if _cli_show_status_table():
                            history = event.get("plan_history")
                            state = event.get("execution_state")
                            if history and state:
                                latest_plan = history[-1]
                                latest_table = get_status_table(latest_plan, state)

                        # Print ONLY NEW reasoning logs
                        logs = event.get("reasoning_logs", [])
                        if logs is not None:
                            final_logs = logs
                        if len(logs) < printed_log_count:
                            # History reset (e.g., no checkpoint). Treat as fresh turn.
                            printed_log_count = 0
                        if len(logs) > printed_log_count:
                            new_logs = logs[printed_log_count:]
                            for log in new_logs:
                                progress_update = _progress_update_from_reasoning_log(log)
                                if progress_update is not None:
                                    progress_stage = progress_update.stage
                                    if progress_update.message != last_progress_message:
                                        print(f"Assistant: {progress_update.message}")
                                        last_progress_message = progress_update.message
                                if _should_print_reasoning_log(log):
                                    print(f"  > {log}")
                            printed_log_count = len(logs)

                        if "messages" in event and event["messages"]:
                            msgs = event["messages"]
                            final_messages = msgs
                            if len(msgs) < printed_message_count:
                                # History reset (e.g., no checkpoint). Treat as fresh turn.
                                printed_message_count = 0
                            if len(msgs) > printed_message_count:
                                for msg in msgs[printed_message_count:]:
                                    if msg.type == "ai":
                                        new_ai_messages.append(msg)
                                printed_message_count = len(msgs)
                finally:
                    if not stream_task.done():
                        stream_task.cancel()
                    with contextlib.suppress(Exception):
                        await stream_task
        except Exception as exc:
            turn_failed = True
            print("Assistant: Internal error while processing that turn.")
            print(f"  > {exc.__class__.__name__}: {exc}")
            print(traceback.format_exc().rstrip())
            latest_table = ""
            new_ai_messages = []

        # Print the final state of the DAG for this turn
        if _cli_show_status_table() and _should_print_status_table(
            pre_turn_table, latest_table
        ):
            print("\n" + latest_table + "\n")

        if turn_failed:
            pass
        elif new_ai_messages:
            for msg in new_ai_messages:
                print(f"Assistant: {msg.content}")
                last_ai_signature = _ai_signature(msg)
        else:
            fallback_msg = _select_unprinted_latest_ai_message(
                final_messages, last_ai_signature
            )
            if fallback_msg is not None:
                print(f"Assistant: {fallback_msg.content}")
                last_ai_signature = _ai_signature(fallback_msg)
            else:
                print("Assistant: (No response generated)")

        if final_messages is not None:
            message_count = len(final_messages)
        if final_logs is not None:
            log_count = len(final_logs)
        if final_event is not None and "selected_task_number" in final_event:
            selected_task_number = final_event.get("selected_task_number")
            try:
                current_selected_task_number = (
                    int(selected_task_number)
                    if selected_task_number is not None
                    else None
                )
            except Exception:
                current_selected_task_number = None
            current_selected_task_number = await finalize_conversation_turn(
                provider=provider,
                provider_user_id=user_id,
                conversation_id=conversation_id,
                thread_id=current_thread_id,
                event_selected_task_number=selected_task_number,
                current_selected_task_number=current_selected_task_number,
            )
        elif current_thread_id != base_thread_id:
            selected_task_number = await finalize_conversation_turn(
                provider=provider,
                provider_user_id=user_id,
                conversation_id=conversation_id,
                thread_id=current_thread_id,
            )
            if selected_task_number is not None:
                current_selected_task_number = selected_task_number

    if event_task is not None:
        stop_event_listener.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(event_task, timeout=2.0)


if __name__ == "__main__":
    load_dotenv()
    # Disable CDP analytics in CLI by default to avoid pending-task warnings on exit.
    if os.getenv("VOLO_CLI_DISABLE_CDP_ANALYTICS", "").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }:
        os.environ.setdefault("DISABLE_CDP_USAGE_TRACKING", "true")
        os.environ.setdefault("DISABLE_CDP_ERROR_REPORTING", "true")
    parser = argparse.ArgumentParser(description="Volo ReAct Agent CLI")
    parser.add_argument(
        "--relay-discover",
        action="store_true",
        help="Query Relay /chains and /currencies to discover supported testnet assets.",
    )
    parser.add_argument(
        "--relay-mainnet",
        action="store_true",
        help="Use mainnet Relay base URL for --relay-discover (default: testnet).",
    )
    parser.add_argument(
        "--relay-chain-ids",
        help="Comma-separated chain IDs for Relay discovery (e.g. 11155111,50312).",
    )
    parser.add_argument(
        "--relay-term",
        help="Token symbol or address for Relay currency lookup (e.g. USDC).",
    )
    parser.add_argument(
        "--relay-limit",
        type=int,
        default=50,
        help="Max currencies returned for --relay-discover (default: 50).",
    )
    parser.add_argument(
        "--skip-mongodb",
        action="store_true",
        help="Skip MongoDB startup health check (useful for local dev).",
    )
    parser.add_argument(
        "--provider",
        default=os.getenv("VOLO_CLI_PROVIDER", DEFAULT_CLI_PROVIDER),
        help=f"Identity provider for onboarding (default: {DEFAULT_CLI_PROVIDER}).",
    )
    parser.add_argument(
        "--user-id",
        default=os.getenv("VOLO_CLI_USER_ID", DEFAULT_CLI_USER_ID),
        help=f"Stable provider_user_id for CLI identity (default: {DEFAULT_CLI_USER_ID}).",
    )
    parser.add_argument(
        "--username",
        default=os.getenv("VOLO_CLI_USERNAME", DEFAULT_CLI_USERNAME),
        help=f"Username attached to the CLI identity (default: {DEFAULT_CLI_USERNAME}).",
    )
    parser.add_argument(
        "--thread-id",
        default=os.getenv("VOLO_CLI_THREAD_ID", DEFAULT_CLI_THREAD_ID),
        help=f"Conversation thread id to resume across runs (default: {DEFAULT_CLI_THREAD_ID}).",
    )
    parser.add_argument(
        "--new-thread",
        action="store_true",
        help="Generate a fresh random thread id for this run.",
    )
    parser.add_argument(
        "--verbose-debug",
        action="store_true",
        help="Show internal CLI debug lines, event notifications, and status tables.",
    )
    parser.add_argument(
        "--show-status-table",
        action="store_true",
        help="Show the execution status table during turns.",
    )
    parser.epilog = (
        "Example:\n"
        "  uv run command_line_tools/cli.py --skip-mongodb\n"
        "\n"
        "Tip: set CLI_SENDER_ADDRESS and CLI_SUB_ORG_ID in .env to bypass MongoDB user lookup."
    )
    args = parser.parse_args()

    if args.relay_discover:
        base_url = (
            MAINNET_RELAY_API_BASE_URL
            if args.relay_mainnet
            else TESTNET_RELAY_API_BASE_URL
        )
        chain_ids = None
        if args.relay_chain_ids:
            chain_ids = []
            for raw in args.relay_chain_ids.split(","):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    chain_ids.append(int(raw))
                except ValueError:
                    print(f"Invalid chain id: {raw}")
                    raise SystemExit(2)
        relay_discover(
            base_url=base_url,
            chain_ids=chain_ids,
            term=args.relay_term,
            limit=args.relay_limit,
        )
        raise SystemExit(0)

    if args.skip_mongodb:
        os.environ["SKIP_MONGODB_HEALTHCHECK"] = "1"
        os.environ["SKIP_MONGODB_USERS"] = "1"
    if args.verbose_debug:
        os.environ["VOLO_CLI_VERBOSE_DEBUG"] = "1"
    if args.show_status_table:
        os.environ["VOLO_CLI_SHOW_STATUS_TABLE"] = "1"
    _configure_cli_logging()

    thread_id = args.thread_id
    if args.new_thread:
        thread_id = str(uuid.uuid4())

    try:
        asyncio.run(
            run_cli(
                thread_id=thread_id,
                user_id=str(args.user_id),
                username=str(args.username),
                provider=str(args.provider),
            )
        )
    except KeyboardInterrupt:
        print("\nExiting...")
