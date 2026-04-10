from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict

from langchain_core.messages import AIMessage

from core.conversation.account_query_parser import AccountQuery
from core.tasks.presentation import (
    format_task_detail,
    format_task_line,
    task_latest_update_line,
)
from core.tasks.registry import resolve_conversation_id
from core.tasks.router import (
    active_tasks,
    failed_tasks,
    find_current_task,
    find_task_by_number,
    is_clear_task_selection_request,
    is_generic_task_status_request,
    is_task_cancel_request,
    is_task_detail_request,
    is_task_selection_request,
    parse_task_number,
    task_reference,
)
from core.tasks.updater import upsert_task_from_state


@dataclass(frozen=True)
class RouterNodeDeps:
    TriggerRegistry: Any
    TaskHistoryRegistry: Any
    TaskRegistry: Any
    AsyncIdentityService: Any
    UnlinkAccountError: type[Exception]
    LINK_TOKEN_TTL_SECONDS: int
    is_link_account_request: Callable[[str], bool]
    is_link_status_request: Callable[[str], bool]
    is_unlink_account_request: Callable[[str], bool]
    extract_unlink_target: Callable[[str], str | None]


@dataclass(frozen=True)
class RouterNodeHelpers:
    is_confirm: Callable[[str], bool]
    is_decline: Callable[[str], bool]
    is_retry_request: Callable[[str], bool]
    is_recovery_accept: Callable[[str], bool]
    is_edit_request: Callable[[str], bool]
    is_cancel_request: Callable[[str], bool]
    is_history_request: Callable[[str], bool]
    parse_cancel_request: Callable[[str], str | None]
    parse_account_query: Callable[[str], AccountQuery | None]
    retry_failed_steps: Callable[[Any], Any]
    retry_failed_steps_with_fix: Callable[[Any], Any]
    summarize_failed_node: Callable[[Any, Any], dict | None]
    mark_pending_steps_skipped: Callable[[Any, Any, str], Any]
    format_pending_tx: Callable[[dict], str | None]
    describe_condition: Callable[[dict], str]
    is_trigger_expired: Callable[[dict, datetime], bool]
    get_sender_address: Callable[[Dict[str, Any]], str | None]
    resolve_volo_user_id: Callable[[Dict[str, Any]], str | None]
    format_linked_identity: Callable[[Dict[str, Any]], str]
    resolve_unlink_identity: Callable[
        [list[Dict[str, Any]], str | None],
        tuple[Dict[str, Any] | None, list[Dict[str, Any]]],
    ]
    unlink_examples: Callable[[list[Dict[str, Any]]], list[str]]


def _get_state_address(state: Dict[str, Any], key: str) -> str | None:
    user_info = state.get("user_info")
    if isinstance(user_info, dict):
        value = user_info.get(key)
        if value:
            return str(value)
    artifacts = state.get("artifacts")
    if isinstance(artifacts, dict):
        value = artifacts.get(key)
        if value:
            return str(value)
    return None


def _format_wallet_address_response(
    state: Dict[str, Any], query: AccountQuery, evm_address: str | None
) -> str:
    requested_chain = query.chain_family
    # Normalize chain to a string for safe dict access and comparisons
    requested_chain = str(requested_chain) if requested_chain is not None else None

    # Generic mapping for chain-specific address keys in state
    chain_key_map = {
        "solana": "solana_address",
        "evm": "evm_address",  # For clarity, though we also pass it as an argument
    }

    address_key = (
        chain_key_map.get(requested_chain) if requested_chain is not None else None
    )
    address = _get_state_address(state, address_key) if address_key else None

    # Fallback for EVM if not in mapping or missing from state but passed as argument
    if requested_chain == "evm" and not address:
        address = evm_address

    if address:
        chain_label = requested_chain.capitalize() if requested_chain else "wallet"
        return f"Your {chain_label} address is {address}."

    # Handle missing addresses with specific feedback
    if requested_chain == "solana":
        user_info = state.get("user_info")
        warning = (
            str(user_info.get("wallet_setup_warning") or "").strip()
            if isinstance(user_info, dict)
            else ""
        )
        if warning:
            return f"I couldn't find your Solana wallet address yet. {warning}"
        return (
            "I couldn't find your Solana wallet address yet. "
            "Solana setup may still be in progress. Reply 'retry' in about a minute."
        )

    if requested_chain == "evm":
        return (
            "I couldn't find your EVM wallet address yet. Please try again after "
            "onboarding completes."
        )

    # Final fallback for unspecified or unknown chains
    evm_state_address = _get_state_address(state, "evm_address")
    solana_state_address = _get_state_address(state, "solana_address")
    resolved_evm = evm_state_address or evm_address

    if resolved_evm and solana_state_address:
        return (
            "Your wallet addresses are:\n"
            f"- EVM: {resolved_evm}\n"
            f"- Solana: {solana_state_address}"
        )
    if resolved_evm:
        return f"Your EVM address is {resolved_evm}."
    if solana_state_address:
        return f"Your Solana address is {solana_state_address}."
    return (
        "I couldn't find your wallet address yet. Please try again after "
        "onboarding completes."
    )


def _wallet_retry_status_message(state: Dict[str, Any]) -> str | None:
    user_info = state.get("user_info")
    if not isinstance(user_info, dict):
        return None
    if not user_info.get("volo_user_id"):
        return None
    if user_info.get("solana_address"):
        return None

    warning = str(user_info.get("wallet_setup_warning") or "").strip()
    if warning:
        return warning

    metadata = user_info.get("metadata")
    detail = ""
    if isinstance(metadata, dict):
        last_error = str(metadata.get("solana_provision_last_error") or "").strip()
        if last_error:
            detail = f" Latest issue: {last_error}."
        elif metadata.get("solana_provision_last_failed_at"):
            detail = " Latest issue: temporary provisioning failure."

    if not detail:
        return None

    return (
        "Solana wallet setup is still pending."
        f"{detail} Please wait about a minute, then reply 'retry' again."
    )


async def _load_conversation_tasks(
    state: Dict[str, Any], task_registry_factory: Any
) -> list[dict[str, Any]]:
    conversation_id = resolve_conversation_id(
        provider=state.get("provider"),
        provider_user_id=state.get("user_id"),
        context=state.get("context"),
    )
    if not conversation_id:
        return []
    try:
        task_registry = task_registry_factory()
    except Exception:
        return []
    try:
        return await task_registry.list_recent(str(conversation_id), limit=10)
    except Exception:
        return []


def _task_detail_message(task: dict[str, Any]) -> str:
    return format_task_detail(task, task_label=task_reference(task))


def _task_status_value(task: dict[str, Any] | None) -> str:
    if not isinstance(task, dict):
        return ""
    return str(task.get("status") or "").strip().upper()


def _clear_pending_follow_up_state() -> dict[str, Any]:
    return {
        "pending_intent": None,
        "pending_intent_queue": None,
        "pending_clarification": None,
    }


async def _handle_task_navigation(
    state: Dict[str, Any],
    last_user_msg: str,
    conversation_tasks: list[dict[str, Any]],
    requested_task_number: int | None,
    referenced_task: dict[str, Any] | None,
    task_number_control_request: bool,
    *,
    helpers: RouterNodeHelpers,
) -> Dict[str, Any] | None:
    if is_clear_task_selection_request(last_user_msg):
        return {
            "route_decision": "STATUS",
            "selected_task_number": None,
            "messages": [
                AIMessage(
                    content=(
                        "Task selection cleared. New requests will use the main conversation again."
                    )
                )
            ],
        }

    if task_number_control_request and referenced_task is None:
        return {
            "route_decision": "STATUS",
            "messages": [
                AIMessage(
                    content=(
                        f"I couldn't find Task {requested_task_number} in this chat. "
                        "Type 'show my tasks' to review your tasks."
                    )
                )
            ],
        }

    if requested_task_number is not None and is_task_selection_request(last_user_msg):
        if referenced_task is None:
            return {
                "route_decision": "STATUS",
                "messages": [
                    AIMessage(
                        content=(
                            f"I couldn't find Task {requested_task_number} in this chat. "
                            "Type 'show my tasks' to review your tasks."
                        )
                    )
                ],
            }
        return {
            "route_decision": "STATUS",
            "selected_task_number": requested_task_number,
            "messages": [
                AIMessage(
                    content=(
                        f"{task_reference(referenced_task)} is selected for task details in this chat. "
                        f"To review it, say 'show task {requested_task_number}'. "
                        "Execution still follows the current live task for now."
                    )
                )
            ],
        }

    if requested_task_number is not None and is_task_detail_request(last_user_msg):
        if referenced_task is None:
            return {
                "route_decision": "STATUS",
                "messages": [
                    AIMessage(
                        content=(
                            f"I couldn't find Task {requested_task_number} in this chat. "
                            "Type 'show my tasks' to review your tasks."
                        )
                    )
                ],
            }
        return {
            "route_decision": "STATUS",
            "messages": [AIMessage(content=_task_detail_message(referenced_task))],
        }

    if requested_task_number is not None and referenced_task is not None:
        referenced_task_status = _task_status_value(referenced_task)
        if (
            "confirm task " in last_user_msg or "proceed task " in last_user_msg
        ) and referenced_task_status != "WAITING_CONFIRMATION":
            return {
                "route_decision": "STATUS",
                "selected_task_number": requested_task_number,
                "messages": [
                    AIMessage(
                        content=f"{task_reference(referenced_task)} is no longer waiting for confirmation."
                    )
                ],
            }
        if is_task_cancel_request(last_user_msg) and referenced_task_status in {
            "COMPLETED",
            "CANCELLED",
            "FAILED",
        }:
            return {
                "route_decision": "STATUS",
                "selected_task_number": requested_task_number,
                "messages": [
                    AIMessage(
                        content=f"{task_reference(referenced_task)} is not running right now."
                    )
                ],
            }

    return None


async def _handle_status_requests(
    state: Dict[str, Any],
    last_user_msg: str,
    conversation_tasks: list[dict[str, Any]],
    selected_task: dict[str, Any] | None,
    *,
    deps: RouterNodeDeps,
    helpers: RouterNodeHelpers,
) -> Dict[str, Any] | None:
    if is_generic_task_status_request(last_user_msg):
        if selected_task is None:
            return {
                "route_decision": "STATUS",
                "selected_task_number": None,
                "messages": [
                    AIMessage(
                        content=(
                            "No task is selected right now. Say 'show my tasks' to review them, "
                            "then 'use task <number>'."
                        )
                    )
                ],
            }
        return {
            "route_decision": "STATUS",
            "messages": [AIMessage(content=_task_detail_message(selected_task))],
        }

    if helpers.is_history_request(last_user_msg):
        conversation_id = resolve_conversation_id(
            provider=state.get("provider"),
            provider_user_id=state.get("user_id"),
            context=state.get("context"),
        )
        try:
            task_registry = deps.TaskRegistry()
        except Exception:
            task_registry = None

        if conversation_id and task_registry is not None:
            if conversation_tasks:
                lines = ["Your recent tasks:"]
                for task in conversation_tasks:
                    lines.append(format_task_line(task))
                return {
                    "route_decision": "STATUS",
                    "messages": [AIMessage(content="\n".join(lines))],
                }

        user_info = state.get("user_info") or {}
        user_id = (
            user_info.get("volo_user_id")
            if isinstance(user_info, dict)
            else state.get("user_id")
        )
        try:
            history_registry = deps.TaskHistoryRegistry()
        except Exception:
            history_registry = None

        if not user_id or history_registry is None:
            return {
                "route_decision": "STATUS",
                "messages": [AIMessage(content="I couldn't load your history yet.")],
            }

        try:
            records = await history_registry.list_recent(
                str(user_id), limit=10, status="SUCCESS"
            )
        except Exception:
            records = []

        if not records:
            return {
                "route_decision": "STATUS",
                "messages": [AIMessage(content="No successful tasks yet.")],
            }

        lines = ["Recent successful tasks:"]
        for record in records:
            summary = record.get("summary") or "Transaction"
            lines.append(f"- {summary}")

        return {
            "route_decision": "STATUS",
            "messages": [AIMessage(content="\n".join(lines))],
        }

    return None


async def _handle_cancellation(
    state: Dict[str, Any],
    last_user_msg: str,
    requested_task_number: int | None,
    *,
    helpers: RouterNodeHelpers,
) -> Dict[str, Any] | None:
    if not (
        helpers.is_cancel_request(last_user_msg)
        or is_task_cancel_request(last_user_msg)
    ):
        return None

    execution_state = state.get("execution_state")
    plan_history = state.get("plan_history") or []
    latest_plan = plan_history[-1] if plan_history else None

    cancelled_delta = helpers.mark_pending_steps_skipped(
        latest_plan, execution_state, "user_cancelled"
    )

    if cancelled_delta is not None:
        await upsert_task_from_state(
            state,
            title=str(getattr(latest_plan, "goal", "") or "Task"),
            status="CANCELLED",
            latest_summary="Cancelled.",
            tool=(
                next(iter(latest_plan.nodes.values())).tool
                if latest_plan and getattr(latest_plan, "nodes", None)
                else None
            ),
        )
        return {
            "route_decision": "CANCELLED",
            "confirmation_status": None,
            "execution_state": cancelled_delta,
            "selected_task_number": requested_task_number,
            **_clear_pending_follow_up_state(),
            "messages": [
                AIMessage(
                    content=(
                        "Cancelled this run locally. "
                        "If a transaction was already submitted, it may still "
                        "settle on-chain."
                    )
                )
            ],
        }

    return {
        "route_decision": "STATUS",
        "selected_task_number": requested_task_number,
        **_clear_pending_follow_up_state(),
        "messages": [
            AIMessage(content="Cancelled. If you want to try again, just tell me.")
        ],
    }


async def _handle_error_recovery(
    state: Dict[str, Any],
    last_user_msg: str,
    conversation_tasks: list[dict[str, Any]],
    requested_task_number: int | None,
    referenced_task: dict[str, Any] | None,
    current_task: dict[str, Any] | None,
    *,
    helpers: RouterNodeHelpers,
) -> Dict[str, Any] | None:
    exec_state = state.get("execution_state")
    has_failed_steps = exec_state and any(
        ns.status.value == "failed"
        if hasattr(ns.status, "value")
        else str(ns.status) == "StepStatus.FAILED"
        for ns in exec_state.node_states.values()
    )
    has_suggested_fix = exec_state and any(
        ns.status.value == "failed"
        if hasattr(ns.status, "value")
        else str(ns.status) == "StepStatus.FAILED" and ns.mutated_args
        for ns in exec_state.node_states.values()
    )

    if (
        has_failed_steps
        and state.get("confirmation_status") != "WAITING"
        and helpers.is_recovery_accept(last_user_msg)
    ):
        delta = helpers.retry_failed_steps_with_fix(exec_state)
        if delta is not None:
            message = (
                "Trying a safer option now."
                if has_suggested_fix
                else "Retrying the failed step now."
            )
            return {
                "route_decision": "CONFIRMED",
                "execution_state": delta,
                "messages": [AIMessage(content=message)],
            }

    if (
        helpers.is_retry_request(last_user_msg)
        and state.get("confirmation_status") != "WAITING"
    ):
        if (
            requested_task_number is not None
            and referenced_task is not None
            and current_task is not None
            and referenced_task.get("task_number") != current_task.get("task_number")
        ):
            return {
                "route_decision": "STATUS",
                "messages": [
                    AIMessage(
                        content=(
                            f"{task_reference(referenced_task)} is not the current live task in this chat yet. "
                            f"Type 'show task {requested_task_number}' to review it."
                        )
                    )
                ],
            }

        delta = helpers.retry_failed_steps(exec_state)
        if delta is None:
            if len(failed_tasks(conversation_tasks)) > 1:
                failed_refs = ", ".join(
                    task_reference(task)
                    for task in failed_tasks(conversation_tasks)[:3]
                )
                return {
                    "route_decision": "STATUS",
                    "messages": [
                        AIMessage(
                            content=(
                                "You have more than one failed task in this chat. "
                                f"Type 'show my tasks' to review them. {failed_refs}"
                            )
                        )
                    ],
                }
            wallet_retry_message = _wallet_retry_status_message(state)
            if wallet_retry_message is not None:
                return {
                    "route_decision": "STATUS",
                    "messages": [AIMessage(content=wallet_retry_message)],
                }
            if current_task is not None:
                return {
                    "route_decision": "STATUS",
                    "messages": [
                        AIMessage(
                            content=f"{task_reference(current_task)} is not failed, so there is nothing to retry."
                        )
                    ],
                }
            return {
                "route_decision": "STATUS",
                "messages": [AIMessage(content="There is nothing to retry.")],
            }
        return {
            "route_decision": "CONFIRMED",
            "execution_state": delta,
            "messages": [AIMessage(content="Retrying the failed step now.")],
        }

    if helpers.is_edit_request(last_user_msg):
        if requested_task_number is not None and referenced_task is not None:
            if _task_status_value(referenced_task) != "FAILED":
                return {
                    "route_decision": "STATUS",
                    "selected_task_number": requested_task_number,
                    "messages": [
                        AIMessage(
                            content=f"{task_reference(referenced_task)} has no failed step to edit."
                        )
                    ],
                }
        execution_state = state.get("execution_state")
        plan_history = state.get("plan_history") or []
        plan = plan_history[-1] if plan_history else None
        summary = (
            helpers.summarize_failed_node(plan, execution_state)
            if plan and execution_state
            else None
        )

        if not summary:
            return {
                "route_decision": "STATUS",
                "messages": [AIMessage(content="There is no failed step to edit.")],
            }

        return {
            "route_decision": "STATUS",
            "pending_edit": summary,
            "messages": [
                AIMessage(
                    content=(
                        f"Current failed step: {summary['summary']}.\n"
                        "Tell me what you want to change."
                    )
                )
            ],
        }

    return None


async def _handle_waiting_confirmation(
    state: Dict[str, Any],
    last_user_msg: str,
    *,
    helpers: RouterNodeHelpers,
) -> Dict[str, Any] | None:
    if state.get("confirmation_status") != "WAITING":
        return None

    if helpers.is_confirm(last_user_msg):
        history = state.get("plan_history", [])
        if history:
            latest_plan = history[-1]
            for node_id in latest_plan.nodes:
                latest_plan.nodes[node_id].approval_required = False

            return {
                "route_decision": "CONFIRMED",
                "plan_history": history,
            }
        return {"route_decision": "CONFIRMED"}

    if helpers.is_decline(last_user_msg):
        plan_history = state.get("plan_history", [])
        latest_plan = plan_history[-1] if plan_history else None
        execution_state = state.get("execution_state")
        cancelled_delta = helpers.mark_pending_steps_skipped(
            latest_plan, execution_state, "user_cancelled"
        )
        await upsert_task_from_state(
            state,
            title=str(getattr(latest_plan, "goal", "") or "Task"),
            status="CANCELLED",
            latest_summary="Cancelled.",
            tool=(
                next(iter(latest_plan.nodes.values())).tool
                if latest_plan and getattr(latest_plan, "nodes", None)
                else None
            ),
        )
        return {
            "route_decision": "CANCELLED",
            "confirmation_status": None,
            "execution_state": cancelled_delta,
            "pending_transactions": [],
            "messages": [AIMessage(content="Transaction cancelled.")],
        }

    return None


async def handle_control_requests(
    state: Dict[str, Any],
    last_user_msg: str,
    *,
    deps: RouterNodeDeps,
    helpers: RouterNodeHelpers,
) -> Dict[str, Any] | None:
    requested_task_number = parse_task_number(last_user_msg)
    parsed_account_query = helpers.parse_account_query(last_user_msg)
    task_number_control_request = requested_task_number is not None and (
        helpers.is_retry_request(last_user_msg)
        or helpers.is_edit_request(last_user_msg)
        or is_task_cancel_request(last_user_msg)
        or "confirm task " in last_user_msg
        or "proceed task " in last_user_msg
    )

    in_flight_state = state.get("execution_state")
    has_running = (
        in_flight_state
        and not getattr(in_flight_state, "completed", False)
        and any(
            getattr(ns.status, "value", str(ns.status)).lower() == "running"
            for ns in in_flight_state.node_states.values()
        )
    )

    should_load_tasks = (
        requested_task_number is not None
        or helpers.is_retry_request(last_user_msg)
        or helpers.is_history_request(last_user_msg)
        or is_generic_task_status_request(last_user_msg)
        or (
            has_running
            and not (
                parsed_account_query is not None
                or helpers.is_confirm(last_user_msg)
                or helpers.is_cancel_request(last_user_msg)
                or helpers.is_retry_request(last_user_msg)
            )
        )
    )

    conversation_tasks = (
        await _load_conversation_tasks(state, deps.TaskRegistry)
        if should_load_tasks
        else []
    )
    referenced_task = find_task_by_number(conversation_tasks, requested_task_number)
    selected_task_number = state.get("selected_task_number")
    selected_task = find_task_by_number(conversation_tasks, selected_task_number)
    current_task = find_current_task(
        conversation_tasks,
        thread_id=(state.get("context") or {}).get("thread_id"),
        execution_id=state.get("execution_id"),
    )

    nav_response = await _handle_task_navigation(
        state,
        last_user_msg,
        conversation_tasks,
        requested_task_number,
        referenced_task,
        task_number_control_request,
        helpers=helpers,
    )
    if nav_response:
        return nav_response

    if (
        requested_task_number is not None
        and referenced_task is not None
        and current_task is not None
        and referenced_task.get("task_number") != current_task.get("task_number")
    ):
        if (
            helpers.is_retry_request(last_user_msg)
            or helpers.is_edit_request(last_user_msg)
            or is_task_cancel_request(last_user_msg)
            or "confirm task " in last_user_msg
            or "proceed task " in last_user_msg
        ):
            return {
                "route_decision": "STATUS",
                "selected_task_number": requested_task_number,
                "messages": [
                    AIMessage(
                        content=(
                            f"{task_reference(referenced_task)} is not the current live task in this chat yet. "
                            f"Type 'show task {requested_task_number}' to review it."
                        )
                    )
                ],
            }

    status_response = await _handle_status_requests(
        state,
        last_user_msg,
        conversation_tasks,
        selected_task,
        deps=deps,
        helpers=helpers,
    )
    if status_response:
        return status_response

    pending_cancel = state.get("pending_cancel")
    if pending_cancel and last_user_msg:
        short_id = pending_cancel.get("short_id")
        trigger_id = pending_cancel.get("trigger_id")
        if helpers.is_confirm(last_user_msg):
            registry = deps.TriggerRegistry()
            user_info = state.get("user_info") or {}
            user_id = (
                user_info.get("volo_user_id")
                if isinstance(user_info, dict)
                else state.get("user_id")
            )
            cancelled = await registry.cancel_trigger(
                str(trigger_id), user_id=str(user_id) if user_id else None
            )
            if cancelled:
                message = f"Cancelled order {short_id}."
            else:
                message = "I couldn't cancel that order. It may have already executed."
            return {
                "route_decision": "STATUS",
                "pending_cancel": None,
                "messages": [AIMessage(content=message)],
            }
        if helpers.is_decline(last_user_msg):
            return {
                "route_decision": "STATUS",
                "pending_cancel": None,
                "messages": [AIMessage(content="Cancellation request cleared.")],
            }

    pending_edit = state.get("pending_edit")
    if pending_edit and last_user_msg:
        if helpers.is_decline(last_user_msg):
            return {
                "route_decision": "STATUS",
                "pending_edit": None,
                "messages": [AIMessage(content="Edit request cleared.")],
            }
        return {
            "route_decision": "ACTION",
            "pending_edit": None,
            "parse_scope": "last_user",
        }

    cancel_id = helpers.parse_cancel_request(last_user_msg)
    has_specific_cancel_target = bool(cancel_id) and (
        not is_task_cancel_request(last_user_msg)
        and str(last_user_msg or "").strip().lower() != "cancel"
    )

    recovery_response = await _handle_error_recovery(
        state,
        last_user_msg,
        conversation_tasks,
        requested_task_number,
        referenced_task,
        current_task,
        helpers=helpers,
    )
    if recovery_response:
        return recovery_response

    if not has_specific_cancel_target:
        cancel_response = await _handle_cancellation(
            state, last_user_msg, requested_task_number, helpers=helpers
        )
        if cancel_response:
            return cancel_response

    if has_running and not (
        parsed_account_query is not None
        or helpers.is_confirm(last_user_msg)
        or helpers.is_cancel_request(last_user_msg)
        or helpers.is_retry_request(last_user_msg)
    ):
        current_task = find_current_task(
            active_tasks(conversation_tasks),
            thread_id=(state.get("context") or {}).get("thread_id"),
            execution_id=state.get("execution_id"),
        )
        if current_task is not None:
            latest_update = task_latest_update_line(current_task)
            detail = f" {latest_update}" if latest_update else ""
            return {
                "route_decision": "STATUS",
                "messages": [
                    AIMessage(
                        content=(
                            f"{task_reference(current_task)} is still in progress. "
                            f"{detail} I'll notify you when it's done. Type 'cancel' to stop it."
                        )
                    )
                ],
            }
        return {
            "route_decision": "STATUS",
            "messages": [
                AIMessage(
                    content=(
                        "A transaction is still being processed on-chain. "
                        "I'll notify you automatically when it's done. "
                        "Type 'cancel' to abort."
                    )
                )
            ],
        }

    waiting_response = await _handle_waiting_confirmation(
        state, last_user_msg, helpers=helpers
    )
    if waiting_response:
        return waiting_response

    if cancel_id:
        registry = deps.TriggerRegistry()
        user_info = state.get("user_info") or {}
        user_id = (
            user_info.get("volo_user_id")
            if isinstance(user_info, dict)
            else state.get("user_id")
        )
        pending_triggers = []
        if user_id:
            pending_triggers = await registry.get_triggers_for_user(
                str(user_id), status="pending", limit=50
            )
        match = None
        for trigger in pending_triggers:
            trigger_id = str(trigger.get("trigger_id", ""))
            if trigger_id.lower().startswith(cancel_id):
                match = trigger
                break

        if not match:
            return {
                "route_decision": "STATUS",
                "messages": [
                    AIMessage(
                        content=(
                            "I couldn't find a pending order with that ID. "
                            "Type 'status' to see your pending orders."
                        )
                    )
                ],
            }

        trigger_id = str(match.get("trigger_id"))
        short_id = trigger_id[:8]
        return {
            "route_decision": "STATUS",
            "pending_cancel": {"trigger_id": trigger_id, "short_id": short_id},
            "messages": [
                AIMessage(
                    content=(
                        f"Confirm cancellation of order {short_id}. "
                        "Reply 'confirm' or 'no'."
                    )
                )
            ],
        }

    return None


async def handle_account_requests(
    state: Dict[str, Any],
    last_user_msg: str,
    *,
    deps: RouterNodeDeps,
    helpers: RouterNodeHelpers,
) -> Dict[str, Any] | None:
    account_query = helpers.parse_account_query(last_user_msg)
    if account_query and account_query.kind == "wallet_address":
        address = helpers.get_sender_address(state)
        content = _format_wallet_address_response(state, account_query, address)
        return {
            "route_decision": "STATUS",
            "messages": [AIMessage(content=content)],
        }

    if account_query and account_query.kind == "balance":
        if account_query.chain_family == "evm" and not account_query.chain:
            return {
                "route_decision": "STATUS",
                "messages": [
                    AIMessage(
                        content=(
                            "I can check balances on EVM networks, but I still need the specific chain. "
                            "Try Ethereum, Base, Arbitrum One, Optimism, Polygon, BNB Smart Chain, Avalanche, or Sepolia."
                        )
                    )
                ],
            }

        if account_query.chain:
            evm_addr = helpers.get_sender_address(state)
            user_info = state.get("user_info") or {}
            sol_addr = (
                user_info.get("solana_address") if isinstance(user_info, dict) else None
            )

            return {
                "route_decision": "ACTION",
                "parse_scope": "last_user",
                "pending_intent": {
                    "intent_type": "balance",
                    "slots": {
                        "chain": account_query.chain,
                        "token": account_query.token,
                        "sender": evm_addr,
                        "solana_address": sol_addr,
                    },
                },
                "pending_intent_queue": None,
                "pending_clarification": None,
            }

        return {"route_decision": "ACTION", "parse_scope": "last_user"}

    if deps.is_link_account_request(last_user_msg):
        volo_user_id = helpers.resolve_volo_user_id(state)
        if not volo_user_id:
            return {
                "route_decision": "STATUS",
                "messages": [
                    AIMessage(
                        content=(
                            "I couldn't find your wallet yet. Please try again after "
                            "onboarding completes."
                        )
                    )
                ],
            }

        token = await deps.AsyncIdentityService().generate_link_token(str(volo_user_id))
        ttl_minutes = max(1, int(round(deps.LINK_TOKEN_TTL_SECONDS / 60)))
        content = (
            f"Link code: {token}\n"
            f"On the other platform, type: link {token}\n"
            f"Valid for {ttl_minutes} minutes."
        )
        return {
            "route_decision": "STATUS",
            "messages": [AIMessage(content=content)],
        }

    if deps.is_link_status_request(last_user_msg):
        user_info = state.get("user_info") or {}
        identities = (
            user_info.get("identities") if isinstance(user_info, dict) else None
        )
        if not identities:
            return {
                "route_decision": "STATUS",
                "messages": [
                    AIMessage(content="I don't have any linked accounts on record yet.")
                ],
            }

        parts = [helpers.format_linked_identity(identity) for identity in identities]
        examples = helpers.unlink_examples(list(identities))
        content = "Linked accounts: " + ", ".join(parts)
        if examples:
            content += "\nTo unlink one, say: " + " or ".join(
                f"'{example}'" for example in examples[:3]
            )
        return {
            "route_decision": "STATUS",
            "messages": [AIMessage(content=content)],
        }

    unlink_response = await _handle_unlink_request(
        state, last_user_msg, deps=deps, helpers=helpers
    )
    if unlink_response:
        return unlink_response

    return None


async def _handle_unlink_request(
    state: Dict[str, Any],
    last_user_msg: str,
    *,
    deps: RouterNodeDeps,
    helpers: RouterNodeHelpers,
) -> Dict[str, Any] | None:
    if not deps.is_unlink_account_request(last_user_msg):
        return None

    provider = state.get("provider")
    provider_user_id = state.get("user_id")
    if not provider or not provider_user_id:
        return {
            "route_decision": "STATUS",
            "messages": [AIMessage(content="I couldn't identify your account.")],
        }

    user_info = state.get("user_info") or {}
    identities = (
        list(user_info.get("identities") or []) if isinstance(user_info, dict) else []
    )
    if not identities:
        return {
            "route_decision": "STATUS",
            "messages": [
                AIMessage(
                    content=(
                        "I couldn't load your linked accounts yet. "
                        "Try 'linked accounts' first, then unlink the specific provider."
                    )
                )
            ],
        }
    if len(identities) <= 1:
        return {
            "route_decision": "STATUS",
            "messages": [
                AIMessage(
                    content=(
                        "You only have one linked provider. "
                        "Link another platform first, then unlink this one."
                    )
                )
            ],
        }

    target_hint = deps.extract_unlink_target(last_user_msg)
    target_identity = None
    ambiguous_matches: list[Dict[str, Any]] = []
    if target_hint:
        target_identity, ambiguous_matches = helpers.resolve_unlink_identity(
            identities, target_hint
        )
        if ambiguous_matches:
            matches = ", ".join(
                helpers.format_linked_identity(identity)
                for identity in ambiguous_matches
            )
            examples = helpers.unlink_examples(ambiguous_matches)
            content = (
                f"I found multiple linked accounts matching '{target_hint}': {matches}. "
                "Use a more specific unlink command."
            )
            if examples:
                content += (
                    " Try "
                    + " or ".join(f"'{example}'" for example in examples[:3])
                    + "."
                )
            return {
                "route_decision": "STATUS",
                "messages": [AIMessage(content=content)],
            }
        if not target_identity:
            examples = helpers.unlink_examples(identities)
            content = (
                f"I couldn't match '{target_hint}' to one of your linked accounts. "
                "Use one of the linked providers shown below."
            )
            if examples:
                content += (
                    " Try "
                    + " or ".join(f"'{example}'" for example in examples[:3])
                    + "."
                )
            return {
                "route_decision": "STATUS",
                "messages": [AIMessage(content=content)],
            }
    else:
        examples = helpers.unlink_examples(identities)
        return {
            "route_decision": "STATUS",
            "messages": [
                AIMessage(
                    content=(
                        "You have multiple linked accounts. "
                        "Tell me exactly which one to unlink."
                        + (
                            " Try "
                            + " or ".join(f"'{example}'" for example in examples[:3])
                            + "."
                            if examples
                            else ""
                        )
                    )
                )
            ],
        }

    try:
        updated_user = await deps.AsyncIdentityService().unlink_identity(
            str(target_identity.get("provider")),
            str(target_identity.get("provider_user_id")),
        )
    except deps.UnlinkAccountError as exc:
        return {
            "route_decision": "STATUS",
            "messages": [
                AIMessage(
                    content=(
                        getattr(exc, "user_message", None)
                        or getattr(exc, "message", None)
                        or str(exc)
                        or "We couldn't unlink this account right now. Please try again in a moment."
                    )
                )
            ],
        }
    except Exception:
        return {
            "route_decision": "STATUS",
            "messages": [
                AIMessage(
                    content=(
                        "We couldn't unlink this account right now. "
                        "Please try again in a moment."
                    )
                )
            ],
        }

    remaining = []
    if isinstance(updated_user, dict):
        remaining = updated_user.get("identities") or []
    remaining_count = max(0, len(remaining))
    if remaining_count == 0:
        content = "This account has been unlinked."
    elif remaining_count == 1:
        content = (
            "This account has been unlinked. One linked account remains on your wallet."
        )
    else:
        content = (
            f"This account has been unlinked. "
            f"{remaining_count} linked accounts remain on your wallet."
        )
    return {
        "route_decision": "STATUS",
        "messages": [AIMessage(content=content)],
        "user_info": None,
        "artifacts": {},
    }


async def build_status_response(
    state: Dict[str, Any],
    updates: Dict[str, Any],
    *,
    deps: RouterNodeDeps,
    helpers: RouterNodeHelpers,
) -> Dict[str, Any]:
    user_info = state.get("user_info") or {}
    user_id = (
        user_info.get("volo_user_id")
        if isinstance(user_info, dict)
        else state.get("user_id")
    )
    conversation_id = resolve_conversation_id(
        provider=state.get("provider"),
        provider_user_id=state.get("user_id"),
        context=state.get("context"),
    )
    registry = deps.TriggerRegistry()
    try:
        history_registry = deps.TaskHistoryRegistry()
    except Exception:
        history_registry = None
    try:
        task_registry = deps.TaskRegistry()
    except Exception:
        task_registry = None
    pending_triggers = []
    expired_triggers = []
    if user_id:
        pending_triggers = await registry.get_triggers_for_user(
            str(user_id), status="pending", limit=50
        )
        expired_triggers = await registry.get_triggers_for_user(
            str(user_id), status="expired", limit=20
        )

    pending_txs = state.get("pending_transactions", [])
    tx_lines = []
    for tx in pending_txs:
        line = helpers.format_pending_tx(tx)
        if line:
            tx_lines.append(line)

    task_lines = []
    if conversation_id:
        task_records = await _load_conversation_tasks(state, deps.TaskRegistry)
        for record in task_records:
            task_lines.append(format_task_line(record))

    history_lines = []
    if user_id and history_registry is not None:
        try:
            records = await history_registry.list_recent(str(user_id), limit=10)
        except Exception:
            records = []
        for record in records:
            status = str(record.get("status", "UNKNOWN")).lower()
            summary = record.get("summary") or "Transaction"
            history_lines.append(f"- {summary} ({status})")

    lines = []
    if (
        not pending_triggers
        and not expired_triggers
        and not tx_lines
        and not task_lines
        and not history_lines
    ):
        updates["messages"] = [AIMessage(content="You don't have any pending tasks.")]
        return updates

    now = datetime.now(timezone.utc)
    expired_lines = []
    pending_lines = []

    for trigger in pending_triggers:
        trigger_id = str(trigger.get("trigger_id", ""))
        short_id = trigger_id[:8] if trigger_id else "unknown"
        condition_desc = helpers.describe_condition(
            trigger.get("trigger_condition", {}) or {}
        )
        if helpers.is_trigger_expired(trigger, now):
            expired_lines.append(f"- Order {short_id}: {condition_desc}")
        else:
            pending_lines.append(f"- Order {short_id}: {condition_desc}")

    for trigger in expired_triggers:
        trigger_id = str(trigger.get("trigger_id", ""))
        short_id = trigger_id[:8] if trigger_id else "unknown"
        condition_desc = helpers.describe_condition(
            trigger.get("trigger_condition", {}) or {}
        )
        line = f"- Order {short_id}: {condition_desc}"
        if line not in expired_lines:
            expired_lines.append(line)

    if pending_lines:
        lines.append("Pending orders:")
        lines.extend(pending_lines)

    if tx_lines:
        lines.append("Pending transactions:")
        for line in tx_lines:
            lines.append(f"- {line}")

    if expired_lines:
        lines.append("Expired orders:")
        lines.extend(expired_lines)
        lines.append(
            "These were not executed. If you still want them, create a new order."
        )

    if task_lines:
        lines.append("Tasks:")
        lines.extend(task_lines)

    if history_lines:
        lines.append("Recent tasks:")
        lines.extend(history_lines)

    if pending_lines:
        lines.append("To cancel an order, reply: cancel <order_id>.")

    updates["messages"] = [AIMessage(content="\n".join(lines))]
    return updates
