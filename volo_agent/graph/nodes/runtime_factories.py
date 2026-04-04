from __future__ import annotations

from typing import Any, Callable

from core.conversation.router_handlers import RouterNodeDeps, RouterNodeHelpers
from core.execution.runtime import ExecutionRuntime, ExecutionRuntimeDeps
from core.preflight.balance_runtime import BridgePreflightDeps
from core.routing.application import PreflightEstimateService, RoutingApplicationService


def build_execution_runtime(
    *,
    task_history_registry_cls: Any,
    task_registry_cls: Any,
    idempotency_store_cls: Any,
    reservation_service_getter: Callable[..., Any],
    run_with_timing: Callable[..., Any],
    run_blocking: Callable[..., Any],
    tools_registry: Any,
    normalize_output: Callable[..., Any],
    swap_failure_message: Callable[..., str],
    bridge_failure_message: Callable[..., str],
    tx_receipt_status: Callable[..., str],
    publish_event: Callable[..., Any],
    publish_event_async: Callable[..., Any] | None,
    task_history_write_timeout_seconds: float,
    native_marker: str,
) -> ExecutionRuntime:
    return ExecutionRuntime(
        ExecutionRuntimeDeps(
            task_history_registry_cls=task_history_registry_cls,
            task_registry_cls=task_registry_cls,
            idempotency_store_cls=idempotency_store_cls,
            reservation_service_getter=reservation_service_getter,
            run_with_timing=run_with_timing,
            run_blocking=run_blocking,
            tools_registry=tools_registry,
            normalize_output=normalize_output,
            swap_failure_message=swap_failure_message,
            bridge_failure_message=bridge_failure_message,
            tx_receipt_status=tx_receipt_status,
            publish_event=publish_event,
            publish_event_async=publish_event_async,
            task_history_write_timeout_seconds=task_history_write_timeout_seconds,
            native_marker=native_marker,
        )
    )


def build_routing_service(*, global_timeout_seconds: float) -> RoutingApplicationService:
    return RoutingApplicationService(global_timeout_seconds=global_timeout_seconds)


def build_preflight_estimate_service() -> PreflightEstimateService:
    return PreflightEstimateService()


def build_router_handler_context(
    *,
    trigger_registry_cls: Any,
    task_history_registry_cls: Any,
    task_registry_cls: Any,
    identity_service_cls: Any,
    unlink_account_error_cls: type[Exception],
    link_token_ttl_seconds: int,
    is_link_account_request: Callable[[str], bool],
    is_link_status_request: Callable[[str], bool],
    is_unlink_account_request: Callable[[str], bool],
    extract_unlink_target: Callable[[str], str | None],
    is_confirm: Callable[[str], bool],
    is_decline: Callable[[str], bool],
    is_retry_request: Callable[[str], bool],
    is_recovery_accept: Callable[[str], bool],
    is_edit_request: Callable[[str], bool],
    is_cancel_request: Callable[[str], bool],
    is_history_request: Callable[[str], bool],
    parse_cancel_request: Callable[[str], str | None],
    parse_account_query: Callable[[str], Any],
    retry_failed_steps: Callable[..., Any],
    retry_failed_steps_with_fix: Callable[..., Any],
    summarize_failed_node: Callable[..., Any],
    mark_pending_steps_skipped: Callable[..., Any],
    format_pending_tx: Callable[[dict], str | None],
    describe_condition: Callable[[dict], str],
    is_trigger_expired: Callable[..., bool],
    get_sender_address: Callable[..., str | None],
    resolve_volo_user_id: Callable[..., str | None],
    format_linked_identity: Callable[[dict[str, Any]], str],
    resolve_unlink_identity: Callable[..., Any],
    unlink_examples: Callable[..., list[str]],
) -> tuple[RouterNodeDeps, RouterNodeHelpers]:
    return (
        RouterNodeDeps(
            TriggerRegistry=trigger_registry_cls,
            TaskHistoryRegistry=task_history_registry_cls,
            TaskRegistry=task_registry_cls,
            AsyncIdentityService=identity_service_cls,
            UnlinkAccountError=unlink_account_error_cls,
            LINK_TOKEN_TTL_SECONDS=link_token_ttl_seconds,
            is_link_account_request=is_link_account_request,
            is_link_status_request=is_link_status_request,
            is_unlink_account_request=is_unlink_account_request,
            extract_unlink_target=extract_unlink_target,
        ),
        RouterNodeHelpers(
            is_confirm=is_confirm,
            is_decline=is_decline,
            is_retry_request=is_retry_request,
            is_recovery_accept=is_recovery_accept,
            is_edit_request=is_edit_request,
            is_cancel_request=is_cancel_request,
            is_history_request=is_history_request,
            parse_cancel_request=parse_cancel_request,
            parse_account_query=parse_account_query,
            retry_failed_steps=retry_failed_steps,
            retry_failed_steps_with_fix=retry_failed_steps_with_fix,
            summarize_failed_node=summarize_failed_node,
            mark_pending_steps_skipped=mark_pending_steps_skipped,
            format_pending_tx=format_pending_tx,
            describe_condition=describe_condition,
            is_trigger_expired=is_trigger_expired,
            get_sender_address=get_sender_address,
            resolve_volo_user_id=resolve_volo_user_id,
            format_linked_identity=format_linked_identity,
            resolve_unlink_identity=resolve_unlink_identity,
            unlink_examples=unlink_examples,
        ),
    )


def build_bridge_preflight_deps(
    *,
    cached_bridge_quote: Callable[[dict[str, Any]], dict[str, Any] | None],
    simulate_bridge_preview: Callable[..., Any],
    suggest_bridge_options: Callable[..., Any],
    bridge_not_supported: Callable[..., Any],
    is_bridge_unsupported_error: Callable[[str], bool],
    get_chain_by_name: Callable[[str], Any],
    is_native: Callable[[str, Any], bool],
    normalize_token_key: Callable[[str, Any], str],
    balance_key: Callable[[str, str, str], str],
) -> BridgePreflightDeps:
    return BridgePreflightDeps(
        cached_bridge_quote=cached_bridge_quote,
        simulate_bridge_preview=simulate_bridge_preview,
        suggest_bridge_options=suggest_bridge_options,
        bridge_not_supported=bridge_not_supported,
        is_bridge_unsupported_error=is_bridge_unsupported_error,
        get_chain_by_name=get_chain_by_name,
        is_native=is_native,
        normalize_token_key=normalize_token_key,
        balance_key=balance_key,
    )
