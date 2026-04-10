import asyncio
import inspect
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from functools import partial
from typing import Any, Awaitable, Callable, Dict, Mapping, Sequence, Tuple, Union, cast

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from pydantic import ValidationError

from config.chains import RPC_ENV_VARS, get_chain_by_name
from config.solana_chains import get_solana_chain
from core.fees.fee_collector import FeeCollectionError, collect_fee
from core.fees.models import FeeQuote
from core.history.task_history import summarize_task
from core.idempotency.store import compute_args_hash, compute_idempotency_key
from core.memory.ledger import ErrorCategory, get_ledger
from core.planning.execution_plan import (
    ExecutionState,
    NodeState,
    StepStatus,
    check_plan_complete,
    get_ready_nodes,
    resolve_dynamic_args,
)
from core.routing.route_meta import (
    canonicalize_route_meta,
    enforce_fallback_policy,
    is_route_expired,
    log_execution_comparison,
    log_route_expiry,
    log_route_validation,
    route_meta_required,
    route_meta_strictly_enforced,
    validate_route_meta,
)
from core.security.guardrails import (
    GuardrailPolicy,
    GuardrailService,
    RiskViolationError,
)
from core.tasks.cleanup import schedule_terminal_task_cleanup
from core.tasks.registry import resolve_conversation_id
from core.utils.bridge_status_registry import fetch_bridge_status
from core.utils.errors import (
    NonRetryableError,
    RouteExpiredError,
    SlippageExceededError,
    categorize_error,
)
from core.utils.evm_async import make_async_web3
from core.utils.user_feedback import execution_failed
from core.volume.tracker import track_execution_volume
from graph.replay_guard import compute_execution_dedup_key, extract_client_dedup_fields
from wallet_service.evm.get_native_bal import get_native_balance_async
from wallet_service.evm.get_single_erc20_token_bal import get_token_balance_async
from wallet_service.solana.get_native_bal import (
    get_native_balance_async as get_native_balance_solana_async,
)
from wallet_service.solana.get_single_token_bal import (
    get_token_balance_async as get_token_balance_solana_async,
)

_EVENT_PUBLISH_TIMEOUT_SECONDS = 0.2
_EVENT_DRAIN_TIMEOUT_SECONDS = 0.25
_EVENT_PUBLISH_MAX_WORKERS = 2
_EVENT_PUBLISH_COOLDOWN_KEY = "event_publish"
_EVENT_PUBLISH_UNAVAILABLE_COOLDOWN_SECONDS = 5.0
_event_publish_executor: ThreadPoolExecutor | None = None

_SIDE_EFFECT_MAX_WORKERS = 4
_IDEMPOTENCY_IO_TIMEOUT_SECONDS = 1.0
_LEDGER_IO_TIMEOUT_SECONDS = 0.2
_side_effect_executor: ThreadPoolExecutor | None = None
_TASK_REGISTRY_COOLDOWN_KEY = "task_registry"
_TASK_REGISTRY_UNAVAILABLE_COOLDOWN_SECONDS = 30.0
_side_effect_cooldowns: dict[str, float] = {}
_ROUTE_FAST_PATH_KEYS = {
    "calldata",
    "to",
    "approval_address",
    "tool_data",
    "swap_transaction",
}
_IDEMPOTENCY_RECLAIM_COOLDOWN_SECONDS = max(
    1,
    int(os.getenv("VOLO_IDEMPOTENCY_RECLAIM_COOLDOWN_SECONDS", "90") or "90"),
)
_TX_CONFIRMATION_CONFIRMED_SUCCESS = "CONFIRMED_SUCCESS"
_TX_CONFIRMATION_CONFIRMED_REVERT = "CONFIRMED_REVERT"
_TX_CONFIRMATION_UNKNOWN = "UNKNOWN"
_TX_CONFIRMATION_RPC_ERROR = "RPC_ERROR"
_LOGGER = logging.getLogger("volo.execution.runtime")


def _get_event_publish_executor() -> ThreadPoolExecutor:
    global _event_publish_executor
    if _event_publish_executor is None:
        _event_publish_executor = ThreadPoolExecutor(
            max_workers=_EVENT_PUBLISH_MAX_WORKERS
        )
    return _event_publish_executor


def _get_side_effect_executor() -> ThreadPoolExecutor:
    global _side_effect_executor
    if _side_effect_executor is None:
        _side_effect_executor = ThreadPoolExecutor(max_workers=_SIDE_EFFECT_MAX_WORKERS)
    return _side_effect_executor


def _side_effect_cooldown_active(key: str) -> bool:
    until = _side_effect_cooldowns.get(str(key), 0.0)
    return until > time.monotonic()


def _trip_side_effect_cooldown(key: str, *, seconds: float) -> None:
    _side_effect_cooldowns[str(key)] = time.monotonic() + max(0.0, seconds)

def _find_unresolved_marker(value: Any, *, path: str = "args") -> str | None:
    if isinstance(value, str) and "{{" in value and "}}" in value:
        return path
    if isinstance(value, dict):
        for key, nested in value.items():
            nested_path = f"{path}.{key}"
            found = _find_unresolved_marker(nested, path=nested_path)
            if found:
                return found
        return None
    if isinstance(value, (list, tuple)):
        for idx, nested in enumerate(value):
            nested_path = f"{path}[{idx}]"
            found = _find_unresolved_marker(nested, path=nested_path)
            if found:
                return found
        return None
    return None

def _to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _extract_actual_output(tool: str, result: Dict[str, Any]) -> Decimal | None:
    if not isinstance(result, dict):
        return None
    details = result.get("details")
    if tool == "bridge":
        for candidate in (
            result.get("output_amount"),
            details.get("output_amount") if isinstance(details, dict) else None,
        ):
            amount = _to_decimal(candidate)
            if amount is not None:
                return amount
        return None

    for candidate in (
        result.get("amount_out"),
        result.get("output_amount"),
        result.get("amount_out_minimum"),
        details.get("amount_out") if isinstance(details, dict) else None,
        details.get("output_amount") if isinstance(details, dict) else None,
        details.get("amount_out_minimum") if isinstance(details, dict) else None,
    ):
        amount = _to_decimal(candidate)
        if amount is not None:
            return amount
    return None


def _to_base_units_non_negative(amount: Decimal, decimals: int) -> int:
    if decimals < 0:
        return 0
    try:
        value = Decimal(str(amount))
    except Exception:
        return 0
    if not value.is_finite() or value < 0:
        return 0
    scaled = value * (Decimal(10) ** int(decimals))
    try:
        return max(0, int(scaled))
    except Exception:
        return 0


def _normalize_confirmation_status(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {
        "success",
        "confirmed_success",
        "confirmed",
        "ok",
        "succeeded",
        "1",
    }:
        return _TX_CONFIRMATION_CONFIRMED_SUCCESS
    if raw in {
        "failed",
        "failure",
        "revert",
        "reverted",
        "confirmed_revert",
        "0",
    }:
        return _TX_CONFIRMATION_CONFIRMED_REVERT
    if raw in {"rpc_error", "provider_error"}:
        return _TX_CONFIRMATION_RPC_ERROR
    return _TX_CONFIRMATION_UNKNOWN


def _latest_human_message(
    messages: Sequence[BaseMessage] | None,
) -> HumanMessage | None:
    if not messages:
        return None
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            return message
    return None


@dataclass(frozen=True)
class ExecutionRuntimeDeps:
    task_history_registry_cls: Any
    task_registry_cls: Any
    idempotency_store_cls: Any
    reservation_service_getter: Callable[[], Awaitable[Any]]
    run_with_timing: Callable[
        [Any, Dict[str, Any]], Awaitable[Tuple[Union[Dict[str, Any], Exception], float]]
    ]
    run_blocking: Callable[..., Awaitable[Any]]
    tools_registry: Any
    normalize_output: Callable[[Any, Dict[str, Any]], Dict[str, Any] | None]
    swap_failure_message: Callable[[str | None, bool], str]
    bridge_failure_message: Callable[[str | None, bool], str]
    tx_receipt_status: Callable[[str, str], str]
    publish_event: Callable[[Dict[str, Any]], None]
    task_history_write_timeout_seconds: float
    native_marker: str
    publish_event_async: Callable[[Dict[str, Any]], Awaitable[bool]] | None = None


class ExecutionRuntime:
    def __init__(self, deps: ExecutionRuntimeDeps) -> None:
        self._deps = deps

    async def run(
        self,
        *,
        plan: Any,
        execution_state: ExecutionState,
        state: Mapping[str, Any],
    ) -> Dict[str, Any]:
        route_planner_meta = getattr(plan, "metadata", {}) or {}
        route_meta_enforced = route_meta_strictly_enforced(route_planner_meta)
        raw_quotes = state.get("fee_quotes") or []
        fee_map: Dict[str, FeeQuote] = {}
        for d in raw_quotes:
            try:
                q = FeeQuote.from_dict(d)
                fee_map[q.node_id] = q
            except Exception:
                pass

        all_messages = []
        reasoning_logs = []
        policy_payload = state.get("guardrail_policy")
        guardrail_policy = None
        invalid_guardrail_message = None
        if isinstance(policy_payload, dict):
            try:
                guardrail_policy = GuardrailPolicy(**policy_payload)
            except Exception as exc:
                invalid_guardrail_message = (
                    "I couldn't verify the security policy for this task, so I paused "
                    "before submitting anything. Reply 'retry' to try again."
                )
                reasoning_logs.append(
                    f"[SECURITY] Invalid guardrail policy payload: {exc}"
                )
        elif policy_payload is not None:
            invalid_guardrail_message = (
                "I couldn't verify the security policy for this task, so I paused "
                "before submitting anything. Reply 'retry' to try again."
            )
            reasoning_logs.append(
                "[SECURITY] Guardrail policy payload was not a dictionary."
            )
        guardrail = GuardrailService(guardrail_policy)
        ledger = get_ledger()
        resource_snapshots: Dict[str, Dict[str, Any]] = (
            state.get("resource_snapshots") or {}
        )
        reservation_requirements: Dict[str, list[Dict[str, Any]]] = (
            state.get("reservation_requirements") or {}
        )

        updated_execution_state = ExecutionState(node_states={})
        pending = list(state.get("pending_transactions", []))
        execution_id = state.get("execution_id") or "unknown"
        thread_id = (
            (state.get("context") or {}).get("thread_id")
            or (state.get("user_info") or {}).get("thread_id")
            or "unknown"
        )
        provider = str(state.get("provider") or "")
        provider_user_id = str(state.get("user_id") or "")
        user_info = state.get("user_info") or {}
        user_id = (
            user_info.get("volo_user_id")
            if isinstance(user_info, dict) and user_info.get("volo_user_id")
            else state.get("user_id") or "unknown"
        )
        conversation_id = resolve_conversation_id(
            provider=provider,
            provider_user_id=provider_user_id,
            context=state.get("context"),
        )
        history_registry = None
        history_registry_loaded = False
        task_registry = None
        task_registry_loaded = False
        idempotency_store = None
        idempotency_store_loaded = False
        reservation_service = None
        reservation_service_loaded = False
        idempotency_keys: Dict[str, str] = {}
        active_reservations: Dict[str, str] = {}
        event_tasks: set[asyncio.Task[Any]] = set()
        side_effect_tasks: set[asyncio.Future[Any]] = set()
        waiting_for_funds: Dict[str, Any] | None = None

        def _get_history_registry() -> Any | None:
            nonlocal history_registry, history_registry_loaded
            if history_registry_loaded:
                return history_registry
            history_registry_loaded = True
            try:
                history_registry = self._deps.task_history_registry_cls()
            except Exception:
                history_registry = None
            return history_registry

        def _get_task_registry() -> Any | None:
            nonlocal task_registry, task_registry_loaded
            if task_registry_loaded:
                return task_registry
            task_registry_loaded = True
            try:
                task_registry = self._deps.task_registry_cls()
            except Exception:
                task_registry = None
            return task_registry

        def _get_idempotency_store() -> Any | None:
            nonlocal idempotency_store, idempotency_store_loaded
            if idempotency_store_loaded:
                return idempotency_store
            idempotency_store_loaded = True
            try:
                idempotency_store = self._deps.idempotency_store_cls()
            except Exception:
                idempotency_store = None
            return idempotency_store

        async def _get_reservation_service() -> Any | None:
            nonlocal reservation_service, reservation_service_loaded
            if reservation_service_loaded:
                return reservation_service
            reservation_service_loaded = True
            try:
                reservation_service = await self._deps.reservation_service_getter()
            except Exception:
                reservation_service = None
            return reservation_service

        async def _require_reservation_service() -> Any:
            service = await _get_reservation_service()
            if service is None:
                reason = (
                    "Global reservation service unavailable; aborting spend execution "
                    "to avoid cross-instance overspending."
                )
                _LOGGER.error(
                    "reservation_service_unavailable execution_id=%s thread_id=%s",
                    execution_id,
                    thread_id,
                )
                raise RuntimeError(reason)
            return service

        def _current_client_dedup_fields() -> Dict[str, str | None]:
            context_payload = state.get("context")
            context_map = context_payload if isinstance(context_payload, dict) else {}
            latest_message = _latest_human_message(state.get("messages"))
            return extract_client_dedup_fields(latest_message, context=context_map)

        def _dedup_intent_payload() -> Any:
            intents_payload = state.get("intents")
            if intents_payload:
                return intents_payload
            goal_parameters = state.get("goal_parameters")
            if goal_parameters:
                return goal_parameters
            return {
                node_id: {
                    "tool": node.tool,
                    "args": node.args,
                }
                for node_id, node in getattr(plan, "nodes", {}).items()
            }

        def _candidate_rpc_urls(chain_name: str) -> list[str]:
            urls: list[str] = []
            try:
                chain_cfg = get_chain_by_name(str(chain_name))
            except Exception:
                return urls

            if str(chain_cfg.rpc_url or "").strip():
                urls.append(str(chain_cfg.rpc_url).strip())
            env_name = RPC_ENV_VARS.get(chain_cfg.chain_id)
            if env_name:
                for suffix in ("_SECONDARY", "_BACKUP", "_FALLBACK"):
                    candidate = os.getenv(f"{env_name}{suffix}", "").strip()
                    if candidate and candidate not in urls:
                        urls.append(candidate)
            return urls

        async def _probe_evm_receipt_status(
            rpc_url: str,
            tx_hash: str,
        ) -> str:
            try:
                w3 = make_async_web3(rpc_url)
                receipt = await w3.eth.get_transaction_receipt(str(tx_hash))
            except Exception:
                return _TX_CONFIRMATION_RPC_ERROR
            if receipt is None:
                return _TX_CONFIRMATION_UNKNOWN
            status = getattr(receipt, "status", None)
            if status == 1:
                return _TX_CONFIRMATION_CONFIRMED_SUCCESS
            if status == 0:
                return _TX_CONFIRMATION_CONFIRMED_REVERT
            return _TX_CONFIRMATION_UNKNOWN

        async def _assess_tx_confirmation(
            *,
            chain_name: str,
            tx_hash: str,
        ) -> tuple[str, bool, list[str]]:
            evidence: list[str] = []
            try:
                primary_raw = await self._deps.run_blocking(
                    self._deps.tx_receipt_status,
                    str(chain_name),
                    str(tx_hash),
                )
            except Exception:
                primary_raw = "rpc_error"
            primary_status = _normalize_confirmation_status(primary_raw)
            evidence.append(f"primary:{primary_status}")
            provider_statuses: list[str] = []
            rpc_urls = _candidate_rpc_urls(chain_name)
            if rpc_urls:
                provider_statuses = await asyncio.gather(
                    *[_probe_evm_receipt_status(url, tx_hash) for url in rpc_urls],
                    return_exceptions=False,
                )
                evidence.extend(
                    f"rpc[{idx}]:{status}"
                    for idx, status in enumerate(provider_statuses)
                )

            success_votes = sum(
                1
                for status in [primary_status, *provider_statuses]
                if status == _TX_CONFIRMATION_CONFIRMED_SUCCESS
            )
            revert_votes = sum(
                1
                for status in [primary_status, *provider_statuses]
                if status == _TX_CONFIRMATION_CONFIRMED_REVERT
            )
            rpc_error_votes = sum(
                1
                for status in [primary_status, *provider_statuses]
                if status == _TX_CONFIRMATION_RPC_ERROR
            )

            if success_votes > 0:
                return _TX_CONFIRMATION_CONFIRMED_SUCCESS, True, evidence
            if revert_votes >= 2:
                # Require corroboration before treating a revert as final. A
                # single provider saying "failed" is not enough to safely retry.
                return _TX_CONFIRMATION_CONFIRMED_REVERT, True, evidence
            if revert_votes == 1:
                return _TX_CONFIRMATION_UNKNOWN, False, evidence
            if rpc_error_votes and not provider_statuses:
                return _TX_CONFIRMATION_RPC_ERROR, False, evidence
            return _TX_CONFIRMATION_UNKNOWN, False, evidence

        async def _fetch_live_balance_base_units(
            requirement: Dict[str, Any],
        ) -> int | None:
            sender = str(requirement.get("sender") or "").strip()
            chain_name = str(requirement.get("chain") or "").strip()
            token_ref = str(requirement.get("token_ref") or "").strip()
            decimals = int(requirement.get("decimals") or 0)
            if not sender or not chain_name or not token_ref:
                return None

            try:
                chain_cfg = get_chain_by_name(chain_name)
                is_native = token_ref.lower() == str(self._deps.native_marker).lower()
                if is_native:
                    amount = await get_native_balance_async(sender, chain_cfg.rpc_url)
                else:
                    amount = await get_token_balance_async(
                        sender,
                        token_ref,
                        decimals,
                        chain_cfg.rpc_url,
                    )
                return _to_base_units_non_negative(Decimal(str(amount)), decimals)
            except Exception:
                pass

            try:
                solana_chain = get_solana_chain(chain_name)
                is_native = token_ref.lower() == str(solana_chain.native_mint).lower()
                if is_native:
                    amount = await get_native_balance_solana_async(
                        sender,
                        network=solana_chain.network,
                    )
                else:
                    amount = await get_token_balance_solana_async(
                        sender,
                        token_ref,
                        network=solana_chain.network,
                    )
                return _to_base_units_non_negative(Decimal(str(amount)), decimals)
            except Exception:
                return None

        async def _jit_validate_balances(
            *,
            node_id: str,
            resolved_args: Dict[str, Any],
        ) -> str | None:
            raw_requirements = reservation_requirements.get(node_id) or []
            if not isinstance(raw_requirements, list) or not raw_requirements:
                return None

            service = await _require_reservation_service()
            wallet_scopes = {
                str(item.get("wallet_scope") or "").strip().lower()
                for item in raw_requirements
                if isinstance(item, dict)
            }
            wallet_scopes.discard("")
            if len(wallet_scopes) != 1:
                return "Just-in-time balance validation requires a single wallet scope."
            wallet_scope = next(iter(wallet_scopes))
            resource_keys = [
                str(item.get("resource_key") or "").strip().lower()
                for item in raw_requirements
                if isinstance(item, dict)
                and str(item.get("resource_key") or "").strip()
            ]
            reserved_totals = await service.get_reserved_totals(
                wallet_scope=wallet_scope,
                resource_keys=resource_keys,
            )
            for requirement in raw_requirements:
                if not isinstance(requirement, dict):
                    return "Malformed reservation requirement blocked signing."
                resource_key = (
                    str(requirement.get("resource_key") or "").strip().lower()
                )
                required_base_units = max(
                    0,
                    int(requirement.get("required_base_units") or 0),
                )
                live_balance = await _fetch_live_balance_base_units(requirement)
                if live_balance is None:
                    return (
                        f"Could not refresh balance for {resource_key or 'required resource'} "
                        "immediately before signing."
                    )
                reserved_total = reserved_totals.get(resource_key)
                if reserved_total is None:
                    return (
                        f"Reservation totals missing for {resource_key or 'required resource'} "
                        "during just-in-time validation."
                    )
                reserved_base_units = max(required_base_units, int(reserved_total))
                if live_balance < reserved_base_units:
                    return (
                        f"Live balance dropped below reserved funds for {resource_key}: "
                        f"live={live_balance} reserved={reserved_base_units}."
                    )

            _LOGGER.info(
                "jit_balance_validation_passed node_id=%s execution_id=%s wallet_scope=%s",
                node_id,
                execution_id,
                wallet_scope,
            )
            return None

        async def _evaluate_reclaim_uncertainty(
            *,
            node: Any,
            resolved_args: Dict[str, Any],
            record: Any,
        ) -> tuple[bool, str]:
            record_metadata = (
                record.metadata if isinstance(record.metadata, dict) else {}
            )
            age_seconds = max(
                0.0,
                (datetime.now(timezone.utc) - record.created_at).total_seconds(),
            )
            if age_seconds < float(_IDEMPOTENCY_RECLAIM_COOLDOWN_SECONDS):
                return (
                    True,
                    (
                        "cooldown_active "
                        f"age_seconds={age_seconds:.1f} "
                        f"cooldown_seconds={_IDEMPOTENCY_RECLAIM_COOLDOWN_SECONDS}"
                    ),
                )

            sender = str(
                record_metadata.get("sender") or resolved_args.get("sender") or ""
            ).strip()
            chain_name = str(
                record_metadata.get("chain")
                or resolved_args.get("chain")
                or resolved_args.get("source_chain")
                or ""
            ).strip()
            if not sender or not chain_name:
                return True, "missing_sender_or_chain_for_reclaim_check"

            active_history = [
                tx
                for tx in pending
                if isinstance(tx, dict)
                and str(tx.get("sender") or "").strip().lower() == sender.lower()
                and str(tx.get("chain") or tx.get("source_chain") or "").strip().lower()
                == chain_name.lower()
                and str(tx.get("status") or "").strip().lower()
                in {"pending", "pending_on_chain", "running"}
            ]
            if any(not str(tx.get("tx_hash") or "").strip() for tx in active_history):
                return True, "recent_pending_history_without_tx_hash"

            rpc_urls = _candidate_rpc_urls(chain_name)
            if not rpc_urls:
                return True, "no_rpc_provider_for_reclaim_check"
            try:
                w3 = make_async_web3(rpc_urls[0])
                pending_nonce = int(
                    await w3.eth.get_transaction_count(sender, "pending")
                )
                latest_nonce = int(await w3.eth.get_transaction_count(sender, "latest"))
            except Exception as exc:
                return True, f"nonce_probe_failed:{type(exc).__name__}"

            if pending_nonce > latest_nonce:
                return (
                    True,
                    f"nonce_gap_detected pending_nonce={pending_nonce} latest_nonce={latest_nonce}",
                )

            _LOGGER.info(
                "idempotency_reclaim_cleared node_id=%s sender=%s chain=%s age_seconds=%.1f pending_nonce=%s latest_nonce=%s",
                node.id,
                sender,
                chain_name,
                age_seconds,
                pending_nonce,
                latest_nonce,
            )
            return False, "safe_to_reclaim"

        async def _publish_event_with_timeout(event_payload: Dict[str, Any]) -> None:
            if _side_effect_cooldown_active(_EVENT_PUBLISH_COOLDOWN_KEY):
                return
            apublish_event = self._deps.publish_event_async
            if callable(apublish_event):
                await _call_with_timeout(
                    apublish_event,
                    event_payload,
                    timeout=_EVENT_PUBLISH_TIMEOUT_SECONDS,
                    default=False,
                    cooldown_key=_EVENT_PUBLISH_COOLDOWN_KEY,
                    cooldown_seconds=_EVENT_PUBLISH_UNAVAILABLE_COOLDOWN_SECONDS,
                )
                return
            await _call_with_timeout(
                self._deps.publish_event,
                event_payload,
                timeout=_EVENT_PUBLISH_TIMEOUT_SECONDS,
                default=False,
                cooldown_key=_EVENT_PUBLISH_COOLDOWN_KEY,
                cooldown_seconds=_EVENT_PUBLISH_UNAVAILABLE_COOLDOWN_SECONDS,
                executor=_get_event_publish_executor(),
            )

        def _schedule_event_publish(event_payload: Dict[str, Any]) -> None:
            if _side_effect_cooldown_active(_EVENT_PUBLISH_COOLDOWN_KEY):
                return
            task = asyncio.create_task(_publish_event_with_timeout(event_payload))
            event_tasks.add(task)
            task.add_done_callback(event_tasks.discard)

        async def _drain_event_tasks() -> None:
            if not event_tasks:
                return
            pending_tasks = [task for task in event_tasks if not task.done()]
            if not pending_tasks:
                return
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending_tasks, return_exceptions=True),
                    timeout=_EVENT_DRAIN_TIMEOUT_SECONDS,
                )
            except Exception:
                for task in pending_tasks:
                    task.cancel()
                await asyncio.gather(*pending_tasks, return_exceptions=True)

        async def _drain_side_effect_tasks() -> None:
            if not side_effect_tasks:
                return
            pending_futures = [
                future for future in side_effect_tasks if not future.done()
            ]
            if not pending_futures:
                return
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending_futures, return_exceptions=True),
                    timeout=self._deps.task_history_write_timeout_seconds,
                )
            except Exception:
                for future in pending_futures:
                    future.cancel()
                await asyncio.gather(*pending_futures, return_exceptions=True)

        def _build_event_payload(
            *,
            event: str,
            node_id: str,
            tool: str | None,
            status: str,
            stage: str | None = None,
            summary: str | None = None,
            tx_hash: str | None = None,
            recovery_hint: str | None = None,
            error_category: str | None = None,
        ) -> Dict[str, Any]:
            payload: Dict[str, Any] = {
                "event": event,
                "thread_id": str(thread_id),
                "execution_id": str(execution_id),
                "node_id": str(node_id),
                "tool": str(tool or ""),
                "status": str(status),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            if stage:
                payload["stage"] = str(stage)
            if summary:
                payload["summary"] = str(summary)
            if tx_hash:
                payload["tx_hash"] = str(tx_hash)
            if recovery_hint:
                payload["recovery_hint"] = str(recovery_hint)
            if error_category:
                payload["error_category"] = str(error_category)
            return payload

        def _callable_is_async(func: Callable[..., Any]) -> bool:
            return bool(
                inspect.iscoroutinefunction(func)
                or inspect.iscoroutinefunction(getattr(func, "__call__", None))
            )

        async def _run_side_effect_io(
            func: Callable[[], Any],
            *,
            timeout: float,
            default: Any = None,
            cooldown_key: str | None = None,
            cooldown_seconds: float | None = None,
            executor: ThreadPoolExecutor | None = None,
        ) -> Any:
            loop = asyncio.get_running_loop()
            fut = loop.run_in_executor(executor or _get_side_effect_executor(), func)
            try:
                return await asyncio.wait_for(fut, timeout=timeout)
            except Exception:
                fut.cancel()
                if cooldown_key:
                    _trip_side_effect_cooldown(
                        cooldown_key,
                        seconds=(
                            cooldown_seconds
                            if cooldown_seconds is not None
                            else _TASK_REGISTRY_UNAVAILABLE_COOLDOWN_SECONDS
                        ),
                    )
                return default

        async def _call_with_timeout(
            func: Callable[..., Any],
            *args: Any,
            timeout: float,
            default: Any = None,
            cooldown_key: str | None = None,
            cooldown_seconds: float | None = None,
            executor: ThreadPoolExecutor | None = None,
            **kwargs: Any,
        ) -> Any:
            if not callable(func):
                return default
            if _callable_is_async(func):
                try:
                    return await asyncio.wait_for(
                        func(*args, **kwargs), timeout=timeout
                    )
                except Exception:
                    if cooldown_key:
                        _trip_side_effect_cooldown(
                            cooldown_key,
                            seconds=(
                                cooldown_seconds
                                if cooldown_seconds is not None
                                else _TASK_REGISTRY_UNAVAILABLE_COOLDOWN_SECONDS
                            ),
                        )
                    return default
            return await _run_side_effect_io(
                partial(func, *args, **kwargs),
                timeout=timeout,
                default=default,
                cooldown_key=cooldown_key,
                cooldown_seconds=cooldown_seconds,
                executor=executor,
            )

        def _schedule_async_side_effect_task(
            awaitable_factory: Callable[[], Awaitable[Any]],
            *,
            timeout: float,
            cooldown_key: str | None = None,
        ) -> None:
            async def _await_runner() -> None:
                try:
                    await asyncio.wait_for(awaitable_factory(), timeout=timeout)
                except BaseException:
                    if cooldown_key:
                        _trip_side_effect_cooldown(
                            cooldown_key,
                            seconds=_TASK_REGISTRY_UNAVAILABLE_COOLDOWN_SECONDS,
                        )
                    return

            task = asyncio.create_task(_await_runner())
            side_effect_tasks.add(task)
            task.add_done_callback(side_effect_tasks.discard)

        def _schedule_sync_side_effect(
            call_factory: Callable[[], Any],
            *,
            cooldown_key: str | None = None,
        ) -> None:
            def _runner_sync() -> None:
                try:
                    call_factory()
                except Exception:
                    if cooldown_key:
                        _trip_side_effect_cooldown(
                            cooldown_key,
                            seconds=_TASK_REGISTRY_UNAVAILABLE_COOLDOWN_SECONDS,
                        )
                    return

            loop = asyncio.get_running_loop()
            future = loop.run_in_executor(_get_side_effect_executor(), _runner_sync)
            side_effect_tasks.add(future)
            future.add_done_callback(side_effect_tasks.discard)

        async def _record_execution_non_blocking(
            *,
            tool: str,
            chain: str,
            success: bool,
            execution_time: float = 0.0,
            error_msg: str | None = None,
            category: ErrorCategory = ErrorCategory.NONE,
        ) -> None:
            async_method = getattr(ledger, "arecord_execution", None)
            if callable(async_method):
                await _call_with_timeout(
                    async_method,
                    tool=tool,
                    chain=chain,
                    success=success,
                    execution_time=execution_time,
                    error_msg=error_msg,
                    category=category,
                    timeout=_LEDGER_IO_TIMEOUT_SECONDS,
                )
                return
            sync_method = getattr(ledger, "record_execution", None)
            if callable(sync_method):
                try:
                    sync_method(
                        tool,
                        chain,
                        success,
                        execution_time,
                        error_msg,
                        category,
                    )
                except Exception:
                    pass

        async def _record_fee_non_blocking(
            *,
            tool: str,
            chain: str,
            fee_amount_native: Decimal,
        ) -> None:
            async_method = getattr(ledger, "arecord_fee", None)
            if callable(async_method):
                await _call_with_timeout(
                    async_method,
                    tool=tool,
                    chain=chain,
                    fee_amount_native=fee_amount_native,
                    timeout=_LEDGER_IO_TIMEOUT_SECONDS,
                )
                return
            sync_method = getattr(ledger, "record_fee", None)
            if callable(sync_method):
                try:
                    sync_method(
                        tool=tool,
                        chain=chain,
                        fee_amount_native=fee_amount_native,
                    )
                except Exception:
                    pass

        async def _idempotency_claim_non_blocking(
            *,
            key: str,
            metadata: Dict[str, Any],
        ) -> tuple[Any, bool] | None:
            store = _get_idempotency_store()
            if store is None:
                return None
            async_method = getattr(store, "aclaim", None)
            if callable(async_method):
                return await _call_with_timeout(
                    async_method,
                    key=key,
                    metadata=metadata,
                    timeout=_IDEMPOTENCY_IO_TIMEOUT_SECONDS,
                    default=None,
                )
            sync_method = getattr(store, "claim", None)
            if callable(sync_method):
                try:
                    return cast(
                        tuple[Any, bool] | None,
                        sync_method(key=key, metadata=metadata),
                    )
                except Exception:
                    return None
            return None

        async def _idempotency_mark_success_non_blocking(
            *,
            key: str,
            result: Dict[str, Any],
        ) -> None:
            store = _get_idempotency_store()
            if store is None:
                return
            async_method = getattr(store, "amark_success", None)
            if callable(async_method):
                await _call_with_timeout(
                    async_method,
                    key=key,
                    result=result,
                    timeout=_IDEMPOTENCY_IO_TIMEOUT_SECONDS,
                )
                return
            sync_method = getattr(store, "mark_success", None)
            if callable(sync_method):
                try:
                    sync_method(key=key, result=result)
                except Exception:
                    pass

        async def _idempotency_mark_failed_non_blocking(
            *,
            key: str,
            error: str,
        ) -> None:
            store = _get_idempotency_store()
            if store is None:
                return
            async_method = getattr(store, "amark_failed", None)
            if callable(async_method):
                await _call_with_timeout(
                    async_method,
                    key=key,
                    error=error,
                    timeout=_IDEMPOTENCY_IO_TIMEOUT_SECONDS,
                )
                return
            sync_method = getattr(store, "mark_failed", None)
            if callable(sync_method):
                try:
                    sync_method(key=key, error=error)
                except Exception:
                    pass

        async def _idempotency_mark_inflight_non_blocking(
            *,
            key: str,
            tx_hash: str,
        ) -> None:
            store = _get_idempotency_store()
            if store is None:
                return
            async_method = getattr(store, "amark_inflight", None)
            if callable(async_method):
                await _call_with_timeout(
                    async_method,
                    key=key,
                    tx_hash=tx_hash,
                    timeout=_IDEMPOTENCY_IO_TIMEOUT_SECONDS,
                )
                return
            sync_method = getattr(store, "mark_inflight", None)
            if callable(sync_method):
                try:
                    sync_method(key=key, tx_hash=tx_hash)
                except Exception:
                    pass

        async def _emit_node_events(delta: ExecutionState) -> None:
            if not delta or not delta.node_states:
                return
            for node_id, node_state in delta.node_states.items():
                status = node_state.status
                if status not in {StepStatus.SUCCESS, StepStatus.FAILED}:
                    continue
                node = plan.nodes.get(node_id)
                tool = node.tool if node else None
                summary = None
                if node and node.args:
                    summary = summarize_task(tool, node.args)
                result = node_state.result or {}
                if isinstance(result, dict) and result.get("message"):
                    summary = result.get("message")
                status_value = status.value if hasattr(status, "value") else str(status)
                recovery_hint = (
                    node_state.user_message if status == StepStatus.FAILED else None
                )
                error_category = (
                    node_state.error_category if status == StepStatus.FAILED else None
                )
                _schedule_event_publish(
                    _build_event_payload(
                        event=(
                            "node_completed"
                            if status == StepStatus.SUCCESS
                            else "node_failed"
                        ),
                        node_id=node_id,
                        tool=tool,
                        status=str(status_value).upper(),
                        summary=summary,
                        stage=None,
                        tx_hash=(
                            result.get("tx_hash") if isinstance(result, dict) else None
                        ),
                        recovery_hint=recovery_hint,
                        error_category=error_category,
                    )
                )

        def _derive_task_status(current_state: ExecutionState) -> str:
            if waiting_for_funds:
                return "WAITING_FUNDS"
            statuses = [
                node_state.status
                for node_state in current_state.node_states.values()
                if node_state.status is not None
            ]
            if any(status == StepStatus.FAILED for status in statuses):
                return "FAILED"
            if current_state.completed or check_plan_complete(plan, current_state):
                return "COMPLETED"
            if any(status == StepStatus.RUNNING for status in statuses):
                return "WAITING_EXTERNAL"
            return "RUNNING"

        def _task_latest_summary(current_state: ExecutionState) -> str:
            if waiting_for_funds and waiting_for_funds.get("message"):
                return str(waiting_for_funds.get("message"))
            failed_states = [
                node_state
                for node_state in current_state.node_states.values()
                if node_state.status == StepStatus.FAILED
            ]
            if failed_states:
                latest_failed = failed_states[-1]
                return str(
                    latest_failed.user_message or latest_failed.error or plan.goal
                )
            if any(
                node_state.status == StepStatus.RUNNING
                for node_state in current_state.node_states.values()
            ):
                return "Waiting for the network."
            if all_messages:
                return str(all_messages[-1])
            return str(plan.goal)

        async def _upsert_task_non_blocking(
            *,
            status: str,
            latest_summary: str | None = None,
            tool: str | None = None,
            tx_hash: str | None = None,
            error_category: str | None = None,
        ) -> None:
            if not conversation_id:
                return
            if _side_effect_cooldown_active(_TASK_REGISTRY_COOLDOWN_KEY):
                return
            registry = _get_task_registry()
            if registry is None:
                return
            upsert_kwargs = {
                "conversation_id": str(conversation_id),
                "execution_id": str(execution_id),
                "thread_id": str(thread_id),
                "provider": provider,
                "provider_user_id": provider_user_id,
                "user_id": str(user_id),
                "title": str(getattr(plan, "goal", "") or "Task"),
                "status": status,
                "latest_summary": latest_summary,
                "tool": tool,
                "tx_hash": tx_hash,
                "error_category": error_category,
            }
            upsert_callable = getattr(registry, "upsert_execution_task", None)
            if not callable(upsert_callable):
                return
            if _callable_is_async(upsert_callable):

                async def _upsert_and_cleanup() -> None:
                    record_obj = await cast(
                        Awaitable[Any], upsert_callable(**upsert_kwargs)
                    )
                    record = record_obj if isinstance(record_obj, dict) else None
                    schedule_terminal_task_cleanup(
                        task_record=record,
                        task_registry_cls=self._deps.task_registry_cls,
                    )

                _schedule_async_side_effect_task(
                    _upsert_and_cleanup,
                    timeout=self._deps.task_history_write_timeout_seconds,
                    cooldown_key=_TASK_REGISTRY_COOLDOWN_KEY,
                )
            else:
                loop = asyncio.get_running_loop()

                def _sync_upsert_and_cleanup() -> None:
                    record_obj = upsert_callable(**upsert_kwargs)
                    record = record_obj if isinstance(record_obj, dict) else None
                    loop.call_soon_threadsafe(
                        lambda: schedule_terminal_task_cleanup(
                            task_record=record,
                            task_registry_cls=self._deps.task_registry_cls,
                        )
                    )

                _schedule_sync_side_effect(
                    _sync_upsert_and_cleanup,
                    cooldown_key=_TASK_REGISTRY_COOLDOWN_KEY,
                )

        def _idempotency_scope() -> str:
            trigger_id = state.get("trigger_id")
            if trigger_id:
                fire_id = state.get("trigger_fire_id") or "once"
                return f"trigger:{trigger_id}:{fire_id}"
            dedup_fields = _current_client_dedup_fields()
            dedup_key = compute_execution_dedup_key(
                intent_payload=_dedup_intent_payload(),
                client_message_id=dedup_fields.get("client_message_id"),
                client_nonce=dedup_fields.get("client_nonce"),
            )
            if dedup_key:
                _LOGGER.info(
                    "execution_dedup_scope scope=message execution_id=%s client_message_id=%s client_nonce=%s",
                    execution_id,
                    dedup_fields.get("client_message_id"),
                    bool(dedup_fields.get("client_nonce")),
                )
                return f"message:{dedup_key}"
            current_execution_id = state.get("execution_id")
            if current_execution_id:
                return str(current_execution_id)
            local_user_id = state.get("user_id") or "unknown"
            return f"user:{local_user_id}"

        idempotency_scope = _idempotency_scope()
        first_tool = (
            next(iter(plan.nodes.values())).tool
            if getattr(plan, "nodes", None)
            else None
        )
        await _upsert_task_non_blocking(
            status="RUNNING",
            latest_summary=str(getattr(plan, "goal", "") or "Task started."),
            tool=first_tool,
        )

        def _mark_node_running(node_id: str) -> None:
            updated_execution_state.node_states[node_id] = NodeState(
                node_id=node_id, status=StepStatus.RUNNING
            )

        def _mark_node_success(node_id: str, result: Dict[str, Any]) -> None:
            updated_execution_state.node_states[node_id] = NodeState(
                node_id=node_id,
                status=StepStatus.SUCCESS,
                result=result,
            )

        def _mark_node_failure(
            node_id: str,
            error: str,
            *,
            retries: int = 0,
            category: str | None = None,
            user_message: str | None = None,
            mutated_args: Dict[str, Any] | None = None,
        ) -> None:
            updated_execution_state.node_states[node_id] = NodeState(
                node_id=node_id,
                status=StepStatus.FAILED,
                error=error,
                retries=retries,
                error_category=category,
                user_message=user_message,
                mutated_args=mutated_args,
            )

        def _mark_node_pending_preserving_state(
            node_id: str,
            current_state: ExecutionState,
        ) -> None:
            existing_node_state = current_state.node_states.get(node_id)
            updated_execution_state.node_states[node_id] = NodeState(
                node_id=node_id,
                status=StepStatus.PENDING,
                retries=existing_node_state.retries if existing_node_state else 0,
                mutated_args=(
                    existing_node_state.mutated_args if existing_node_state else None
                ),
            )

        def _set_waiting_for_funds(
            *,
            node_id: str,
            tool: str,
            message: str,
            deferred_reason: str,
            wait_id: str | None = None,
            resume_token: str | None = None,
            wallet_scope: str | None = None,
        ) -> None:
            nonlocal waiting_for_funds
            waiting_for_funds = {
                "wait_id": wait_id,
                "resume_token": resume_token,
                "wallet_scope": wallet_scope,
                "node_id": node_id,
                "execution_id": str(execution_id),
                "thread_id": str(thread_id),
                "tool": tool,
                "message": message,
                "deferred_reason": deferred_reason,
            }

        async def _release_reservation(
            node_id: str,
            *,
            status: str,
            reason: str | None = None,
            tx_hash: str | None = None,
        ) -> None:
            reservation_id = active_reservations.pop(str(node_id), None)
            if not reservation_id:
                return
            service = await _get_reservation_service()
            if service is None:
                return
            try:
                await service.release(
                    reservation_id,
                    status=status,
                    reason=reason,
                    tx_hash=tx_hash,
                )
            except Exception:
                pass

        async def _mark_reservation_broadcast(
            node_id: str,
            *,
            tx_hash: str | None = None,
        ) -> None:
            reservation_id = active_reservations.get(str(node_id))
            if not reservation_id:
                return
            service = await _get_reservation_service()
            if service is None:
                return
            try:
                await service.mark_broadcast(reservation_id, tx_hash=tx_hash)
            except Exception:
                pass

        def _should_request_fix(category: ErrorCategory) -> bool:
            # Only request tool-level fix suggestions for categories where
            # mutation strategies are expected to help quickly.
            return category in {
                ErrorCategory.LIQUIDITY,
                ErrorCategory.SLIPPAGE,
                ErrorCategory.GAS,
            }

        while True:
            combined_state = execution_state.merge(updated_execution_state)
            ready_nodes = get_ready_nodes(plan, combined_state)

            if not ready_nodes:
                if check_plan_complete(plan, combined_state):
                    updated_execution_state.completed = True
                break

            if invalid_guardrail_message:
                blocked_nodes = [
                    node for node in ready_nodes if not node.approval_required
                ]
                if blocked_nodes:
                    for node in blocked_nodes:
                        _mark_node_failure(
                            node.id,
                            "Invalid guardrail policy payload.",
                            category=ErrorCategory.SECURITY.value,
                            user_message=invalid_guardrail_message,
                        )
                    all_messages.append(invalid_guardrail_message)
                break

            nodes_to_run = []
            for node in ready_nodes:
                if node.approval_required:
                    continue

                node_state = combined_state.node_states.get(node.id)
                base_args = (
                    node_state.mutated_args
                    if node_state and node_state.mutated_args
                    else node.args
                )

                resolved_args = resolve_dynamic_args(
                    base_args, combined_state, context=state.get("artifacts")
                )
                unresolved_marker_path = _find_unresolved_marker(resolved_args)
                if unresolved_marker_path:
                    err_msg = f"Unresolved execution marker remained in {unresolved_marker_path}."
                    user_message = (
                        "I couldn't safely execute this step because some required values "
                        "were still unresolved. Reply with: retry, edit, cancel"
                    )
                    _mark_node_failure(
                        node.id,
                        err_msg,
                        category=ErrorCategory.LOGIC.value,
                        user_message=user_message,
                    )
                    all_messages.append(user_message)
                    reasoning_logs.append(f"[ERROR] {node.id}: {err_msg}")
                    await _release_reservation(
                        node.id,
                        status="failed",
                        reason="unresolved_marker",
                    )
                    await _record_execution_non_blocking(
                        tool=node.tool,
                        chain=str(
                            resolved_args.get("chain")
                            or resolved_args.get("source_chain")
                            or "unknown"
                        ),
                        success=False,
                        error_msg=err_msg,
                        category=ErrorCategory.LOGIC,
                    )
                    continue
                if guardrail.policy is None:
                    nodes_to_run.append((node, resolved_args))
                else:
                    node_to_validate = node.model_copy(update={"args": resolved_args})
                    try:
                        guardrail.validate_node(node_to_validate, combined_state)
                        nodes_to_run.append((node, resolved_args))
                    except RiskViolationError as e:
                        err_msg = str(e)
                        _mark_node_failure(
                            node.id,
                            err_msg,
                            category=ErrorCategory.SECURITY.value,
                        )
                        all_messages.append(f"Security Alert: {err_msg}")
                        reasoning_logs.append(f"[RISK] {err_msg}")
                        chain = node.args.get("chain") or "unknown"
                        await _record_execution_non_blocking(
                            tool=node.tool,
                            chain=chain,
                            success=False,
                            error_msg=err_msg,
                            category=ErrorCategory.SECURITY,
                        )

            if not nodes_to_run:
                break

            async def _prepare_node(node, resolved_args):
                use_idempotency = (
                    node.tool != "check_balance"
                    and _get_idempotency_store() is not None
                )
                idem_key = None
                if use_idempotency and idempotency_scope is not None:
                    try:
                        args_hash = compute_args_hash(resolved_args)
                    except Exception:
                        args_hash = compute_args_hash(node.args)

                    idem_key = compute_idempotency_key(
                        scope_id=idempotency_scope,
                        node_id=node.id,
                        tool=node.tool,
                        args_hash=args_hash,
                    )

                    route_meta = node.metadata.get("route") if node.metadata else None
                    protocol_hint = None
                    if isinstance(route_meta, dict):
                        protocol_hint = route_meta.get("aggregator")
                    claim_result = await _idempotency_claim_non_blocking(
                        key=idem_key,
                        metadata={
                            "scope_id": idempotency_scope,
                            "node_id": node.id,
                            "tool": node.tool,
                            "args_hash": args_hash,
                            "sender": resolved_args.get("sender"),
                            "chain": resolved_args.get("chain")
                            or resolved_args.get("source_chain"),
                            "protocol": protocol_hint,
                        },
                    )
                    if claim_result is None:
                        reasoning_logs.append(
                            f"[IDEMPOTENT] Store unavailable for {node.id}; "
                            "continuing without idempotency guard."
                        )
                    else:
                        record, created = claim_result

                        if not created:
                            if record.status == "success" and record.result:
                                return node.id, "REUSE", record.result

                            if record.status == "pending":
                                tx_hash = record.tx_hash
                                record_metadata = (
                                    record.metadata
                                    if isinstance(record.metadata, dict)
                                    else {}
                                )
                                record_scope_id = record_metadata.get("scope_id")
                                reclaim_current_execution = False
                                chain_name = record_metadata.get("chain")
                                if not chain_name:
                                    chain_name = resolved_args.get(
                                        "chain"
                                    ) or resolved_args.get("source_chain")

                                if not tx_hash and (
                                    record_scope_id is None
                                    or str(record_scope_id) == str(idempotency_scope)
                                ):
                                    (
                                        uncertain,
                                        uncertainty_reason,
                                    ) = await _evaluate_reclaim_uncertainty(
                                        node=node,
                                        resolved_args=resolved_args,
                                        record=record,
                                    )
                                    if uncertain:
                                        wait_reason = (
                                            f"Idempotency guard: uncertain broadcast state for {node.id}; "
                                            f"not reclaiming pending execution ({uncertainty_reason})."
                                        )
                                        _LOGGER.warning(
                                            "idempotency_reclaim_blocked node_id=%s execution_id=%s reason=%s",
                                            node.id,
                                            execution_id,
                                            uncertainty_reason,
                                        )
                                        return node.id, "WAITING_PENDING", wait_reason
                                    reasoning_logs.append(
                                        f"[IDEMPOTENT] Pending record for {node.id} had no tx hash after cooldown; reclaiming current execution."
                                    )
                                    reclaim_current_execution = True
                                else:
                                    if (
                                        tx_hash
                                        and chain_name
                                        and node.tool in {"swap", "transfer", "unwrap"}
                                    ):
                                        (
                                            confirmation,
                                            high_confidence,
                                            evidence,
                                        ) = await _assess_tx_confirmation(
                                            chain_name=str(chain_name),
                                            tx_hash=str(tx_hash),
                                        )
                                        if (
                                            confirmation
                                            == _TX_CONFIRMATION_CONFIRMED_SUCCESS
                                        ):
                                            result = {
                                                "status": "success",
                                                "tx_hash": tx_hash,
                                                "chain": chain_name,
                                                "message": (
                                                    f"Step {node.id} already confirmed on-chain "
                                                    f"(tx {tx_hash})."
                                                ),
                                            }
                                            return (
                                                node.id,
                                                "REUSE_PENDING_SUCCESS",
                                                (result, idem_key),
                                            )
                                        if (
                                            confirmation
                                            == _TX_CONFIRMATION_CONFIRMED_REVERT
                                            and high_confidence
                                        ):
                                            err_msg = (
                                                "Idempotency guard: pending tx "
                                                f"{tx_hash} reverted."
                                            )
                                            return (
                                                node.id,
                                                "FAILURE_PENDING_FAILED",
                                                (err_msg, idem_key),
                                            )
                                        wait_reason = (
                                            f"Idempotency guard: tx {tx_hash} has "
                                            f"{confirmation.lower()} receipt status; "
                                            "not retrying while confirmation is uncertain."
                                        )
                                        _LOGGER.warning(
                                            "idempotency_confirmation_uncertain node_id=%s tx_hash=%s evidence=%s",
                                            node.id,
                                            tx_hash,
                                            evidence,
                                        )
                                        return node.id, "WAITING_PENDING", wait_reason

                                    if tx_hash and node.tool == "bridge":
                                        bridge_protocol = "across"
                                        if isinstance(record.metadata, dict):
                                            bridge_protocol = (
                                                record.metadata.get("protocol")
                                                or record.metadata.get(
                                                    "bridge_protocol"
                                                )
                                                or "across"
                                            )

                                        is_testnet = False
                                        try:
                                            chain_cfg = get_chain_by_name(
                                                str(chain_name)
                                            )
                                            is_testnet = bool(chain_cfg.is_testnet)
                                        except Exception:
                                            if isinstance(record.metadata, dict):
                                                is_testnet = bool(
                                                    record.metadata.get(
                                                        "is_testnet", False
                                                    )
                                                )

                                        bridge_meta = (
                                            record.metadata
                                            if isinstance(record.metadata, dict)
                                            else {}
                                        )

                                        status_result = await self._deps.run_blocking(
                                            partial(
                                                fetch_bridge_status,
                                                str(bridge_protocol),
                                                str(tx_hash),
                                                is_testnet=is_testnet,
                                                meta=bridge_meta,
                                            )
                                        )
                                        status_raw = status_result.raw_status
                                        status = status_result.normalized_status

                                        if status == "success":
                                            result = {
                                                "status": "success",
                                                "tx_hash": tx_hash,
                                                "chain": chain_name,
                                                "message": (
                                                    f"Step {node.id} bridge already finalized "
                                                    f"({bridge_protocol} status "
                                                    f"{status_raw or 'confirmed'})."
                                                ),
                                            }
                                            return (
                                                node.id,
                                                "REUSE_PENDING_SUCCESS",
                                                (result, idem_key),
                                            )
                                        if status == "failed":
                                            err_msg = (
                                                f"Idempotency guard: bridge tx {tx_hash} failed "
                                                f"({bridge_protocol} status {status_raw})."
                                            )
                                            return (
                                                node.id,
                                                "FAILURE_PENDING_FAILED",
                                                (err_msg, idem_key),
                                            )

                                    return (
                                        node.id,
                                        "WAITING_PENDING",
                                        (
                                            f"Idempotency guard: step {node.id} remains pending "
                                            "from a prior attempt; refusing uncertain retry."
                                        ),
                                    )

                                if not reclaim_current_execution:
                                    err_msg = (
                                        f"Idempotency guard: step {node.id} is {record.status} "
                                        "from a prior attempt. Skipping re-execution."
                                    )
                                    return node.id, "FAILURE_IDEMPOTENCY", err_msg

                            else:
                                err_msg = (
                                    f"Idempotency guard: step {node.id} is {record.status} "
                                    "from a prior attempt. Skipping re-execution."
                                )
                                return node.id, "FAILURE_IDEMPOTENCY", err_msg

                structured_requirements = []
                if isinstance(reservation_requirements, dict):
                    raw_requirements = reservation_requirements.get(node.id) or []
                    if isinstance(raw_requirements, list):
                        structured_requirements = raw_requirements

                if structured_requirements:
                    service = await _require_reservation_service()
                    claim_result = await service.claim(
                        execution_id=str(execution_id),
                        thread_id=str(thread_id),
                        node_id=node.id,
                        tool=node.tool,
                        requirements=structured_requirements,
                        resource_snapshots=resource_snapshots,
                        conversation_id=conversation_id,
                        task_number=state.get("selected_task_number"),
                        title=str(getattr(plan, "goal", "") or "Task"),
                    )
                    if claim_result.acquired:
                        return node.id, "CLAIMED", (claim_result, idem_key)
                    if not claim_result.store_unavailable:
                        return node.id, "DEFERRED", (claim_result, idem_key)
                    raise RuntimeError(
                        claim_result.deferred_reason
                        or "Reservation store unavailable; refusing local fallback."
                    )

                return node.id, "CLAIMED", (None, idem_key)

            try:
                preparations = await asyncio.gather(
                    *[
                        _prepare_node(node, resolved_args)
                        for node, resolved_args in nodes_to_run
                    ]
                )
            except RuntimeError as exc:
                err_msg = str(exc)
                user_message = (
                    "I stopped before sending anything because secure global fund "
                    "reservation was unavailable. Please retry once the service recovers."
                )
                _LOGGER.error(
                    "execution_aborted_preparation execution_id=%s thread_id=%s reason=%s",
                    execution_id,
                    thread_id,
                    err_msg,
                )
                for node, _resolved_args in nodes_to_run:
                    _mark_node_failure(
                        node.id,
                        err_msg,
                        category=ErrorCategory.SECURITY.value,
                        user_message=user_message,
                    )
                all_messages.append(user_message)
                reasoning_logs.append(f"[SECURITY] {err_msg}")
                break

            # Process preparation results sequentially to maintain internal consistency
            prep_results_by_node = {res[0]: res for res in preparations}

            scheduled: list[tuple[Any, Dict[str, Any]]] = []

            for node, resolved_args in nodes_to_run:
                prep = prep_results_by_node.get(node.id)
                if not prep:
                    continue

                status, data = prep[1], prep[2]

                if status == "REUSE":
                    _mark_node_success(node.id, data)
                    msg = data.get("message", f"Step {node.id} reused from cache")
                    all_messages.append(msg)
                    reasoning_logs.append(
                        f"[IDEMPOTENT] Reused cached result for {node.id}."
                    )
                    continue

                if status == "REUSE_PENDING_SUCCESS":
                    result, idem_key = data
                    _mark_node_success(node.id, result)
                    reasoning_logs.append(
                        f"[IDEMPOTENT] Confirmed pending tx for {node.id}."
                    )
                    all_messages.append(result["message"])
                    await _idempotency_mark_success_non_blocking(
                        key=idem_key, result=result
                    )
                    continue

                if status == "FAILURE_PENDING_FAILED":
                    err_msg, idem_key = data
                    _mark_node_failure(
                        node.id, err_msg, category=ErrorCategory.NON_RETRYABLE.value
                    )
                    reasoning_logs.append(f"[IDEMPOTENT] {err_msg}")
                    all_messages.append(err_msg)
                    await _idempotency_mark_failed_non_blocking(
                        key=idem_key, error=err_msg
                    )
                    continue

                if status == "WAITING_PENDING":
                    _mark_node_running(node.id)
                    msg = str(
                        data
                        or f"Step {node.id} is already pending from a prior attempt. Waiting for confirmation."
                    )
                    all_messages.append(msg)
                    reasoning_logs.append(f"[IDEMPOTENT] {msg}")
                    continue

                if status == "FAILURE_IDEMPOTENCY":
                    _mark_node_failure(
                        node.id, data, category=ErrorCategory.NON_RETRYABLE.value
                    )
                    all_messages.append(data)
                    reasoning_logs.append(f"[IDEMPOTENT] {data}")
                    continue

                idem_key = data[1] if status in {"CLAIMED", "DEFERRED"} else None
                if idem_key:
                    idempotency_keys[node.id] = idem_key

                if status == "CLAIMED":
                    claim_result = data[0]
                    if claim_result is not None and claim_result.reservation_id:
                        active_reservations[node.id] = claim_result.reservation_id
                        reasoning_logs.append(
                            f"[RESERVE:GLOBAL] Claimed funds for {node.id}."
                        )
                    elif reservation_requirements.get(node.id):
                        reasoning_logs.append(
                            f"[RESERVE:GLOBAL] Reservation already satisfied for {node.id}."
                        )
                    if (
                        waiting_for_funds
                        and waiting_for_funds.get("node_id") == node.id
                    ):
                        waiting_for_funds = None
                    scheduled.append((node, resolved_args))
                    continue

                if status == "DEFERRED":
                    claim_result = data[0]
                    service = await _require_reservation_service()
                    reason = (
                        claim_result.deferred_reason
                        or f"Global reservation denied for {node.id}."
                    )
                    reasoning_logs.append(f"[RESERVE:GLOBAL] {reason}")
                    wait_record = await service.enqueue_wait(
                        execution_id=str(execution_id),
                        thread_id=str(thread_id),
                        node_id=node.id,
                        tool=node.tool,
                        requirements=(reservation_requirements.get(node.id) or []),
                        resource_snapshots=resource_snapshots,
                        conversation_id=str(conversation_id or ""),
                        task_number=state.get("selected_task_number"),
                        title=str(getattr(plan, "goal", "") or "Task"),
                        deferred_reason=reason,
                        conflicts=claim_result.conflicts,
                    )
                    _mark_node_pending_preserving_state(node.id, combined_state)
                    wait_message = f"{reason} I will retry automatically when funds free up. Recovery path: wait, inspect the active task, or cancel it."
                    _set_waiting_for_funds(
                        node_id=node.id,
                        tool=node.tool,
                        message=wait_message,
                        deferred_reason=reason,
                        wait_id=(
                            wait_record.wait_id
                            if wait_record is not None
                            else claim_result.wait_id
                        ),
                        resume_token=(
                            wait_record.resume_token
                            if wait_record is not None
                            else None
                        ),
                        wallet_scope=(
                            wait_record.wallet_scope
                            if wait_record is not None
                            else None
                        ),
                    )
                    all_messages.append(wait_message)
                    continue

            if not scheduled:
                reasoning_logs.append(
                    "[RESERVE] No runnable nodes after reservation checks."
                )
                break

            tasks = []
            node_ids = []
            resolved_args_by_node_id: Dict[str, Dict[str, Any]] = {}
            canonical_route_meta_by_node_id: Dict[str, Any] = {}
            fallback_policy_by_node_id: Dict[str, Any] = {}

            for node, resolved_args in scheduled:
                tool_obj = self._deps.tools_registry.get(node.tool)
                if not tool_obj:
                    msg = f"Tool '{node.tool}' not found."
                    _mark_node_failure(node.id, msg)
                    all_messages.append(f"Error: {msg}")
                    reasoning_logs.append(f"[ERROR] {msg}")
                    await _release_reservation(
                        node.id,
                        status="failed",
                        reason="tool_not_found",
                    )
                    continue
                if tool_obj.args_schema:
                    validation_args = resolved_args.copy()
                    for k, v in validation_args.items():
                        if isinstance(v, str) and "{{" in v:
                            validation_args[k] = "0.0"
                    try:
                        tool_obj.args_schema(**validation_args)
                    except ValidationError as e:
                        msg = f"ValidationError: {str(e)}"
                        _mark_node_failure(node.id, msg)
                        all_messages.append(f"Error: {msg}")
                        reasoning_logs.append(f"[ERROR] {msg}")
                        await _release_reservation(
                            node.id,
                            status="failed",
                            reason="validation_error",
                        )
                        continue

                route_meta = node.metadata.get("route") if node.metadata else None
                route_validation = validate_route_meta(
                    tool=node.tool,
                    resolved_args=resolved_args,
                    route_meta=route_meta,
                    strict_missing=route_meta_enforced,
                )
                if not route_validation.valid:
                    payload = {
                        "event": "route_validation",
                        "node_id": node.id,
                        "tool": node.tool,
                        "valid": False,
                        "provider": None,
                        "token_in": None,
                        "token_out": None,
                        "amount_in": None,
                        "expected_output": None,
                        "min_output": None,
                        "error": route_validation.reason or "invalid route metadata",
                    }
                    _LOGGER.warning("route_validation %s", payload)
                    msg = (
                        f"Route validation failed for {node.id}: "
                        f"{route_validation.reason or 'invalid route metadata'}."
                    )
                    _mark_node_failure(
                        node.id,
                        msg,
                        category=ErrorCategory.NON_RETRYABLE.value,
                    )
                    reasoning_logs.append(f"[ROUTE] {node.id}: {msg}")
                    all_messages.append(msg)
                    await _release_reservation(
                        node.id,
                        status="failed",
                        reason="route_validation_failed",
                    )
                    continue

                canonical_route_meta = None
                if route_validation.should_use_route_meta and isinstance(
                    route_meta, dict
                ):
                    try:
                        validate_route_meta(route_meta)
                        canonical_route_meta = canonicalize_route_meta(
                            route_meta,
                            tool=node.tool,
                        )
                    except Exception as exc:
                        payload = log_route_validation(
                            route_meta=None,
                            valid=False,
                            error=str(exc),
                            tool=node.tool,
                        )
                        payload["node_id"] = node.id
                        _LOGGER.warning("route_validation %s", payload)
                        msg = f"Route validation failed for {node.id}: {exc}."
                        _mark_node_failure(
                            node.id,
                            msg,
                            category=ErrorCategory.NON_RETRYABLE.value,
                        )
                        reasoning_logs.append(f"[ROUTE] {node.id}: {msg}")
                        all_messages.append(msg)
                        await _release_reservation(
                            node.id,
                            status="failed",
                            reason="route_validation_failed",
                        )
                        continue
                    payload = log_route_validation(
                        route_meta=canonical_route_meta,
                        valid=True,
                        tool=node.tool,
                    )
                    payload["node_id"] = node.id
                    _LOGGER.info("route_validation %s", payload)
                elif route_meta_required(node.tool):
                    payload = {
                        "event": "route_validation",
                        "node_id": node.id,
                        "tool": node.tool,
                        "valid": True,
                        "provider": None,
                        "token_in": None,
                        "token_out": None,
                        "amount_in": None,
                        "expected_output": None,
                        "min_output": None,
                        "error": None,
                    }
                    _LOGGER.info("route_validation %s", payload)

                execution_args = resolved_args
                route_meta_usable = (
                    route_validation.should_use_route_meta
                    and canonical_route_meta is not None
                )
                fallback_policy = (
                    canonical_route_meta.fallback_policy
                    if canonical_route_meta is not None
                    else route_validation.fallback_policy
                )
                if canonical_route_meta is not None:
                    now = int(time.time())
                    expiry_payload = log_route_expiry(
                        route_meta=canonical_route_meta,
                        now=now,
                    )
                    expiry_payload["node_id"] = node.id
                    if is_route_expired(canonical_route_meta, now):
                        _LOGGER.warning("route_expiry %s", expiry_payload)
                        if fallback_policy.allow_fallback:
                            enforce_fallback_policy(
                                policy=fallback_policy,
                                route_meta=canonical_route_meta,
                                detail=f"expired route for {node.id}",
                            )
                            route_meta_usable = False
                            reasoning_logs.append(
                                f"[ROUTE] {node.id}: fallback permitted after route expiry"
                            )
                        else:
                            exc = RouteExpiredError(
                                f"planned route expired at "
                                f"{canonical_route_meta.expiry_timestamp}"
                            )
                            msg = f"Route expired for {node.id}: {exc}."
                            _mark_node_failure(
                                node.id,
                                msg,
                                category=ErrorCategory.NON_RETRYABLE.value,
                            )
                            reasoning_logs.append(f"[ROUTE] {node.id}: {msg}")
                            all_messages.append(msg)
                            await _release_reservation(
                                node.id,
                                status="failed",
                                reason="route_expired",
                            )
                            continue
                    else:
                        _LOGGER.info("route_expiry %s", expiry_payload)

                if route_meta_usable and canonical_route_meta is not None:
                    fetched_at = float((route_meta or {}).get("fetched_at", 0.0) or 0.0)
                    age_seconds = (
                        max(0, int(time.time() - fetched_at)) if fetched_at else 0
                    )
                    reasoning_logs.append(
                        f"[ROUTE] {node.id}: using planned "
                        f"{(route_meta or {}).get('aggregator') or node.tool} route "
                        f"(route_meta_used=true, age={age_seconds}s)"
                    )
                    execution_args = {
                        **resolved_args,
                        "_route_meta": route_meta,
                        "_fallback_policy": fallback_policy.to_dict(),
                    }
                elif route_validation.allow_dynamic_fallback:
                    execution_args = {
                        **resolved_args,
                        "_fallback_policy": fallback_policy.to_dict(),
                    }

                jit_balance_error = await _jit_validate_balances(
                    node_id=node.id,
                    resolved_args=resolved_args,
                )
                if jit_balance_error:
                    user_message = (
                        "Your balance changed before signing, so I stopped before "
                        "broadcasting anything. Refresh balances and retry if needed."
                    )
                    _LOGGER.warning(
                        "jit_balance_validation_failed node_id=%s execution_id=%s reason=%s",
                        node.id,
                        execution_id,
                        jit_balance_error,
                    )
                    _mark_node_failure(
                        node.id,
                        jit_balance_error,
                        category=ErrorCategory.SECURITY.value,
                        user_message=user_message,
                    )
                    all_messages.append(user_message)
                    reasoning_logs.append(f"[SECURITY] {node.id}: {jit_balance_error}")
                    await _release_reservation(
                        node.id,
                        status="failed",
                        reason="jit_balance_validation_failed",
                    )
                    continue

                reasoning_logs.append(f"[ACTION] Starting '{node.tool}' ({node.id})")
                _mark_node_running(node.id)
                if node.tool in {"swap", "bridge", "transfer", "unwrap"}:
                    _schedule_event_publish(
                        _build_event_payload(
                            event="node_progress",
                            node_id=node.id,
                            tool=node.tool,
                            status="RUNNING",
                            stage="sending",
                            summary=summarize_task(node.tool, resolved_args),
                        )
                    )

                resolved_args_by_node_id[node.id] = resolved_args
                canonical_route_meta_by_node_id[node.id] = canonical_route_meta
                fallback_policy_by_node_id[node.id] = fallback_policy
                tasks.append(self._deps.run_with_timing(tool_obj, execution_args))
                node_ids.append(node.id)

            if tasks:
                results_with_timing = await asyncio.gather(*tasks)

                for node_id, (result, elapsed) in zip(node_ids, results_with_timing):
                    node = plan.nodes[node_id]
                    tool_obj = self._deps.tools_registry.get(node.tool)
                    if not tool_obj:
                        continue

                    node_state = combined_state.node_states.get(node_id) or NodeState(
                        node_id=node_id
                    )
                    resolved_args = resolved_args_by_node_id.get(node_id) or node.args
                    chain = str(
                        node.args.get("chain")
                        or node.args.get("source_chain")
                        or "unknown"
                    )

                    if isinstance(result, Exception):
                        retries = node_state.retries + 1
                        max_retries = node.retry_policy.get("max_retries", 3)
                        category = categorize_error(result)
                        err_msg = str(result)

                        if isinstance(result, NonRetryableError):
                            feedback = execution_failed(category, node.tool, chain)
                            _mark_node_failure(
                                node_id,
                                err_msg,
                                retries=retries,
                                category=category.value,
                                user_message=feedback.render(),
                            )
                            all_messages.append(feedback.render())
                            reasoning_logs.append(
                                f"[ERROR] {node_id} failed permanently ({category.value}): {err_msg}"
                            )
                            idem_key = idempotency_keys.get(node_id)
                            if idem_key:
                                await _idempotency_mark_failed_non_blocking(
                                    key=idem_key,
                                    error=err_msg,
                                )

                            await _record_execution_non_blocking(
                                tool=node.tool,
                                chain=chain,
                                success=False,
                                execution_time=elapsed,
                                error_msg=err_msg,
                                category=category,
                            )
                            await _release_reservation(
                                node_id,
                                status="failed",
                                reason=category.value,
                            )
                            continue

                        if retries <= max_retries:
                            if category in {
                                ErrorCategory.SECURITY,
                                ErrorCategory.NON_RETRYABLE,
                                ErrorCategory.LOGIC,
                            }:
                                feedback = execution_failed(category, node.tool, chain)
                                _mark_node_failure(
                                    node_id,
                                    err_msg,
                                    retries=retries,
                                    category=category.value,
                                    user_message=feedback.render(),
                                )
                                all_messages.append(feedback.render())
                                reasoning_logs.append(
                                    f"[ERROR] {node_id} failed permanently ({category.value}): {err_msg}"
                                )
                                idem_key = idempotency_keys.get(node_id)
                                if idem_key:
                                    await _idempotency_mark_failed_non_blocking(
                                        key=idem_key, error=err_msg
                                    )

                                await _record_execution_non_blocking(
                                    tool=node.tool,
                                    chain=chain,
                                    success=False,
                                    execution_time=elapsed,
                                    error_msg=err_msg,
                                    category=category,
                                )
                                await _release_reservation(
                                    node_id,
                                    status="failed",
                                    reason=category.value,
                                )
                                continue

                            mutated_args = None
                            if _should_request_fix(category):
                                mutated_args = tool_obj.suggest_fix(
                                    category, resolved_args, err_msg
                                )

                            retry_log = (
                                f"Retrying {node_id} (Attempt {retries}/{max_retries}) "
                                f"due to [{category.value}]: {err_msg}"
                            )
                            if mutated_args:
                                reasoning_logs.append(
                                    f"[HEAL] Tool suggested local fix for {node_id}"
                                )

                            if node.tool == "swap":
                                all_messages.append(
                                    "The swap didn't go through. I'm trying again now."
                                )
                            elif node.tool == "bridge":
                                all_messages.append(
                                    "The bridge didn't go through. I'm trying again now."
                                )
                            else:
                                feedback = execution_failed(
                                    category, node.tool, chain, retrying_now=True
                                )
                                all_messages.append(feedback.render())
                            reasoning_logs.append(f"[RETRY] {retry_log}")
                            updated_execution_state.node_states[node_id] = NodeState(
                                node_id=node_id,
                                status=StepStatus.PENDING,
                                retries=retries,
                                mutated_args=mutated_args or node_state.mutated_args,
                            )
                            await _release_reservation(
                                node_id,
                                status="released",
                                reason=f"retry:{category.value}",
                            )
                        else:
                            suggested_args = None
                            if category not in {
                                ErrorCategory.SECURITY,
                                ErrorCategory.NON_RETRYABLE,
                                ErrorCategory.LOGIC,
                            }:
                                try:
                                    suggested_args = tool_obj.suggest_fix(
                                        category, resolved_args, err_msg
                                    )
                                except Exception:
                                    suggested_args = None
                            if (
                                suggested_args is None
                                and node_state.mutated_args
                                and node_state.mutated_args != resolved_args
                            ):
                                suggested_args = node_state.mutated_args

                            if node.tool == "swap" and category not in {
                                ErrorCategory.SECURITY,
                                ErrorCategory.NON_RETRYABLE,
                                ErrorCategory.LOGIC,
                            }:
                                user_message = self._deps.swap_failure_message(
                                    chain, bool(suggested_args)
                                )
                                _mark_node_failure(
                                    node_id,
                                    err_msg,
                                    retries=retries,
                                    category=category.value,
                                    user_message=user_message,
                                    mutated_args=suggested_args,
                                )
                                all_messages.append(user_message)
                            elif node.tool == "bridge" and category not in {
                                ErrorCategory.SECURITY,
                                ErrorCategory.NON_RETRYABLE,
                                ErrorCategory.LOGIC,
                            }:
                                user_message = self._deps.bridge_failure_message(
                                    chain, bool(suggested_args)
                                )
                                _mark_node_failure(
                                    node_id,
                                    err_msg,
                                    retries=retries,
                                    category=category.value,
                                    user_message=user_message,
                                    mutated_args=suggested_args,
                                )
                                all_messages.append(user_message)
                            else:
                                feedback = execution_failed(category, node.tool, chain)
                                _mark_node_failure(
                                    node_id,
                                    err_msg,
                                    retries=retries,
                                    category=category.value,
                                    user_message=feedback.render(),
                                    mutated_args=suggested_args,
                                )
                                all_messages.append(feedback.render())

                            reasoning_logs.append(
                                f"[ERROR] {node_id} failed permanently ({category.value}): {err_msg}"
                            )
                            idem_key = idempotency_keys.get(node_id)
                            if idem_key:
                                await _idempotency_mark_failed_non_blocking(
                                    key=idem_key,
                                    error=err_msg,
                                )

                            await _record_execution_non_blocking(
                                tool=node.tool,
                                chain=chain,
                                success=False,
                                execution_time=elapsed,
                                error_msg=err_msg,
                                category=category,
                            )
                            await _release_reservation(
                                node_id,
                                status="failed",
                                reason=category.value,
                            )

                    else:
                        if not isinstance(result, dict):
                            result = {}

                        canonical_route_meta = canonical_route_meta_by_node_id.get(
                            node_id
                        )
                        fallback_policy = fallback_policy_by_node_id[node_id]

                        if result.get("fallback_used"):
                            fallback_reason = (
                                result.get("fallback_reason") or "fallback applied"
                            )
                            try:
                                fallback_payload = enforce_fallback_policy(
                                    policy=fallback_policy,
                                    route_meta=canonical_route_meta,
                                    detail=str(fallback_reason),
                                )
                                fallback_payload["node_id"] = node_id
                                _LOGGER.warning("route_fallback %s", fallback_payload)
                                reasoning_logs.append(
                                    f"[FALLBACK] {node_id} used planned fallback: {fallback_reason}"
                                )
                            except Exception as exc:
                                err_msg = str(exc)
                                feedback = execution_failed(
                                    ErrorCategory.NON_RETRYABLE,
                                    node.tool,
                                    chain,
                                )
                                _mark_node_failure(
                                    node_id,
                                    err_msg,
                                    category=ErrorCategory.NON_RETRYABLE.value,
                                    user_message=feedback.render(),
                                )
                                all_messages.append(feedback.render())
                                reasoning_logs.append(
                                    f"[ERROR] {node_id} violated fallback policy: {err_msg}"
                                )
                                await _record_execution_non_blocking(
                                    tool=node.tool,
                                    chain=chain,
                                    success=False,
                                    execution_time=elapsed,
                                    error_msg=err_msg,
                                    category=ErrorCategory.NON_RETRYABLE,
                                )
                                await _release_reservation(
                                    node_id,
                                    status="failed",
                                    reason="determinism_violation",
                                )
                                continue

                        actual_output = _extract_actual_output(node.tool, result)
                        if canonical_route_meta is not None:
                            comparison_payload = log_execution_comparison(
                                route_meta=canonical_route_meta,
                                node_id=node_id,
                                tool=node.tool,
                                actual_output=actual_output,
                            )
                            _LOGGER.info(
                                "route_execution_output %s", comparison_payload
                            )
                            if (
                                actual_output is not None
                                and actual_output < canonical_route_meta.min_output
                            ):
                                err = SlippageExceededError(
                                    f"actual output {actual_output} is below minimum "
                                    f"output {canonical_route_meta.min_output}"
                                )
                                feedback = execution_failed(
                                    ErrorCategory.SLIPPAGE,
                                    node.tool,
                                    chain,
                                )
                                _mark_node_failure(
                                    node_id,
                                    str(err),
                                    category=ErrorCategory.NON_RETRYABLE.value,
                                    user_message=feedback.render(),
                                )
                                all_messages.append(feedback.render())
                                reasoning_logs.append(
                                    f"[ERROR] {node_id} failed slippage enforcement: {err}"
                                )
                                await _record_execution_non_blocking(
                                    tool=node.tool,
                                    chain=chain,
                                    success=False,
                                    execution_time=elapsed,
                                    error_msg=str(err),
                                    category=ErrorCategory.SLIPPAGE,
                                )
                                await _release_reservation(
                                    node_id,
                                    status="failed",
                                    reason="slippage_exceeded",
                                )
                                continue

                        is_bridge_pending = (
                            node.tool == "bridge"
                            and str(result.get("status", "")).lower() == "pending"
                        )
                        if is_bridge_pending:
                            _mark_node_running(node_id)
                        else:
                            _mark_node_success(node_id, result)

                        default_msg = (
                            f"Step {node_id} submitted; awaiting bridge finalization"
                            if is_bridge_pending
                            else f"Step {node_id} succeeded"
                        )
                        msg = result.get("message", default_msg)
                        all_messages.append(msg)
                        if is_bridge_pending:
                            reasoning_logs.append(f"[PENDING] {msg} ({elapsed:.2f}s)")
                        else:
                            reasoning_logs.append(f"[SUCCESS] {msg} ({elapsed:.2f}s)")

                        output_artifact = self._deps.normalize_output(node, result)
                        if output_artifact:
                            outputs = {}
                            if isinstance(updated_execution_state.artifacts, dict):
                                outputs = updated_execution_state.artifacts.get(
                                    "outputs", {}
                                )
                                if not isinstance(outputs, dict):
                                    outputs = {}
                            outputs[node_id] = output_artifact
                            updated_execution_state.artifacts = {
                                **(updated_execution_state.artifacts or {}),
                                "outputs": outputs,
                            }

                        idem_key = idempotency_keys.get(node_id)
                        if idem_key:
                            tx_hash = result.get("tx_hash")
                            if tx_hash:
                                await _idempotency_mark_inflight_non_blocking(
                                    key=idem_key, tx_hash=tx_hash
                                )
                            if not is_bridge_pending:
                                await _idempotency_mark_success_non_blocking(
                                    key=idem_key,
                                    result=result,
                                )

                        tx_hash = result.get("tx_hash")
                        if tx_hash and node.tool in {
                            "swap",
                            "bridge",
                            "transfer",
                            "unwrap",
                        }:
                            await _mark_reservation_broadcast(
                                node_id,
                                tx_hash=str(tx_hash),
                            )
                            _schedule_event_publish(
                                _build_event_payload(
                                    event="node_progress",
                                    node_id=node.id,
                                    tool=node.tool,
                                    status="SUBMITTED",
                                    stage="submitted",
                                    summary=msg,
                                    tx_hash=str(tx_hash),
                                )
                            )
                        else:
                            await _release_reservation(
                                node_id,
                                status="released",
                                reason="execution_succeeded",
                                tx_hash=str(tx_hash) if tx_hash else None,
                            )

                        if node.tool in {"swap", "bridge"}:
                            try:
                                track_execution_volume(node.tool, resolved_args, result)
                            except Exception:
                                pass

                        await _record_execution_non_blocking(
                            tool=node.tool,
                            chain=chain,
                            success=True,
                            execution_time=elapsed,
                        )
                        history_registry = _get_history_registry()
                        if history_registry is not None:
                            status = "PENDING" if is_bridge_pending else "SUCCESS"
                            protocol = result.get("protocol")
                            tx_hash = result.get("tx_hash")
                            try:
                                await asyncio.wait_for(
                                    history_registry.record_event(
                                        user_id=str(user_id),
                                        thread_id=str(thread_id),
                                        execution_id=str(execution_id),
                                        node_id=node_id,
                                        tool=node.tool,
                                        status=status,
                                        summary=summarize_task(
                                            node.tool, resolved_args
                                        ),
                                        chain=chain,
                                        protocol=str(protocol) if protocol else None,
                                        tx_hash=str(tx_hash) if tx_hash else None,
                                    ),
                                    timeout=self._deps.task_history_write_timeout_seconds,
                                )
                            except Exception:
                                pass

                        final_tools = {"swap", "bridge", "unwrap"}
                        if is_bridge_pending:
                            tx_status = "PENDING"
                        elif node.tool in final_tools:
                            tx_status = "SUCCESS"
                        else:
                            tx_status = "PENDING_ON_CHAIN"

                        tx_record = {
                            "type": node.tool,
                            "tx_hash": result.get("tx_hash"),
                            "status": tx_status,
                            "node_id": node.id,
                            "submitted_at": datetime.now(timezone.utc).isoformat(),
                        }
                        reservation_id = active_reservations.get(node.id)
                        if reservation_id:
                            tx_record["reservation_id"] = reservation_id
                        sender = resolved_args.get("sender")
                        if sender:
                            tx_record["sender"] = sender
                        if execution_id:
                            tx_record["execution_id"] = str(execution_id)
                        if thread_id:
                            tx_record["thread_id"] = str(thread_id)
                        if user_id:
                            tx_record["user_id"] = str(user_id)
                        if result.get("nonce") is not None:
                            tx_record["nonce"] = result.get("nonce")
                        if result.get("raw_tx"):
                            tx_record["raw_tx"] = result.get("raw_tx")
                        if result.get("tx_payload"):
                            tx_record["tx_payload"] = result.get("tx_payload")
                        sub_org_id = resolved_args.get("sub_org_id")
                        if sub_org_id:
                            tx_record["sub_org_id"] = sub_org_id
                        if node.tool == "bridge":
                            tx_record["protocol"] = result.get("protocol")
                            tx_record["source_chain"] = result.get("source_chain")
                            tx_record["dest_chain"] = result.get("dest_chain")
                            meta: Dict[str, Any] = {}
                            request_id = result.get("relay_request_id")
                            if request_id:
                                meta["request_id"] = request_id
                            bridge_meta = result.get("bridge_meta")
                            if isinstance(bridge_meta, dict):
                                meta.update(bridge_meta)
                            if meta:
                                tx_record["meta"] = meta
                            try:
                                is_testnet = False
                                source_chain = result.get("source_chain")
                                dest_chain = result.get("dest_chain")
                                if source_chain:
                                    is_testnet = bool(
                                        get_chain_by_name(str(source_chain)).is_testnet
                                    )
                                if dest_chain and not is_testnet:
                                    is_testnet = bool(
                                        get_chain_by_name(str(dest_chain)).is_testnet
                                    )
                                tx_record["is_testnet"] = is_testnet
                            except Exception:
                                pass
                        pending.append(tx_record)

                        quote = fee_map.get(node_id)
                        if quote is not None and node.tool not in {
                            "check_balance",
                            "unwrap",
                        }:
                            reasoning_logs.append(
                                f"[FEE] Collecting fee for '{node_id}' …"
                            )
                            if node.tool in {"swap", "bridge", "transfer", "unwrap"}:
                                _schedule_event_publish(
                                    _build_event_payload(
                                        event="node_progress",
                                        node_id=node_id,
                                        tool=node.tool,
                                        status="FINALIZING",
                                        stage="finalizing",
                                        summary=msg,
                                        tx_hash=str(tx_hash) if tx_hash else None,
                                    )
                                )
                            try:
                                fee_tx_hash = await collect_fee(quote, resolved_args)
                                if fee_tx_hash:
                                    reasoning_logs.append(
                                        f"[FEE] Collected → tx: {fee_tx_hash}"
                                    )
                                    await _record_fee_non_blocking(
                                        tool=node.tool,
                                        chain=chain,
                                        fee_amount_native=quote.fee_amount_native,
                                    )
                                    pending.append(
                                        {
                                            "type": "fee_collection",
                                            "tx_hash": fee_tx_hash,
                                            "fee_for": node_id,
                                            "amount": str(quote.fee_amount_native),
                                            "native_symbol": quote.native_symbol,
                                            "status": "SUCCESS",
                                        }
                                    )
                            except FeeCollectionError as fee_err:
                                reasoning_logs.append(
                                    f"[FEE] Collection failed: {fee_err}"
                                )

        await _emit_node_events(updated_execution_state)

        final_state = execution_state.merge(updated_execution_state)
        final_failed_state = next(
            (
                node_state
                for node_state in final_state.node_states.values()
                if node_state.status == StepStatus.FAILED
            ),
            None,
        )
        final_tx_hash = None
        for tx_record in pending:
            tx_hash = tx_record.get("tx_hash")
            if tx_hash:
                final_tx_hash = str(tx_hash)
                break
        await _upsert_task_non_blocking(
            status=_derive_task_status(final_state),
            latest_summary=_task_latest_summary(final_state),
            tool=first_tool,
            tx_hash=final_tx_hash,
            error_category=(
                str(final_failed_state.error_category)
                if final_failed_state and final_failed_state.error_category
                else None
            ),
        )
        await _drain_side_effect_tasks()
        await _drain_event_tasks()

        return {
            "pending_transactions": pending,
            "execution_state": updated_execution_state,
            "confirmation_status": "EXECUTED",
            "waiting_for_funds": waiting_for_funds,
            "auto_resume_execution": False,
            "reasoning_logs": reasoning_logs,
            "messages": [AIMessage(content="\n".join(all_messages))]
            if all_messages
            else [],
        }
