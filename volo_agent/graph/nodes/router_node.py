import inspect
import logging
import re
from datetime import datetime
from typing import Any, Dict, cast

from langchain_core.messages import AIMessage, HumanMessage

from core.conversation.account_query_parser import parse_account_query
from core.conversation.responder import (
    conversation_failure_message,
    respond_conversation,
)
from core.conversation.router_handlers import (
    RouterNodeDeps,
    RouterNodeHelpers,
    build_status_response,
    handle_account_requests,
    handle_control_requests,
)
from core.history.task_history import TaskHistoryRegistry
from core.identity.errors import UnlinkAccountError
from core.observer.trigger_registry import TriggerRegistry
from core.planning.execution_plan import (
    ExecutionPlan,
    ExecutionState,
    NodeState,
    StepStatus,
    create_node_reset_state,
)
from core.tasks.registry import ConversationTaskRegistry
from core.utils.linking import (
    extract_unlink_target,
    is_link_account_request,
    is_link_status_request,
    is_unlink_account_request,
)
from graph.agent_state import AgentState
from graph.nodes.runtime_factories import build_router_handler_context
from graph.replay_guard import (
    extract_client_dedup_fields,
    observe_replay_guard,
    replay_guard_enabled,
)
from intent_hub.ontology.trigger import TriggerCondition
from intent_hub.parser.router import route_conversation

logger = logging.getLogger(__name__)

# --- Pre-compiled Regex Patterns ---
_RE_TOKENS = re.compile(r"[a-z0-9_.:%-]+")
_RE_NORMALIZE = re.compile(r"[^a-z0-9]+")
_RE_CANCEL_ID = re.compile(r"cancel\s+(?:task|order|trigger)?\s*([a-z0-9-]{6,})")
_RE_CLEAN_CANDIDATE = re.compile(r"[^a-z0-9-]")
_RE_IDENTITY_HINT = re.compile(r"[^a-z0-9:@._-]+")
_RE_DEDUP_CONTENT = re.compile(r"\s+")

_IDENTITY_SERVICE_CACHE: dict[str, Any] = {}


def _get_identity_service_ctx():
    """Lazy loader for identity service to avoid circular imports and overhead."""
    if not _IDENTITY_SERVICE_CACHE:
        from core.identity.service import (
            LINK_TOKEN_TTL_SECONDS,
            AsyncIdentityService,
        )

        _IDENTITY_SERVICE_CACHE["cls"] = AsyncIdentityService
        _IDENTITY_SERVICE_CACHE["ttl"] = LINK_TOKEN_TTL_SECONDS

    return _IDENTITY_SERVICE_CACHE["cls"], _IDENTITY_SERVICE_CACHE["ttl"]


def _get_content_str(content: Any) -> str:
    """Helper to ensure content is a string."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        # Join text parts if it's a list (multimodal content)
        return "".join(map(str, content))
    return str(content)


def _normalize_text(text: str) -> str:
    """Standardized text normalization: lowercase and alphanumeric only."""
    if not text:
        return ""
    return _RE_NORMALIZE.sub(" ", text.lower()).strip()


def _is_status_request(text: str) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    if not t:
        return False
    direct = {
        "status",
        "pending",
        "pending tasks",
        "pending orders",
        "recent tasks",
        "recent history",
        "task history",
    }
    if t in direct:
        return True
    phrases = (
        "what is the status",
        "what's the status",
        "show status",
        "show my status",
        "any pending",
        "in progress",
        "did it go through",
    )
    return any(p in t for p in phrases)


def _is_explicit_action_request(text: str) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    if not t:
        return False
    action_markers = (
        "swap",
        "convert",
        "exchange",
        "bridge",
        "transfer",
        "send",
        "buy",
        "sell",
        "unwrap",
        "check balance",
        "balance",
        "portfolio",
    )
    return any(marker in t for marker in action_markers)


def _parse_scope_for_post_completion_action(text: str) -> str:
    tokens = _RE_TOKENS.findall((text or "").lower())
    # Slot-filling replies are often short ("base", "eth", "0.5", "yes").
    # Keep only the most recent assistant question and user reply in that case.
    if len(tokens) <= 3 and not _is_explicit_action_request(text):
        return "recent_turn"
    return "last_user"


def _has_pending_intent(state: AgentState) -> bool:
    pending_intent = state.get("pending_intent")
    return isinstance(pending_intent, dict) and bool(pending_intent)


def _should_continue_pending_intent(
    state: AgentState,
    messages: list[Any] | None,
    text: str,
) -> bool:
    if not _has_pending_intent(state):
        return False
    if not str(text or "").strip():
        return False
    if _is_status_request(text):
        return False
    if _is_cancel_request(text):
        return False
    if _is_history_request(text):
        return False
    if (
        is_link_account_request(text)
        or is_link_status_request(text)
        or is_unlink_account_request(text)
    ):
        return False
    # If we are waiting for missing slots, prefer continuing that action flow
    # unless the user clearly starts a new explicit action.
    if _is_explicit_action_request(text):
        return False
    latest_ai = _latest_ai_text(messages)
    if "?" in latest_ai:
        return True
    pending_clarification = state.get("pending_clarification")
    return isinstance(pending_clarification, dict) and bool(pending_clarification)


def _latest_ai_text(messages: list[Any] | None) -> str:
    if not messages:
        return ""
    for message in reversed(messages):
        if isinstance(message, AIMessage):
            return _get_content_str(message.content).strip().lower()
    return ""


def _latest_human_message(messages: list[Any] | None) -> HumanMessage | None:
    if not messages:
        return None
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            return message
    return None


def _build_dedup_context_update(
    state: AgentState,
    latest_human_message: HumanMessage | None,
) -> Dict[str, Any] | None:
    fields = extract_client_dedup_fields(
        latest_human_message,
        context=state.get("context"),
    )
    if not fields.get("client_message_id") and not fields.get("client_nonce"):
        return None

    current_context = state.get("context")
    merged_context = dict(current_context) if isinstance(current_context, dict) else {}
    if fields.get("client_message_id"):
        merged_context["client_message_id"] = fields["client_message_id"]
    if fields.get("client_nonce"):
        merged_context["client_nonce"] = fields["client_nonce"]
    return merged_context


def _merge_context_update(
    updates: Dict[str, Any],
    *,
    context_update: Dict[str, Any] | None,
) -> Dict[str, Any]:
    if not context_update:
        return updates
    existing_context = updates.get("context")
    if isinstance(existing_context, dict):
        return {
            **updates,
            "context": {
                **context_update,
                **existing_context,
            },
        }
    return {**updates, "context": context_update}


def _looks_like_transaction_clarification(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if "?" not in str(text or ""):
        return False
    keywords = {
        "which",
        "what",
        "amount",
        "token",
        "chain",
        "network",
        "recipient",
        "receive",
        "bridging",
        "bridge",
        "swap",
        "swapping",
        "send",
        "sending",
        "use",
        "from",
        "to",
    }
    return any(keyword in normalized.split() for keyword in keywords)


def _is_short_slot_fill_reply(messages: list[Any] | None, text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    tokens = _RE_TOKENS.findall(t)
    if not tokens or len(tokens) > 3:
        return False
    if _is_explicit_action_request(t):
        return False
    if _is_status_request(t):
        return False
    if _is_cancel_request(t):
        return False
    if t in {
        "hi",
        "hello",
        "hey",
        "thanks",
        "thank you",
        "ok",
        "okay",
        "cool",
        "great",
    }:
        return False
    if not _looks_like_transaction_clarification(_latest_ai_text(messages)):
        return False
    return True


def _parse_cancel_request(text: str) -> str | None:
    if not text:
        return None
    t = text.strip().lower()
    if not t.startswith("cancel"):
        return None
    match = _RE_CANCEL_ID.search(t)
    if not match:
        parts = t.split()
        if len(parts) < 2:
            return None
        # Improved: skip common filler words
        candidate_raw = parts[1]
        if candidate_raw in {"my", "the", "this", "that", "order", "task", "trigger"}:
            if len(parts) >= 3:
                candidate_raw = parts[2]
            else:
                return None
        candidate = _RE_CLEAN_CANDIDATE.sub("", candidate_raw)
        return candidate if len(candidate) >= 6 else None
    return match.group(1)


def _is_confirm(text: str) -> bool:
    return any(w in text for w in ["confirm", "yes", "proceed"])


def _is_decline(text: str) -> bool:
    return any(w in text for w in ["no", "stop", "never", "don't", "do not"])


def _is_retry_request(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    hay = f" {normalized} "
    phrases = {"retry", "try again", "try-again", "tryagain"}
    return any(f" {phrase} " in hay for phrase in phrases)


def _is_recovery_accept(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    hay = f" {normalized} "
    phrases = {"ok", "okay", "yes", "go on", "go ahead", "goahead", "goon"}
    return any(f" {phrase} " in hay for phrase in phrases)


def _is_edit_request(text: str) -> bool:
    return any(w in text for w in ["edit", "change"])


def _is_cancel_request(text: str) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    if t == "cancel":
        return True
    # If it starts with cancel, check if it's a specific task/order cancellation
    if t.startswith("cancel "):
        return _parse_cancel_request(t) is not None
    return False


def _is_history_request(text: str) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    return t in {
        "history",
        "recent tasks",
        "show history",
        "task history",
        "my tasks",
        "show my tasks",
        "show tasks",
    }


def _retry_failed_steps(
    execution_state: ExecutionState | None,
) -> ExecutionState | None:
    if not execution_state:
        return None
    failed_ids = [
        node_id
        for node_id, node_state in execution_state.node_states.items()
        if node_state.status == StepStatus.FAILED
    ]
    if not failed_ids:
        return None
    delta = ExecutionState(node_states={})
    for node_id in failed_ids:
        delta = delta.merge(create_node_reset_state(node_id))
    return delta


def _retry_failed_steps_with_fix(
    execution_state: ExecutionState | None,
) -> ExecutionState | None:
    if not execution_state:
        return None
    delta = ExecutionState(node_states={})
    for node_id, node_state in execution_state.node_states.items():
        if node_state.status != StepStatus.FAILED:
            continue
        delta.node_states[node_id] = NodeState(
            node_id=node_id,
            status=StepStatus.PENDING,
            retries=0,
            error=None,
            error_category=None,
            mutated_args=node_state.mutated_args,
        )
    return delta if delta.node_states else None


def _node_status_value(status: Any) -> str:
    if status is None:
        return ""
    value = getattr(status, "value", status)
    if isinstance(value, str):
        return value.strip().lower().replace("stepstatus.", "")
    return str(value).strip().lower().replace("stepstatus.", "")


def _extract_latest_plan(state: AgentState) -> Any:
    history = state.get("plan_history") or []
    if not history:
        return None
    return history[-1]


def _plan_node_ids(plan: Any) -> list[str]:
    if isinstance(plan, ExecutionPlan):
        return list(plan.nodes.keys())
    if isinstance(plan, dict):
        nodes = plan.get("nodes")
        if isinstance(nodes, dict):
            return [str(node_id) for node_id in nodes.keys()]
    return []


def _is_execution_finished(state: AgentState) -> bool:
    execution_state = state.get("execution_state")
    if execution_state is None:
        return False

    if bool(getattr(execution_state, "completed", False)):
        return True

    node_ids = _plan_node_ids(_extract_latest_plan(state))
    if not node_ids:
        return False

    node_states = getattr(execution_state, "node_states", None)
    if not isinstance(node_states, dict):
        return False

    terminal = {
        StepStatus.SUCCESS.value,
        StepStatus.SKIPPED.value,
        StepStatus.FAILED.value,
    }
    for node_id in node_ids:
        node_state = node_states.get(node_id)
        if node_state is None:
            return False
        if _node_status_value(getattr(node_state, "status", None)) not in terminal:
            return False
    return True


async def _route_conversation_non_blocking(messages) -> Dict[str, Any]:
    try:
        maybe_result = route_conversation(messages)
        result = (
            await maybe_result if inspect.isawaitable(maybe_result) else maybe_result
        )
        if isinstance(result, dict):
            return result
    except Exception as exc:
        logger.error(f"Router LLM error: {exc}", exc_info=True)
    return {"category": "CONVERSATION", "response": None}


async def _respond_conversation_non_blocking(messages, state: AgentState) -> str:
    try:
        maybe_response = respond_conversation(messages, state=cast(dict, state))
        response = (
            await maybe_response
            if inspect.isawaitable(maybe_response)
            else maybe_response
        )
        if isinstance(response, str):
            response = response.strip()
            if response:
                return response
    except Exception:
        pass
    return conversation_failure_message()


async def _conversation_reply_non_blocking(
    route_result: Dict[str, Any], messages, state: AgentState
) -> str:
    route_response = route_result.get("response")
    if isinstance(route_response, str):
        route_response = route_response.strip()
        if route_response:
            return route_response
    return await _respond_conversation_non_blocking(messages, state=state)


def _mark_pending_steps_skipped(
    plan: ExecutionPlan | None,
    execution_state: ExecutionState | None,
    reason: str,
) -> ExecutionState | None:
    if execution_state is None:
        execution_state = ExecutionState(node_states={})

    delta = ExecutionState(node_states={})
    node_ids = list(plan.nodes) if plan else list(execution_state.node_states)
    for node_id in node_ids:
        node_state = execution_state.node_states.get(node_id)
        status = node_state.status if node_state else StepStatus.PENDING
        if status in {StepStatus.PENDING, StepStatus.RUNNING}:
            delta.node_states[node_id] = NodeState(
                node_id=node_id,
                status=StepStatus.SKIPPED,
                error=reason,
                user_message="Transaction cancelled.",
            )
    if not delta.node_states:
        return None
    return delta


def _describe_condition(condition_dict: dict) -> str:
    try:
        condition = TriggerCondition(**condition_dict)
        return condition.description
    except Exception:
        return str(condition_dict)


def _format_pending_tx(tx: dict) -> str | None:
    status = str(tx.get("status", "")).strip().lower()
    if status not in {"pending", "pending_on_chain", "running"}:
        return None
    tx_type = str(tx.get("type", "transaction")).lower()
    if tx_type == "bridge":
        protocol = tx.get("protocol")
        source = tx.get("source_chain")
        dest = tx.get("dest_chain")
        if protocol and source and dest:
            return f"Bridge ({protocol}) {source} → {dest}"
        if source and dest:
            return f"Bridge {source} → {dest}"
        return "Bridge"
    if tx_type == "swap":
        chain = tx.get("chain") or tx.get("network")
        return f"Swap on {chain}" if chain else "Swap"
    if tx_type == "transfer":
        chain = tx.get("network") or tx.get("chain")
        return f"Transfer on {chain}" if chain else "Transfer"
    return "Transaction"


def _summarize_failed_node(
    plan: ExecutionPlan, execution_state: ExecutionState
) -> dict | None:
    failed_node_id = None
    for node_id in reversed(list(plan.nodes.keys())):
        node_state = execution_state.node_states.get(node_id)
        if node_state and node_state.status == StepStatus.FAILED:
            failed_node_id = node_id
            break
    if not failed_node_id:
        return None

    node = plan.nodes.get(failed_node_id)
    if not node:
        return None
    args = node.args or {}
    tool = node.tool

    if tool == "swap":
        summary = (
            f"Swap {args.get('amount_in')} {args.get('token_in_symbol')} "
            f"to {args.get('token_out_symbol')} on {args.get('chain')}"
        )
    elif tool == "bridge":
        summary = (
            f"Bridge {args.get('amount')} {args.get('token_symbol')} "
            f"from {args.get('source_chain')} to {args.get('target_chain')}"
        )
    elif tool == "transfer":
        summary = (
            f"Transfer {args.get('amount')} "
            f"{args.get('asset_symbol') or args.get('token_symbol')} "
            f"to {args.get('recipient')} on {args.get('network') or args.get('chain')}"
        )
    else:
        summary = f"{tool.capitalize()} with current settings"

    return {
        "node_id": failed_node_id,
        "tool": tool,
        "args": args,
        "summary": summary,
    }


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        value = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _is_trigger_expired(trigger: dict, now: datetime) -> bool:
    status = str(trigger.get("status", "")).strip().lower()
    if status == "expired":
        return True
    if status != "pending":
        return False
    expires_at = _parse_iso(str(trigger.get("expires_at", "")))
    if not expires_at:
        return False
    return expires_at <= now


def _get_sender_address(state: AgentState) -> str | None:
    user_info = state.get("user_info")
    if isinstance(user_info, dict):
        address = user_info.get("sender_address")
        if address:
            return str(address)
    artifacts = state.get("artifacts") or {}
    if isinstance(artifacts, dict):
        address = artifacts.get("sender_address")
        if address:
            return str(address)
    return None


def _resolve_volo_user_id(state: AgentState) -> str | None:
    user_info = state.get("user_info")
    if isinstance(user_info, dict) and user_info.get("volo_user_id"):
        return str(user_info["volo_user_id"])
    return None


def _format_linked_identity(identity: Dict[str, Any]) -> str:
    provider = identity.get("provider", "unknown")
    provider_user_id = identity.get("provider_user_id", "unknown")
    username = identity.get("username")
    tags = []
    if username:
        tags.append(str(username))
    if identity.get("is_primary"):
        tags.append("primary")
    if tags:
        return f"{provider}:{provider_user_id} ({', '.join(tags)})"
    return f"{provider}:{provider_user_id}"


def _normalize_identity_hint(value: str | None) -> str:
    return _RE_IDENTITY_HINT.sub("", str(value or "").strip().lower())


def _identity_keys(identity: Dict[str, Any]) -> set[str]:
    provider = _normalize_identity_hint(identity.get("provider"))
    provider_user_id = _normalize_identity_hint(identity.get("provider_user_id"))
    username_raw = str(identity.get("username") or "").strip()
    username = _normalize_identity_hint(username_raw.lstrip("@"))

    keys = set()
    if provider:
        keys.add(provider)
    if provider_user_id:
        keys.add(provider_user_id)
    if provider and provider_user_id:
        keys.add(f"{provider}:{provider_user_id}")
    if username:
        keys.add(username)
        keys.add(f"@{username}")
        if provider:
            keys.add(f"{provider}:{username}")
            keys.add(f"{provider}:@{username}")
    return keys


def _resolve_unlink_identity(
    identities: list[Dict[str, Any]], target_hint: str | None
) -> tuple[Dict[str, Any] | None, list[Dict[str, Any]]]:
    if not target_hint:
        return None, []

    target = _normalize_identity_hint(target_hint)
    if not target:
        return None, []

    exact_matches = [
        identity for identity in identities if target in _identity_keys(identity)
    ]
    if exact_matches:
        if len(exact_matches) == 1:
            return exact_matches[0], []
        return None, exact_matches

    provider_matches = [
        identity
        for identity in identities
        if _normalize_identity_hint(identity.get("provider")) == target
    ]
    if len(provider_matches) == 1:
        return provider_matches[0], []
    if provider_matches:
        return None, provider_matches
    return None, []


def _unlink_examples(identities: list[Dict[str, Any]]) -> list[str]:
    provider_counts: Dict[str, int] = {}
    for identity in identities:
        provider = str(identity.get("provider") or "account").lower()
        provider_counts[provider] = provider_counts.get(provider, 0) + 1

    examples = []
    for identity in identities:
        provider = str(identity.get("provider") or "account").lower()
        username = str(identity.get("username") or "").strip().lstrip("@")
        provider_user_id = str(identity.get("provider_user_id") or "").strip()
        if username:
            examples.append(f"unlink @{username}")
            continue
        if provider_counts.get(provider, 0) == 1:
            examples.append(f"unlink {provider}")
            continue
        if provider_user_id:
            examples.append(f"unlink {provider}:{provider_user_id}")

    seen = set()
    deduped = []
    for example in examples:
        if example in seen:
            continue
        seen.add(example)
        deduped.append(example)
    return deduped


def _build_router_handler_deps() -> tuple[RouterNodeDeps, RouterNodeHelpers]:
    identity_service_cls, link_token_ttl_seconds = _get_identity_service_ctx()

    return build_router_handler_context(
        trigger_registry_cls=TriggerRegistry,
        task_history_registry_cls=TaskHistoryRegistry,
        task_registry_cls=ConversationTaskRegistry,
        identity_service_cls=identity_service_cls,
        unlink_account_error_cls=UnlinkAccountError,
        link_token_ttl_seconds=link_token_ttl_seconds,
        is_link_account_request=is_link_account_request,
        is_link_status_request=is_link_status_request,
        is_unlink_account_request=is_unlink_account_request,
        extract_unlink_target=extract_unlink_target,
        is_confirm=_is_confirm,
        is_decline=_is_decline,
        is_retry_request=_is_retry_request,
        is_recovery_accept=_is_recovery_accept,
        is_edit_request=_is_edit_request,
        is_cancel_request=_is_cancel_request,
        is_history_request=_is_history_request,
        parse_cancel_request=_parse_cancel_request,
        parse_account_query=parse_account_query,
        retry_failed_steps=_retry_failed_steps,
        retry_failed_steps_with_fix=_retry_failed_steps_with_fix,
        summarize_failed_node=_summarize_failed_node,
        mark_pending_steps_skipped=_mark_pending_steps_skipped,
        format_pending_tx=_format_pending_tx,
        describe_condition=_describe_condition,
        is_trigger_expired=_is_trigger_expired,
        get_sender_address=_get_sender_address,
        resolve_volo_user_id=_resolve_volo_user_id,
        format_linked_identity=_format_linked_identity,
        resolve_unlink_identity=_resolve_unlink_identity,
        unlink_examples=_unlink_examples,
    )


async def conversational_router_node(state: AgentState) -> Dict[str, Any]:
    """
    Categorizes the user's input as CONVERSATION, STATUS, or ACTION.
    """
    messages = state["messages"]
    latest_human_message = _latest_human_message(messages)
    context_update = _build_dedup_context_update(state, latest_human_message)

    # Check if we are waiting for a confirmation
    last_user_msg = ""
    if latest_human_message is not None:
        last_user_msg = _get_content_str(latest_human_message.content).lower()
    deps, helpers = _build_router_handler_deps()
    control_result = await handle_control_requests(
        cast(dict, state),
        last_user_msg,
        deps=deps,
        helpers=helpers,
    )
    if control_result is not None:
        return _merge_context_update(control_result, context_update=context_update)

    account_result = await handle_account_requests(
        cast(dict, state),
        last_user_msg,
        deps=deps,
        helpers=helpers,
    )
    if account_result is not None:
        return _merge_context_update(account_result, context_update=context_update)

    if _is_status_request(last_user_msg):
        return _merge_context_update(
            await build_status_response(
                cast(dict, state),
                {"route_decision": "STATUS"},
                deps=deps,
                helpers=helpers,
            ),
            context_update=context_update,
        )

    execution_finished = _is_execution_finished(state)
    guard_enabled = replay_guard_enabled()
    explicit_action = _is_explicit_action_request(last_user_msg)

    if _should_continue_pending_intent(state, messages, last_user_msg):
        parse_scope = "recent_turn"
        observe_replay_guard(
            replay_prevented=True,
            parse_scope=parse_scope,
            messages=messages,
            reason="pending_intent_follow_up",
        )
        return _merge_context_update(
            {
                "route_decision": "ACTION",
                "parse_scope": parse_scope,
            },
            context_update=context_update,
        )

    # Fast path: explicit action language should not require router LLM.
    if explicit_action:
        if guard_enabled and execution_finished:
            parse_scope = _parse_scope_for_post_completion_action(last_user_msg)
            observe_replay_guard(
                replay_prevented=True,
                parse_scope=parse_scope,
                messages=messages,
                reason="explicit_action_fast_path",
            )
            return _merge_context_update(
                {
                    "route_decision": "ACTION",
                    "parse_scope": parse_scope,
                },
                context_update=context_update,
            )
        if execution_finished and not guard_enabled:
            observe_replay_guard(
                replay_prevented=False,
                parse_scope=None,
                messages=messages,
                reason="guard_disabled_explicit_action_fast_path",
            )
        return _merge_context_update(
            {"route_decision": "ACTION"},
            context_update=context_update,
        )

    # ── Post-completion parse scope guard ────────────────────────────────────
    # If the previous plan finished, limit the intent parser to the newest
    # user message only.  Without this guard the parser receives the full
    # conversation history and re-extracts old intents on every new turn.
    if guard_enabled and execution_finished:
        if _is_short_slot_fill_reply(messages, last_user_msg):
            parse_scope = _parse_scope_for_post_completion_action(last_user_msg)
            observe_replay_guard(
                replay_prevented=True,
                parse_scope=parse_scope,
                messages=messages,
                reason="post_completion_short_reply_fast_path",
            )
            return _merge_context_update(
                {
                    "route_decision": "ACTION",
                    "parse_scope": parse_scope,
                },
                context_update=context_update,
            )
        result = await _route_conversation_non_blocking(messages)
        category = result.get("category", "ACTION")
        if category == "ACTION":
            parse_scope = _parse_scope_for_post_completion_action(last_user_msg)
            observe_replay_guard(
                replay_prevented=True,
                parse_scope=parse_scope,
                messages=messages,
                reason="post_completion_action_scoped",
            )
            return _merge_context_update(
                {
                    "route_decision": "ACTION",
                    "parse_scope": parse_scope,
                },
                context_update=context_update,
            )
        updates: Dict[str, Any] = {"route_decision": category}
        if category == "CONVERSATION":
            updates["messages"] = [
                AIMessage(
                    content=await _conversation_reply_non_blocking(
                        result, messages, state=state
                    )
                )
            ]
        # STATUS falls through to the full status-building block below
        if category != "STATUS":
            observe_replay_guard(
                replay_prevented=False,
                parse_scope=None,
                messages=messages,
                reason=f"post_completion_non_action:{str(category).lower()}",
            )
            return _merge_context_update(updates, context_update=context_update)
        observe_replay_guard(
            replay_prevented=False,
            parse_scope=None,
            messages=messages,
            reason="post_completion_status",
        )
    else:
        if execution_finished and not guard_enabled:
            observe_replay_guard(
                replay_prevented=False,
                parse_scope=None,
                messages=messages,
                reason="guard_disabled",
            )
        result = await _route_conversation_non_blocking(messages)
        category = result.get("category", "ACTION")
        updates = {"route_decision": category}

    if category == "CONVERSATION":
        updates["messages"] = [
            AIMessage(
                content=await _conversation_reply_non_blocking(
                    result, messages, state=state
                )
            )
        ]
    elif category == "STATUS":
        updates = await build_status_response(
            cast(dict, state),
            updates,
            deps=deps,
            helpers=helpers,
        )

    return _merge_context_update(updates, context_update=context_update)
