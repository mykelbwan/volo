from typing import Any, Dict, cast

from langchain_core.messages import AIMessage

from core.conversation.responder import respond_conversation
from core.tasks.updater import task_title_from_intent, upsert_task_from_state
from core.utils.user_feedback import intent_missing_info, intent_parsing_failed
from graph.agent_state import AgentState
from graph.pending_intent import (
    clarification_recovery_prompt,
    clarification_state,
    latest_user_text,
    merge_pending_intent,
    resolve_pending_follow_up,
)
from graph.replay_guard import (
    build_rolling_summary_artifact,
    estimate_message_tokens,
    observe_parse_scope,
    parse_scope_default,
    select_messages_for_scope,
)
from intent_hub.ontology.intent import Intent, IntentStatus
from intent_hub.parser.router import has_positive_action_evidence
from intent_hub.parser.semantic_parser import _get_token_registry_async, parse_async
from intent_hub.parser.validation import validate_intent
from intent_hub.resolver.templates import apply_templates


def _load_pending_intent(state: AgentState) -> Intent | None:
    pending_data = state.get("pending_intent")
    if not isinstance(pending_data, dict):
        return None
    try:
        return Intent(**pending_data)
    except Exception:
        return None


def _load_pending_intent_queue(state: AgentState) -> list[Intent]:
    pending_queue = state.get("pending_intent_queue")
    if not isinstance(pending_queue, list):
        return []
    intents: list[Intent] = []
    for item in pending_queue:
        if not isinstance(item, dict):
            continue
        try:
            intents.append(Intent(**item))
        except Exception:
            continue
    return intents


def _symbol_from_slot(slot_value: Any) -> str | None:
    if isinstance(slot_value, dict):
        symbol = slot_value.get("symbol")
    else:
        symbol = slot_value
    text = str(symbol or "").strip().upper()
    return text or None


def _token_slot_name(intent: Intent) -> str:
    if intent.intent_type in {"transfer", "unwrap"}:
        return "token"
    return "token_in"


def _output_token_symbol(intent: Intent) -> str | None:
    slots = intent.slots or {}
    if intent.intent_type == "swap":
        return _symbol_from_slot(slots.get("token_out"))
    if intent.intent_type == "bridge":
        return _symbol_from_slot(slots.get("token_in"))
    if intent.intent_type == "transfer":
        return _symbol_from_slot(slots.get("token"))
    if intent.intent_type == "unwrap":
        return _symbol_from_slot(slots.get("token"))
    return None


def _output_chain(intent: Intent) -> str | None:
    slots = intent.slots or {}
    target_chain = str(slots.get("target_chain") or "").strip().lower()
    if target_chain:
        return target_chain
    chain = str(slots.get("chain") or "").strip().lower()
    return chain or None


def _carry_dependencies(
    anchor_intent: Intent,
    queued_intents: list[Intent],
    *,
    token_registry: dict[str, Any],
) -> list[Intent]:
    if not queued_intents:
        return []

    carried: list[Intent] = []
    previous = anchor_intent
    for intent in queued_intents:
        updated = intent.model_copy(deep=True)
        slots = dict(updated.slots or {})

        carry_amount = bool(slots.get("_carry_amount_from_prev"))
        carry_token = bool(slots.get("_carry_token_from_prev"))
        carry_chain_from_prev = bool(slots.get("_carry_chain_from_prev"))
        carry_chain_from_prev_target = bool(slots.get("_carry_chain_from_prev_target"))

        if carry_amount and slots.get("amount") is None:
            prev_amount = (previous.slots or {}).get("amount")
            if prev_amount is not None:
                slots["amount"] = prev_amount

        token_slot = _token_slot_name(updated)
        if carry_token:
            prev_token = _output_token_symbol(previous)
            current_token = _symbol_from_slot(slots.get(token_slot))
            if prev_token and current_token and current_token != prev_token:
                # Explicit mismatch on a dependent "it" reference: force a safe
                # clarification instead of guessing.
                slots[token_slot] = None
                slots["_force_token_clarification"] = True
            elif prev_token and not current_token:
                slots[token_slot] = {"symbol": prev_token}

        if (
            (carry_chain_from_prev or carry_chain_from_prev_target)
            and not str(slots.get("chain") or "").strip()
        ):
            inherited_chain = None
            if carry_chain_from_prev_target:
                inherited_chain = _output_chain(previous)
            if not inherited_chain and carry_chain_from_prev:
                inherited_chain = str((previous.slots or {}).get("chain") or "").strip().lower()
            if inherited_chain:
                slots["chain"] = inherited_chain

        updated.slots = slots
        updated = validate_intent(updated, token_registry)
        carried.append(updated)
        previous = updated

    return carried


def _enforce_forced_token_clarification(
    intents: list[Intent], *, token_registry: dict[str, Any]
) -> list[Intent]:
    if not intents:
        return intents
    resolved: list[Intent] = []
    for intent in intents:
        slots = dict(intent.slots or {})
        if not slots.get("_force_token_clarification"):
            resolved.append(intent)
            continue
        slot_name = _token_slot_name(intent)
        slots[slot_name] = None
        intent.slots = slots
        resolved.append(validate_intent(intent, token_registry))
    return resolved


async def _continue_pending_intent(
    pending_intent: Intent,
    *,
    messages,
    token_registry: dict[str, Any],
    pending_intent_queue: list[Intent] | None = None,
    pending_clarification: dict[str, Any] | None = None,
) -> tuple[list[Intent], list[Intent], dict[str, Any] | None, str | None]:
    queued_intents = list(pending_intent_queue or [])
    resolution = resolve_pending_follow_up(
        pending_intent,
        messages,
        token_registry=token_registry,
        pending_clarification=pending_clarification,
    )
    if resolution.kind != "slot_update":
        attempt_count = 0
        if isinstance(pending_clarification, dict):
            try:
                attempt_count = int(pending_clarification.get("attempt_count") or 0)
            except Exception:
                attempt_count = 0
        next_attempt_count = attempt_count + 1
        prompt = resolution.prompt or clarification_recovery_prompt(
            pending_intent,
            attempt_count=attempt_count,
            reason=resolution.reason or "unknown",
        )
        return (
            [],
            queued_intents,
            clarification_state(
                pending_intent,
                attempt_count=next_attempt_count,
                last_resolution_error=resolution.reason or "unknown",
            ),
            prompt,
        )

    extracted_slots = resolution.slot_updates
    parsed_intent = None

    # If deterministic extraction could not fill the missing slots, fall back
    # to parsing the scoped follow-up and merge any slot values into the stored
    # incomplete action instead of trusting a fresh intent classification.
    if len(extracted_slots) < len(pending_intent.missing_slots):
        try:
            follow_up_intents = await parse_async(messages)
        except Exception:
            follow_up_intents = []
        if follow_up_intents:
            parsed_intent = follow_up_intents[0]

    merged = merge_pending_intent(
        pending_intent,
        parsed_intent=parsed_intent,
        extracted_slots=extracted_slots,
        latest_reply=latest_user_text(messages),
        token_registry=token_registry,
    )
    if queued_intents:
        queued_intents = _carry_dependencies(
            merged,
            queued_intents,
            token_registry=token_registry,
        )
    return ([merged, *queued_intents], [], None, None)


async def intent_parser_node(state: AgentState) -> Dict[str, Any]:
    full_messages = state["messages"]
    requested_scope = state.get("parse_scope") or parse_scope_default()
    messages, older_messages, effective_scope = select_messages_for_scope(
        full_messages,
        requested_scope,
    )
    observe_parse_scope(
        scope=effective_scope,
        messages_total=len(full_messages),
        messages_selected=len(messages),
        token_estimate_total=estimate_message_tokens(full_messages),
        token_estimate_selected=estimate_message_tokens(messages),
    )

    artifacts = dict(state.get("artifacts") or {})
    summary_artifact = build_rolling_summary_artifact(
        older_messages, scope=effective_scope
    )
    if summary_artifact:
        artifacts["rolling_message_summary"] = summary_artifact
    elif "rolling_message_summary" in artifacts:
        artifacts.pop("rolling_message_summary", None)

    pending_intent = _load_pending_intent(state)
    pending_intent_queue = _load_pending_intent_queue(state)
    pending_clarification = state.get("pending_clarification")

    async def _conversation_handoff_updates() -> Dict[str, Any]:
        updates = {
            "intents": [],
            "parse_scope": None,
            "route_decision": "CONVERSATION",
            "messages": [AIMessage(content=await respond_conversation(messages))],
        }
        if artifacts != (state.get("artifacts") or {}):
            updates["artifacts"] = artifacts
        return updates

    def _should_handoff_to_conversation() -> bool:
        latest_text = latest_user_text(messages)
        if not str(latest_text or "").strip():
            return False
        return not has_positive_action_evidence(messages)

    try:
        token_registry = await _get_token_registry_async()
        if pending_intent is not None and effective_scope == "recent_turn":
            (
                intents,
                next_pending_queue,
                next_pending_clarification,
                clarification_message,
            ) = await _continue_pending_intent(
                pending_intent,
                messages=messages,
                token_registry=token_registry,
                pending_intent_queue=pending_intent_queue,
                pending_clarification=(
                    pending_clarification
                    if isinstance(pending_clarification, dict)
                    else None
                ),
            )
            if clarification_message:
                updates = {
                    "intents": [],
                    "parse_scope": None,
                    "pending_intent": pending_intent.model_dump(),
                    "pending_intent_queue": (
                        [intent.model_dump() for intent in next_pending_queue]
                        if next_pending_queue
                        else None
                    ),
                    "pending_clarification": next_pending_clarification,
                    "messages": [AIMessage(content=clarification_message)],
                }
                if artifacts != (state.get("artifacts") or {}):
                    updates["artifacts"] = artifacts
                await upsert_task_from_state(
                    cast(dict, state),
                    title=task_title_from_intent(pending_intent),
                    status="WAITING_INPUT",
                    latest_summary=clarification_message,
                    tool=str(pending_intent.intent_type),
                )
                return updates
        else:
            intents = await parse_async(messages)
    except Exception:
        if _should_handoff_to_conversation():
            return await _conversation_handoff_updates()
        feedback = intent_parsing_failed()
        updates = {
            "intents": [],
            "parse_scope": None,
            "messages": [AIMessage(content=feedback.render())],
        }
        if artifacts != (state.get("artifacts") or {}):
            updates["artifacts"] = artifacts
        return updates

    intents = apply_templates(list(intents))
    intents = _enforce_forced_token_clarification(
        list(intents), token_registry=token_registry
    )
    if not intents:
        if _should_handoff_to_conversation():
            return await _conversation_handoff_updates()
        feedback = intent_parsing_failed()
        updates = {
            "intents": [],
            "parse_scope": None,
            "messages": [AIMessage(content=feedback.render())],
        }
        if artifacts != (state.get("artifacts") or {}):
            updates["artifacts"] = artifacts
        return updates

    # Store the dict versions for serialization
    intent_dicts = [i.model_dump() for i in intents]

    updates: Dict[str, Any] = {
        "intents": intent_dicts,
        "parse_scope": None,
        "pending_intent": None,
        "pending_intent_queue": None,
        "pending_clarification": None,
    }
    if artifacts != (state.get("artifacts") or {}):
        updates["artifacts"] = artifacts

    # Find the first incomplete intent and use its prompt
    for intent_index, intent in enumerate(intents):
        if intent.status == IntentStatus.INCOMPLETE and intent.clarification_prompt:
            updates["pending_intent"] = intent.model_dump()
            remaining = intents[intent_index + 1 :]
            updates["pending_intent_queue"] = (
                [remaining_intent.model_dump() for remaining_intent in remaining]
                if remaining
                else None
            )
            updates["pending_clarification"] = clarification_state(
                intent,
                attempt_count=0,
                last_resolution_error=None,
            )
            updates["messages"] = [AIMessage(content=intent.clarification_prompt)]
            await upsert_task_from_state(
                cast(dict, state),
                title=task_title_from_intent(intent),
                status="WAITING_INPUT",
                latest_summary=intent.clarification_prompt,
                tool=str(intent.intent_type),
            )
            break
        if intent.status == IntentStatus.INCOMPLETE:
            updates["pending_intent"] = intent.model_dump()
            remaining = intents[intent_index + 1 :]
            updates["pending_intent_queue"] = (
                [remaining_intent.model_dump() for remaining_intent in remaining]
                if remaining
                else None
            )
            updates["pending_clarification"] = clarification_state(
                intent,
                attempt_count=0,
                last_resolution_error=None,
            )
            feedback = intent_missing_info(intent.missing_slots)
            updates["messages"] = [AIMessage(content=feedback.render())]
            await upsert_task_from_state(
                cast(dict, state),
                title=task_title_from_intent(intent),
                status="WAITING_INPUT",
                latest_summary=feedback.render(),
                tool=str(intent.intent_type),
            )
            break

    return updates
