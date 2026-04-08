from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from langgraph.types import interrupt

from core.observer.trigger_registry import TriggerRegistry
from graph.agent_state import AgentState
from intent_hub.ontology.trigger import TriggerCondition

logger = logging.getLogger(__name__)
_TRIGGER_RESUME_SECRET = b"volo-agent-v1-secure-resume-key-2024"


def _stable_json_bytes(payload: Dict[str, Any]) -> bytes:
    return json.dumps(
        payload, sort_keys=True, default=str, separators=(",", ":")
    ).encode("utf-8")


def _hash_payload(payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    padded = raw + ("=" * ((4 - (len(raw) % 4)) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _build_trigger_resume_token(
    *,
    thread_id: str,
    user_id: str,
    trigger_condition_dict: Dict[str, Any],
    payload_intents: list[dict[str, Any]],
) -> str:
    claims = {
        "v": 1,
        "thread_id": str(thread_id),
        "user_id": str(user_id),
        "issued_at": datetime.now(timezone.utc).isoformat(),
        # Bind the token to the exact trigger condition and queued actions so a
        # forged resume payload cannot redirect execution to another action set.
        "condition_hash": _hash_payload(trigger_condition_dict),
        "intents_hash": _hash_payload(payload_intents),
    }
    claims_bytes = _stable_json_bytes(claims)
    signature = hmac.new(_TRIGGER_RESUME_SECRET, claims_bytes, hashlib.sha256).digest()
    return f"{_b64url_encode(claims_bytes)}.{_b64url_encode(signature)}"


def _validate_trigger_resume_token(
    *,
    resume_token: str,
    thread_id: str,
    user_id: str,
    trigger_condition_dict: Dict[str, Any],
    payload_intents: list[dict[str, Any]],
) -> bool:
    if not resume_token or "." not in resume_token:
        return False

    try:
        encoded_claims, encoded_signature = resume_token.split(".", 1)
        claims_bytes = _b64url_decode(encoded_claims)
        provided_signature = _b64url_decode(encoded_signature)
        expected_signature = hmac.new(
            _TRIGGER_RESUME_SECRET, claims_bytes, hashlib.sha256
        ).digest()
        if not secrets.compare_digest(provided_signature, expected_signature):
            return False
        claims = json.loads(claims_bytes.decode("utf-8"))
    except Exception:
        return False

    expected_claims = {
        "v": 1,
        "thread_id": str(thread_id),
        "user_id": str(user_id),
        "condition_hash": _hash_payload(trigger_condition_dict),
        "intents_hash": _hash_payload(payload_intents),
    }
    for key, expected in expected_claims.items():
        if str(claims.get(key) or "") != str(expected):
            return False
    return True


async def wait_for_trigger_node(
    state: AgentState, config: RunnableConfig
) -> Dict[str, Any]:
    thread_id: str = (config.get("configurable") or {}).get("thread_id", "")
    intents: list[dict[str, Any]] = state.get("intents") or []
    user_info: dict[str, Any] = state.get("user_info") or {}
    user_id: str = user_info.get("volo_user_id") or state.get("user_id") or "unknown"

    registry = TriggerRegistry()

    conditional_intents = [i for i in intents if i.get("condition")]
    action_intents = [i for i in intents if not i.get("condition")]

    if not conditional_intents:
        # Shouldn't happen (routing guard), but handle defensively.
        logger.warning(
            "wait_for_trigger_node: reached with no conditional intents "
            "(thread=%s).  Routing to resolver as immediate action.",
            thread_id[:8],
        )
        return {"route_decision": "resolve"}

    first_conditional = conditional_intents[0]
    condition_data = first_conditional.get("condition") or {}

    try:
        trigger_condition = TriggerCondition(**condition_data)
    except Exception as exc:
        logger.error(
            "wait_for_trigger_node: invalid trigger condition in intent: %s",
            exc,
            exc_info=True,
        )
        return {
            "route_decision": "end",
            "messages": [
                AIMessage(
                    content=(
                        "I couldn't register that condition because it looks "
                        "malformed. Please rephrase your request."
                    )
                )
            ],
        }

    trigger_condition_dict = trigger_condition.to_dict()

    # Enrich price triggers with chain
    if trigger_condition.is_price_trigger():
        slots = first_conditional.get("slots", {}) or {}
        chain = trigger_condition_dict.get("chain") or slots.get("chain")
        if chain:
            trigger_condition_dict["chain"] = chain

    # Handle time-based execution dates
    if trigger_condition.type.value == "time_at":
        if not trigger_condition_dict.get("execute_at"):
            now = datetime.now(tz=timezone.utc)
            delay_seconds = trigger_condition_dict.get("delay_seconds")
            if delay_seconds:
                execute_at = now + timedelta(seconds=int(delay_seconds))
                trigger_condition_dict["execute_at"] = execute_at.isoformat()
            elif trigger_condition.schedule:
                try:
                    delta = trigger_condition.schedule.to_timedelta()
                    execute_at = now + delta
                    trigger_condition_dict["execute_at"] = execute_at.isoformat()
                except Exception:
                    pass

    if action_intents:
        payload_intents = action_intents
    else:
        payload_intents = [
            {k: v for k, v in i.items() if k != "condition"}
            for i in conditional_intents
        ]

    current_condition_hash = _hash_payload(trigger_condition_dict)
    current_intents_hash = _hash_payload(payload_intents)

    existing_triggers = await registry.get_triggers_for_thread(thread_id)
    match_doc = None
    for t in existing_triggers:
        if t["status"] != "pending":
            continue

        stored_payload = t.get("payload") or {}
        stored_intents = stored_payload.get("intents") or []
        stored_resume_auth = stored_payload.get("resume_auth") or {}
        stored_token = stored_resume_auth.get("resume_token")

        if not stored_token:
            continue

        # Check if the stored trigger matches our current condition and intents
        if (
            _hash_payload(t["trigger_condition"]) == current_condition_hash
            and _hash_payload(stored_intents) == current_intents_hash
        ):
            match_doc = t
            break

    trigger_id: str
    resume_token: str | None = None

    if match_doc:
        # Already registered — use the existing record.
        trigger_id = match_doc["trigger_id"]
        stored_payload = match_doc.get("payload") or {}
        resume_auth = stored_payload.get("resume_auth") or {}
        resume_token = str(resume_auth.get("resume_token") or "").strip() or None

        condition_desc = _describe_condition(match_doc["trigger_condition"])
        logger.info(
            "wait_for_trigger_node: found existing identical pending trigger %s for thread %s.",
            trigger_id[:8],
            thread_id[:8],
        )
        interrupt_message = (
            "Your limit order is already registered.\n\n"
            f"Waiting for: {condition_desc}\n"
            f"Trigger ID: {trigger_id[:8]}…\n\n"
            "I will execute automatically when the condition is met. "
            "Type 'status' to see all your pending orders."
        )
    else:
        condition_desc = trigger_condition.description
        resume_token = _build_trigger_resume_token(
            thread_id=thread_id,
            user_id=user_id,
            trigger_condition_dict=trigger_condition_dict,
            payload_intents=payload_intents,
        )

        payload = {
            "intents": payload_intents,
            "resume_auth": {
                "resume_token": resume_token,
            },
        }

        try:
            trigger_id = await registry.register_trigger(
                user_id=user_id,
                thread_id=thread_id,
                trigger_condition=trigger_condition_dict,
                payload=payload,
            )
        except Exception as exc:
            logger.error(
                "wait_for_trigger_node: failed to register trigger: %s",
                exc,
                exc_info=True,
            )
            return {
                "route_decision": "end",
                "messages": [
                    AIMessage(
                        content=(
                            "⚠️ I couldn't save your trigger to the database. "
                            "Please try again in a moment."
                        )
                    )
                ],
            }

        logger.info(
            "wait_for_trigger_node: registered trigger %s for user %s "
            "(thread=%s, condition=%s).",
            trigger_id[:8],
            user_id,
            thread_id[:8],
            condition_desc,
        )

        interrupt_message = (
            "Limit order registered.\n\n"
            f"Condition: {condition_desc}\n"
            f"Action: {_describe_actions(payload_intents)}\n"
            f"Trigger ID: {trigger_id[:8]}…\n"
            "Expires in: 7 days\n\n"
            "You can close this chat. I will execute automatically when the "
            "condition is met and notify you of the result. "
            "Type 'status' to check your pending orders."
        )

    interrupt_value: dict[str, Any] = {
        "trigger_id": trigger_id,
        "message": interrupt_message,  # surfaced to the caller / bot layer
        "condition": trigger_condition_dict,
        "status": "waiting",
    }

    resume_payload: dict[str, Any] = interrupt(interrupt_value)
    condition_met: bool = bool(resume_payload.get("condition_met", False))
    resumed_trigger_id: str = resume_payload.get("trigger_id") or trigger_id
    submitted_resume_token = str(resume_payload.get("resume_token") or "").strip()
    matched_price: float | None = resume_payload.get("matched_price")
    asset: str = resume_payload.get("asset") or trigger_condition_dict.get("asset", "")
    trigger_type: str = resume_payload.get("trigger_type", "")
    trigger_fire_id: str | None = resume_payload.get("trigger_fire_id")

    trigger_doc: dict[str, Any] | None = None
    try:
        trigger_doc = await registry.get_trigger(resumed_trigger_id)
    except Exception as exc:
        logger.warning(
            "wait_for_trigger_node: could not load trigger %s for resume auth: %s",
            resumed_trigger_id[:8],
            exc,
        )
    stored_payload = (trigger_doc or {}).get("payload") or {}
    stored_payload_intents = list(stored_payload.get("intents") or payload_intents)
    stored_resume_auth = stored_payload.get("resume_auth") or {}
    stored_resume_token = str(stored_resume_auth.get("resume_token") or "").strip()

    if (
        not stored_resume_token
        or not submitted_resume_token
        or not secrets.compare_digest(submitted_resume_token, stored_resume_token)
        or not _validate_trigger_resume_token(
            resume_token=stored_resume_token,
            thread_id=thread_id,
            user_id=user_id,
            trigger_condition_dict=trigger_condition_dict,
            payload_intents=stored_payload_intents,
        )
    ):
        logger.warning(
            "wait_for_trigger_node: rejected trigger resume for %s due to invalid resume token.",
            resumed_trigger_id[:8],
        )
        return {
            "route_decision": "end",
            "messages": [
                AIMessage(
                    content=(
                        "The trigger resume request failed authentication, so I stopped "
                        "before executing anything. Please create the trigger again."
                    )
                )
            ],
        }

    if not condition_met:
        # Trigger was cancelled, expired, or the resume was a no-op.
        logger.info(
            "wait_for_trigger_node: trigger %s resumed with condition_met=False "
            "(thread=%s).",
            resumed_trigger_id[:8],
            thread_id[:8],
        )
        return {
            "trigger_id": resumed_trigger_id,
            "route_decision": "end",
            "messages": [
                AIMessage(
                    content=(
                        "Your pending order was cancelled or expired before the "
                        "condition was met. You can create a new one anytime."
                    )
                )
            ],
        }

    if matched_price is not None and asset:
        direction = "dropped below" if "below" in trigger_type else "rose above"
        target = trigger_condition_dict.get("target", "")
        fired_msg = (
            f"Condition met. {asset} {direction} ${target:,.2f} "
            f"(current: ${matched_price:,.4f}).\n\n"
            "Executing your pre-approved transaction now."
        )
    else:
        fired_msg = (
            "Trigger condition met. Executing your pre-approved transaction now."
        )

    if stored_payload_intents:
        resolved_intents = stored_payload_intents
    else:
        # Fallback: strip conditions from the current state intents
        resolved_intents = [
            {k: v for k, v in i.items() if k != "condition"} for i in intents
        ]

    logger.info(
        "wait_for_trigger_node: resuming thread %s with %d resolved intent(s). "
        "is_triggered_execution=True.",
        thread_id[:8],
        len(resolved_intents),
    )

    return {
        "intents": resolved_intents,
        "trigger_id": resumed_trigger_id,
        "trigger_fire_id": trigger_fire_id,
        "execution_id": (
            f"trigger:{resumed_trigger_id}:{trigger_fire_id}"
            if trigger_fire_id
            else f"trigger:{resumed_trigger_id}"
        ),
        "is_triggered_execution": True,
        "route_decision": "resolve",
        "messages": [AIMessage(content=fired_msg)],
        "reasoning_logs": [
            f"[TRIGGER] Condition met for trigger {resumed_trigger_id[:8]}. "
            f"Resuming with {len(resolved_intents)} intent(s). "
            f"Matched price: {matched_price}"
        ],
    }

def _describe_condition(condition_dict: dict) -> str:
    try:
        condition = TriggerCondition(**condition_dict)
        return condition.description
    except Exception:
        # Provide specific fallback info
        ctype = condition_dict.get("type", "unknown")
        asset = condition_dict.get("asset")
        target = condition_dict.get("target")
        if asset and target:
            return f"{ctype.replace('_', ' ')} alert for {asset} at ${target}"
        return f"custom {ctype.replace('_', ' ')} condition"


def _describe_actions(intents: list[dict]) -> str:
    if not intents:
        return "No actions queued"

    if len(intents) == 1:
        intent = intents[0]
        itype = intent.get("intent_type", "action")
        slots = intent.get("slots", {})

        if itype == "swap":
            token_in = (slots.get("token_in") or {}).get("symbol", "?")
            token_out = (slots.get("token_out") or {}).get("symbol", "?")
            amount = slots.get("amount", "?")
            chain = slots.get("chain", "?")
            return f"Swap {amount} {token_in} → {token_out} on {chain}"

        if itype == "bridge":
            token = (slots.get("token_in") or {}).get("symbol", "?")
            amount = slots.get("amount", "?")
            src = slots.get("chain", "?")
            dst = slots.get("target_chain", "?")
            return f"Bridge {amount} {token} from {src} → {dst}"

        if itype == "transfer":
            token = (slots.get("token") or {}).get("symbol", "?")
            amount = slots.get("amount", "?")
            recipient = str(slots.get("recipient", "?"))
            # Truncate long addresses for readability
            if len(recipient) > 12:
                recipient = recipient[:6] + "…" + recipient[-4:]
            chain = slots.get("chain", "?")
            return f"Transfer {amount} {token} to {recipient} on {chain}"

        return f"{itype.capitalize()} action"

    return f"{len(intents)} actions queued"
