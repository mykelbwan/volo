from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Sequence

from langchain_core.messages import BaseMessage, HumanMessage

from config.chains import get_chain_by_name
from config.solana_chains import get_solana_chain
from core.utils.balance_chains import (
    ALL_SUPPORTED_CHAIN_KEY,
    canonicalize_balance_chain,
)
from intent_hub.ontology.intent import Intent
from intent_hub.parser.validation import validate_intent

_AMOUNT_RE = re.compile(
    r"(?<![a-z0-9])(\d+(?:\.\d+)?)(?![a-z])", re.IGNORECASE
)
_EVM_ADDRESS_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
_SOLANA_ADDRESS_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
_TOKEN_WORD_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9._-]{0,31}")

_NOISE_WORDS = {
    "a",
    "an",
    "again",
    "as",
    "before",
    "chain",
    "different",
    "exact",
    "first",
    "it",
    "maybe",
    "me",
    "network",
    "not",
    "one",
    "on",
    "same",
    "second",
    "sure",
    "know",
    "dont",
    "idk",
    "that",
    "the",
    "this",
    "token",
    "to",
    "use",
    "wallet",
    "with",
}
_REFERENCE_PHRASES = (
    "again",
    "same token",
    "same chain",
    "same one",
    "same as before",
    "that token",
    "that chain",
    "that one",
    "that one again",
    "this token",
    "this chain",
    "the first one",
    "the second one",
    "before",
)
_UNCERTAIN_PHRASES = (
    "not sure",
    "dont know",
    "don't know",
    "idk",
    "not certain",
    "whatever",
    "whichever",
    "not sure yet",
)


@dataclass(frozen=True)
class FollowUpResolution:
    kind: str
    slot_updates: dict[str, Any] = field(default_factory=dict)
    prompt: str | None = None
    reason: str | None = None


def _content_str(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(str(part) for part in content)
    return str(content)


def latest_user_text(messages: Sequence[BaseMessage]) -> str:
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            return _content_str(message.content).strip()
    return ""


def _is_present(slot_name: str, value: Any) -> bool:
    if slot_name in {"token_in", "token_out", "token"}:
        if isinstance(value, dict):
            return bool(str(value.get("symbol") or "").strip())
        return bool(str(value or "").strip())
    if value is None:
        return False
    if isinstance(value, str):
        return bool(str(value).strip())
    return True


def _copy_slot_value(value: Any) -> Any:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, list):
        return list(value)
    return value


def _candidate_chain_values(text: str) -> list[str]:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if not normalized:
        return []

    candidates = [normalized]

    simplified = re.sub(r"\b(?:chain|network)\b", " ", normalized)
    simplified = re.sub(r"\s+", " ", simplified).strip()
    if simplified and simplified not in candidates:
        candidates.append(simplified)

    tokens = [token for token in re.findall(r"[a-z0-9]+", normalized) if token]
    for start in range(len(tokens)):
        for width in (3, 2, 1):
            end = start + width
            if end > len(tokens):
                continue
            phrase = " ".join(tokens[start:end]).strip()
            if not phrase or phrase in candidates:
                continue
            if all(token in _NOISE_WORDS for token in phrase.split()):
                continue
            candidates.append(phrase)

    return candidates


def _extract_chain(text: str, *, allow_all_supported: bool) -> str | None:
    for candidate in _candidate_chain_values(text):
        canonical = canonicalize_balance_chain(candidate)
        if canonical and (
            allow_all_supported or canonical != ALL_SUPPORTED_CHAIN_KEY
        ):
            return canonical

        try:
            return get_chain_by_name(candidate).name.strip().lower()
        except Exception:
            pass

        try:
            return get_solana_chain(candidate).network.strip().lower()
        except Exception:
            continue

    return None


def _extract_amount(text: str) -> float | None:
    match = _AMOUNT_RE.search(str(text or ""))
    if not match:
        return None
    try:
        return float(match.group(1))
    except (TypeError, ValueError):
        return None


def _extract_symbol(text: str) -> dict[str, str] | None:
    matches = _TOKEN_WORD_RE.findall(str(text or ""))
    if not matches:
        return None

    for raw in reversed(matches):
        cleaned = raw.strip().lower()
        if not cleaned or cleaned in _NOISE_WORDS:
            continue
        if cleaned.endswith("'s") and cleaned[:-2] in _NOISE_WORDS:
            continue
        return {"symbol": raw.strip().upper()}

    return None


def _extract_recipient(text: str) -> str | None:
    evm_match = _EVM_ADDRESS_RE.search(str(text or ""))
    if evm_match:
        return evm_match.group(0)

    solana_match = _SOLANA_ADDRESS_RE.search(str(text or ""))
    if solana_match:
        return solana_match.group(0)

    return None


def extract_slot_updates(
    intent: Intent,
    messages: Sequence[BaseMessage],
    token_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    text = latest_user_text(messages)
    if not text:
        return {}

    updates: dict[str, Any] = {}

    for slot_name in intent.missing_slots:
        if slot_name in {"chain", "target_chain"}:
            chain = _extract_chain(
                text,
                allow_all_supported=intent.intent_type == "balance",
            )
            if chain:
                updates[slot_name] = chain
            continue

        if slot_name == "amount":
            amount = _extract_amount(text)
            if amount is not None:
                updates[slot_name] = amount
            continue

        if slot_name in {"token", "token_in", "token_out"}:
            symbol = _extract_symbol(text)
            if symbol:
                updates[slot_name] = symbol
            continue

        if slot_name == "recipient":
            recipient = _extract_recipient(text)
            if recipient:
                updates[slot_name] = recipient

    return updates


def _normalize_follow_up_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").strip().lower()).strip()


def _looks_uncertain(text: str) -> bool:
    normalized = _normalize_follow_up_text(text)
    if not normalized:
        return False
    hay = f" {normalized} "
    if any(f" {phrase} " in hay for phrase in _UNCERTAIN_PHRASES):
        return True
    return " maybe " in hay and len(normalized.split()) <= 6


def _looks_referential(text: str) -> bool:
    normalized = _normalize_follow_up_text(text)
    if not normalized:
        return False
    hay = f" {normalized} "
    return any(f" {phrase} " in hay for phrase in _REFERENCE_PHRASES)


def _primary_missing_slot(intent: Intent) -> str | None:
    missing = list(intent.missing_slots or [])
    return missing[0] if missing else None


def _slot_label(slot_name: str | None) -> str:
    labels = {
        "token": "token",
        "token_in": "token",
        "token_out": "output token",
        "amount": "amount",
        "chain": "chain",
        "target_chain": "destination chain",
        "recipient": "wallet address",
    }
    return labels.get(str(slot_name or ""), "missing detail")


def _slot_examples(slot_name: str | None) -> str:
    examples = {
        "token": "USDC, ETH, or STT",
        "token_in": "USDC, ETH, or STT",
        "token_out": "USDC, NIA, or WETH",
        "amount": "0.5 or 10",
        "chain": "Base, Ethereum, or Somnia",
        "target_chain": "Base, Ethereum, or Solana",
        "recipient": "0xabc... or a Solana address",
    }
    return examples.get(str(slot_name or ""), "the exact value")


def _known_slot_summary(intent: Intent) -> str:
    slots = dict(intent.slots or {})
    parts: list[str] = []

    token_in = slots.get("token_in")
    if isinstance(token_in, dict) and token_in.get("symbol"):
        parts.append(f"input token {token_in['symbol']}")

    token_out = slots.get("token_out")
    if isinstance(token_out, dict) and token_out.get("symbol"):
        parts.append(f"output token {token_out['symbol']}")

    token = slots.get("token")
    if isinstance(token, dict) and token.get("symbol"):
        parts.append(f"token {token['symbol']}")

    if slots.get("amount") is not None:
        parts.append(f"amount {slots['amount']}")

    if slots.get("chain"):
        parts.append(f"chain {slots['chain']}")

    if slots.get("target_chain"):
        parts.append(f"destination chain {slots['target_chain']}")

    if slots.get("recipient"):
        parts.append("recipient already set")

    if not parts:
        return ""
    return "I already have " + ", ".join(parts) + "."


def clarification_state(
    intent: Intent,
    *,
    attempt_count: int,
    last_resolution_error: str | None,
) -> dict[str, Any]:
    return {
        "slot_name": _primary_missing_slot(intent),
        "question_type": "missing_slot",
        "attempt_count": max(0, int(attempt_count)),
        "last_resolution_error": last_resolution_error,
    }


def clarification_recovery_prompt(
    intent: Intent,
    *,
    attempt_count: int,
    reason: str,
) -> str:
    slot_name = _primary_missing_slot(intent)
    slot_label = _slot_label(slot_name)
    examples = _slot_examples(slot_name)
    prefix = {
        "uncertain": f"I still need the {slot_label} for this {intent.intent_type}.",
        "reference": f"I can't safely infer the {slot_label} from that reference.",
        "unknown": f"I still need the exact {slot_label} for this {intent.intent_type}.",
    }.get(reason, f"I still need the {slot_label}.")

    prompt = f"{prefix} Please name it directly, like {examples}."
    if attempt_count >= 1:
        summary = _known_slot_summary(intent)
        if summary:
            prompt = f"{prompt} {summary}"
    return prompt


def resolve_pending_follow_up(
    intent: Intent,
    messages: Sequence[BaseMessage],
    token_registry: dict[str, Any] | None = None,
    pending_clarification: dict[str, Any] | None = None,
) -> FollowUpResolution:
    updates = extract_slot_updates(intent, messages, token_registry=token_registry)
    if updates:
        return FollowUpResolution(kind="slot_update", slot_updates=updates)

    text = latest_user_text(messages)
    if not text:
        return FollowUpResolution(kind="unknown")

    attempt_count = 0
    if isinstance(pending_clarification, dict):
        try:
            attempt_count = int(pending_clarification.get("attempt_count") or 0)
        except Exception:
            attempt_count = 0

    if _looks_uncertain(text):
        return FollowUpResolution(
            kind="uncertain",
            prompt=clarification_recovery_prompt(
                intent,
                attempt_count=attempt_count,
                reason="uncertain",
            ),
            reason="uncertain",
        )

    if _looks_referential(text):
        return FollowUpResolution(
            kind="reference",
            prompt=clarification_recovery_prompt(
                intent,
                attempt_count=attempt_count,
                reason="reference",
            ),
            reason="reference",
        )

    return FollowUpResolution(
        kind="unknown",
        prompt=clarification_recovery_prompt(
            intent,
            attempt_count=attempt_count,
            reason="unknown",
        ),
        reason="unknown",
    )


def merge_pending_intent(
    pending_intent: Intent,
    *,
    parsed_intent: Intent | None,
    extracted_slots: dict[str, Any] | None,
    latest_reply: str,
    token_registry: dict[str, Any],
) -> Intent:
    merged = pending_intent.model_copy(deep=True)
    slots = dict(merged.slots or {})
    missing_slots = list(merged.missing_slots or [])

    for slot_name in missing_slots:
        if parsed_intent is not None:
            parsed_value = (parsed_intent.slots or {}).get(slot_name)
            if _is_present(slot_name, parsed_value):
                slots[slot_name] = _copy_slot_value(parsed_value)
                continue
        extracted_value = (extracted_slots or {}).get(slot_name)
        if _is_present(slot_name, extracted_value):
            slots[slot_name] = _copy_slot_value(extracted_value)

    merged.slots = slots
    if latest_reply:
        merged.raw_input = f"{pending_intent.raw_input}\nFollow-up: {latest_reply}"

    return validate_intent(merged, token_registry)
