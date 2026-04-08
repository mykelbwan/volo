from typing import Dict, Optional

from core.utils.balance_chains import (
    ALL_SUPPORTED_CHAIN_KEY,
    canonicalize_balance_chain,
    is_all_supported_chain_request,
)
from intent_hub.ontology.intent import Intent, IntentStatus


def _build_alias_lookup(token_registry: Dict) -> dict[str, str]:
    alias_lookup: dict[str, str] = {}
    for canonical, data in token_registry.items():
        canonical_symbol = str(canonical).upper()
        if not isinstance(data, dict):
            continue
        aliases = data.get("aliases", [])
        if not isinstance(aliases, list):
            continue
        for alias in aliases:
            alias_text = str(alias or "").strip().lower()
            if alias_text:
                alias_lookup[alias_text] = canonical_symbol
    return alias_lookup


def _resolve_symbol(
    symbol: Optional[str],
    token_registry: Dict,
    *,
    alias_lookup: dict[str, str],
) -> Optional[str]:
    if not symbol:
        return None

    symbol_upper = symbol.upper()
    # Direct match
    if symbol_upper in token_registry:
        return symbol_upper

    # Alias match (case-insensitive)
    symbol_lower = symbol.lower()
    alias_match = alias_lookup.get(symbol_lower)
    if alias_match:
        return alias_match

    return None


def _slot_symbol(slot: object) -> Optional[str]:
    if isinstance(slot, dict):
        symbol = slot.get("symbol")
    else:
        symbol = slot
    text = str(symbol or "").strip()
    return text or None


def validate_intent(intent: Intent, token_registry: Dict) -> Intent:
    alias_lookup = _build_alias_lookup(token_registry)

    if intent.intent_type == "balance":
        slots = dict(intent.slots or {})
        raw_chain = slots.get("chain")
        canonical_chain = canonicalize_balance_chain(str(raw_chain)) if raw_chain else None
        scope = str(slots.get("scope") or "").strip().lower()
        if canonical_chain:
            slots["chain"] = canonical_chain
        if (
            scope == ALL_SUPPORTED_CHAIN_KEY
            or is_all_supported_chain_request(str(raw_chain) if raw_chain else "")
            or canonical_chain == ALL_SUPPORTED_CHAIN_KEY
        ):
            slots["scope"] = ALL_SUPPORTED_CHAIN_KEY
            slots["chain"] = ALL_SUPPORTED_CHAIN_KEY

        missing_slots = []
        if not slots.get("chain"):
            missing_slots.append("chain")

        intent.missing_slots = missing_slots
        if not missing_slots:
            intent.status = IntentStatus.COMPLETE
            intent.clarification_prompt = None
        else:
            intent.status = IntentStatus.INCOMPLETE
            intent.clarification_prompt = (
                "Which network (e.g., Somnia, Ethereum, Base) "
                "would you like to check your balances on?"
            )
        intent.slots = slots
        return intent

    if intent.intent_type not in ["swap", "bridge", "transfer", "unwrap"]:
        return intent

    slots = dict(intent.slots or {})

    token_slot_keys = ["token_in", "token_out"]
    if intent.intent_type == "transfer":
        token_slot_keys = ["token"]
    elif intent.intent_type == "bridge":
        token_slot_keys = ["token_in"]
    elif intent.intent_type == "unwrap":
        token_slot_keys = ["token"]

    for token_key in token_slot_keys:
        token_value = slots.get(token_key)
        if not token_value:
            continue
        symbol = (
            token_value.get("symbol")
            if isinstance(token_value, dict)
            else str(token_value)
        )
        if symbol:
            symbol_str = str(symbol)
            canonical = _resolve_symbol(
                symbol_str, token_registry, alias_lookup=alias_lookup
            )
            if canonical:
                slots[token_key] = {"symbol": canonical}
            else:
                slots[token_key] = {"symbol": symbol_str.upper()}

    # Re-calculate missing slots
    missing_slots = []

    if intent.intent_type == "transfer":
        token = slots.get("token")
        if not _slot_symbol(token):
            missing_slots.append("token")
    elif intent.intent_type == "unwrap":
        token = slots.get("token")
        if not _slot_symbol(token):
            missing_slots.append("token")
        if not slots.get("chain"):
            missing_slots.append("chain")
    else:
        token_in = slots.get("token_in")
        if not _slot_symbol(token_in):
            missing_slots.append("token_in")

        token_out = slots.get("token_out")
        if intent.intent_type == "swap" and not _slot_symbol(token_out):
            missing_slots.append("token_out")

    if slots.get("amount") is None and intent.intent_type != "unwrap":
        missing_slots.append("amount")

    if intent.intent_type in {"swap", "bridge"}:
        if not slots.get("chain"):
            missing_slots.append("chain")
    if intent.intent_type == "bridge":
        if not slots.get("target_chain"):
            missing_slots.append("target_chain")
    elif intent.intent_type == "transfer":
        if not slots.get("recipient"):
            missing_slots.append("recipient")
        if not slots.get("chain"):
            missing_slots.append("chain")

    intent.missing_slots = missing_slots
    intent.slots = slots

    if not missing_slots:
        intent.status = IntentStatus.COMPLETE
        intent.clarification_prompt = None
    else:
        intent.status = IntentStatus.INCOMPLETE

        # Natural language prompts
        if intent.intent_type == "unwrap" and "token" in missing_slots:
            intent.clarification_prompt = "Which native token would you like to unwrap?"
        elif "token_in" in missing_slots or "token" in missing_slots:
            intent.clarification_prompt = "Which token would you like to use?"
        elif "amount" in missing_slots:
            token_slot = slots.get("token_in") or slots.get("token")
            token_in_symbol = _slot_symbol(token_slot) or "the token"
            intent.clarification_prompt = (
                f"How much {token_in_symbol} would you like to {intent.intent_type}?"
            )
        elif "token_out" in missing_slots:
            intent.clarification_prompt = "Which token would you like to receive?"
        elif "recipient" in missing_slots:
            intent.clarification_prompt = "Which wallet address should receive it?"
        elif "chain" in missing_slots:
            intent.clarification_prompt = "Which chain?"
        elif "target_chain" in missing_slots:
            intent.clarification_prompt = "Which chain are you bridging to?"

    return intent
