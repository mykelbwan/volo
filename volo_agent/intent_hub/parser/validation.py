import logging
from typing import Dict, Optional

from core.chains.chain_canonicalization_parity import (
    SCOPE_ACTION,
    compare_chain_canonicalization,
)
from core.utils.balance_chains import (
    ALL_SUPPORTED_CHAIN_KEY,
    canonicalize_balance_chain,
    is_all_supported_chain_request,
)
from core.utils.user_feedback import chain_ambiguity_prompt
from intent_hub.ontology.intent import Intent, IntentStatus
from intent_hub.parser.chain_inference import (
    DEFAULT_CHAIN_INFERENCE_ENGINE,
    ChainInferenceContext,
    is_testnet_chain,
    should_include_testnet_candidates,
)

logger = logging.getLogger(__name__)
_CHAIN_AMBIGUITY_SLOT_KEY = "_chain_ambiguity"


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


def _parity_compare_action_chain_slots(
    intent_type: str,
    slots: dict[str, object],
) -> None:
    if intent_type not in {"swap", "bridge", "transfer", "unwrap"}:
        return

    for slot_name in ("chain", "target_chain"):
        raw_value = slots.get(slot_name)
        raw_text = str(raw_value or "").strip()
        if not raw_text:
            continue
        comparison = compare_chain_canonicalization(raw_text, scope=SCOPE_ACTION)
        if comparison.matched:
            continue
        logger.info(
            "chain_parity_mismatch scope=%s slot=%s raw=%r legacy=%r catalog=%r legacy_error=%r catalog_error=%r",
            comparison.scope,
            slot_name,
            comparison.raw_value,
            comparison.legacy_value,
            comparison.catalog_value,
            comparison.legacy_error,
            comparison.catalog_error,
        )


def _canonicalize_action_chain_slots_with_catalog(
    intent_type: str,
    slots: dict[str, object],
) -> None:
    if intent_type not in {"swap", "bridge", "transfer", "unwrap"}:
        return

    for slot_name in ("chain", "target_chain"):
        raw_value = slots.get(slot_name)
        raw_text = str(raw_value or "").strip()
        if not raw_text:
            continue
        comparison = compare_chain_canonicalization(raw_text, scope=SCOPE_ACTION)
        if comparison.catalog_value:
            slots[slot_name] = comparison.catalog_value


def _chain_disambiguation_token_slot(intent_type: str) -> str | None:
    if intent_type in {"swap", "bridge"}:
        return "token_in"
    if intent_type in {"transfer", "unwrap"}:
        return "token"
    return None


def _chain_options_from_token_registry(
    token_symbol: str,
    token_registry: Dict,
    *,
    alias_lookup: dict[str, str],
    include_testnets: bool,
) -> list[str]:
    symbol = str(token_symbol or "").strip()
    if not symbol:
        return []

    canonical_symbol = (
        _resolve_symbol(symbol, token_registry, alias_lookup=alias_lookup)
        or symbol.upper()
    )
    token_data = token_registry.get(canonical_symbol)
    if not isinstance(token_data, dict):
        return []

    chains = token_data.get("chains")
    if isinstance(chains, dict):
        chain_values = list(chains.keys())
    elif isinstance(chains, (list, tuple, set)):
        chain_values = list(chains)
    else:
        return []

    options: list[str] = []
    seen: set[str] = set()
    for item in chain_values:
        raw = str(item or "").strip()
        if not raw:
            continue
        comparison = compare_chain_canonicalization(raw, scope=SCOPE_ACTION)
        normalized = str(comparison.catalog_value or raw).strip().lower()
        if not normalized:
            continue
        if not include_testnets and is_testnet_chain(normalized):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        options.append(normalized)
    return options


def _build_chain_ambiguity_payload(
    *,
    intent_type: str,
    missing_slots: list[str],
    slots: dict[str, object],
    token_registry: Dict,
    alias_lookup: dict[str, str],
    raw_input: str | None,
) -> dict[str, object] | None:
    if intent_type not in {"swap", "bridge", "transfer", "unwrap"}:
        return None

    target_slot = None
    if "chain" in missing_slots:
        target_slot = "chain"
    elif "target_chain" in missing_slots:
        target_slot = "target_chain"
    if target_slot is None:
        return None

    token_slot_name = _chain_disambiguation_token_slot(intent_type)
    if not token_slot_name:
        return None
    token_symbol = _slot_symbol(slots.get(token_slot_name))
    if not token_symbol:
        return None

    include_testnets = should_include_testnet_candidates(
        raw_input=raw_input,
        slots=slots,
    )
    options = _chain_options_from_token_registry(
        token_symbol,
        token_registry,
        alias_lookup=alias_lookup,
        include_testnets=include_testnets,
    )
    if len(options) < 2:
        return None

    return {
        "slot_name": target_slot,
        "token_symbol": str(token_symbol).strip().upper(),
        "chain_options": options,
    }


def _chain_ambiguity_prompt_args(
    payload: dict[str, object] | None,
    *,
    slot_name: str,
) -> tuple[str | None, object | None]:
    if not isinstance(payload, dict):
        return None, None
    if payload.get("slot_name") != slot_name:
        return None, None

    token_symbol = payload.get("token_symbol")
    typed_token_symbol = token_symbol if isinstance(token_symbol, str) else None
    return typed_token_symbol, payload.get("chain_options")


def _apply_chain_inference(
    *,
    intent_type: str,
    slots: dict[str, object],
    token_registry: Dict,
    alias_lookup: dict[str, str],
    raw_input: str | None,
) -> None:
    for slot_name in ("chain", "target_chain"):
        if str(slots.get(slot_name) or "").strip():
            continue
        decision = DEFAULT_CHAIN_INFERENCE_ENGINE.infer(
            ChainInferenceContext(
                intent_type=intent_type,
                slot_name=slot_name,
                slots=slots,
                token_registry=token_registry,
                alias_lookup=alias_lookup,
                raw_input=raw_input,
            )
        )
        if decision.chain:
            slots[slot_name] = decision.chain


def validate_intent(intent: Intent, token_registry: Dict) -> Intent:
    alias_lookup = _build_alias_lookup(token_registry)

    if intent.intent_type == "balance":
        slots = dict(intent.slots or {})
        raw_chain = slots.get("chain")
        canonical_chain = (
            canonicalize_balance_chain(str(raw_chain)) if raw_chain else None
        )
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
    slots.pop(_CHAIN_AMBIGUITY_SLOT_KEY, None)
    # Catalog cutover for action-intent chain slots.
    _canonicalize_action_chain_slots_with_catalog(intent.intent_type, slots)
    # Keep parity comparison logs as a safety signal during rollout.
    _parity_compare_action_chain_slots(intent.intent_type, slots)

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

    _apply_chain_inference(
        intent_type=intent.intent_type,
        slots=slots,
        token_registry=token_registry,
        alias_lookup=alias_lookup,
        raw_input=intent.raw_input,
    )

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

    chain_ambiguity_payload = _build_chain_ambiguity_payload(
        intent_type=intent.intent_type,
        missing_slots=missing_slots,
        slots=slots,
        token_registry=token_registry,
        alias_lookup=alias_lookup,
        raw_input=intent.raw_input,
    )
    if chain_ambiguity_payload:
        slots[_CHAIN_AMBIGUITY_SLOT_KEY] = chain_ambiguity_payload

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
            token_symbol, chain_options = _chain_ambiguity_prompt_args(
                chain_ambiguity_payload,
                slot_name="chain",
            )
            prompt = chain_ambiguity_prompt(
                token_symbol=token_symbol,
                chain_options=chain_options,
                slot_name="chain",
            )
            intent.clarification_prompt = prompt or "Which chain?"
        elif "target_chain" in missing_slots:
            token_symbol, chain_options = _chain_ambiguity_prompt_args(
                chain_ambiguity_payload,
                slot_name="target_chain",
            )
            prompt = chain_ambiguity_prompt(
                token_symbol=token_symbol,
                chain_options=chain_options,
                slot_name="target_chain",
            )
            intent.clarification_prompt = prompt or "Which chain are you bridging to?"

    return intent
