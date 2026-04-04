from __future__ import annotations

from typing import List

from intent_hub.ontology.intent import Intent, IntentStatus


def _symbol_from_slot(slot: object) -> str | None:
    if isinstance(slot, dict):
        sym = slot.get("symbol")
        return str(sym) if sym else None
    if slot:
        return str(slot)
    return None


def _set_symbol_slot(slot_container: dict, key: str, symbol: str | None) -> None:
    if not symbol:
        return
    slot_container[key] = {"symbol": str(symbol)}


def _output_marker(step_id: str) -> str:
    return f"{{{{OUTPUT_OF:{step_id}}}}}"


def _balance_marker(step_id: str, symbol: str) -> str:
    return f"{{{{BALANCE_OF:{step_id}:{symbol}}}}}"


def _required_slots(intent: Intent) -> list[str]:
    t = intent.intent_type
    if t == "swap":
        return ["token_in", "token_out", "amount", "chain"]
    if t == "bridge":
        return ["token_in", "amount", "chain", "target_chain"]
    if t == "transfer":
        return ["token", "amount", "recipient", "chain"]
    if t == "unwrap":
        return ["token", "chain"]
    if t == "balance":
        return ["chain"]
    return []


def _missing_slots(intent: Intent) -> list[str]:
    slots = intent.slots or {}
    missing: list[str] = []
    for req in _required_slots(intent):
        if req in ("token_in", "token_out", "token"):
            token_slot = slots.get(req)
            if not token_slot or not _symbol_from_slot(token_slot):
                missing.append(req)
            continue
        if slots.get(req) is None:
            missing.append(req)
    return missing


def _refresh_status(intent: Intent) -> None:
    missing = _missing_slots(intent)
    intent.missing_slots = missing
    if not missing:
        intent.status = IntentStatus.COMPLETE
        intent.clarification_prompt = None
    else:
        intent.status = IntentStatus.INCOMPLETE


def apply_templates(intents: List[Intent]) -> List[Intent]:
    """
    Apply deterministic intent-to-plan templates for common multi-step flows.
    This fills missing slots using prior intent outputs when safe.
    """
    patched = [intent.model_copy(deep=True) for intent in intents]
    for intent in patched:
        _refresh_status(intent)

    for i in range(len(patched) - 1):
        first = patched[i]
        second = patched[i + 1]
        if first.status != IntentStatus.COMPLETE:
            break

        step_id = f"step_{i}"
        first_slots = first.slots or {}
        second_slots = second.slots or {}

        if first.intent_type == "swap" and second.intent_type == "bridge":
            if not second_slots.get("chain"):
                second_slots["chain"] = first_slots.get("chain")
            if not _symbol_from_slot(second_slots.get("token_in")):
                _set_symbol_slot(
                    second_slots,
                    "token_in",
                    _symbol_from_slot(first_slots.get("token_out")),
                )
            if second_slots.get("amount") is None:
                second_slots["amount"] = _output_marker(step_id)
            second.slots = second_slots
            _refresh_status(second)
            continue

        if first.intent_type == "bridge" and second.intent_type == "swap":
            if not second_slots.get("chain"):
                second_slots["chain"] = first_slots.get("target_chain")
            if not _symbol_from_slot(second_slots.get("token_in")):
                _set_symbol_slot(
                    second_slots,
                    "token_in",
                    _symbol_from_slot(first_slots.get("token_in")),
                )
            if second_slots.get("amount") is None:
                second_slots["amount"] = _output_marker(step_id)
            second.slots = second_slots
            _refresh_status(second)
            continue

        if first.intent_type == "swap" and second.intent_type == "transfer":
            if not second_slots.get("chain"):
                second_slots["chain"] = first_slots.get("chain")
            if not _symbol_from_slot(second_slots.get("token")):
                _set_symbol_slot(
                    second_slots,
                    "token",
                    _symbol_from_slot(first_slots.get("token_out")),
                )
            if second_slots.get("amount") is None:
                second_slots["amount"] = _output_marker(step_id)
            second.slots = second_slots
            _refresh_status(second)
            continue

        if first.intent_type == "bridge" and second.intent_type == "transfer":
            if not second_slots.get("chain"):
                second_slots["chain"] = first_slots.get("target_chain")
            if not _symbol_from_slot(second_slots.get("token")):
                _set_symbol_slot(
                    second_slots,
                    "token",
                    _symbol_from_slot(first_slots.get("token_in")),
                )
            if second_slots.get("amount") is None:
                second_slots["amount"] = _output_marker(step_id)
            second.slots = second_slots
            _refresh_status(second)
            continue

        if first.intent_type == "swap" and second.intent_type == "swap":
            if not second_slots.get("chain"):
                second_slots["chain"] = first_slots.get("chain")
            if not _symbol_from_slot(second_slots.get("token_in")):
                _set_symbol_slot(
                    second_slots,
                    "token_in",
                    _symbol_from_slot(first_slots.get("token_out")),
                )
            if second_slots.get("amount") is None:
                second_slots["amount"] = _output_marker(step_id)
            second.slots = second_slots
            _refresh_status(second)
            continue

        if first.intent_type == "balance" and second.intent_type == "swap":
            if not second_slots.get("chain"):
                second_slots["chain"] = first_slots.get("chain")
            token_in_symbol = _symbol_from_slot(second_slots.get("token_in"))
            if second_slots.get("amount") is None and token_in_symbol:
                second_slots["amount"] = _balance_marker(step_id, token_in_symbol)
            second.slots = second_slots
            _refresh_status(second)
            continue

    return patched


def can_apply_templates(intent_dicts: List[dict]) -> bool:
    """
    Return True if applying templates would result in all intents complete.
    """
    intents = [Intent(**data) for data in intent_dicts]
    patched = apply_templates(intents)
    return all(intent.status == IntentStatus.COMPLETE for intent in patched)
