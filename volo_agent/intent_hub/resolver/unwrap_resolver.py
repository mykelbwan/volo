from __future__ import annotations

from config.chains import get_chain_by_name
from intent_hub.ontology.intent import ExecutionPlan, Intent
from intent_hub.resolver.common import require_complete_intent, symbol_from_slot
from intent_hub.utils.messages import format_with_recovery, require_non_empty_str


def _accepted_unwrap_symbols(native_symbol: str) -> set[str]:
    symbol = str(native_symbol or "").strip().upper()
    if not symbol:
        return set()
    return {
        symbol,
        f"W{symbol}",
        f"WRAPPED{symbol}",
        f"WRAPPED_{symbol}",
    }


async def resolve_unwrap(intent: Intent) -> ExecutionPlan:
    require_complete_intent(intent)

    slots = intent.slots or {}
    chain_name = require_non_empty_str(slots.get("chain"), field="chain").lower()
    requested_symbol = symbol_from_slot(slots.get("token"))
    if not requested_symbol:
        raise ValueError(
            format_with_recovery(
                "Unwrap token is missing",
                "provide the native token name to unwrap (for example ETH) and retry",
            )
        )

    chain = get_chain_by_name(chain_name)
    wrapped_native = str(getattr(chain, "wrapped_native", "") or "").strip()
    if not wrapped_native:
        raise ValueError(
            format_with_recovery(
                f"Wrapped native token is not configured for {chain.name}",
                "use another chain that supports wrapped native unwrap",
            )
        )

    requested_symbol_upper = str(requested_symbol).strip().upper()
    native_symbol = str(chain.native_symbol or "").strip().upper()
    if requested_symbol_upper not in _accepted_unwrap_symbols(native_symbol):
        raise ValueError(
            format_with_recovery(
                (
                    f"Unwrap token must match the chain native token on {chain.name} "
                    f"(expected {native_symbol})"
                ),
                f"use '{native_symbol}' and retry",
            )
        )

    amount = slots.get("amount")
    parameters = {
        "token_symbol": native_symbol,
        "token_address": wrapped_native,
        "chain": chain_name,
        "sub_org_id": "{{SUB_ORG_ID}}",
        "sender": "{{SENDER_ADDRESS}}",
    }
    if amount is not None:
        parameters["amount"] = amount

    return ExecutionPlan(
        intent_type="unwrap",
        chain=chain_name,
        parameters=parameters,
        constraints=intent.constraints,
    )
