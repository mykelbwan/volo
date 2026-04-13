from __future__ import annotations

from config.chains import find_chain_by_id
from core.chains.catalog import resolve_chain
from core.identity.wallet_bindings import wallet_markers_for_family
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
    requested_chain = require_non_empty_str(slots.get("chain"), field="chain").lower()
    chain_entry = resolve_chain(requested_chain)
    if chain_entry is None:
        raise ValueError(f"Invalid chain: {requested_chain}")
    chain_name = str(chain_entry.key).strip().lower()

    if chain_entry.family != "evm":
        raise ValueError(
            format_with_recovery(
                (
                    f"Unwrap is only supported on EVM chains "
                    f"(received {chain_entry.display_name})"
                ),
                ("use an EVM chain (e.g Base, Ethereum, Arbitrum) and retry"),
            )
        )
    wallet_markers = wallet_markers_for_family(chain_entry.family)

    requested_symbol = symbol_from_slot(slots.get("token"))
    if not requested_symbol:
        raise ValueError(
            format_with_recovery(
                "Unwrap token is missing",
                "provide the native token name to unwrap (for example ETH) and retry",
            )
        )

    chain = find_chain_by_id(int(chain_entry.chain_id))
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
        "sub_org_id": wallet_markers.sub_org_marker,
        "sender": wallet_markers.sender_marker,
    }
    if amount is not None:
        parameters["amount"] = amount

    return ExecutionPlan(
        intent_type="unwrap",
        chain=chain_name,
        parameters=parameters,
        constraints=intent.constraints,
    )
