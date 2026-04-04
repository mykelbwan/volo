from intent_hub.ontology.intent import ExecutionPlan, Intent
from intent_hub.registry.token_service import (
    get_address_for_chain_async,
    get_token_data_async,
)
from intent_hub.resolver.common import (
    is_dynamic_marker,
    require_amount,
    require_complete_intent,
    resolve_token_on_chain,
    symbol_from_slot,
    unresolved_addresses_error,
)
from intent_hub.utils.amount import to_wei
from config.chains import get_chain_by_name
from core.token_security.registry_lookup import get_registry_decimals_by_address_async
from intent_hub.utils.messages import format_with_recovery, require_non_empty_str

_NATIVE_ADDRESS = "0x0000000000000000000000000000000000000000"


async def resolve_swap(intent: Intent) -> ExecutionPlan:
    require_complete_intent(intent)

    slots = intent.slots or {}
    token_in_symbol = symbol_from_slot(slots.get("token_in"))
    token_out_symbol = symbol_from_slot(slots.get("token_out"))
    if not token_in_symbol or not token_out_symbol:
        raise ValueError(
            format_with_recovery(
                "Swap token symbols are missing or invalid",
                "provide both source and destination token symbols and retry",
            )
        )

    amount_val = slots.get("amount")
    chain = require_non_empty_str(slots.get("chain"), field="chain").lower()

    token_in_resolution = await resolve_token_on_chain(
        token_in_symbol,
        chain,
        get_token_data_fn=get_token_data_async,
        get_address_for_chain_fn=get_address_for_chain_async,
        allow_amount_prefixed=True,
    )
    token_out_resolution = await resolve_token_on_chain(
        token_out_symbol,
        chain,
        get_token_data_fn=get_token_data_async,
        get_address_for_chain_fn=get_address_for_chain_async,
        allow_amount_prefixed=False,
    )

    token_in_data = token_in_resolution.token_data if token_in_resolution else {}
    token_out_data = token_out_resolution.token_data if token_out_resolution else {}
    token_in_address = token_in_resolution.address if token_in_resolution else None
    token_out_address = token_out_resolution.address if token_out_resolution else None
    token_in_symbol_resolved = token_in_resolution.symbol if token_in_resolution else token_in_symbol.upper()
    token_out_symbol_resolved = token_out_resolution.symbol if token_out_resolution else token_out_symbol.upper()
    if amount_val is None and token_in_resolution and token_in_resolution.inferred_amount:
        amount_val = token_in_resolution.inferred_amount

    try:
        chain_cfg = get_chain_by_name(chain)
    except Exception:
        raise ValueError(f"Invalid chain: {chain}")

    if not token_in_address and token_in_symbol_resolved.upper() == chain_cfg.native_symbol.upper():
        token_in_address = _NATIVE_ADDRESS
    if not token_out_address and token_out_symbol_resolved.upper() == chain_cfg.native_symbol.upper():
        token_out_address = _NATIVE_ADDRESS

    if not token_in_address or not token_out_address:
        raise unresolved_addresses_error(
            [token_in_symbol_resolved, token_out_symbol_resolved],
            chain_context=chain,
        )
    amount_val = require_amount(amount_val, action="swap")

    decimals = token_in_data.get("decimals")
    if decimals is None:
        if str(token_in_address).lower() == _NATIVE_ADDRESS:
            decimals = 18
        else:
            decimals = await get_registry_decimals_by_address_async(
                token_in_address, chain_cfg.chain_id
            )

    if decimals is None:
        raise ValueError(f"Could not resolve decimals for {token_in_symbol_resolved}")

    decimals = int(decimals)
    if is_dynamic_marker(amount_val):
        amount_in_wei = amount_val
    else:
        amount_in_wei = str(to_wei(amount_val, decimals))

    return ExecutionPlan(
        intent_type="swap",
        chain=chain,
        parameters={
            "token_in_symbol": token_in_symbol_resolved,
            "token_out_symbol": token_out_symbol_resolved,
            "token_in_address": token_in_address,
            "token_out_address": token_out_address,
            "amount_in": amount_val,
            "amount_in_wei": amount_in_wei,
            "sub_org_id": "{{SUB_ORG_ID}}",
            "sender": "{{SENDER_ADDRESS}}",
        },
        constraints=intent.constraints,
    )
