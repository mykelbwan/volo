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


async def resolve_bridge(intent: Intent) -> ExecutionPlan:
    require_complete_intent(intent)

    slots = intent.slots or {}
    symbol = symbol_from_slot(slots.get("token_in"))
    source_chain = require_non_empty_str(slots.get("chain"), field="chain").lower()
    target_chain = require_non_empty_str(
        slots.get("target_chain"), field="target_chain"
    ).lower()

    if not symbol:
        raise ValueError(
            format_with_recovery(
                "Bridge token symbol is missing",
                "provide the token symbol to bridge and retry",
            )
        )

    amount_val = slots.get("amount")

    source_resolution = await resolve_token_on_chain(
        symbol,
        source_chain,
        get_token_data_fn=get_token_data_async,
        get_address_for_chain_fn=get_address_for_chain_async,
        allow_amount_prefixed=True,
    )
    if source_resolution is None:
        raise unresolved_addresses_error(
            [symbol],
            chain_context=f"{source_chain}/{target_chain}",
        )
    symbol_resolved = source_resolution.symbol
    token_data = source_resolution.token_data
    source_address = source_resolution.address
    if amount_val is None and source_resolution.inferred_amount is not None:
        amount_val = source_resolution.inferred_amount

    # If not on target chain, try discovering it there too
    target_address = await get_address_for_chain_async(token_data, target_chain)
    if not target_address:
        discovered_target = await get_token_data_async(symbol_resolved, target_chain)
        target_address = await get_address_for_chain_async(discovered_target, target_chain)

    if not source_address or not target_address:
        raise unresolved_addresses_error(
            [symbol_resolved],
            chain_context=f"{source_chain}/{target_chain}",
        )

    try:
        source_chain_cfg = get_chain_by_name(source_chain)
    except Exception:
        raise ValueError(f"Invalid source chain: {source_chain}")

    decimals = token_data.get("decimals")
    if decimals is None:
        decimals = await get_registry_decimals_by_address_async(
            source_address, source_chain_cfg.chain_id
        )

    if decimals is None:
        raise ValueError(f"Could not resolve decimals for {symbol_resolved}")

    decimals = int(decimals)
    amount_val = require_amount(amount_val, action="bridge")

    # If amount is a dynamic marker, don't convert to wei here
    if is_dynamic_marker(amount_val):
        amount_in_wei = amount_val
    else:
        amount_in_wei = str(to_wei(amount_val, decimals))

    return ExecutionPlan(
        intent_type="bridge",
        chain=source_chain,
        parameters={
            "token_symbol": symbol_resolved,
            "source_chain": source_chain,
            "target_chain": target_chain,
            "source_address": source_address,
            "target_address": target_address,
            "amount": amount_val,
            "amount_in_wei": amount_in_wei,
            "sub_org_id": "{{SUB_ORG_ID}}",
            "sender": "{{SENDER_ADDRESS}}",
        },
        constraints=intent.constraints,
    )
