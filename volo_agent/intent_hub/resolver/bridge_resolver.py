from decimal import Decimal
from typing import cast

from core.chains.catalog import resolve_chain
from core.identity.wallet_bindings import wallet_markers_for_family
from core.token_security.registry_lookup import get_registry_decimals_by_address_async
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
from intent_hub.utils.messages import format_with_recovery, require_non_empty_str


async def resolve_bridge(intent: Intent) -> ExecutionPlan:
    require_complete_intent(intent)

    slots = intent.slots or {}
    symbol = symbol_from_slot(slots.get("token_in"))
    requested_source_chain = require_non_empty_str(
        slots.get("chain"), field="chain"
    ).lower()
    requested_target_chain = require_non_empty_str(
        slots.get("target_chain"), field="target_chain"
    ).lower()
    source_chain_entry = resolve_chain(requested_source_chain)
    target_chain_entry = resolve_chain(requested_target_chain)
    if source_chain_entry is None:
        raise ValueError(f"Invalid source chain: {requested_source_chain}")
    if target_chain_entry is None:
        raise ValueError(f"Invalid target chain: {requested_target_chain}")

    source_chain = str(source_chain_entry.key).strip().lower()
    target_chain = str(target_chain_entry.key).strip().lower()
    wallet_markers = wallet_markers_for_family(source_chain_entry.family)

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
        target_address = await get_address_for_chain_async(
            discovered_target, target_chain
        )

    source_native_symbol = str(source_chain_entry.native_symbol or "").strip().upper()
    source_native_asset_ref = str(source_chain_entry.native_asset_ref or "").strip()
    target_native_symbol = str(target_chain_entry.native_symbol or "").strip().upper()
    target_native_asset_ref = str(target_chain_entry.native_asset_ref or "").strip()
    if (
        not source_address
        and symbol_resolved.upper() == source_native_symbol
        and source_native_asset_ref
    ):
        source_address = source_native_asset_ref
    if (
        not target_address
        and symbol_resolved.upper() == target_native_symbol
        and target_native_asset_ref
    ):
        target_address = target_native_asset_ref

    if not source_address or not target_address:
        raise unresolved_addresses_error(
            [symbol_resolved],
            chain_context=f"{source_chain}/{target_chain}",
        )

    decimals = token_data.get("decimals")
    if decimals is None:
        if source_native_asset_ref and (
            str(source_address).strip().lower() == source_native_asset_ref.lower()
        ):
            decimals = 9 if source_chain_entry.family == "solana" else 18
        else:
            decimals = await get_registry_decimals_by_address_async(
                source_address, int(source_chain_entry.chain_id)
            )

    if decimals is None:
        raise ValueError(f"Could not resolve decimals for {symbol_resolved}")

    decimals = int(decimals)
    amount_val = require_amount(amount_val, action="bridge")

    # If amount is a dynamic marker, don't convert to wei here
    if is_dynamic_marker(amount_val):
        amount_in_wei = amount_val
    else:
        amount_in_wei = str(to_wei(cast(Decimal | str, amount_val), decimals))

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
            "sub_org_id": wallet_markers.sub_org_marker,
            "sender": wallet_markers.sender_marker,
        },
        constraints=intent.constraints,
    )
