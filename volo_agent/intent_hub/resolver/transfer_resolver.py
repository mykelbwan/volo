from core.identity.wallet_bindings import wallet_markers_for_family
from core.transfers.chains import get_transfer_chain_spec
from intent_hub.ontology.intent import ExecutionPlan, Intent
from intent_hub.registry.token_service import (
    get_address_for_chain_async,
    get_token_data_async,
)
from intent_hub.resolver.common import (
    require_amount,
    require_complete_intent,
    resolve_token_on_chain,
    symbol_from_slot,
    unresolved_addresses_error,
)
from intent_hub.utils.messages import require_non_empty_str


async def resolve_transfer(intent: Intent) -> ExecutionPlan:
    require_complete_intent(intent)
    slots = intent.slots or {}
    token_symbol = symbol_from_slot(slots.get("token"))
    requested_chain = require_non_empty_str(slots.get("chain"), field="chain").lower()
    chain_spec = get_transfer_chain_spec(requested_chain)
    wallet_markers = wallet_markers_for_family(chain_spec.family)
    amount = slots.get("amount")
    recipient = require_non_empty_str(slots.get("recipient"), field="recipient")

    resolution = None
    if token_symbol:
        resolution = await resolve_token_on_chain(
            token_symbol,
            chain_spec.network,
            get_token_data_fn=get_token_data_async,
            get_address_for_chain_fn=get_address_for_chain_async,
            allow_amount_prefixed=True,
        )
    asset_ref = resolution.address if resolution else None
    token_symbol_resolved = resolution.symbol if resolution else token_symbol
    if amount is None and resolution and resolution.inferred_amount is not None:
        amount = resolution.inferred_amount

    if not asset_ref:
        raise unresolved_addresses_error(
            [token_symbol_resolved],
            chain_context=chain_spec.network,
        )
    amount = require_amount(amount, action="transfer")

    # Construct parameters for the tool
    parameters = {
        "asset_symbol": token_symbol_resolved,
        "asset_ref": asset_ref,
        "amount": amount,
        "recipient": recipient,
        "network": chain_spec.network,
        # These will be filled by the final orchestration layer or dynamic markers
        "sub_org_id": wallet_markers.sub_org_marker,
        "sender": wallet_markers.sender_marker,
    }

    return ExecutionPlan(
        intent_type="transfer",
        chain=chain_spec.network,
        parameters=parameters,
        constraints=intent.constraints,
    )
