from core.utils.balance_chains import (
    ALL_SUPPORTED_CHAIN_KEY,
    canonicalize_balance_chain,
    is_all_supported_chain_request,
)
from config.solana_chains import is_solana_network
from intent_hub.ontology.intent import ExecutionPlan, Intent
from intent_hub.resolver.common import require_complete_intent
from intent_hub.utils.messages import format_with_recovery


async def resolve_balance(intent: Intent) -> ExecutionPlan:
    """
    Resolves a BalanceIntent into an ExecutionPlan.
    """
    require_complete_intent(intent)
    slots = intent.slots or {}
    chain_name = str(slots.get("chain") or "").strip()
    if not chain_name:
        raise ValueError(
            format_with_recovery(
                "Missing required balance chain",
                "provide the target network (for example, Ethereum, Base, or Solana) and retry",
            )
        )
    canonical_chain = canonicalize_balance_chain(chain_name) or chain_name.lower()
    scope = str(slots.get("scope") or "").strip().lower()
    all_supported = (
        scope == ALL_SUPPORTED_CHAIN_KEY
        or is_all_supported_chain_request(chain_name)
        or canonical_chain == ALL_SUPPORTED_CHAIN_KEY
    )

    if all_supported:
        return ExecutionPlan(
            intent_type="check_balance",
            chain=ALL_SUPPORTED_CHAIN_KEY,
            parameters={
                "chain": ALL_SUPPORTED_CHAIN_KEY,
                "scope": ALL_SUPPORTED_CHAIN_KEY,
                "sender": "{{SENDER_ADDRESS}}",
                "solana_sender": "{{SOLANA_ADDRESS}}",
            },
            constraints=intent.constraints,
        )

    sender_marker = "{{SENDER_ADDRESS}}"
    if is_solana_network(canonical_chain):
        sender_marker = "{{SOLANA_ADDRESS}}"

    return ExecutionPlan(
        intent_type="check_balance",
        chain=canonical_chain or "unknown",
        parameters={
            "chain": canonical_chain or chain_name,
            "sender": sender_marker,
            "solana_sender": "{{SOLANA_ADDRESS}}",
        },
        constraints=intent.constraints,
    )
