import asyncio

from intent_hub.ontology.intent import Intent, IntentStatus
from intent_hub.resolver.balance_resolver import resolve_balance


def test_resolve_balance_single_chain_uses_evm_sender_marker():
    intent = Intent(
        intent_type="balance",
        slots={"chain": "somnia testnet"},
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="somnia balance",
    )

    plan = asyncio.run(resolve_balance(intent))
    assert plan.intent_type == "check_balance"
    assert plan.chain == "somnia testnet"
    assert plan.parameters["chain"] == "somnia testnet"
    assert plan.parameters["sender"] == "{{SENDER_ADDRESS}}"


def test_resolve_balance_solana_uses_solana_sender_marker():
    intent = Intent(
        intent_type="balance",
        slots={"chain": "solana"},
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="solana balance",
    )

    plan = asyncio.run(resolve_balance(intent))
    assert plan.intent_type == "check_balance"
    assert plan.chain == "solana"
    assert plan.parameters["sender"] == "{{SOLANA_ADDRESS}}"


def test_resolve_balance_all_supported_sets_scope():
    intent = Intent(
        intent_type="balance",
        slots={"chain": "all_supported"},
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="balance",
    )

    plan = asyncio.run(resolve_balance(intent))
    assert plan.intent_type == "check_balance"
    assert plan.chain == "all_supported"
    assert plan.parameters["chain"] == "all_supported"
    assert plan.parameters["scope"] == "all_supported"
    assert plan.parameters["sender"] == "{{SENDER_ADDRESS}}"
    assert plan.parameters["solana_sender"] == "{{SOLANA_ADDRESS}}"
