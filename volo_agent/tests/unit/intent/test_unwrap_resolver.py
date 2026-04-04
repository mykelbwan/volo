import asyncio

import pytest

from intent_hub.ontology.intent import Intent, IntentStatus
from intent_hub.resolver.unwrap_resolver import resolve_unwrap


def _intent(token_symbol: str, chain: str = "base sepolia", amount=None) -> Intent:
    slots = {
        "token": {"symbol": token_symbol},
        "chain": chain,
    }
    if amount is not None:
        slots["amount"] = amount
    return Intent(
        intent_type="unwrap",
        slots=slots,
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="unwrap",
    )


def test_resolve_unwrap_builds_plan_for_native_symbol():
    plan = asyncio.run(resolve_unwrap(_intent("ETH", amount=0.25)))

    assert plan.intent_type == "unwrap"
    assert plan.chain == "base sepolia"
    assert plan.parameters["token_symbol"] == "ETH"
    assert plan.parameters["token_address"] == "0x4200000000000000000000000000000000000006"
    assert plan.parameters["amount"] == 0.25
    assert plan.parameters["sender"] == "{{SENDER_ADDRESS}}"
    assert plan.parameters["sub_org_id"] == "{{SUB_ORG_ID}}"


def test_resolve_unwrap_accepts_wrapped_symbol_alias():
    plan = asyncio.run(resolve_unwrap(_intent("WETH")))

    assert plan.parameters["token_symbol"] == "ETH"
    assert "amount" not in plan.parameters


def test_resolve_unwrap_rejects_non_native_symbol():
    with pytest.raises(ValueError, match="expected ETH"):
        asyncio.run(resolve_unwrap(_intent("USDC")))
