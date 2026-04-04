from intent_hub.ontology.intent import Intent, IntentStatus
from intent_hub.resolver.templates import apply_templates, can_apply_templates


def _intent(intent_type: str, slots: dict) -> Intent:
    return Intent(
        intent_type=intent_type,
        slots=slots,
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.INCOMPLETE,
        raw_input="",
    )


def test_template_swap_bridge():
    intents = [
        _intent(
            "swap",
            {
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 1,
                "chain": "base",
            },
        ),
        _intent(
            "bridge",
            {
                "token_in": None,
                "amount": None,
                "chain": None,
                "target_chain": "arbitrum one",
            },
        ),
    ]

    patched = apply_templates(intents)
    bridge = patched[1]
    assert bridge.status == IntentStatus.COMPLETE
    assert bridge.slots["chain"] == "base"
    assert bridge.slots["token_in"]["symbol"] == "USDC"
    assert bridge.slots["amount"] == "{{OUTPUT_OF:step_0}}"


def test_template_bridge_swap():
    intents = [
        _intent(
            "bridge",
            {
                "token_in": {"symbol": "USDC"},
                "amount": 50,
                "chain": "ethereum",
                "target_chain": "base",
            },
        ),
        _intent(
            "swap",
            {
                "token_in": None,
                "token_out": {"symbol": "ETH"},
                "amount": None,
                "chain": None,
            },
        ),
    ]

    patched = apply_templates(intents)
    swap = patched[1]
    assert swap.status == IntentStatus.COMPLETE
    assert swap.slots["chain"] == "base"
    assert swap.slots["token_in"]["symbol"] == "USDC"
    assert swap.slots["amount"] == "{{OUTPUT_OF:step_0}}"


def test_template_swap_transfer():
    intents = [
        _intent(
            "swap",
            {
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 0.5,
                "chain": "base",
            },
        ),
        _intent(
            "transfer",
            {
                "token": None,
                "amount": None,
                "recipient": "0x000000000000000000000000000000000000dead",
                "chain": None,
            },
        ),
    ]

    patched = apply_templates(intents)
    transfer = patched[1]
    assert transfer.status == IntentStatus.COMPLETE
    assert transfer.slots["chain"] == "base"
    assert transfer.slots["token"]["symbol"] == "USDC"
    assert transfer.slots["amount"] == "{{OUTPUT_OF:step_0}}"


def test_template_bridge_transfer():
    intents = [
        _intent(
            "bridge",
            {
                "token_in": {"symbol": "USDT"},
                "amount": 25,
                "chain": "ethereum",
                "target_chain": "base",
            },
        ),
        _intent(
            "transfer",
            {
                "token": None,
                "amount": None,
                "recipient": "0x000000000000000000000000000000000000dead",
                "chain": None,
            },
        ),
    ]

    patched = apply_templates(intents)
    transfer = patched[1]
    assert transfer.status == IntentStatus.COMPLETE
    assert transfer.slots["chain"] == "base"
    assert transfer.slots["token"]["symbol"] == "USDT"
    assert transfer.slots["amount"] == "{{OUTPUT_OF:step_0}}"


def test_template_swap_swap():
    intents = [
        _intent(
            "swap",
            {
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 1,
                "chain": "base",
            },
        ),
        _intent(
            "swap",
            {
                "token_in": None,
                "token_out": {"symbol": "DAI"},
                "amount": None,
                "chain": None,
            },
        ),
    ]

    patched = apply_templates(intents)
    swap = patched[1]
    assert swap.status == IntentStatus.COMPLETE
    assert swap.slots["chain"] == "base"
    assert swap.slots["token_in"]["symbol"] == "USDC"
    assert swap.slots["amount"] == "{{OUTPUT_OF:step_0}}"


def test_template_balance_swap():
    intents = [
        _intent("balance", {"chain": "base"}),
        _intent(
            "swap",
            {
                "token_in": {"symbol": "USDC"},
                "token_out": {"symbol": "ETH"},
                "amount": None,
                "chain": None,
            },
        ),
    ]

    patched = apply_templates(intents)
    swap = patched[1]
    assert swap.status == IntentStatus.COMPLETE
    assert swap.slots["chain"] == "base"
    assert swap.slots["amount"] == "{{BALANCE_OF:step_0:USDC}}"


def test_can_apply_templates_all_complete():
    intents = [
        _intent(
            "swap",
            {
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 1,
                "chain": "base",
            },
        ),
        _intent(
            "transfer",
            {
                "token": None,
                "amount": None,
                "recipient": "0x000000000000000000000000000000000000dead",
                "chain": None,
            },
        ),
    ]

    intent_dicts = [i.model_dump() for i in intents]
    assert can_apply_templates(intent_dicts) is True
