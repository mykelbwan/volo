from intent_hub.ontology.intent import Intent, IntentStatus
from intent_hub.parser.validation import validate_intent


def test_validate_intent_resolves_symbols_and_marks_complete():
    intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "eth"},
            "token_out": {"symbol": "usdc"},
            "amount": 1,
            "chain": "base",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap 1 eth to usdc",
    )

    registry = {
        "WETH": {"aliases": ["eth"]},
        "USDC": {"aliases": ["usd coin"]},
    }

    validated = validate_intent(intent, registry)

    assert validated.slots["token_in"]["symbol"] == "WETH"
    assert validated.slots["token_out"]["symbol"] == "USDC"
    assert validated.status == IntentStatus.COMPLETE
    assert validated.clarification_prompt is None
    assert validated.missing_slots == []


def test_validate_intent_sets_bridge_prompt():
    intent = Intent(
        intent_type="bridge",
        slots={
            "token_in": {"symbol": "USDC"},
            "token_out": {"symbol": "USDT"},
            "amount": 10,
            "chain": "ethereum",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.8,
        status=IntentStatus.INCOMPLETE,
        raw_input="bridge 10 usdc",
    )

    validated = validate_intent(intent, {})

    assert validated.status == IntentStatus.INCOMPLETE
    assert "target_chain" in validated.missing_slots
    assert validated.clarification_prompt == "Which chain are you bridging to?"


def test_validate_balance_requires_chain():
    intent = Intent(
        intent_type="balance",
        slots={"chain": None},
        missing_slots=[],
        constraints=None,
        confidence=0.8,
        status=IntentStatus.COMPLETE,
        raw_input="check my balance",
    )

    validated = validate_intent(intent, {})

    assert validated.status == IntentStatus.INCOMPLETE
    assert validated.missing_slots == ["chain"]
    assert (
        validated.clarification_prompt
        == "Which network (e.g., Somnia, Ethereum, Base) would you like to check your balances on?"
    )


def test_validate_balance_with_chain_is_complete():
    intent = Intent(
        intent_type="balance",
        slots={"chain": "somnia testnet"},
        missing_slots=["chain"],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.INCOMPLETE,
        raw_input="check my balance on somnia testnet",
    )

    validated = validate_intent(intent, {})

    assert validated.status == IntentStatus.COMPLETE
    assert validated.missing_slots == []
    assert validated.clarification_prompt is None


def test_validate_balance_all_supported_scope_is_complete():
    intent = Intent(
        intent_type="balance",
        slots={"chain": "all chains"},
        missing_slots=["chain"],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.INCOMPLETE,
        raw_input="balance",
    )

    validated = validate_intent(intent, {})

    assert validated.status == IntentStatus.COMPLETE
    assert validated.slots["chain"] == "all_supported"
    assert validated.slots["scope"] == "all_supported"
    assert validated.missing_slots == []


def test_validate_transfer_requires_recipient_and_chain():
    intent = Intent(
        intent_type="transfer",
        slots={"token": {"symbol": "usdc"}, "amount": 5},
        missing_slots=[],
        constraints=None,
        confidence=0.8,
        status=IntentStatus.COMPLETE,
        raw_input="send 5 usdc",
    )

    registry = {"USDC": {"aliases": ["usd coin"]}}
    validated = validate_intent(intent, registry)

    assert validated.slots["token"]["symbol"] == "USDC"
    assert validated.status == IntentStatus.INCOMPLETE
    assert validated.missing_slots == ["recipient", "chain"]
    assert validated.clarification_prompt == "Which wallet address should receive it?"


def test_validate_bridge_missing_chain_uses_short_prompt():
    intent = Intent(
        intent_type="bridge",
        slots={
            "token_in": {"symbol": "USDC"},
            "token_out": {"symbol": "USDC"},
            "amount": 0.2,
            "target_chain": "base",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.8,
        status=IntentStatus.COMPLETE,
        raw_input="bridge 0.2 usdc to base",
    )

    validated = validate_intent(intent, {})

    assert validated.status == IntentStatus.INCOMPLETE
    assert validated.missing_slots == ["chain"]
    assert validated.clarification_prompt == "Which chain?"


def test_validate_swap_missing_chain_uses_short_prompt():
    intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "STT"},
            "token_out": {"symbol": "NIA"},
            "amount": 0.2,
        },
        missing_slots=[],
        constraints=None,
        confidence=0.8,
        status=IntentStatus.COMPLETE,
        raw_input="swap 0.2 stt for nia",
    )

    validated = validate_intent(intent, {})

    assert validated.status == IntentStatus.INCOMPLETE
    assert validated.missing_slots == ["chain"]
    assert validated.clarification_prompt == "Which chain?"


def test_validate_unwrap_allows_missing_amount_and_marks_complete():
    intent = Intent(
        intent_type="unwrap",
        slots={"token": {"symbol": "ETH"}, "chain": "base"},
        missing_slots=["amount"],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.INCOMPLETE,
        raw_input="unwrap eth on base",
    )

    validated = validate_intent(intent, {})

    assert validated.status == IntentStatus.COMPLETE
    assert validated.missing_slots == []
    assert validated.clarification_prompt is None


def test_validate_unwrap_missing_chain_uses_short_prompt():
    intent = Intent(
        intent_type="unwrap",
        slots={"token": {"symbol": "ETH"}},
        missing_slots=[],
        constraints=None,
        confidence=0.8,
        status=IntentStatus.COMPLETE,
        raw_input="unwrap eth",
    )

    validated = validate_intent(intent, {})

    assert validated.status == IntentStatus.INCOMPLETE
    assert validated.missing_slots == ["chain"]
    assert validated.clarification_prompt == "Which chain?"
