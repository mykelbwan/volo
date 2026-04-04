import asyncio
from unittest.mock import AsyncMock, patch

from langchain_core.messages import HumanMessage

from intent_hub.parser.semantic_parser import parse_async


def test_parse_async_supports_convert_swap_phrase_without_llm():
    messages = [HumanMessage(content="convert 0.2 stt to nia on somnia")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "swap"
    assert intent.slots["amount"] == 0.2
    assert intent.slots["token_in"]["symbol"] == "STT"
    assert intent.slots["token_out"]["symbol"] == "NIA"
    assert intent.slots["chain"] == "somnia"


def test_parse_async_supports_chain_before_output_token_without_llm():
    messages = [HumanMessage(content="swap 0.2 stt on somnia for nia")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "swap"
    assert intent.slots["amount"] == 0.2
    assert intent.slots["token_in"]["symbol"] == "STT"
    assert intent.slots["token_out"]["symbol"] == "NIA"
    assert intent.slots["chain"] == "somnia"


def test_parse_async_supports_incomplete_swap_without_output_token_llm():
    messages = [HumanMessage(content="swap 0.2 stt on base")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "swap"
    assert intent.status.value == "incomplete"
    assert intent.slots["amount"] == 0.2
    assert intent.slots["token_in"]["symbol"] == "STT"
    assert intent.slots["chain"] == "base"
    assert intent.missing_slots == ["token_out"]
    assert intent.clarification_prompt == "Which token would you like to receive?"


def test_parse_async_supports_incomplete_transfer_without_llm():
    messages = [HumanMessage(content="send 5 usdc")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "transfer"
    assert intent.status.value == "incomplete"
    assert intent.slots["amount"] == 5
    assert intent.slots["token"]["symbol"] == "USDC"
    assert intent.missing_slots == ["recipient", "chain"]
    assert intent.clarification_prompt == "Which wallet address should receive it?"


def test_parse_async_supports_transfer_with_recipient_without_llm():
    messages = [
        HumanMessage(content="send 5 usdc to 0x000000000000000000000000000000000000dead")
    ]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "transfer"
    assert intent.status.value == "incomplete"
    assert intent.slots["recipient"] == "0x000000000000000000000000000000000000dead"
    assert intent.missing_slots == ["chain"]
    assert intent.clarification_prompt == "Which chain?"


def test_parse_async_supports_complete_bridge_without_llm():
    messages = [HumanMessage(content="bridge 0.1 eth from ethereum to base")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "bridge"
    assert intent.status.value == "complete"
    assert intent.slots["token_in"]["symbol"] == "ETH"
    assert intent.slots["amount"] == 0.1
    assert intent.slots["chain"] == "ethereum"
    assert intent.slots["target_chain"] == "base"
    assert intent.missing_slots == []


def test_parse_async_supports_incomplete_bridge_missing_source_chain_without_llm():
    messages = [HumanMessage(content="bridge 0.1 eth to base")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "bridge"
    assert intent.status.value == "incomplete"
    assert intent.slots["token_in"]["symbol"] == "ETH"
    assert intent.slots["target_chain"] == "base"
    assert intent.missing_slots == ["chain"]
    assert intent.clarification_prompt == "Which chain?"


def test_parse_async_supports_chain_first_balance_phrase_without_llm():
    messages = [HumanMessage(content="somnia balance")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "balance"
    assert intent.slots["chain"] == "somnia testnet"


def test_parse_async_supports_solana_balance_phrase_without_llm():
    messages = [HumanMessage(content="solana balance")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "balance"
    assert intent.slots["chain"] == "solana"


def test_parse_async_supports_balance_typo_without_llm():
    messages = [HumanMessage(content="etherum balnce")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "balance"
    assert intent.slots["chain"] == "ethereum"


def test_parse_async_supports_global_balance_without_llm():
    messages = [HumanMessage(content="balance")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "balance"
    assert intent.slots["chain"] == "all_supported"


def test_parse_async_supports_how_much_phrase_without_llm():
    messages = [HumanMessage(content="how much do i have")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "balance"
    assert intent.slots["chain"] == "all_supported"


def test_parse_async_preserves_unknown_balance_chain_without_llm():
    messages = [HumanMessage(content="foochain balance")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "balance"
    assert intent.slots["chain"] == "foochain"


def test_parse_async_supports_incomplete_swap_missing_amount_without_llm():
    messages = [HumanMessage(content="swap eth for usdc on base")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "swap"
    assert intent.status.value == "incomplete"
    assert intent.slots["token_in"]["symbol"] == "ETH"
    assert intent.slots["token_out"]["symbol"] == "USDC"
    assert intent.slots["chain"] == "base"
    assert intent.missing_slots == ["amount"]
    assert intent.clarification_prompt == "How much ETH would you like to swap?"


def test_parse_async_supports_incomplete_transfer_missing_amount_without_llm():
    messages = [
        HumanMessage(content="send usdc to 0x000000000000000000000000000000000000dead on base")
    ]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "transfer"
    assert intent.status.value == "incomplete"
    assert intent.slots["token"]["symbol"] == "USDC"
    assert intent.slots["recipient"] == "0x000000000000000000000000000000000000dead"
    assert intent.slots["chain"] == "base"
    assert intent.missing_slots == ["amount"]
    assert intent.clarification_prompt == "How much USDC would you like to transfer?"


def test_parse_async_supports_unwrap_without_amount_without_llm():
    messages = [HumanMessage(content="unwrap eth on base sepolia")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "unwrap"
    assert intent.status.value == "complete"
    assert intent.slots["token"]["symbol"] == "ETH"
    assert intent.slots["chain"] == "base sepolia"
    assert "amount" not in intent.missing_slots


def test_parse_async_supports_incomplete_unwrap_missing_chain_without_llm():
    messages = [HumanMessage(content="unwrap eth")]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 1
    intent = intents[0]
    assert intent.intent_type == "unwrap"
    assert intent.status.value == "incomplete"
    assert intent.slots["token"]["symbol"] == "ETH"
    assert intent.missing_slots == ["chain"]
    assert intent.clarification_prompt == "Which chain?"


def test_parse_async_supports_incomplete_bridge_missing_amount_in_sequence_without_llm():
    messages = [
        HumanMessage(
            content="bridge usdc from base sepolia to sepolia and swap it for eth"
        )
    ]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 2
    bridge_intent = intents[0]
    swap_intent = intents[1]

    assert bridge_intent.intent_type == "bridge"
    assert bridge_intent.status.value == "incomplete"
    assert bridge_intent.slots["token_in"]["symbol"] == "USDC"
    assert bridge_intent.slots["chain"] == "base sepolia"
    assert bridge_intent.slots["target_chain"] == "sepolia"
    assert bridge_intent.missing_slots == ["amount"]
    assert bridge_intent.clarification_prompt == "How much USDC would you like to bridge?"

    assert swap_intent.intent_type == "swap"
    assert swap_intent.status.value == "incomplete"
    assert swap_intent.slots["token_in"]["symbol"] == "USDC"
    assert swap_intent.slots["token_out"]["symbol"] == "ETH"
    assert swap_intent.slots["chain"] == "sepolia"
    assert swap_intent.missing_slots == ["amount"]
    assert swap_intent.slots["_carry_amount_from_prev"] is True


def test_parse_async_dependent_sequence_carries_amount_when_present():
    messages = [
        HumanMessage(
            content="bridge 2 usdc from base sepolia to sepolia and swap it for eth"
        )
    ]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 2
    bridge_intent = intents[0]
    swap_intent = intents[1]

    assert bridge_intent.intent_type == "bridge"
    assert bridge_intent.status.value == "complete"
    assert bridge_intent.slots["amount"] == 2
    assert bridge_intent.slots["token_in"]["symbol"] == "USDC"
    assert bridge_intent.slots["target_chain"] == "sepolia"

    assert swap_intent.intent_type == "swap"
    assert swap_intent.status.value == "complete"
    assert swap_intent.slots["token_in"]["symbol"] == "USDC"
    assert swap_intent.slots["token_out"]["symbol"] == "ETH"
    assert swap_intent.slots["chain"] == "sepolia"
    assert swap_intent.slots["amount"] == 2


def test_parse_async_dependent_sequence_keeps_downstream_missing_amount():
    messages = [
        HumanMessage(
            content="bridge usdc from base sepolia to sepolia and swap it for eth"
        )
    ]

    with patch(
        "intent_hub.parser.semantic_parser.call_llm_async",
        new=AsyncMock(side_effect=AssertionError("LLM should not be called")),
    ):
        intents = asyncio.run(parse_async(messages))

    assert len(intents) == 2
    bridge_intent = intents[0]
    swap_intent = intents[1]

    assert bridge_intent.intent_type == "bridge"
    assert bridge_intent.status.value == "incomplete"
    assert bridge_intent.missing_slots == ["amount"]

    assert swap_intent.intent_type == "swap"
    assert swap_intent.status.value == "incomplete"
    assert swap_intent.slots["token_in"]["symbol"] == "USDC"
    assert swap_intent.slots["token_out"]["symbol"] == "ETH"
    assert swap_intent.slots["chain"] == "sepolia"
    assert swap_intent.missing_slots == ["amount"]
    assert swap_intent.slots["_carry_amount_from_prev"] is True
