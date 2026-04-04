import asyncio
from unittest.mock import MagicMock, patch

from langchain_core.messages import AIMessage
from intent_hub.parser.router import route_conversation
from langchain_core.messages import HumanMessage


def test_route_conversation_routes_explicit_action_without_llm():
    result = asyncio.run(route_conversation([HumanMessage(content="bridge it")]))

    assert result["category"] == "ACTION"
    assert result["response"] is None


def test_route_conversation_routes_unwrap_action_without_llm():
    result = asyncio.run(route_conversation([HumanMessage(content="unwrap eth on base")]))

    assert result["category"] == "ACTION"
    assert result["response"] is None


def test_route_conversation_routes_unknown_text_to_conversation_without_llm():
    result = asyncio.run(route_conversation([HumanMessage(content="just vibing here")]))

    assert result["category"] == "CONVERSATION"
    assert result["response"] is None


def test_route_conversation_routes_joke_request_to_conversation_without_llm():
    fake_llm = MagicMock()
    fake_llm.invoke.side_effect = AssertionError("Router LLM should not be called")

    with patch("intent_hub.parser.router.logger") as fake_logger:
        result = asyncio.run(route_conversation([HumanMessage(content="tell me a joke")]))

    assert result["category"] == "CONVERSATION"
    assert result["response"] is None
    fake_logger.warning.assert_not_called()


def test_route_conversation_routes_slot_fill_reply_without_llm():
    result = asyncio.run(
        route_conversation(
            [
                AIMessage(content="Which chain?"),
                HumanMessage(content="base"),
            ]
        )
    )

    assert result["category"] == "ACTION"
    assert result["response"] is None


def test_route_conversation_treats_hi_as_conversation_without_llm():
    result = asyncio.run(route_conversation([HumanMessage(content="hi")]))

    assert result["category"] == "CONVERSATION"
    assert result["response"] == "Hey. How can I help?"


def test_route_conversation_treats_sup_as_conversation_without_llm():
    result = asyncio.run(route_conversation([HumanMessage(content="sup")]))

    assert result["category"] == "CONVERSATION"
    assert result["response"] == "Hey. How can I help?"


def test_route_conversation_routes_hi_dear_without_llm():
    result = asyncio.run(route_conversation([HumanMessage(content="hi dear")]))

    assert result["category"] == "CONVERSATION"
    assert result["response"] == "Hey. How can I help?"


def test_route_conversation_routes_hello_there_without_llm():
    result = asyncio.run(route_conversation([HumanMessage(content="hello there")]))

    assert result["category"] == "CONVERSATION"
    assert result["response"] == "Hey. How can I help?"


def test_route_conversation_greeting_with_action_hint_routes_to_action_without_llm():
    result = asyncio.run(
        route_conversation([HumanMessage(content="hi swap 1 eth to usdc on base")])
    )

    assert result["category"] == "ACTION"
    assert result["response"] is None


def test_route_conversation_treats_typoed_name_question_as_conversation_without_llm():
    result = asyncio.run(route_conversation([HumanMessage(content="wats your name?")]))

    assert result["category"] == "CONVERSATION"
    assert result["response"] is None


def test_route_conversation_treats_capability_question_as_conversation_without_llm():
    result = asyncio.run(route_conversation([HumanMessage(content="what can you do?")]))

    assert result["category"] == "CONVERSATION"
    assert result["response"] is None
