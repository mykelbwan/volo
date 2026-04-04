import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from langchain_core.messages import AIMessage, HumanMessage

from core.conversation import responder as responder_module
from core.conversation.responder import respond_conversation


def _reset_conversation_health() -> None:
    responder_module._CONVERSATION_HEALTH.consecutive_failures = 0
    responder_module._CONVERSATION_HEALTH.cooldown_until = 0.0


def setup_function():
    _reset_conversation_health()


def test_respond_conversation_returns_model_text():
    fake_llm = AsyncMock()
    fake_llm.ainvoke.return_value = AIMessage(content="I'm Volo.")

    with patch("core.conversation.responder.conversation_llm", fake_llm):
        result = asyncio.run(
            respond_conversation(
                [
                    HumanMessage(content="hi"),
                    AIMessage(content="Hello."),
                    HumanMessage(content="what is your name?"),
                ]
            )
        )

    assert result == "I'm Volo."


def test_respond_conversation_timeout_returns_clear_recovery_path():
    fake_llm = AsyncMock()
    fake_llm.ainvoke.side_effect = asyncio.TimeoutError

    with patch("core.conversation.responder.conversation_llm", fake_llm):
        result = asyncio.run(respond_conversation([HumanMessage(content="how are you?")]))

    lowered = result.lower()
    assert "try again" in lowered
    assert "swap" not in lowered
    assert "reply" in lowered


def test_respond_conversation_opens_cooldown_after_repeated_failures():
    fake_llm = AsyncMock()
    fake_llm.ainvoke.side_effect = asyncio.TimeoutError

    with patch("core.conversation.responder.conversation_llm", fake_llm), patch.dict(
        "os.environ",
        {
            "CONVERSATION_LLM_FAILURE_THRESHOLD": "2",
            "CONVERSATION_LLM_COOLDOWN_SECONDS": "60",
        },
        clear=False,
    ):
        first = asyncio.run(respond_conversation([HumanMessage(content="tell me a joke")]))
        second = asyncio.run(respond_conversation([HumanMessage(content="tell me a joke")]))
        third = asyncio.run(respond_conversation([HumanMessage(content="tell me a joke")]))

    assert "moment" in first.lower()
    assert "moment" in second.lower()
    assert "little bit" in third.lower()
    assert fake_llm.ainvoke.await_count == 2


def test_respond_conversation_success_resets_cooldown_state():
    failing_llm = AsyncMock()
    failing_llm.ainvoke.side_effect = asyncio.TimeoutError
    healthy_llm = AsyncMock()
    healthy_llm.ainvoke.return_value = AIMessage(content="Here and ready.")

    with patch.dict(
        "os.environ",
        {
            "CONVERSATION_LLM_FAILURE_THRESHOLD": "1",
            "CONVERSATION_LLM_COOLDOWN_SECONDS": "60",
        },
        clear=False,
    ), patch("core.conversation.responder.conversation_llm", failing_llm):
        cooldown_reply = asyncio.run(
            respond_conversation([HumanMessage(content="tell me a joke")])
        )

    _reset_conversation_health()

    with patch("core.conversation.responder.conversation_llm", healthy_llm):
        healthy_reply = asyncio.run(
            respond_conversation([HumanMessage(content="tell me a joke")])
        )

    assert "moment" in cooldown_reply.lower()
    assert healthy_reply == "Here and ready."


def test_respond_conversation_isolates_small_talk_from_prior_action_history():
    fake_response = MagicMock()
    fake_response.content = "Hey. How can I help?"
    fake_llm = type("SyncLLM", (), {})()
    fake_llm.ainvoke = None
    fake_llm.invoke = MagicMock(return_value=fake_response)

    with patch("core.conversation.responder.conversation_llm", fake_llm):
        result = asyncio.run(
            respond_conversation(
                [
                    HumanMessage(content="swap 1 eth to usdc"),
                    AIMessage(content="Swap complete."),
                    HumanMessage(content="hi"),
                ]
            )
        )

    assert result == "Hey. How can I help?"
    prompt = fake_llm.invoke.call_args[0][0][0].content
    assert "User: hi" in prompt
    assert "swap 1 eth to usdc" not in prompt.lower()
    assert "swap complete." not in prompt.lower()


def test_respond_conversation_sync_invoke_uses_recent_history_only():
    fake_response = MagicMock()
    fake_response.content = "Doing well."
    fake_llm = type("SyncLLM", (), {})()
    fake_llm.ainvoke = None
    fake_llm.invoke = MagicMock(return_value=fake_response)

    messages = [
        HumanMessage(content=f"user-{idx}") if idx % 2 == 0 else AIMessage(content=f"ai-{idx}")
        for idx in range(12)
    ]

    with patch("core.conversation.responder.conversation_llm", fake_llm):
        result = asyncio.run(respond_conversation(messages))

    assert result == "Doing well."
    prompt = fake_llm.invoke.call_args[0][0][0].content
    assert "User: user-0\n" not in prompt
    assert "Assistant: ai-1\n" not in prompt
    assert "User: user-4\n" in prompt
    assert "Assistant: ai-11\n" in prompt
