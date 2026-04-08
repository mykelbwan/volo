import logging
import re
from difflib import get_close_matches
from typing import Dict, Sequence, Union

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

logger = logging.getLogger(__name__)
_MINIMAL_CONVERSATION_RESPONSE = "Hey. How can I help?"
_MINIMAL_GREETING_FALLBACK = {
    "hi",
    "hello",
    "hey",
    "yo",
    "gm",
    "hiya",
    "sup",
    "howdy",
}
_NON_CONVERSATION_HINTS = {
    "swap",
    "bridge",
    "transfer",
    "send",
    "buy",
    "sell",
    "convert",
    "exchange",
    "unwrap",
    "balance",
    "balances",
    "portfolio",
    "holding",
    "holdings",
    "address",
    "wallet",
    "status",
    "pending",
    "confirm",
    "cancel",
    "retry",
}
_ACTION_HINTS = {
    "swap",
    "bridge",
    "transfer",
    "send",
    "buy",
    "sell",
    "convert",
    "exchange",
    "trade",
    "move",
    "unwrap",
}
_STATUS_HINTS = {
    "status",
    "pending",
    "history",
    "recent",
    "tasks",
}
_SLOT_FILL_PROMPT_HINTS = {
    "which",
    "what",
    "where",
    "amount",
    "token",
    "chain",
    "network",
    "recipient",
    "receive",
    "from",
    "to",
    "use",
}


def _normalize_text(value: str | None) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower())
    return re.sub(r"\s+", " ", normalized).strip()


def _latest_user_text(messages: Sequence[BaseMessage]) -> str:
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            return _normalize_text(str(msg.content or ""))
    return ""


def _latest_ai_text(messages: Sequence[BaseMessage]) -> str:
    for msg in reversed(messages):
        if isinstance(msg, AIMessage):
            return _normalize_text(str(msg.content or ""))
    return ""


def _matches_greeting_token(token: str) -> bool:
    if not token or not token.isalpha():
        return False
    if token in _MINIMAL_GREETING_FALLBACK:
        return True
    return bool(get_close_matches(token, tuple(_MINIMAL_GREETING_FALLBACK), n=1, cutoff=0.84))


def _is_minimal_conversation_fallback(messages: Sequence[BaseMessage]) -> bool:
    text = _latest_user_text(messages)
    if not text:
        return False
    if _matches_greeting_token(text):
        return True

    tokens = text.split()
    if not tokens:
        return False
    if not _matches_greeting_token(tokens[0]):
        return False
    if any(any(char.isdigit() for char in token) for token in tokens[1:]):
        return False
    if any(token in _NON_CONVERSATION_HINTS for token in tokens[1:]):
        return False
    return True


def _minimal_conversation_result() -> Dict[str, Union[str, None]]:
    return {
        "category": "CONVERSATION",
        "response": _MINIMAL_CONVERSATION_RESPONSE,
    }


def _looks_like_explicit_action(text: str) -> bool:
    if not text:
        return False
    tokens = text.split()
    if not tokens:
        return False
    return any(token in _ACTION_HINTS for token in tokens)


def _looks_like_status(text: str) -> bool:
    if not text:
        return False
    tokens = text.split()
    if not tokens:
        return False
    if text in {"status", "pending", "recent tasks", "task history"}:
        return True
    return any(token in _STATUS_HINTS for token in tokens)


def _looks_like_slot_fill_reply(messages: Sequence[BaseMessage]) -> bool:
    user_text = _latest_user_text(messages)
    if not user_text:
        return False
    tokens = user_text.split()
    if not tokens or len(tokens) > 3:
        return False
    if _looks_like_explicit_action(user_text) or _looks_like_status(user_text):
        return False

    last_ai = _latest_ai_text(messages)
    if not last_ai:
        return False
    if "?" not in str(getattr(next((m for m in reversed(messages) if isinstance(m, AIMessage)), None), "content", "")):
        return False
    return any(hint in last_ai.split() for hint in _SLOT_FILL_PROMPT_HINTS)


def has_positive_action_evidence(messages: Sequence[BaseMessage]) -> bool:
    user_text = _latest_user_text(messages)
    if not user_text:
        return False
    if _looks_like_explicit_action(user_text):
        return True
    return _looks_like_slot_fill_reply(messages)


def _fallback_route(messages: Sequence[BaseMessage]) -> Dict[str, Union[str, None]]:
    user_text = _latest_user_text(messages)
    if _is_minimal_conversation_fallback(messages):
        return _minimal_conversation_result()
    if has_positive_action_evidence(messages):
        return {"category": "ACTION", "response": None}
    if _looks_like_status(user_text):
        return {"category": "STATUS", "response": None}
    return {"category": "CONVERSATION", "response": None}


async def route_conversation(
    messages: Sequence[BaseMessage],
) -> Dict[str, Union[str, None]]:
    return _fallback_route(messages)
