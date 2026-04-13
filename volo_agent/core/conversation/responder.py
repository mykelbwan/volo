from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from difflib import get_close_matches
from functools import lru_cache
from typing import Any, Sequence

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

from core.utils.async_tools import run_blocking
from intent_hub.utils.messages import format_with_recovery

conversation_llm: Any | None = None
_CONVERSATION_LLM_CACHE: Any | None = None
_DEFAULT_CONVERSATION_TIMEOUT_SECONDS = 10.0
_DEFAULT_CONVERSATION_COOLDOWN_SECONDS = 30.0
_DEFAULT_CONVERSATION_FAILURE_THRESHOLD = 2
_MAX_CONVERSATION_HISTORY_MESSAGES = 8
_NON_CONVERSATION_HINTS = {
    "swap",
    "bridge",
    "transfer",
    "send",
    "buy",
    "sell",
    "unwrap",
    "convert",
    "exchange",
    "balance",
    "balances",
    "portfolio",
    "wallet",
    "address",
    "status",
    "pending",
    "task",
    "tasks",
    "confirm",
    "cancel",
    "retry",
}
logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_chain_summary() -> str:
    from config.chains import supported_chains, _CHAIN_ALIASES as EVM_ALIASES
    from config.solana_chains import SOLANA_CHAINS, _CHAIN_ALIASES as SOLANA_ALIASES
    
    evm_names = sorted(supported_chains())
    solana_names = sorted(SOLANA_CHAINS.keys())
    
    # Collect all unique aliases for hints
    all_aliases = set(EVM_ALIASES.keys()) | set(SOLANA_ALIASES.keys())
    # Filter out very short or generic aliases to keep prompt clean
    clean_aliases = {a for a in all_aliases if len(a) > 2}
    
    return (
        f"Supported Networks: {', '.join(evm_names + solana_names)}\n"
        f"Known Aliases: {', '.join(sorted(clean_aliases))}"
    )


class _ConversationHealthState:
    def __init__(self) -> None:
        self.consecutive_failures = 0
        self.cooldown_until = 0.0


_CONVERSATION_HEALTH = _ConversationHealthState()

CONVERSATION_SYSTEM_PROMPT = """You are Volo, a fast onchain assistant with a natural conversational style.
You help with swaps, bridges, transfers, balances, and wallet questions.

Behavior rules:
- Sound natural, warm, and lightly alive, not scripted or stiff.
- Be friendly and conversational instead of defaulting to business tone.
- Keep replies concise by default, usually 1-3 sentences.
- Answer the user's actual question directly.
- If a user mispells a network name (e.g., 'bace' for 'Base' or 'sol' for 'Solana'), proactively suggest the correct name based on your 'Supported Networks' and 'Known Aliases' list.
- Do not claim real-world feelings or actions you cannot take.
- Do not produce JSON or structured output. Reply with plain text only."""


def conversation_failure_message(reason: str | None = None) -> str:
    if reason == "cooldown":
        return (
            "I'm having trouble replying live right now. "
            "Please try again in a little bit."
        )
    if reason == "timeout":
        return "I'm taking too long to reply right now. Please try again in a moment."
    return "I'm having trouble replying right now. Please try again in a moment."


def _get_conversation_llm() -> Any:
    global conversation_llm, _CONVERSATION_LLM_CACHE
    if conversation_llm is not None:
        return conversation_llm
    if _CONVERSATION_LLM_CACHE is None:
        from llms.llms_init import conversation_llm as _conversation_llm

        _CONVERSATION_LLM_CACHE = _conversation_llm
    return _CONVERSATION_LLM_CACHE


def _conversation_timeout_seconds() -> float:
    raw = os.getenv("CONVERSATION_LLM_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return _DEFAULT_CONVERSATION_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        return _DEFAULT_CONVERSATION_TIMEOUT_SECONDS
    if value <= 0:
        return _DEFAULT_CONVERSATION_TIMEOUT_SECONDS
    return value


def _conversation_cooldown_seconds() -> float:
    raw = os.getenv("CONVERSATION_LLM_COOLDOWN_SECONDS", "").strip()
    if not raw:
        return _DEFAULT_CONVERSATION_COOLDOWN_SECONDS
    try:
        value = float(raw)
    except ValueError:
        return _DEFAULT_CONVERSATION_COOLDOWN_SECONDS
    if value <= 0:
        return _DEFAULT_CONVERSATION_COOLDOWN_SECONDS
    return value


def _conversation_failure_threshold() -> int:
    raw = os.getenv("CONVERSATION_LLM_FAILURE_THRESHOLD", "").strip()
    if not raw:
        return _DEFAULT_CONVERSATION_FAILURE_THRESHOLD
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_CONVERSATION_FAILURE_THRESHOLD
    if value <= 0:
        return _DEFAULT_CONVERSATION_FAILURE_THRESHOLD
    return value


def _conversation_circuit_open(now: float | None = None) -> bool:
    current = time.monotonic() if now is None else now
    return current < _CONVERSATION_HEALTH.cooldown_until


def _record_conversation_success() -> None:
    _CONVERSATION_HEALTH.consecutive_failures = 0
    _CONVERSATION_HEALTH.cooldown_until = 0.0


def _record_conversation_failure() -> None:
    _CONVERSATION_HEALTH.consecutive_failures += 1
    if _CONVERSATION_HEALTH.consecutive_failures < _conversation_failure_threshold():
        return
    _CONVERSATION_HEALTH.cooldown_until = max(
        _CONVERSATION_HEALTH.cooldown_until,
        time.monotonic() + _conversation_cooldown_seconds(),
    )


def _select_recent_messages(
    messages: Sequence[BaseMessage],
    *,
    limit: int = _MAX_CONVERSATION_HISTORY_MESSAGES,
) -> Sequence[BaseMessage]:
    if limit <= 0:
        return []
    selected = [msg for msg in messages if isinstance(msg, (HumanMessage, AIMessage))]
    if len(selected) <= limit:
        return selected
    return selected[-limit:]


def _normalize_conversation_text(text: object) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").strip().lower()).strip()


def _latest_user_text(messages: Sequence[BaseMessage]) -> str:
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            return str(msg.content or "").strip()
    return ""


def _latest_ai_text(messages: Sequence[BaseMessage]) -> str:
    for msg in reversed(messages):
        if isinstance(msg, AIMessage):
            return str(msg.content or "").strip()
    return ""


def _should_isolate_latest_user_turn(messages: Sequence[BaseMessage]) -> bool:
    normalized = _normalize_conversation_text(_latest_user_text(messages))
    if not normalized:
        return False
    tokens = normalized.split()
    if not tokens or len(tokens) > 6:
        return False
    if any(any(char.isdigit() for char in token) for token in tokens):
        return False
    if any(token in _NON_CONVERSATION_HINTS for token in tokens):
        return False
    latest_ai = _latest_ai_text(messages)
    if "?" in latest_ai:
        return False
    return True


def _build_context_facts(state: dict[str, Any] | None, messages: Sequence[BaseMessage] | None = None) -> str:
    facts = []    
    facts.append(_get_chain_summary())
    
    if state:
        user_info = state.get("user_info")
        if isinstance(user_info, dict):
            evm_addr = user_info.get("evm_address")
            if evm_addr:
                facts.append(f"User's EVM address: {evm_addr}")
            sol_addr = user_info.get("solana_address")
            if sol_addr:
                facts.append(f"User's Solana address: {sol_addr}")
                
        # Add recent execution facts if any
        exec_state = state.get("execution_state")
        if exec_state:
            if getattr(exec_state, "completed", False):
                facts.append("Last transaction finished successfully.")
            else:
                node_states = getattr(exec_state, "node_states", {})
                if any(str(getattr(ns, "status", "")).lower().endswith("failed") for ns in node_states.values()):
                    facts.append("The last transaction attempt failed.")
                else:
                    facts.append("A transaction is currently in progress.")

    if messages:
        last_user = _latest_user_text(messages).lower()
        if last_user:
            from config.chains import supported_chains, _CHAIN_ALIASES as EVM_ALIASES
            from config.solana_chains import SOLANA_CHAINS, _CHAIN_ALIASES as SOLANA_ALIASES
            all_known = set(supported_chains()) | set(SOLANA_CHAINS.keys()) | set(EVM_ALIASES.keys()) | set(SOLANA_ALIASES.keys())
            
            tokens = re.findall(r"[a-z]{3,}", last_user)
            ignore = {
                "balance",
                "address",
                "swap",
                "bridge",
                "transfer",
                "send",
                "unwrap",
                "check",
                "wallet",
                "show",
                "what",
            }

            for token in tokens:
                if token not in all_known and token not in ignore:
                    matches = get_close_matches(token, list(all_known), n=1, cutoff=0.7)
                    if matches:
                        facts.append(f"Hint: User mentioned '{token}', likely meant '{matches[0]}'.")

    if not facts:
        return ""
    return "\nContext Facts:\n" + "\n".join(f"- {f}" for f in facts)


def _build_conversation_prompt(messages: Sequence[BaseMessage], state: dict[str, Any] | None = None) -> list[HumanMessage]:
    if _should_isolate_latest_user_turn(messages):
        latest_user = _latest_user_text(messages)
        recent_messages = [HumanMessage(content=latest_user)] if latest_user else []
    else:
        recent_messages = _select_recent_messages(messages)
    lines: list[str] = []
    for msg in recent_messages:
        if isinstance(msg, HumanMessage):
            lines.append(f"User: {msg.content}")
        elif isinstance(msg, AIMessage):
            lines.append(f"Assistant: {msg.content}")
    formatted_history = "\n".join(lines)
    context_facts = _build_context_facts(state, messages=messages)
    return [
        HumanMessage(
            content=(
                f"{CONVERSATION_SYSTEM_PROMPT}\n{context_facts}\n"
                f"Conversation History:\n{formatted_history}\n"
                "Assistant:"
            )
        )
    ]


def _coerce_response_text(response: object) -> str:
    content = getattr(response, "content", response)
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = str(item.get("text") or "").strip()
                if text:
                    parts.append(text)
            else:
                text = str(item).strip()
                if text:
                    parts.append(text)
        return " ".join(parts).strip()
    return str(content).strip()


async def _invoke_conversation_llm(prompt: Sequence[BaseMessage]) -> object:
    llm = _get_conversation_llm()
    timeout_seconds = _conversation_timeout_seconds()
    ainvoke = getattr(llm, "ainvoke", None)
    if callable(ainvoke):
        result = ainvoke(prompt)
        # If the call returned an awaitable (coroutine / Future), await it with timeout.
        # Otherwise treat it as a synchronous result and return directly.
        from typing import Awaitable, cast

        if hasattr(result, "__await__"):
            return await asyncio.wait_for(
                cast(Awaitable[object], result), timeout=timeout_seconds
            )
        return result
    invoke = getattr(llm, "invoke", None)
    if callable(invoke):
        return await run_blocking(invoke, prompt, timeout=timeout_seconds)
    raise RuntimeError(
        format_with_recovery(
            "Conversation model is missing invoke/ainvoke",
            "verify conversation LLM initialization and retry",
        )
    )


async def respond_conversation(messages: Sequence[BaseMessage], state: dict[str, Any] | None = None) -> str:
    if _conversation_circuit_open():
        logger.warning("respond_conversation: skipping model call during cooldown")
        return conversation_failure_message("cooldown")

    prompt = _build_conversation_prompt(messages, state=state)
    try:
        response = await _invoke_conversation_llm(prompt)
        text = _coerce_response_text(response)
        if text:
            _record_conversation_success()
            return text
        raise ValueError(
            format_with_recovery(
                "Conversation model returned an empty reply",
                "retry with a short follow-up message",
            )
        )
    except asyncio.TimeoutError:
        _record_conversation_failure()
        logger.warning("respond_conversation: conversation model timed out")
        return conversation_failure_message("timeout")
    except Exception as exc:
        _record_conversation_failure()
        logger.warning("respond_conversation: fallback reply due to error: %s", exc)
        return conversation_failure_message()
