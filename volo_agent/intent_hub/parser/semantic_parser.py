import json
import logging
import os
import re
from typing import Sequence

from langchain_core.messages import BaseMessage, HumanMessage

from core.conversation.account_query_parser import parse_account_query
from core.utils.balance_chains import (
    ALL_SUPPORTED_CHAIN_KEY,
    canonicalize_balance_chain,
    is_all_supported_chain_request,
)
from intent_hub.ontology.intent import Intent, IntentStatus
from intent_hub.parser.llm_client import call_llm_async
from intent_hub.parser.prompt import get_parser_prompt
from intent_hub.parser.validation import validate_intent
from intent_hub.utils.messages import format_with_recovery

logger = logging.getLogger(__name__)
_TOKEN_REGISTRY_CACHE: dict | None = None

_SIMPLE_SWAP_PATTERNS = (
    re.compile(
        r"^\s*(?:swap|convert|exchange)\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"(?:for|to|into)\s+"
        r"(?P<token_out>[a-zA-Z0-9._-]+)\s+"
        r"on\s+(?P<chain>.+?)\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*(?:swap|convert|exchange)\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"on\s+(?P<chain>.+?)\s+"
        r"(?:for|to|into)\s+"
        r"(?P<token_out>[a-zA-Z0-9._-]+)\s*$",
        re.IGNORECASE,
    ),
)
_INCOMPLETE_SWAP_PATTERNS = (
    re.compile(
        r"^\s*(?:swap|convert|exchange)\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"on\s+(?P<chain>.+?)\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*(?:swap|convert|exchange)\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s*$",
        re.IGNORECASE,
    ),
)
_MISSING_AMOUNT_SWAP_PATTERNS = (
    re.compile(
        r"^\s*(?:swap|convert|exchange)\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"(?:for|to|into)\s+"
        r"(?P<token_out>[a-zA-Z0-9._-]+)"
        r"(?:\s+on\s+(?P<chain>.+?))?\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*(?:swap|convert|exchange)\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"on\s+(?P<chain>.+?)\s+"
        r"(?:for|to|into)\s+"
        r"(?P<token_out>[a-zA-Z0-9._-]+)\s*$",
        re.IGNORECASE,
    ),
)
_SIMPLE_BRIDGE_PATTERNS = (
    re.compile(
        r"^\s*bridge\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"from\s+(?P<chain>.+?)\s+"
        r"to\s+(?P<target_chain>.+?)\s*$",
        re.IGNORECASE,
    ),
)
_INCOMPLETE_BRIDGE_PATTERNS = (
    re.compile(
        r"^\s*bridge\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"to\s+(?P<target_chain>.+?)\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*bridge\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"from\s+(?P<chain>.+?)\s*$",
        re.IGNORECASE,
    ),
)
_MISSING_AMOUNT_BRIDGE_PATTERNS = (
    re.compile(
        r"^\s*bridge\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"from\s+(?P<chain>.+?)\s+"
        r"to\s+(?P<target_chain>.+?)"
        r"(?:\s+(?:and|then)\b.*)?\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*bridge\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"to\s+(?P<target_chain>.+?)\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*bridge\s+"
        r"(?P<token_in>[a-zA-Z0-9._-]+)\s+"
        r"from\s+(?P<chain>.+?)\s*$",
        re.IGNORECASE,
    ),
)
_SIMPLE_TRANSFER_PATTERNS = (
    re.compile(
        r"^\s*(?:send|transfer)\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token>[a-zA-Z0-9._-]+)\s+"
        r"to\s+(?P<recipient>0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})"
        r"(?:\s+on\s+(?P<chain>.+?))?\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*(?:send|transfer)\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token>[a-zA-Z0-9._-]+)\s+"
        r"on\s+(?P<chain>.+?)\s+"
        r"to\s+(?P<recipient>0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})\s*$",
        re.IGNORECASE,
    ),
)
_INCOMPLETE_TRANSFER_PATTERNS = (
    re.compile(
        r"^\s*(?:send|transfer)\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token>[a-zA-Z0-9._-]+)\s+"
        r"on\s+(?P<chain>.+?)\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*(?:send|transfer)\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token>[a-zA-Z0-9._-]+)\s+"
        r"to\s+(?P<recipient>0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*(?:send|transfer)\s+"
        r"(?P<amount>\d+(?:\.\d+)?)\s+"
        r"(?P<token>[a-zA-Z0-9._-]+)\s*$",
        re.IGNORECASE,
    ),
)
_MISSING_AMOUNT_TRANSFER_PATTERNS = (
    re.compile(
        r"^\s*(?:send|transfer)\s+"
        r"(?P<token>[a-zA-Z0-9._-]+)\s+"
        r"to\s+(?P<recipient>0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})"
        r"(?:\s+on\s+(?P<chain>.+?))?\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*(?:send|transfer)\s+"
        r"(?P<token>[a-zA-Z0-9._-]+)\s+"
        r"on\s+(?P<chain>.+?)\s+"
        r"to\s+(?P<recipient>0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})\s*$",
        re.IGNORECASE,
    ),
)
_SIMPLE_UNWRAP_PATTERNS = (
    re.compile(
        r"^\s*unwrap\s+"
        r"(?:(?P<amount>\d+(?:\.\d+)?)\s+)?"
        r"\"?(?P<token>[a-zA-Z0-9._-]+)\"?\s+"
        r"(?:on|in)\s+(?P<chain>.+?)\s*$",
        re.IGNORECASE,
    ),
)
_INCOMPLETE_UNWRAP_PATTERNS = (
    re.compile(
        r"^\s*unwrap\s+"
        r"(?:(?P<amount>\d+(?:\.\d+)?)\s+)?"
        r"\"?(?P<token>[a-zA-Z0-9._-]+)\"?\s*$",
        re.IGNORECASE,
    ),
)
_SEQUENCE_SPLIT_RE = re.compile(r"\s+(?:and then|then|and)\s+", re.IGNORECASE)
_DEPENDENT_SWAP_CLAUSE_RE = re.compile(
    r"^\s*(?:swap|convert|exchange)\s+"
    r"(?:it|them|that)\s+"
    r"(?:for|to|into)\s+"
    r"(?P<token_out>[a-zA-Z0-9._-]+)"
    r"(?:\s+on\s+(?P<chain>.+?))?\s*$",
    re.IGNORECASE,
)
_DEPENDENT_TRANSFER_CLAUSE_RE = re.compile(
    r"^\s*(?:send|transfer)\s+"
    r"(?:it|them|that)\s+"
    r"to\s+(?P<recipient>0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})"
    r"(?:\s+on\s+(?P<chain>.+?))?\s*$",
    re.IGNORECASE,
)
_DEPENDENT_BRIDGE_CLAUSE_RE = re.compile(
    r"^\s*bridge\s+"
    r"(?:it|them|that)\s+"
    r"(?:from\s+(?P<chain>.+?)\s+)?"
    r"to\s+(?P<target_chain>.+?)\s*$",
    re.IGNORECASE,
)

_BALANCE_ACTION_WORDS = (
    "swap",
    "bridge",
    "transfer",
    "send",
    "buy",
    "sell",
    "convert",
    "exchange",
    "unwrap",
)
_BALANCE_HINT_WORDS = ("balance", "balances", "portfolio", "holdings")
_BALANCE_GENERIC_TEXTS = {
    "balance",
    "balances",
    "my balance",
    "my balances",
    "check balance",
    "check my balance",
    "show balance",
    "show my balance",
    "wallet balance",
    "my wallet balance",
    "portfolio",
    "my portfolio",
    "holdings",
    "my holdings",
    "how much do i have",
    "what do i have",
    "what is my balance",
    "what's my balance",
}
_BALANCE_ON_CHAIN_RE = re.compile(
    r"^(?:.*?\b(?:balance|balances|portfolio|holdings)\b)\s+(?:on|in)\s+(?P<chain>.+?)\s*$",
    re.IGNORECASE,
)
_BALANCE_PREFIX_CHAIN_RE = re.compile(
    r"^(?P<chain>[a-z0-9 ._-]+?)\s+(?:balance|balances)\s*$",
    re.IGNORECASE,
)


def _load_token_registry() -> dict:
    registry_path = os.path.join(
        os.path.dirname(__file__), "..", "registry", "tokens.json"
    )
    if not os.path.exists(registry_path):
        return {}
    try:
        with open(registry_path, "r") as f:
            data = json.load(f)
    except Exception as exc:
        logger.warning("_load_token_registry: failed to load tokens.json: %s", exc)
        return {}
    return data if isinstance(data, dict) else {}


async def _get_token_registry_async() -> dict:
    global _TOKEN_REGISTRY_CACHE
    if _TOKEN_REGISTRY_CACHE is not None:
        return _TOKEN_REGISTRY_CACHE
    try:
        # Local registry load is a tiny filesystem read and returns immediately
        # when the registry file is absent. Keeping it synchronous avoids the
        # threadpool timeout overhead that shows up in fast parser paths.
        _TOKEN_REGISTRY_CACHE = _load_token_registry()
    except Exception as exc:
        logger.warning("_get_token_registry_async: failed to load token registry: %s", exc)
        _TOKEN_REGISTRY_CACHE = {}
    return _TOKEN_REGISTRY_CACHE


def _extract_last_user_text(messages: Sequence[BaseMessage]) -> str:
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            return str(message.content or "").strip()
    return ""


def _looks_pure_numeric(value: str | None) -> bool:
    return bool(re.fullmatch(r"\d+(?:\.\d+)?", str(value or "").strip()))


def _symbol_from_slot(slot: object) -> str | None:
    if isinstance(slot, dict):
        symbol = slot.get("symbol")
    else:
        symbol = slot
    text = str(symbol or "").strip().upper()
    return text or None


def _token_slot_name_for_intent(intent_type: str) -> str:
    if intent_type == "transfer":
        return "token"
    if intent_type == "unwrap":
        return "token"
    return "token_in"


def _output_token_symbol(intent: Intent) -> str | None:
    slots = intent.slots or {}
    if intent.intent_type == "swap":
        return _symbol_from_slot(slots.get("token_out"))
    if intent.intent_type == "bridge":
        return _symbol_from_slot(slots.get("token_in"))
    if intent.intent_type == "transfer":
        return _symbol_from_slot(slots.get("token"))
    if intent.intent_type == "unwrap":
        return _symbol_from_slot(slots.get("token"))
    return None


def _output_chain(intent: Intent) -> str | None:
    slots = intent.slots or {}
    target = str(slots.get("target_chain") or "").strip().lower()
    if target:
        return target
    chain = str(slots.get("chain") or "").strip().lower()
    return chain or None


def _parse_bridge_clause_intent(text: str) -> Intent | None:
    clause_messages = [HumanMessage(content=text)]
    candidates = (
        _parse_simple_bridge_message(clause_messages),
        _parse_incomplete_bridge_message(clause_messages),
        _parse_missing_amount_bridge_message(clause_messages),
    )
    for parsed in candidates:
        if parsed:
            return parsed[0]
    return None


def _make_intent(
    *,
    intent_type: str,
    slots: dict[str, object],
    raw_input: str,
) -> Intent:
    return Intent(
        intent_type=intent_type,
        slots=slots,
        missing_slots=[],
        constraints={},
        confidence=0.95,
        status=IntentStatus.INCOMPLETE,
        raw_input=raw_input,
        clarification_prompt=None,
        condition=None,
    )


def _build_dependent_intent_clause(clause: str, previous: Intent, *, raw_input: str) -> Intent | None:
    swap_match = _DEPENDENT_SWAP_CLAUSE_RE.match(clause)
    if swap_match:
        prev_token = _output_token_symbol(previous)
        prev_amount = (previous.slots or {}).get("amount")
        chain = swap_match.groupdict().get("chain")
        slots: dict[str, object] = {
            "token_out": {"symbol": swap_match.group("token_out").upper()},
            "_dependent_reference": "previous_action_output",
        }
        if prev_token:
            slots["token_in"] = {"symbol": prev_token}
        else:
            slots["_carry_token_from_prev"] = True
        if prev_amount is not None:
            slots["amount"] = prev_amount
        else:
            slots["_carry_amount_from_prev"] = True
        chain_value = (chain or _output_chain(previous) or "").strip().lower()
        if chain_value:
            slots["chain"] = chain_value
        else:
            slots["_carry_chain_from_prev_target"] = True
        return _make_intent(intent_type="swap", slots=slots, raw_input=raw_input)

    transfer_match = _DEPENDENT_TRANSFER_CLAUSE_RE.match(clause)
    if transfer_match:
        prev_token = _output_token_symbol(previous)
        prev_amount = (previous.slots or {}).get("amount")
        chain = transfer_match.groupdict().get("chain")
        slots = {
            "recipient": transfer_match.group("recipient").strip(),
            "_dependent_reference": "previous_action_output",
        }
        if prev_token:
            slots["token"] = {"symbol": prev_token}
        else:
            slots["_carry_token_from_prev"] = True
        if prev_amount is not None:
            slots["amount"] = prev_amount
        else:
            slots["_carry_amount_from_prev"] = True
        chain_value = (chain or _output_chain(previous) or "").strip().lower()
        if chain_value:
            slots["chain"] = chain_value
        else:
            slots["_carry_chain_from_prev_target"] = True
        return _make_intent(intent_type="transfer", slots=slots, raw_input=raw_input)

    bridge_match = _DEPENDENT_BRIDGE_CLAUSE_RE.match(clause)
    if bridge_match:
        prev_token = _output_token_symbol(previous)
        prev_amount = (previous.slots or {}).get("amount")
        chain = bridge_match.groupdict().get("chain")
        target_chain = bridge_match.group("target_chain")
        slots = {
            "target_chain": target_chain.strip().lower(),
            "_dependent_reference": "previous_action_output",
        }
        if prev_token:
            slots["token_in"] = {"symbol": prev_token}
        else:
            slots["_carry_token_from_prev"] = True
        if prev_amount is not None:
            slots["amount"] = prev_amount
        else:
            slots["_carry_amount_from_prev"] = True
        chain_value = (chain or _output_chain(previous) or "").strip().lower()
        if chain_value:
            slots["chain"] = chain_value
        else:
            slots["_carry_chain_from_prev_target"] = True
        return _make_intent(intent_type="bridge", slots=slots, raw_input=raw_input)

    return None


def _parse_dependent_sequence_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []
    if not _SEQUENCE_SPLIT_RE.search(text):
        return []

    clauses = [part.strip() for part in _SEQUENCE_SPLIT_RE.split(text) if part.strip()]
    if len(clauses) < 2:
        return []

    first = _parse_bridge_clause_intent(clauses[0])
    if first is None:
        return []

    intents: list[Intent] = [first]
    previous = first
    for clause in clauses[1:]:
        dependent = _build_dependent_intent_clause(clause, previous, raw_input=text)
        if dependent is None:
            break
        intents.append(dependent)
        previous = dependent

    if len(intents) < 2:
        return []
    return intents


def _parse_simple_swap_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _SIMPLE_SWAP_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        try:
            amount = float(match.group("amount"))
        except (TypeError, ValueError):
            continue
        token_in = match.group("token_in").upper()
        token_out = match.group("token_out").upper()
        chain = match.group("chain").strip().lower()
        return [
            Intent(
                intent_type="swap",
                slots={
                    "token_in": {"symbol": token_in},
                    "token_out": {"symbol": token_out},
                    "amount": amount,
                    "chain": chain,
                },
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.COMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _parse_incomplete_swap_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _INCOMPLETE_SWAP_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        try:
            amount = float(match.group("amount"))
        except (TypeError, ValueError):
            continue
        token_in = match.group("token_in").upper()
        chain = match.groupdict().get("chain")
        slots = {
            "token_in": {"symbol": token_in},
            "amount": amount,
        }
        if chain:
            slots["chain"] = chain.strip().lower()
        return [
            Intent(
                intent_type="swap",
                slots=slots,
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.INCOMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _parse_missing_amount_swap_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _MISSING_AMOUNT_SWAP_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        token_in_raw = match.group("token_in")
        if _looks_pure_numeric(token_in_raw):
            continue
        token_out_raw = match.group("token_out")
        if _looks_pure_numeric(token_out_raw):
            continue
        slots: dict[str, object] = {
            "token_in": {"symbol": token_in_raw.upper()},
            "token_out": {"symbol": token_out_raw.upper()},
        }
        chain = match.groupdict().get("chain")
        if chain:
            slots["chain"] = chain.strip().lower()
        return [
            Intent(
                intent_type="swap",
                slots=slots,
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.INCOMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _parse_simple_bridge_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _SIMPLE_BRIDGE_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        try:
            amount = float(match.group("amount"))
        except (TypeError, ValueError):
            continue
        return [
            Intent(
                intent_type="bridge",
                slots={
                    "token_in": {"symbol": match.group("token_in").upper()},
                    "amount": amount,
                    "chain": match.group("chain").strip().lower(),
                    "target_chain": match.group("target_chain").strip().lower(),
                },
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.COMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _parse_incomplete_bridge_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _INCOMPLETE_BRIDGE_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        try:
            amount = float(match.group("amount"))
        except (TypeError, ValueError):
            continue
        slots = {
            "token_in": {"symbol": match.group("token_in").upper()},
            "amount": amount,
        }
        chain = match.groupdict().get("chain")
        if chain:
            slots["chain"] = chain.strip().lower()
        target_chain = match.groupdict().get("target_chain")
        if target_chain:
            slots["target_chain"] = target_chain.strip().lower()
        return [
            Intent(
                intent_type="bridge",
                slots=slots,
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.INCOMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _parse_missing_amount_bridge_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _MISSING_AMOUNT_BRIDGE_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        token_in_raw = match.group("token_in")
        if _looks_pure_numeric(token_in_raw):
            continue
        slots: dict[str, object] = {
            "token_in": {"symbol": token_in_raw.upper()},
        }
        chain = match.groupdict().get("chain")
        if chain:
            slots["chain"] = chain.strip().lower()
        target_chain = match.groupdict().get("target_chain")
        if target_chain:
            slots["target_chain"] = target_chain.strip().lower()
        return [
            Intent(
                intent_type="bridge",
                slots=slots,
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.INCOMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _parse_simple_transfer_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _SIMPLE_TRANSFER_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        try:
            amount = float(match.group("amount"))
        except (TypeError, ValueError):
            continue
        slots = {
            "token": {"symbol": match.group("token").upper()},
            "amount": amount,
            "recipient": match.group("recipient").strip(),
        }
        chain = match.groupdict().get("chain")
        if chain:
            slots["chain"] = chain.strip().lower()
        return [
            Intent(
                intent_type="transfer",
                slots=slots,
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.COMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _parse_incomplete_transfer_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _INCOMPLETE_TRANSFER_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        try:
            amount = float(match.group("amount"))
        except (TypeError, ValueError):
            continue
        slots = {
            "token": {"symbol": match.group("token").upper()},
            "amount": amount,
        }
        recipient = match.groupdict().get("recipient")
        if recipient:
            slots["recipient"] = recipient.strip()
        chain = match.groupdict().get("chain")
        if chain:
            slots["chain"] = chain.strip().lower()
        return [
            Intent(
                intent_type="transfer",
                slots=slots,
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.INCOMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _parse_missing_amount_transfer_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _MISSING_AMOUNT_TRANSFER_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        token_raw = match.group("token")
        if _looks_pure_numeric(token_raw):
            continue
        slots = {
            "token": {"symbol": token_raw.upper()},
            "recipient": match.group("recipient").strip(),
        }
        chain = match.groupdict().get("chain")
        if chain:
            slots["chain"] = chain.strip().lower()
        return [
            Intent(
                intent_type="transfer",
                slots=slots,
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.INCOMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _parse_simple_unwrap_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _SIMPLE_UNWRAP_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        amount = match.groupdict().get("amount")
        amount_value: float | None = None
        if amount:
            try:
                amount_value = float(amount)
            except (TypeError, ValueError):
                amount_value = None
        slots: dict[str, object] = {
            "token": {"symbol": match.group("token").upper()},
            "chain": match.group("chain").strip().lower(),
        }
        if amount_value is not None:
            slots["amount"] = amount_value
        return [
            Intent(
                intent_type="unwrap",
                slots=slots,
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.COMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _parse_incomplete_unwrap_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    for pattern in _INCOMPLETE_UNWRAP_PATTERNS:
        match = pattern.match(text)
        if not match:
            continue
        amount = match.groupdict().get("amount")
        amount_value: float | None = None
        if amount:
            try:
                amount_value = float(amount)
            except (TypeError, ValueError):
                amount_value = None
        slots: dict[str, object] = {
            "token": {"symbol": match.group("token").upper()},
        }
        if amount_value is not None:
            slots["amount"] = amount_value
        return [
            Intent(
                intent_type="unwrap",
                slots=slots,
                missing_slots=[],
                constraints={},
                confidence=0.99,
                status=IntentStatus.INCOMPLETE,
                raw_input=text,
                clarification_prompt=None,
                condition=None,
            )
        ]

    return []


def _clean_balance_text(text: str) -> str:
    cleaned = str(text or "").strip().lower()
    cleaned = re.sub(r"[?!.,]+$", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _looks_like_single_balance_query(cleaned_text: str) -> bool:
    if not cleaned_text:
        return False
    if cleaned_text in _BALANCE_GENERIC_TEXTS:
        return True
    if any(f" {word} " in f" {cleaned_text} " for word in _BALANCE_ACTION_WORDS):
        return False
    if " then " in f" {cleaned_text} ":
        return False
    return any(word in cleaned_text for word in _BALANCE_HINT_WORDS)


def _balance_chain_from_text(cleaned_text: str) -> str | None:
    direct = canonicalize_balance_chain(cleaned_text)
    if direct:
        return direct

    on_match = _BALANCE_ON_CHAIN_RE.match(cleaned_text)
    if on_match:
        chain_hint = on_match.group("chain")
        chain = canonicalize_balance_chain(chain_hint)
        if chain:
            return chain

    prefix_match = _BALANCE_PREFIX_CHAIN_RE.match(cleaned_text)
    if prefix_match:
        chain_hint = prefix_match.group("chain")
        chain = canonicalize_balance_chain(chain_hint)
        if chain:
            return chain
    return None


def _raw_balance_chain_hint(cleaned_text: str) -> str | None:
    on_match = _BALANCE_ON_CHAIN_RE.match(cleaned_text)
    if on_match:
        chain_hint = str(on_match.group("chain") or "").strip()
        if chain_hint:
            return chain_hint

    prefix_match = _BALANCE_PREFIX_CHAIN_RE.match(cleaned_text)
    if prefix_match:
        chain_hint = str(prefix_match.group("chain") or "").strip()
        if chain_hint:
            return chain_hint
    return None


def _is_generic_all_balance_request(cleaned_text: str) -> bool:
    if cleaned_text in _BALANCE_GENERIC_TEXTS:
        return True
    if "all chain" in cleaned_text or "all supported chain" in cleaned_text:
        return True
    if is_all_supported_chain_request(cleaned_text):
        return True
    return False


def _build_balance_intent(raw_text: str, chain: str) -> Intent:
    return Intent(
        intent_type="balance",
        slots={"chain": chain},
        missing_slots=[],
        constraints={},
        confidence=0.99,
        status=IntentStatus.COMPLETE,
        raw_input=raw_text,
        clarification_prompt=None,
        condition=None,
    )


def _build_incomplete_balance_intent(raw_text: str) -> Intent:
    return Intent(
        intent_type="balance",
        slots={"chain": None},
        missing_slots=["chain"],
        constraints={},
        confidence=0.99,
        status=IntentStatus.INCOMPLETE,
        raw_input=raw_text,
        clarification_prompt=None,
        condition=None,
    )


def _parse_simple_balance_message(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    text = _extract_last_user_text(messages)
    if not text:
        return []

    account_query = parse_account_query(text)
    if account_query and account_query.kind == "balance":
        if account_query.chain and account_query.chain != ALL_SUPPORTED_CHAIN_KEY:
            return [_build_balance_intent(text, account_query.chain)]
        if account_query.chain == ALL_SUPPORTED_CHAIN_KEY:
            return [_build_balance_intent(text, ALL_SUPPORTED_CHAIN_KEY)]
        if account_query.chain_family == "evm":
            return [_build_incomplete_balance_intent(text)]

    cleaned = _clean_balance_text(text)
    if not _looks_like_single_balance_query(cleaned):
        return []

    chain = _balance_chain_from_text(cleaned)
    if chain and chain != ALL_SUPPORTED_CHAIN_KEY:
        return [_build_balance_intent(text, chain)]

    if _is_generic_all_balance_request(cleaned) or chain == ALL_SUPPORTED_CHAIN_KEY:
        return [_build_balance_intent(text, ALL_SUPPORTED_CHAIN_KEY)]

    chain_hint = _raw_balance_chain_hint(cleaned)
    if chain_hint:
        # Preserve unknown chain hints so downstream logic can provide deterministic
        # unsupported-chain feedback without requiring LLM fallback.
        return [_build_balance_intent(text, chain_hint)]
    return []


async def parse_async(messages: Sequence[BaseMessage]) -> Sequence[Intent]:
    """
    Async entrypoint: parses conversation history into a list of Intent objects.
    """
    token_registry = await _get_token_registry_async()
    dependent_sequence_intents = _parse_dependent_sequence_message(messages)
    if dependent_sequence_intents:
        return [
            validate_intent(intent, token_registry) for intent in dependent_sequence_intents
        ]
    simple_intents = _parse_simple_swap_message(messages)
    if simple_intents:
        return [validate_intent(intent, token_registry) for intent in simple_intents]
    incomplete_swap_intents = _parse_incomplete_swap_message(messages)
    if incomplete_swap_intents:
        return [
            validate_intent(intent, token_registry)
            for intent in incomplete_swap_intents
        ]
    missing_amount_swap_intents = _parse_missing_amount_swap_message(messages)
    if missing_amount_swap_intents:
        return [
            validate_intent(intent, token_registry)
            for intent in missing_amount_swap_intents
        ]
    simple_bridge_intents = _parse_simple_bridge_message(messages)
    if simple_bridge_intents:
        return [
            validate_intent(intent, token_registry)
            for intent in simple_bridge_intents
        ]
    incomplete_bridge_intents = _parse_incomplete_bridge_message(messages)
    if incomplete_bridge_intents:
        return [
            validate_intent(intent, token_registry)
            for intent in incomplete_bridge_intents
        ]
    missing_amount_bridge_intents = _parse_missing_amount_bridge_message(messages)
    if missing_amount_bridge_intents:
        return [
            validate_intent(intent, token_registry)
            for intent in missing_amount_bridge_intents
        ]
    simple_transfer_intents = _parse_simple_transfer_message(messages)
    if simple_transfer_intents:
        return [
            validate_intent(intent, token_registry)
            for intent in simple_transfer_intents
        ]
    incomplete_transfer_intents = _parse_incomplete_transfer_message(messages)
    if incomplete_transfer_intents:
        return [
            validate_intent(intent, token_registry)
            for intent in incomplete_transfer_intents
        ]
    missing_amount_transfer_intents = _parse_missing_amount_transfer_message(messages)
    if missing_amount_transfer_intents:
        return [
            validate_intent(intent, token_registry)
            for intent in missing_amount_transfer_intents
        ]
    simple_unwrap_intents = _parse_simple_unwrap_message(messages)
    if simple_unwrap_intents:
        return [
            validate_intent(intent, token_registry) for intent in simple_unwrap_intents
        ]
    incomplete_unwrap_intents = _parse_incomplete_unwrap_message(messages)
    if incomplete_unwrap_intents:
        return [
            validate_intent(intent, token_registry)
            for intent in incomplete_unwrap_intents
        ]
    simple_balance_intents = _parse_simple_balance_message(messages)
    if simple_balance_intents:
        return [
            validate_intent(intent, token_registry) for intent in simple_balance_intents
        ]

    prompt = get_parser_prompt(messages)
    json_response = await call_llm_async(prompt)

    if isinstance(json_response, dict):
        payload_items = [json_response]
    elif isinstance(json_response, list):
        payload_items = json_response
    else:
        raise ValueError(
            format_with_recovery(
                "Intent parser returned an unsupported payload type",
                "retry with a concise request that includes action, token, amount, and chain",
            )
        )

    validated_intents = []

    for item in payload_items:
        if not isinstance(item, dict):
            continue
        try:
            intent = Intent(**item)
        except Exception as exc:
            logger.warning("parse_async: skipping invalid intent payload item: %s", exc)
            continue
        validated_intents.append(validate_intent(intent, token_registry))

    if not validated_intents:
        raise ValueError(
            format_with_recovery(
                "Intent parser could not extract a valid intent",
                "retry with explicit action details (token, amount, chain, recipient/target chain)",
            )
        )

    return validated_intents
