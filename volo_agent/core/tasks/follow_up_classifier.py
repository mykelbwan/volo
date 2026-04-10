from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from config.chains import get_chain_by_name
from config.solana_chains import get_solana_chain
from core.utils.balance_chains import (
    ALL_SUPPORTED_CHAIN_KEY,
    canonicalize_balance_chain,
)

_AMOUNT_RE = re.compile(r"(?<![a-z0-9])(\d+(?:\.\d+)?)(?![a-z])", re.IGNORECASE)
_EVM_ADDRESS_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
_SOLANA_ADDRESS_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
_TOKEN_WORD_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9._-]{0,31}")

_NOISE_WORDS = {
    "a",
    "an",
    "again",
    "as",
    "before",
    "chain",
    "different",
    "exact",
    "first",
    "hello",
    "hey",
    "hi",
    "it",
    "maybe",
    "me",
    "network",
    "not",
    "one",
    "on",
    "same",
    "second",
    "sure",
    "know",
    "dont",
    "idk",
    "that",
    "the",
    "this",
    "token",
    "to",
    "use",
    "wallet",
    "with",
}

_CONTROL_REPLIES = {
    "yes",
    "no",
    "confirm",
    "proceed",
    "retry",
    "edit",
    "change",
    "cancel",
    "go ahead",
    "go on",
    "ok",
    "okay",
}


@dataclass(frozen=True)
class FollowUpClassification:
    kind: str
    normalized_text: str
    control_reply: str | None = None
    expected_slot: str | None = None
    slot_value: Any = None


def _normalize(text: str | None) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def _candidate_chain_values(text: str) -> list[str]:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if not normalized:
        return []
    candidates = [normalized]
    simplified = re.sub(r"\b(?:chain|network)\b", " ", normalized)
    simplified = re.sub(r"\s+", " ", simplified).strip()
    if simplified and simplified not in candidates:
        candidates.append(simplified)
    tokens = [token for token in re.findall(r"[a-z0-9]+", normalized) if token]
    for start in range(len(tokens)):
        for width in (3, 2, 1):
            end = start + width
            if end > len(tokens):
                continue
            phrase = " ".join(tokens[start:end]).strip()
            if not phrase or phrase in candidates:
                continue
            if all(token in _NOISE_WORDS for token in phrase.split()):
                continue
            candidates.append(phrase)
    return candidates


def parse_chain_slot_value(
    text: str | None,
    *,
    allow_all_supported: bool = False,
) -> str | None:
    for candidate in _candidate_chain_values(str(text or "")):
        canonical = canonicalize_balance_chain(candidate)
        if canonical and (
            allow_all_supported or canonical != ALL_SUPPORTED_CHAIN_KEY
        ):
            return canonical
        try:
            return get_chain_by_name(candidate).name.strip().lower()
        except Exception:
            pass
        try:
            return get_solana_chain(candidate).network.strip().lower()
        except Exception:
            continue
    return None


def parse_amount_slot_value(text: str | None) -> float | str | None:
    normalized = _normalize(text)
    if normalized in {"all", "max"}:
        return normalized
    match = _AMOUNT_RE.search(str(text or ""))
    if not match:
        return None
    try:
        value = float(match.group(1))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def parse_token_slot_value(text: str | None) -> dict[str, str] | None:
    raw_text = str(text or "")
    evm_match = _EVM_ADDRESS_RE.search(raw_text)
    if evm_match:
        return {"address": evm_match.group(0)}
    matches = _TOKEN_WORD_RE.findall(raw_text)
    if not matches:
        return None
    for raw in reversed(matches):
        cleaned = raw.strip().lower()
        if not cleaned or cleaned in _NOISE_WORDS:
            continue
        if cleaned.endswith("'s") and cleaned[:-2] in _NOISE_WORDS:
            continue
        return {"symbol": raw.strip().upper()}
    return None


def parse_recipient_slot_value(text: str | None) -> str | None:
    raw_text = str(text or "")
    evm_match = _EVM_ADDRESS_RE.search(raw_text)
    if evm_match:
        return evm_match.group(0)
    solana_match = _SOLANA_ADDRESS_RE.search(raw_text)
    if solana_match:
        return solana_match.group(0)
    return None


def classify_follow_up_reply(
    text: str | None,
    *,
    expected_slot: str | None = None,
    allow_all_supported_chain: bool = False,
) -> FollowUpClassification:
    normalized = _normalize(text)
    if not normalized:
        return FollowUpClassification(kind="none", normalized_text="")
    if normalized in _CONTROL_REPLIES:
        return FollowUpClassification(
            kind="control",
            normalized_text=normalized,
            control_reply=normalized,
        )

    slot_name = str(expected_slot or "").strip().lower()
    if slot_name in {"chain", "target_chain"}:
        chain = parse_chain_slot_value(
            normalized,
            allow_all_supported=allow_all_supported_chain,
        )
        if chain:
            return FollowUpClassification(
                kind="valid_slot_value",
                normalized_text=normalized,
                expected_slot=slot_name,
                slot_value=chain,
            )
    elif slot_name == "amount":
        amount = parse_amount_slot_value(normalized)
        if amount is not None:
            return FollowUpClassification(
                kind="valid_slot_value",
                normalized_text=normalized,
                expected_slot=slot_name,
                slot_value=amount,
            )
    elif slot_name in {"token", "token_in", "token_out"}:
        token = parse_token_slot_value(normalized)
        if token:
            return FollowUpClassification(
                kind="valid_slot_value",
                normalized_text=normalized,
                expected_slot=slot_name,
                slot_value=token,
            )
    elif slot_name == "recipient":
        recipient = parse_recipient_slot_value(normalized)
        if recipient:
            return FollowUpClassification(
                kind="valid_slot_value",
                normalized_text=normalized,
                expected_slot=slot_name,
                slot_value=recipient,
            )

    return FollowUpClassification(kind="none", normalized_text=normalized)
