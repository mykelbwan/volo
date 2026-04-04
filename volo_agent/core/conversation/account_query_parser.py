from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import get_close_matches
from functools import lru_cache
from typing import Literal

from config.chains import _CHAIN_ALIASES as EVM_CHAIN_ALIASES
from config.chains import get_chain_by_name, supported_chains
from config.solana_chains import _CHAIN_ALIASES as SOLANA_CHAIN_ALIASES
from config.solana_chains import get_solana_chain
from core.utils.balance_chains import (
    ALL_SUPPORTED_CHAIN_KEY,
    canonicalize_balance_chain,
    is_all_supported_chain_request,
)

# --- TAG Constants ---
TAG_BALANCE = "bal"
TAG_ADDRESS = "addr"
TAG_WALLET = "wal"
TAG_ACTION = "act"
TAG_REQUEST = "req"
TAG_TOKEN = "tok"

# --- Unified Registry for O(1) Lookup ---
_INTENT_REGISTRY: dict[str, set[str]] = {
    "address": {TAG_ADDRESS},
    "addr": {TAG_ADDRESS},
    "addy": {TAG_ADDRESS},
    "balance": {TAG_BALANCE},
    "balances": {TAG_BALANCE},
    "portfolio": {TAG_BALANCE},
    "holding": {TAG_BALANCE},
    "holdings": {TAG_BALANCE},
    "funds": {TAG_BALANCE},
    # "sepolia": {TAG_BALANCE}, # Hint that 'sepolia balance' is a balance query
    "wallet": {TAG_WALLET},
    "add": {TAG_ACTION},
    "send": {TAG_ACTION},
    "transfer": {TAG_ACTION},
    "swap": {TAG_ACTION},
    "bridge": {TAG_ACTION},
    "buy": {TAG_ACTION},
    "sell": {TAG_ACTION},
    "convert": {TAG_ACTION},
    "unwrap": {TAG_ACTION},
    "my": {TAG_REQUEST},
    "show": {TAG_REQUEST},
    "what": {TAG_REQUEST},
    "whats": {TAG_REQUEST},
    "give": {TAG_REQUEST},
    "share": {TAG_REQUEST},
    "where": {TAG_REQUEST},
    "get": {TAG_REQUEST},
    "tell": {TAG_REQUEST},
    "find": {TAG_REQUEST},
    "see": {TAG_REQUEST},
    "check": {TAG_REQUEST},
}

_COMMON_TOKENS = {
    "eth",
    "sol",
    "usdc",
    "usdt",
    "btc",
    "wbtc",
    "dai",
    "link",
    "matic",
    "arb",
    "op",
    "bonk",
    "wif",
    "pepe",
    "shib",
    "uni",
    "aave",
    "crv",
    "ldo",
    "jup",
    "pyth",
    "stt",
    "bnb",
    "avax",
    "pol",
    "tokens",
}
for tok in _COMMON_TOKENS:
    _INTENT_REGISTRY.setdefault(tok, set()).add(TAG_TOKEN)

_BALANCE_PHRASES = {
    "how much do i have",
    "what do i have",
    "what is left",
    "what s left",
    "whats left",
    "remaining funds",
    "how many",
}

AccountQueryKind = Literal["wallet_address", "balance"]
ChainFamily = Literal["evm", "solana"]


@dataclass(frozen=True)
class AccountQuery:
    kind: AccountQueryKind
    chain: str | None = None
    chain_family: ChainFamily | None = None
    raw_chain_hint: str | None = None
    token: str | None = None


@dataclass(frozen=True)
class _ChainMatch:
    chain: str | None
    family: ChainFamily
    raw_hint: str


def _normalize_text(value: str | None) -> str:
    # Keep hyphens for things like 'solana-devnet'
    normalized = re.sub(r"[^a-z0-9\-]+", " ", str(value or "").strip().lower())
    return re.sub(r"\s+", " ", normalized).strip()


def _tokenize(value: str) -> list[str]:
    return value.split() if value else []


@lru_cache(maxsize=1)
def _chain_phrase_index() -> dict[str, _ChainMatch]:
    index: dict[str, _ChainMatch] = {
        "evm": _ChainMatch(chain=None, family="evm", raw_hint="evm"),
    }
    # 1. Base names (EVM)
    for alias in supported_chains():
        try:
            canonical = get_chain_by_name(alias).name.strip().lower()
            index[alias.strip().lower()] = _ChainMatch(
                chain=canonical, family="evm", raw_hint=alias.strip().lower()
            )
            # Explicit fallback for 'bnb' shorthand
            if "bnb" in alias.lower() and "bnb" not in index:
                index["bnb"] = _ChainMatch(
                    chain=canonical, family="evm", raw_hint="bnb"
                )
        except Exception:
            continue

    # 1b. Base names (Solana)
    from config.solana_chains import SOLANA_CHAINS

    for network in SOLANA_CHAINS:
        try:
            cfg = SOLANA_CHAINS[network]
            index[network.strip().lower()] = _ChainMatch(
                chain=cfg.network, family="solana", raw_hint=network.strip().lower()
            )
        except Exception:
            continue

    # 2. EVM Aliases
    for alias, target in EVM_CHAIN_ALIASES.items():
        try:
            canonical = get_chain_by_name(target).name.strip().lower()
            index[alias.strip().lower()] = _ChainMatch(
                chain=canonical, family="evm", raw_hint=alias.strip().lower()
            )
        except Exception:
            continue
    # 3. Solana Aliases
    for alias, target in SOLANA_CHAIN_ALIASES.items():
        try:
            canonical = get_solana_chain(target).network
            index[alias.strip().lower()] = _ChainMatch(
                chain=canonical, family="solana", raw_hint=alias.strip().lower()
            )
        except Exception:
            continue
    # print(f"DEBUG: Index keys: {list(index.keys())}")
    return index


@lru_cache(maxsize=1)
def _chain_phrase_keys() -> tuple[str, ...]:
    return tuple(_chain_phrase_index().keys())


def _extract_spans(tokens: list[str], max_tokens: int = 4) -> list[str]:
    spans: list[str] = []
    seen: set[str] = set()
    upper = min(max_tokens, len(tokens))
    for size in range(upper, 0, -1):
        for start in range(0, len(tokens) - size + 1):
            span = " ".join(tokens[start : start + size]).strip()
            if not span or span in seen:
                continue
            seen.add(span)
            spans.append(span)
    return spans


def _get_token_tags(tokens: list[str]) -> list[set[str]]:
    return [_INTENT_REGISTRY.get(t, set()) for t in tokens]


@lru_cache(maxsize=1)
def _category_words(target_tag: str) -> tuple[str, ...]:
    return tuple(k for k, v in _INTENT_REGISTRY.items() if target_tag in v)


def _has_fuzzy_tag(
    tokens: list[str], token_tags: list[set[str]], target_tag: str, cutoff: float = 0.8
) -> bool:
    if any(target_tag in tags for tags in token_tags):
        return True
    vocab = _category_words(target_tag)
    for t in tokens:
        if len(t) >= 4 and get_close_matches(t, vocab, n=1, cutoff=cutoff):
            return True
    return False


def _is_balance_query(
    tokens: list[str], token_tags: list[set[str]], normalized: str
) -> bool:
    # Multi-step action requests (swap/bridge/transfer/etc.) should not be
    # hijacked into account/balance shortcuts.
    if any(TAG_ACTION in tags for tags in token_tags):
        return False
    if _has_fuzzy_tag(tokens, token_tags, TAG_BALANCE, cutoff=0.78):
        return True

    # If it's a known token and we have some request terms, it's likely a balance query
    # e.g. "my SOL", "Check ETH".
    # BUT: if it also contains address terms, it's likely a wallet address query.
    if any(TAG_TOKEN in tags for tags in token_tags):
        if any(TAG_REQUEST in tags for tags in token_tags):
            if not _has_fuzzy_tag(tokens, token_tags, TAG_ADDRESS, cutoff=0.74):
                return True

    haystack = f" {normalized} "
    if " how much " in haystack and " have " in haystack:
        return True
    if " what " in haystack and (" have " in haystack or " left " in haystack):
        return True
    return any(f" {p} " in haystack for p in _BALANCE_PHRASES)


def _is_wallet_address_query(
    tokens: list[str],
    token_tags: list[set[str]],
    normalized: str,
    chain_match: _ChainMatch | None,
) -> bool:
    if any(TAG_ACTION in tags for tags in token_tags):
        return False
    if _has_fuzzy_tag(tokens, token_tags, TAG_ADDRESS, cutoff=0.74):
        return True
    if any(TAG_WALLET in tags for tags in token_tags):
        if (
            chain_match
            or len(tokens) <= 3
            or any(TAG_REQUEST in tags for tags in token_tags)
        ):
            return True
    return normalized in {
        "wallet",
        "my wallet",
        "show wallet",
        "wallet info",
        "wallet details",
    }


def _resolve_chain_match(tokens: list[str], normalized: str) -> _ChainMatch | None:
    if not tokens:
        return None
    index = _chain_phrase_index()

    # 1. Exact multi-word spans (Fast path)
    for span in _extract_spans(tokens):
        if span in index:
            match = index[span]
            if match.raw_hint not in _COMMON_TOKENS or len(tokens) == 1:
                return match

    # 2. Special markers (All supported)
    if is_all_supported_chain_request(normalized) or "all chains" in normalized:
        return _ChainMatch(
            chain=ALL_SUPPORTED_CHAIN_KEY, family="evm", raw_hint="all_supported"
        )

    # 3. Canonical phrases
    direct = canonicalize_balance_chain(normalized)
    if direct:
        family: ChainFamily = "solana" if "solana" in direct else "evm"
        return _ChainMatch(chain=direct, family=family, raw_hint=normalized)

    # 4. Fuzzy matches for longer strings
    keys = _chain_phrase_keys()
    for span in _extract_spans(tokens):
        if len(span) >= 5:
            m = get_close_matches(span, keys, n=1, cutoff=0.8)
            if m:
                return index[m[0]]

    # 4. Fallback to common token disambiguation
    for span in _extract_spans(tokens):
        if span in index:
            return index[span]

    return None


def _resolve_token_match(tokens: list[str], token_tags: list[set[str]]) -> str | None:
    for i, tags in enumerate(token_tags):
        if TAG_TOKEN in tags:
            tok = tokens[i].upper()
            if tok != "TOKENS":
                return tok
    return None


def parse_account_query(text: str | None) -> AccountQuery | None:
    normalized = _normalize_text(text)
    if not normalized:
        return None
    tokens = _tokenize(normalized)
    token_tags = _get_token_tags(tokens)

    chain_match = _resolve_chain_match(tokens, normalized)
    token_match = _resolve_token_match(tokens, token_tags)

    if _is_balance_query(tokens, token_tags, normalized):
        return AccountQuery(
            kind="balance",
            chain=chain_match.chain if chain_match else None,
            chain_family=chain_match.family if chain_match else None,
            raw_chain_hint=chain_match.raw_hint if chain_match else None,
            token=token_match,
        )
    if _is_wallet_address_query(tokens, token_tags, normalized, chain_match):
        return AccountQuery(
            kind="wallet_address",
            chain=chain_match.chain if chain_match else None,
            chain_family=chain_match.family if chain_match else None,
            raw_chain_hint=chain_match.raw_hint if chain_match else None,
            token=token_match,
        )
    return None
