from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

from config.chains import CHAINS
from config.solana_chains import SOLANA_CHAINS
from core.chains.chain_canonicalization_parity import (
    SCOPE_ACTION,
    compare_chain_canonicalization,
)


@dataclass(frozen=True)
class ChainInferenceContext:
    intent_type: str
    slot_name: str
    slots: Mapping[str, Any]
    token_registry: Mapping[str, Any]
    alias_lookup: Mapping[str, str]
    raw_input: str | None = None


@dataclass(frozen=True)
class ChainInferenceDecision:
    chain: str | None
    signal: str


SignalResolver = Callable[[ChainInferenceContext], str | None]


def _chain_text_variants(value: str | None) -> set[str]:
    text = str(value or "").strip().lower()
    if not text:
        return set()

    collapsed = " ".join(text.replace("-", " ").replace("_", " ").split())
    variants = {text, collapsed}
    if collapsed:
        variants.add(collapsed.replace(" ", "-"))
        variants.add(collapsed.replace(" ", "_"))
    return {variant for variant in variants if variant}


def _chain_text_tokens(value: str | None) -> set[str]:
    text = str(value or "").strip().lower()
    if not text:
        return set()
    return set(re.findall(r"[a-z0-9]+", text))


def _build_testnet_hints() -> tuple[str, ...]:
    testnet_phrases: set[str] = set()
    testnet_tokens: set[str] = set()
    mainnet_tokens: set[str] = set()

    for evm_chain in CHAINS.values():
        variants = _chain_text_variants(evm_chain.name)
        if bool(evm_chain.is_testnet):
            testnet_phrases.update(variants)
            for variant in variants:
                testnet_tokens.update(_chain_text_tokens(variant))
        else:
            for variant in variants:
                mainnet_tokens.update(_chain_text_tokens(variant))

    for solana_chain in SOLANA_CHAINS.values():
        variants = _chain_text_variants(solana_chain.network) | _chain_text_variants(
            solana_chain.name
        )
        if bool(solana_chain.is_testnet):
            testnet_phrases.update(variants)
            for variant in variants:
                testnet_tokens.update(_chain_text_tokens(variant))
        else:
            for variant in variants:
                mainnet_tokens.update(_chain_text_tokens(variant))

    # Keep single-word hints only when they are unique to testnet chain metadata.
    unique_testnet_tokens = {
        token
        for token in (testnet_tokens - mainnet_tokens)
        if len(token) >= 4
    }
    hints = testnet_phrases | unique_testnet_tokens
    return tuple(sorted(hints, key=len, reverse=True))


_TESTNET_HINTS = _build_testnet_hints()


def _slot_symbol(slot: object) -> str | None:
    if isinstance(slot, dict):
        symbol = slot.get("symbol")
    else:
        symbol = slot
    text = str(symbol or "").strip().upper()
    return text or None


def _canonical_action_chain(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    comparison = compare_chain_canonicalization(text, scope=SCOPE_ACTION)
    normalized = str(comparison.catalog_value or text).strip().lower()
    return normalized or None


def is_testnet_chain(chain_value: str | None) -> bool:
    canonical = _canonical_action_chain(chain_value)
    if not canonical:
        return False

    for evm_chain in CHAINS.values():
        if _canonical_action_chain(evm_chain.name) == canonical:
            return bool(evm_chain.is_testnet)
    for solana_chain in SOLANA_CHAINS.values():
        if _canonical_action_chain(solana_chain.network) == canonical:
            return bool(solana_chain.is_testnet)
    return False


def should_include_testnet_candidates(
    *,
    raw_input: str | None,
    slots: Mapping[str, Any],
) -> bool:
    text = str(raw_input or "").strip().lower()
    if any(hint in text for hint in _TESTNET_HINTS):
        return True

    for slot_name in ("chain", "target_chain"):
        slot_text = str(slots.get(slot_name) or "").strip()
        if not slot_text:
            continue
        lower_slot = slot_text.lower()
        if any(hint in lower_slot for hint in _TESTNET_HINTS):
            return True
        if is_testnet_chain(slot_text):
            return True
    return False


def _resolve_symbol(
    symbol: str | None,
    token_registry: Mapping[str, Any],
    *,
    alias_lookup: Mapping[str, str],
) -> str | None:
    text = str(symbol or "").strip()
    if not text:
        return None
    upper = text.upper()
    if upper in token_registry:
        return upper
    alias_match = alias_lookup.get(text.lower())
    if alias_match:
        return alias_match
    return upper


def _token_chains(
    symbol: str | None,
    token_registry: Mapping[str, Any],
    *,
    alias_lookup: Mapping[str, str],
) -> list[str]:
    canonical_symbol = _resolve_symbol(
        symbol, token_registry, alias_lookup=alias_lookup
    )
    if not canonical_symbol:
        return []

    token_data = token_registry.get(canonical_symbol)
    if not isinstance(token_data, dict):
        return []

    raw_chains = token_data.get("chains")
    if isinstance(raw_chains, dict):
        values = list(raw_chains.keys())
    elif isinstance(raw_chains, (list, tuple, set)):
        values = list(raw_chains)
    else:
        return []

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        canonical = _canonical_action_chain(str(value or ""))
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        deduped.append(canonical)
    return deduped


def _tokens_for_slot(context: ChainInferenceContext) -> list[str]:
    intent_type = str(context.intent_type or "").strip().lower()
    slot_name = str(context.slot_name or "").strip().lower()
    slots = context.slots

    if slot_name != "chain":
        return []

    token_slots: Sequence[str]
    if intent_type == "swap":
        token_slots = ("token_in", "token_out")
    elif intent_type == "bridge":
        token_slots = ("token_in",)
    elif intent_type in {"transfer", "unwrap"}:
        token_slots = ("token",)
    else:
        return []

    symbols: list[str] = []
    for token_slot in token_slots:
        symbol = _slot_symbol(slots.get(token_slot))
        if symbol:
            symbols.append(symbol)
    return symbols


def _native_symbol_chains(symbol: str | None, *, include_testnets: bool) -> list[str]:
    target = str(symbol or "").strip().upper()
    if not target:
        return []

    chains: list[str] = []
    seen: set[str] = set()

    for evm_chain in CHAINS.values():
        if not include_testnets and bool(evm_chain.is_testnet):
            continue
        if str(evm_chain.native_symbol or "").strip().upper() != target:
            continue
        canonical = _canonical_action_chain(evm_chain.name)
        if canonical and canonical not in seen:
            seen.add(canonical)
            chains.append(canonical)

    for solana_chain in SOLANA_CHAINS.values():
        if not include_testnets and bool(solana_chain.is_testnet):
            continue
        if str(solana_chain.native_symbol or "").strip().upper() != target:
            continue
        canonical = _canonical_action_chain(solana_chain.network)
        if canonical and canonical not in seen:
            seen.add(canonical)
            chains.append(canonical)

    return chains


def _infer_from_unique_native_symbol(context: ChainInferenceContext) -> str | None:
    symbols = _tokens_for_slot(context)
    if not symbols:
        return None
    include_testnets = should_include_testnet_candidates(
        raw_input=context.raw_input,
        slots=context.slots,
    )
    native_chains = _native_symbol_chains(
        symbols[0],
        include_testnets=include_testnets,
    )
    if len(native_chains) == 1:
        return native_chains[0]
    return None


def _infer_from_registry_intersection(context: ChainInferenceContext) -> str | None:
    symbols = _tokens_for_slot(context)
    if not symbols:
        return None
    include_testnets = should_include_testnet_candidates(
        raw_input=context.raw_input,
        slots=context.slots,
    )

    chain_sets: list[set[str]] = []
    for symbol in symbols:
        options = _token_chains(
            symbol,
            context.token_registry,
            alias_lookup=context.alias_lookup,
        )
        if not include_testnets:
            options = [chain for chain in options if not is_testnet_chain(chain)]
        if options:
            chain_sets.append(set(options))

    if not chain_sets:
        return None

    intersection = set.intersection(*chain_sets)
    if len(intersection) == 1:
        return next(iter(intersection))
    return None


class ChainInferencePolicyEngine:
    def __init__(
        self,
        *,
        deterministic_signals: Iterable[tuple[str, SignalResolver]] | None = None,
    ) -> None:
        signals = deterministic_signals or (
            ("unique_native_symbol", _infer_from_unique_native_symbol),
            ("token_registry_intersection", _infer_from_registry_intersection),
        )
        self._signals = tuple(signals)

    def infer(self, context: ChainInferenceContext) -> ChainInferenceDecision:
        explicit = _canonical_action_chain(str(context.slots.get(context.slot_name) or ""))
        if explicit:
            return ChainInferenceDecision(chain=explicit, signal="explicit_chain")

        for signal_name, resolver in self._signals:
            inferred = _canonical_action_chain(resolver(context))
            if inferred:
                return ChainInferenceDecision(chain=inferred, signal=signal_name)

        return ChainInferenceDecision(chain=None, signal="no_inference")


DEFAULT_CHAIN_INFERENCE_ENGINE = ChainInferencePolicyEngine()
