from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Sequence

from intent_hub.ontology.intent import Intent, IntentStatus
from intent_hub.utils.messages import format_with_recovery, has_template_marker
from intent_hub.utils.token_parse import split_amount_prefixed_symbol

TokenLookupFunc = Callable[[str, str], Awaitable[dict[str, Any]]]
AddressLookupFunc = Callable[[Mapping[str, Any], str], Awaitable[str | None]]


@dataclass(frozen=True)
class TokenResolution:
    symbol: str
    token_data: dict[str, Any]
    address: str
    inferred_amount: str | None = None


def require_complete_intent(intent: Intent) -> None:
    if intent.status != IntentStatus.COMPLETE:
        raise ValueError(
            format_with_recovery(
                "Intent status must be COMPLETE",
                "collect missing slots first, then retry intent resolution",
            )
        )


def require_amount(amount: object, *, action: str) -> object:
    if amount is None:
        raise ValueError(
            format_with_recovery(
                f"Amount is required for {action}",
                "provide a positive amount and retry",
            )
        )
    return amount


def unresolved_addresses_error(
    symbols: Sequence[str | None],
    *,
    chain_context: str,
) -> ValueError:
    cleaned_symbols = [str(symbol or "").strip().upper() for symbol in symbols]
    present_symbols = [symbol for symbol in cleaned_symbols if symbol]
    symbol_text = "/".join(present_symbols) if present_symbols else "UNKNOWN"
    chain_text = str(chain_context or "").strip().lower() or "unknown"
    # Keep this prefix stable for graph resolver feedback extraction.
    return ValueError(f"Could not resolve addresses for {symbol_text} on {chain_text}")


def symbol_from_slot(slot: object) -> str | None:
    if isinstance(slot, dict):
        symbol = slot.get("symbol")
    else:
        symbol = slot
    text = str(symbol or "").strip()
    return text or None


def is_dynamic_marker(value: object) -> bool:
    return has_template_marker(value)


async def resolve_token_on_chain(
    raw_symbol: str,
    chain_name: str,
    *,
    get_token_data_fn: TokenLookupFunc,
    get_address_for_chain_fn: AddressLookupFunc,
    allow_amount_prefixed: bool = True,
) -> TokenResolution | None:
    symbol = str(raw_symbol or "").strip().upper()
    chain = str(chain_name or "").strip().lower()
    if not symbol or not chain:
        return None

    candidates: list[tuple[str, str | None]] = [(symbol, None)]
    if allow_amount_prefixed:
        split = split_amount_prefixed_symbol(symbol)
        if split:
            split_amount, split_symbol = split
            if split_symbol != symbol:
                candidates.append((split_symbol, split_amount))

    for candidate_symbol, inferred_amount in candidates:
        token_data = await get_token_data_fn(candidate_symbol, chain)
        if not isinstance(token_data, dict):
            continue
        address = await get_address_for_chain_fn(token_data, chain)
        if address:
            return TokenResolution(
                symbol=candidate_symbol,
                token_data=token_data,
                address=address,
                inferred_amount=inferred_amount,
            )
    return None
