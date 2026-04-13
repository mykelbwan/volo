from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from config.chains import find_chain_by_name, get_chain_by_name
from config.solana_chains import get_solana_chain
from core.chains.catalog import canonicalize_chain_key

ParityReporter = Callable[["ChainParityComparison"], None]

SCOPE_ACTION = "action"
SCOPE_TRANSFER = "transfer"
SCOPE_BALANCE = "balance"
ALL_SUPPORTED_CHAIN_KEY = "all_supported"
_ALL_SUPPORTED_MARKERS = {
    ALL_SUPPORTED_CHAIN_KEY,
    "all",
    "all chains",
    "all supported",
    "all supported chains",
    "every chain",
    "every supported chain",
    "all networks",
    "all supported networks",
    "across all chains",
}


@dataclass(frozen=True)
class ChainParityComparison:
    scope: str
    raw_value: str | None
    legacy_value: str | None
    catalog_value: str | None
    legacy_error: str | None = None
    catalog_error: str | None = None

    @property
    def matched(self) -> bool:
        return (
            self.legacy_value == self.catalog_value
            and self.legacy_error == self.catalog_error
        )


def _legacy_action_chain(value: str | None) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    try:
        return get_chain_by_name(text).name.strip().lower()
    except Exception:
        pass
    try:
        return get_solana_chain(text).network.strip().lower()
    except Exception:
        return None


def _legacy_transfer_chain(value: str | None) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None

    evm_value = None
    solana_value = None
    try:
        evm_value = find_chain_by_name(text).name.strip().lower()
    except Exception:
        evm_value = None
    try:
        solana_value = get_solana_chain(text).network.strip().lower()
    except Exception:
        solana_value = None

    if evm_value and solana_value and evm_value != solana_value:
        raise KeyError(f"Transfer network {value!r} is ambiguous across chain families.")

    if evm_value:
        return evm_value
    if solana_value:
        return solana_value
    return None


def _legacy_balance_chain(value: str | None) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in _ALL_SUPPORTED_MARKERS:
        return ALL_SUPPORTED_CHAIN_KEY
    try:
        return get_chain_by_name(text).name.strip().lower()
    except Exception:
        pass
    try:
        return get_solana_chain(text).network.strip().lower()
    except Exception:
        return None


def _catalog_chain(value: str | None) -> str | None:
    return canonicalize_chain_key(value)


def _catalog_balance_chain(value: str | None) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in _ALL_SUPPORTED_MARKERS:
        return ALL_SUPPORTED_CHAIN_KEY
    return canonicalize_chain_key(text)


def _safe_call(fn: Callable[[str | None], str | None], value: str | None) -> tuple[str | None, str | None]:
    try:
        return fn(value), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def compare_chain_canonicalization(
    raw_value: str | None,
    *,
    scope: str,
) -> ChainParityComparison:
    scope_normalized = str(scope or "").strip().lower()
    if scope_normalized == SCOPE_TRANSFER:
        legacy_fn = _legacy_transfer_chain
        catalog_fn = _catalog_chain
    elif scope_normalized == SCOPE_BALANCE:
        legacy_fn = _legacy_balance_chain
        catalog_fn = _catalog_balance_chain
    elif scope_normalized == SCOPE_ACTION:
        legacy_fn = _legacy_action_chain
        catalog_fn = _catalog_chain
    else:
        raise ValueError(
            f"Unknown parity comparison scope '{scope}'. "
            f"Use one of: {SCOPE_ACTION}, {SCOPE_TRANSFER}, {SCOPE_BALANCE}."
        )

    legacy_value, legacy_error = _safe_call(legacy_fn, raw_value)
    catalog_value, catalog_error = _safe_call(catalog_fn, raw_value)

    return ChainParityComparison(
        scope=scope_normalized,
        raw_value=raw_value,
        legacy_value=legacy_value,
        catalog_value=catalog_value,
        legacy_error=legacy_error,
        catalog_error=catalog_error,
    )


def canonicalize_chain_with_parity(
    raw_value: str | None,
    *,
    scope: str,
    report: Optional[ParityReporter] = None,
) -> str | None:
    comparison = compare_chain_canonicalization(raw_value, scope=scope)
    if report is not None and not comparison.matched:
        report(comparison)
    return comparison.legacy_value
