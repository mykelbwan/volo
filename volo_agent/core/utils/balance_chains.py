from __future__ import annotations

from dataclasses import dataclass
from typing import List

from core.chains.catalog import canonicalize_chain_key, list_chain_catalog, resolve_chain

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
class BalanceChainSpec:
    key: str
    display_name: str
    family: str  # evm | solana
    is_testnet: bool
    rpc_configured: bool = True


def _normalize_text(value: str | None) -> str:
    return str(value or "").strip().lower()


def is_all_supported_chain_request(value: str | None) -> bool:
    normalized = _normalize_text(value)
    return normalized in _ALL_SUPPORTED_MARKERS


def canonicalize_balance_chain(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    if is_all_supported_chain_request(normalized):
        return ALL_SUPPORTED_CHAIN_KEY
    return canonicalize_chain_key(normalized)


def resolve_balance_chain_spec(
    value: str | None,
) -> BalanceChainSpec | None:
    canonical = canonicalize_balance_chain(value)
    if not canonical or canonical == ALL_SUPPORTED_CHAIN_KEY:
        return None

    entry = resolve_chain(canonical)
    if entry is None:
        return None
    return BalanceChainSpec(
        key=entry.key,
        display_name=entry.display_name,
        family=entry.family,
        is_testnet=bool(entry.is_testnet),
        rpc_configured=(
            bool(entry.rpc_configured) if entry.family == "evm" else True
        ),
    )


def list_supported_balance_chain_specs(
    *,
    include_testnets: bool = True,
    include_solana_devnet: bool = True,
) -> List[BalanceChainSpec]:
    specs: List[BalanceChainSpec] = []

    for entry in list_chain_catalog(include_testnets=include_testnets, require_rpc=False):
        if entry.family == "evm" and not entry.rpc_configured:
            continue
        spec = BalanceChainSpec(
            key=entry.key,
            display_name=entry.display_name,
            family=entry.family,
            is_testnet=bool(entry.is_testnet),
            rpc_configured=(
                bool(entry.rpc_configured) if entry.family == "evm" else True
            ),
        )
        if not include_solana_devnet and spec.key == "solana-devnet":
            continue
        specs.append(spec)

    # Mainnets first for quicker high-signal output.
    specs.sort(key=lambda s: (s.is_testnet, s.family, s.display_name.lower()))
    return specs
