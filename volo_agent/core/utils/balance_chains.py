from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from config.chains import CHAINS, get_chain_by_name
from config.solana_chains import SOLANA_CHAINS, get_solana_chain

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

_EVM_NAME_INDEX: Dict[str, int] = {
    chain.name.strip().lower(): chain_id for chain_id, chain in CHAINS.items()
}


@dataclass(frozen=True)
class BalanceChainSpec:
    key: str
    display_name: str
    family: str  # "evm" | "solana"
    is_testnet: bool
    rpc_configured: bool = True


def _normalize_text(value: str | None) -> str:
    return str(value or "").strip().lower()


def is_all_supported_chain_request(value: str | None) -> bool:
    normalized = _normalize_text(value)
    return normalized in _ALL_SUPPORTED_MARKERS


def _canonicalize_evm_chain(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    try:
        chain = get_chain_by_name(normalized)
        return chain.name.strip().lower()
    except KeyError:
        return None


def _canonicalize_solana_chain(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    try:
        return get_solana_chain(normalized).network
    except Exception:
        return None


def canonicalize_balance_chain(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    if is_all_supported_chain_request(normalized):
        return ALL_SUPPORTED_CHAIN_KEY

    evm = _canonicalize_evm_chain(normalized)
    if evm:
        return evm

    solana = _canonicalize_solana_chain(normalized)
    if solana:
        return solana
    return None


def resolve_balance_chain_spec(
    value: str | None,
) -> BalanceChainSpec | None:
    canonical = canonicalize_balance_chain(value)
    if not canonical or canonical == ALL_SUPPORTED_CHAIN_KEY:
        return None

    evm_chain = _canonicalize_evm_chain(canonical)
    if evm_chain:
        chain_id = _EVM_NAME_INDEX[evm_chain]
        chain = CHAINS[chain_id]
        return BalanceChainSpec(
            key=evm_chain,
            display_name=chain.name,
            family="evm",
            is_testnet=bool(chain.is_testnet),
            rpc_configured=bool(str(chain.rpc_url or "").strip()),
        )

    solana_chain = _canonicalize_solana_chain(canonical)
    if solana_chain and solana_chain in SOLANA_CHAINS:
        cfg = SOLANA_CHAINS[solana_chain]
        return BalanceChainSpec(
            key=cfg.network,
            display_name=cfg.name,
            family="solana",
            is_testnet=bool(cfg.is_testnet),
            rpc_configured=True,
        )
    return None


def list_supported_balance_chain_specs(
    *,
    include_testnets: bool = True,
    include_solana_devnet: bool = True,
) -> List[BalanceChainSpec]:
    specs: List[BalanceChainSpec] = []

    for chain_id in sorted(CHAINS.keys()):
        chain = CHAINS[chain_id]
        key = chain.name.strip().lower()
        spec = BalanceChainSpec(
            key=key,
            display_name=chain.name,
            family="evm",
            is_testnet=bool(chain.is_testnet),
            rpc_configured=bool(str(chain.rpc_url or "").strip()),
        )
        if not spec.rpc_configured:
            continue
        if spec.is_testnet and not include_testnets:
            continue
        specs.append(spec)

    for cfg in sorted(SOLANA_CHAINS.values(), key=lambda c: c.name.lower()):
        if cfg.network == "solana-devnet" and not include_solana_devnet:
            continue
        spec = BalanceChainSpec(
            key=cfg.network,
            display_name=cfg.name,
            family="solana",
            is_testnet=bool(cfg.is_testnet),
            rpc_configured=True,
        )
        if spec.is_testnet and not include_testnets:
            continue
        specs.append(spec)

    # Mainnets first for quicker high-signal output.
    specs.sort(key=lambda s: (s.is_testnet, s.family, s.display_name.lower()))
    return specs
