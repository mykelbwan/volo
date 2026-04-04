from __future__ import annotations

from dataclasses import dataclass

from config.chains import find_chain_by_name
from config.solana_chains import SOLANA_CHAINS, get_solana_chain

_EVM_NATIVE_ASSET_REF = "0x0000000000000000000000000000000000000000"


@dataclass(frozen=True)
class TransferChainSpec:
    family: str
    network: str
    display_name: str
    native_symbol: str
    explorer_url: str | None
    is_testnet: bool
    native_asset_ref: str


def _normalize_text(value: str | None) -> str:
    return str(value or "").strip().lower()


def _canonicalize_evm_network(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    try:
        chain = find_chain_by_name(normalized)
        return chain.name.strip().lower()
    except KeyError:
        return None


def _canonicalize_solana_network(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    try:
        return get_solana_chain(normalized).network
    except KeyError:
        return None


def canonicalize_transfer_network(value: str | None) -> str | None:
    evm_network = _canonicalize_evm_network(value)
    solana_network = _canonicalize_solana_network(value)

    if evm_network and solana_network and evm_network != solana_network:
        raise KeyError(f"Transfer network {value!r} is ambiguous across chain families.")

    if evm_network:
        return evm_network

    if solana_network:
        return solana_network

    return None


def resolve_transfer_chain_spec(value: str | None) -> TransferChainSpec | None:
    canonical = canonicalize_transfer_network(value)
    if not canonical:
        return None

    evm_network = _canonicalize_evm_network(canonical)
    if evm_network:
        chain = find_chain_by_name(evm_network)
        return TransferChainSpec(
            family="evm",
            network=evm_network,
            display_name=chain.name,
            native_symbol=chain.native_symbol,
            explorer_url=chain.explorer_url,
            is_testnet=bool(chain.is_testnet),
            native_asset_ref=_EVM_NATIVE_ASSET_REF,
        )

    solana_network = _canonicalize_solana_network(canonical)
    if solana_network and solana_network in SOLANA_CHAINS:
        chain = SOLANA_CHAINS[solana_network]
        return TransferChainSpec(
            family="solana",
            network=chain.network,
            display_name=chain.name,
            native_symbol=chain.native_symbol,
            explorer_url=chain.explorer_url,
            is_testnet=bool(chain.is_testnet),
            native_asset_ref=chain.native_mint,
        )

    return None


def get_transfer_chain_spec(value: str | None) -> TransferChainSpec:
    spec = resolve_transfer_chain_spec(value)
    if spec is not None:
        return spec
    raise KeyError(f"Transfer network {value!r} is not registered.")
