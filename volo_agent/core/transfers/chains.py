from __future__ import annotations

from dataclasses import dataclass

from core.chains.catalog import canonicalize_chain_key, resolve_chain

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


def canonicalize_transfer_network(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    return canonicalize_chain_key(normalized)


def resolve_transfer_chain_spec(value: str | None) -> TransferChainSpec | None:
    canonical = canonicalize_transfer_network(value)
    if not canonical:
        return None

    entry = resolve_chain(canonical)
    if entry is None:
        return None
    return TransferChainSpec(
        family=entry.family,
        network=entry.key,
        display_name=entry.display_name,
        native_symbol=entry.native_symbol,
        explorer_url=entry.explorer_url,
        is_testnet=bool(entry.is_testnet),
        native_asset_ref=(
            _EVM_NATIVE_ASSET_REF if entry.family == "evm" else entry.native_asset_ref
        ),
    )


def get_transfer_chain_spec(value: str | None) -> TransferChainSpec:
    spec = resolve_transfer_chain_spec(value)
    if spec is not None:
        return spec
    raise KeyError(f"Transfer network {value!r} is not registered.")
