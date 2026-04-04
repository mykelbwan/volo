from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.transfers.chains import TransferChainSpec
from core.transfers.models import (
    resolve_transfer_asset_ref_input,
    resolve_transfer_asset_symbol_input,
    resolve_transfer_chain_spec_input,
)


@dataclass(frozen=True)
class TransferPlanningMetadata:
    chain_spec: TransferChainSpec
    asset_kind: str
    asset_ref: str

    @property
    def family(self) -> str:
        return str(self.chain_spec.family).strip().lower()

    @property
    def network(self) -> str:
        return str(self.chain_spec.network).strip().lower()

    @property
    def native_asset_ref(self) -> str:
        return str(self.chain_spec.native_asset_ref).strip()


def _normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_asset_ref(
    raw_value: Any,
    *,
    chain_spec: TransferChainSpec,
) -> str | None:
    value = _normalize_optional_text(raw_value)
    if value is None:
        return None

    native_asset_ref = str(chain_spec.native_asset_ref).strip()
    if value.lower() == "native":
        return native_asset_ref
    if value.lower() == native_asset_ref.lower():
        return native_asset_ref
    return value


def _native_symbol_matches(
    asset_symbol: Any,
    *,
    chain_spec: TransferChainSpec,
) -> bool:
    symbol = _normalize_optional_text(asset_symbol)
    if symbol is None:
        return False
    return symbol.upper() == str(chain_spec.native_symbol).strip().upper()


def classify_transfer_asset(
    *,
    chain_spec: TransferChainSpec,
    asset_symbol: Any,
    asset_ref: Any,
) -> tuple[str, str]:
    normalized_ref = _normalize_asset_ref(asset_ref, chain_spec=chain_spec)
    native_asset_ref = str(chain_spec.native_asset_ref).strip()

    if chain_spec.family == "evm":
        if normalized_ref is None:
            if not _native_symbol_matches(asset_symbol, chain_spec=chain_spec):
                raise ValueError(
                    "evm token transfer planning requires an explicit asset reference"
                )
            return "native", native_asset_ref
        if normalized_ref.lower() == native_asset_ref.lower():
            if _normalize_optional_text(
                asset_symbol
            ) is not None and not _native_symbol_matches(
                asset_symbol, chain_spec=chain_spec
            ):
                raise ValueError(
                    "evm native transfer planning requires the native asset symbol"
                )
            return "native", native_asset_ref
        return "token", normalized_ref

    if chain_spec.family == "solana":
        if normalized_ref is None:
            if _native_symbol_matches(asset_symbol, chain_spec=chain_spec):
                return "native", native_asset_ref
            raise ValueError(
                "solana token transfer planning requires an explicit asset reference"
            )
        if normalized_ref.lower() == native_asset_ref.lower():
            if not _native_symbol_matches(asset_symbol, chain_spec=chain_spec):
                raise ValueError(
                    "solana native transfer planning requires the native asset symbol"
                )
            return "native", native_asset_ref
        return "token", normalized_ref

    raise ValueError(
        f"transfer planning does not support chain family {chain_spec.family!r}"
    )


def resolve_transfer_planning_metadata(
    node_args: dict[str, Any],
) -> TransferPlanningMetadata:
    if (
        _normalize_optional_text(node_args.get("network") or node_args.get("chain"))
        is None
    ):
        raise ValueError("transfer step is missing network")

    try:
        chain_spec, _ = resolve_transfer_chain_spec_input(node_args)
        asset_symbol = resolve_transfer_asset_symbol_input(node_args)
        asset_ref_input = resolve_transfer_asset_ref_input(
            node_args,
            chain_spec=chain_spec,
        )
    except ValueError as exc:
        if str(exc).startswith("Unsupported transfer network:"):
            raise ValueError("transfer step uses an unsupported network") from exc
        raise

    asset_kind, asset_ref = classify_transfer_asset(
        chain_spec=chain_spec,
        asset_symbol=asset_symbol,
        asset_ref=asset_ref_input,
    )
    return TransferPlanningMetadata(
        chain_spec=chain_spec,
        asset_kind=asset_kind,
        asset_ref=asset_ref,
    )
