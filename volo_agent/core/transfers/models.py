from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from core.transfers.chains import TransferChainSpec, get_transfer_chain_spec
from tool_nodes.common.input_utils import (
    format_with_recovery,
    parse_decimal_field,
    require_fields,
)


@dataclass(frozen=True)
class NormalizedTransferRequest:
    asset_ref: str | None
    asset_symbol: str
    amount: Decimal
    recipient: str
    network: str
    requested_network: str | None
    sender: str
    sub_org_id: str
    decimals: Any | None = None
    idempotency_key: str | None = None


@dataclass(frozen=True)
class TransferExecutionResult:
    status: str
    tx_hash: str
    asset_symbol: str
    amount: Decimal
    recipient: str
    network: str
    message: str


def _normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _raise_conflict(message: str, recovery: str) -> None:
    raise ValueError(format_with_recovery(message, recovery))


def _canonical_network_or_none(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        return get_transfer_chain_spec(value).network
    except KeyError:
        return None


def resolve_transfer_chain_spec_input(
    parameters: dict[str, Any],
) -> tuple[TransferChainSpec, str | None]:
    raw_chain = _normalize_optional_text(parameters.get("chain"))
    raw_network = _normalize_optional_text(parameters.get("network"))

    if raw_chain and raw_network:
        canonical_chain = _canonical_network_or_none(raw_chain)
        canonical_network = _canonical_network_or_none(raw_network)
        if canonical_chain != canonical_network:
            if canonical_chain is None or canonical_network is None:
                if raw_chain.casefold() != raw_network.casefold():
                    _raise_conflict(
                        "Conflicting transfer network inputs: 'chain' and 'network' do not match",
                        "provide one supported network or make both values equivalent",
                    )
            else:
                _raise_conflict(
                    "Conflicting transfer network inputs: 'chain' and 'network' do not match",
                    "provide one supported network or make both values equivalent",
                )

    raw_network_value = raw_network or raw_chain
    if raw_network_value is None:
        # require_fields() raises the public-facing missing-fields error earlier.
        raw_network_value = ""

    try:
        return get_transfer_chain_spec(raw_network_value), raw_network_value
    except KeyError as exc:
        raise ValueError(
            format_with_recovery(
                f"Unsupported transfer network: {raw_network_value!r}",
                "use a supported network and retry",
            )
        ) from exc


def resolve_transfer_asset_symbol_input(parameters: dict[str, Any]) -> str:
    legacy_symbol = _normalize_optional_text(parameters.get("token_symbol"))
    normalized_symbol = _normalize_optional_text(parameters.get("asset_symbol"))

    if legacy_symbol and normalized_symbol:
        if legacy_symbol.upper() != normalized_symbol.upper():
            _raise_conflict(
                "Conflicting transfer asset symbol inputs: 'token_symbol' and 'asset_symbol' do not match",
                "provide one asset symbol or make both values equivalent",
            )

    value = normalized_symbol or legacy_symbol
    return str(value or "").strip().upper()


def _normalize_asset_ref(
    raw_value: str | None,
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


def resolve_transfer_asset_ref_input(
    parameters: dict[str, Any],
    *,
    chain_spec: TransferChainSpec,
) -> str | None:
    legacy_asset_ref = _normalize_asset_ref(
        _normalize_optional_text(parameters.get("token_address")),
        chain_spec=chain_spec,
    )
    normalized_asset_ref = _normalize_asset_ref(
        _normalize_optional_text(parameters.get("asset_ref")),
        chain_spec=chain_spec,
    )

    if legacy_asset_ref and normalized_asset_ref:
        if legacy_asset_ref.lower() != normalized_asset_ref.lower():
            _raise_conflict(
                "Conflicting transfer asset inputs: 'token_address' and 'asset_ref' do not match",
                "provide one asset reference or make both values equivalent",
            )

    return normalized_asset_ref or legacy_asset_ref


def normalize_transfer_request(
    parameters: dict[str, Any],
) -> NormalizedTransferRequest:
    compatibility = dict(parameters)
    raw_asset_symbol = _normalize_optional_text(parameters.get("asset_symbol"))
    raw_token_symbol = _normalize_optional_text(parameters.get("token_symbol"))
    raw_asset_ref = _normalize_optional_text(parameters.get("asset_ref"))
    raw_token_address = _normalize_optional_text(parameters.get("token_address"))
    raw_network = _normalize_optional_text(parameters.get("network"))
    raw_chain = _normalize_optional_text(parameters.get("chain"))

    compatibility["asset_symbol"] = raw_asset_symbol or raw_token_symbol
    compatibility["asset_ref"] = raw_asset_ref or raw_token_address
    compatibility["network"] = raw_network or raw_chain

    require_fields(
        compatibility,
        ["asset_symbol", "amount", "recipient", "network", "sub_org_id", "sender"],
        context="transfer",
    )

    chain_spec, requested_network = resolve_transfer_chain_spec_input(parameters)
    asset_symbol = resolve_transfer_asset_symbol_input(parameters)
    asset_ref = resolve_transfer_asset_ref_input(parameters, chain_spec=chain_spec)

    return NormalizedTransferRequest(
        asset_ref=asset_ref,
        asset_symbol=asset_symbol,
        amount=parse_decimal_field(
            compatibility.get("amount"),
            field="amount",
            positive=True,
            invalid_recovery="use a positive transfer amount (for example, 1.2)",
        ),
        recipient=str(compatibility["recipient"]).strip(),
        network=chain_spec.network,
        requested_network=requested_network,
        sender=str(compatibility["sender"]).strip(),
        sub_org_id=str(compatibility["sub_org_id"]).strip(),
        decimals=compatibility.get("decimals"),
        idempotency_key=(
            str(compatibility["idempotency_key"]).strip()
            if compatibility.get("idempotency_key") is not None
            else None
        ),
    )
