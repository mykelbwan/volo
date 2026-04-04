from __future__ import annotations

from dataclasses import dataclass


class UnsupportedWalletFamilyError(ValueError):
    """Raised when a chain family has no registered wallet binding markers."""


@dataclass(frozen=True)
class WalletBindingMarkers:
    sender_marker: str
    sub_org_marker: str


_WALLET_MARKERS_BY_FAMILY: dict[str, WalletBindingMarkers] = {
    "evm": WalletBindingMarkers(
        sender_marker="{{EVM_ADDRESS}}",
        sub_org_marker="{{EVM_SUB_ORG_ID}}",
    ),
    "solana": WalletBindingMarkers(
        sender_marker="{{SOLANA_ADDRESS}}",
        sub_org_marker="{{SOLANA_SUB_ORG_ID}}",
    ),
}


def wallet_markers_for_family(family: str) -> WalletBindingMarkers:
    normalized_family = str(family or "").strip().lower()
    markers = _WALLET_MARKERS_BY_FAMILY.get(normalized_family)
    if markers is not None:
        return markers
    raise UnsupportedWalletFamilyError(
        f"Unsupported wallet binding family: {family!r}"
    )
