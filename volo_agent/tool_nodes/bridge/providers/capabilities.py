from __future__ import annotations

from dataclasses import dataclass

from .base import BridgeRequest


@dataclass(frozen=True, slots=True)
class BridgeCapability:
    evm_to_evm: bool = False
    evm_to_solana: bool = False
    solana_to_evm: bool = False
    solana_to_solana: bool = False


# Declarative capability matrix. This is intentionally conservative and only
# captures high-level path support, not per-token liquidity or quote-time
# availability.
_CAPABILITY_MATRIX: dict[str, BridgeCapability] = {
    "across": BridgeCapability(evm_to_evm=True),
    "relay": BridgeCapability(evm_to_evm=True),
    "mayan": BridgeCapability(
        evm_to_evm=True,
        evm_to_solana=True,
        solana_to_evm=True,
    ),
    "lifi": BridgeCapability(
        evm_to_evm=True,
        evm_to_solana=True,
        solana_to_evm=True,
    ),
}


def get_provider_capability(provider_name: str) -> BridgeCapability | None:
    return _CAPABILITY_MATRIX.get(str(provider_name or "").strip().lower())


def provider_supports_request(provider_name: str, request: BridgeRequest) -> bool:
    capability = get_provider_capability(provider_name)
    if capability is None:
        return False

    source_is_solana = bool(request.source_is_solana)
    dest_is_solana = bool(request.dest_is_solana)

    if source_is_solana and dest_is_solana:
        return capability.solana_to_solana
    if source_is_solana and not dest_is_solana:
        return capability.solana_to_evm
    if not source_is_solana and dest_is_solana:
        return capability.evm_to_solana
    return capability.evm_to_evm
