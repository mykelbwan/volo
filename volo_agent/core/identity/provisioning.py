from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Protocol, TypeAlias, runtime_checkable

ProvisioningPayload: TypeAlias = Dict[str, Any]
ProvisioningFn: TypeAlias = Callable[[str], ProvisioningPayload]


@runtime_checkable
class WalletProvisioner(Protocol):
    @property
    def chain_label(self) -> str: ...

    def provision(self, volo_user_id: str) -> ProvisioningPayload: ...


@dataclass(frozen=True)
class FunctionWalletProvisioner:
    chain_label: str
    provision_fn: ProvisioningFn

    def provision(self, volo_user_id: str) -> ProvisioningPayload:
        payload = self.provision_fn(volo_user_id)
        if not isinstance(payload, dict):
            payload_type = type(payload).__name__
            raise TypeError(
                f"{self.chain_label} provisioner returned {payload_type}; "
                "expected a dict containing 'sub_org_id' and 'address'. "
                "Update your custom provisioner implementation and retry."
            )
        return payload


def _default_evm_provision(volo_user_id: str) -> ProvisioningPayload:
    # Lazy import: avoids loading CDP/OpenAPI dependencies during module import.
    from wallet_service.evm.create_sub_org import create_sub_org as create_evm_sub_org

    return create_evm_sub_org(volo_user_id)


def _default_solana_provision(volo_user_id: str) -> ProvisioningPayload:
    # Lazy import: avoids loading CDP/OpenAPI dependencies during module import.
    from wallet_service.solana.create_sub_org import (
        create_sub_org as create_solana_sub_org,
    )

    return  create_solana_sub_org(volo_user_id)


class EvmWalletProvisioner(FunctionWalletProvisioner):
    def __init__(self, provision_fn: ProvisioningFn | None = None) -> None:
        super().__init__(
            chain_label="EVM", provision_fn=provision_fn or _default_evm_provision
        )


class SolanaWalletProvisioner(FunctionWalletProvisioner):
    def __init__(self, provision_fn: ProvisioningFn | None = None) -> None:
        super().__init__(
            chain_label="Solana",
            provision_fn=provision_fn or _default_solana_provision,
        )
