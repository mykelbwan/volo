from __future__ import annotations

from typing import Protocol

from core.transfers.chains import TransferChainSpec
from core.transfers.evm_handler import EVM_TRANSFER_HANDLER
from core.transfers.models import NormalizedTransferRequest, TransferExecutionResult
from core.transfers.solana_handler import SOLANA_TRANSFER_HANDLER


class TransferHandler(Protocol):
    async def execute_transfer(
        self,
        request: NormalizedTransferRequest,
        chain_spec: TransferChainSpec,
    ) -> TransferExecutionResult: ...


TRANSFER_HANDLERS: dict[str, TransferHandler] = {
    "evm": EVM_TRANSFER_HANDLER,
    "solana": SOLANA_TRANSFER_HANDLER,
}


def get_transfer_handler(family: str) -> TransferHandler | None:
    return TRANSFER_HANDLERS.get(str(family).strip().lower())
