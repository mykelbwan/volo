from __future__ import annotations

from config.solana_chains import get_solana_chain
from core.transfers.chains import TransferChainSpec
from core.transfers.models import NormalizedTransferRequest, TransferExecutionResult
from core.utils.tx_links import explorer_tx_url
from tool_nodes.common.input_utils import format_with_recovery
from wallet_service.solana.native_transfer import execute_native_transfer
from wallet_service.solana.spl_transfer import execute_spl_transfer


def _response_network(request: NormalizedTransferRequest) -> str:
    return request.requested_network or request.network


def _build_response(
    *,
    request: NormalizedTransferRequest,
    tx_hash: str,
    explorer_url: str | None,
) -> TransferExecutionResult:
    response_network = _response_network(request)
    return TransferExecutionResult(
        status="success",
        tx_hash=tx_hash,
        asset_symbol=request.asset_symbol,
        amount=request.amount,
        recipient=request.recipient,
        network=response_network,
        message=(
            f"Transfer submitted: {request.amount} {request.asset_symbol} "
            f"to {request.recipient} on {response_network}. tx: {tx_hash}"
            + (f" ({explorer_url})" if explorer_url else "")
        ),
    )


def _native_symbol_matches(
    request: NormalizedTransferRequest,
    *,
    chain_spec: TransferChainSpec,
) -> bool:
    return (
        request.asset_symbol.strip().upper() == chain_spec.native_symbol.strip().upper()
    )


def _is_explicit_native_asset_ref(
    asset_ref: str | None,
    *,
    chain_spec: TransferChainSpec,
) -> bool:
    if asset_ref is None:
        return False
    return (
        str(asset_ref).strip().lower()
        == str(chain_spec.native_asset_ref).strip().lower()
    )


def _classify_solana_asset(
    request: NormalizedTransferRequest,
    *,
    chain_spec: TransferChainSpec,
) -> tuple[str, str | None]:
    if _is_explicit_native_asset_ref(request.asset_ref, chain_spec=chain_spec):
        if not _native_symbol_matches(request, chain_spec=chain_spec):
            raise ValueError(
                format_with_recovery(
                    "Solana native asset reference requires the native SOL symbol",
                    "retry with asset_symbol='SOL' for native transfers or provide an SPL mint address",
                )
            )
        return "native", str(chain_spec.native_asset_ref)

    if request.asset_ref is None:
        if _native_symbol_matches(request, chain_spec=chain_spec):
            return "native", str(chain_spec.native_asset_ref)
        raise ValueError(
            format_with_recovery(
                "Solana SPL transfers require an explicit mint address",
                "retry with the token mint as asset_ref or token_address",
            )
        )

    mint_address = str(request.asset_ref).strip()
    if not mint_address:
        raise ValueError(
            format_with_recovery(
                "Malformed Solana asset reference",
                "retry with the native asset reference for SOL or an SPL mint address",
            )
        )
    return "spl", mint_address


class SolanaTransferHandler:
    async def execute_transfer(
        self,
        request: NormalizedTransferRequest,
        chain_spec: TransferChainSpec,
    ) -> TransferExecutionResult:
        if chain_spec.family != "solana":
            raise ValueError(
                format_with_recovery(
                    f"Solana transfer handler cannot execute family {chain_spec.family!r}",
                    "retry on a supported Solana network",
                )
            )

        asset_kind, asset_ref = _classify_solana_asset(request, chain_spec=chain_spec)
        tx_hash: str
        if asset_kind == "native":
            tx_hash = await execute_native_transfer(
                sender=request.sender,
                sub_org_id=request.sub_org_id,
                recipient=request.recipient,
                amount_native=request.amount,
                network=request.network,
                idempotency_key=request.idempotency_key,
            )
        else:
            tx_hash = await execute_spl_transfer(
                sender=request.sender,
                sub_org_id=request.sub_org_id,
                recipient=request.recipient,
                mint_address=str(asset_ref),
                amount=request.amount,
                rpc_url=get_solana_chain(request.network).rpc_url,
                network=request.network,
                decimals=request.decimals,
                idempotency_key=request.idempotency_key,
            )

        return _build_response(
            request=request,
            tx_hash=tx_hash,
            explorer_url=explorer_tx_url(chain_spec.explorer_url, tx_hash),
        )


SOLANA_TRANSFER_HANDLER = SolanaTransferHandler()
