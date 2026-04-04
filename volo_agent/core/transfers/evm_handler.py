from __future__ import annotations

from decimal import Decimal

from config.chains import get_chain_by_name
from core.transfers.chains import TransferChainSpec
from core.transfers.models import NormalizedTransferRequest, TransferExecutionResult
from core.utils.evm_async import async_broadcast_evm, make_async_web3
from core.utils.tx_links import explorer_tx_url
from tool_nodes.common.input_utils import format_with_recovery, parse_decimal_field
from wallet_service.common.transfer_idempotency import (
    claim_transfer_idempotency,
    mark_transfer_failed,
    mark_transfer_inflight,
    mark_transfer_success,
)
from wallet_service.common.wallet_lock import wallet_lock
from wallet_service.evm.erc20_transfer_helper_function import build_erc20_tx
from wallet_service.evm.gas_price import estimate_eip1559_fees, gas_price_cache
from wallet_service.evm.native_transfer import execute_native_transfer
from wallet_service.evm.nonce_manager import (
    get_async_nonce_manager,
    reset_on_error_async,
    rollback_after_signing_error_async,
)
from wallet_service.evm.sign_tx import sign_transaction_async

_ERC20_TRANSFER_GAS_LIMIT = 65_000


def _is_native_asset_ref(
    asset_ref: str | None,
    *,
    chain_spec: TransferChainSpec,
) -> bool:
    if asset_ref is None:
        return True
    return (
        str(asset_ref).strip().lower()
        == str(chain_spec.native_asset_ref).strip().lower()
    )


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


class EvmTransferHandler:
    async def execute_transfer(
        self,
        request: NormalizedTransferRequest,
        chain_spec: TransferChainSpec,
    ) -> TransferExecutionResult:
        if chain_spec.family != "evm":
            raise ValueError(
                format_with_recovery(
                    f"EVM transfer handler cannot execute family {chain_spec.family!r}",
                    "retry on a supported EVM network",
                )
            )

        chain = get_chain_by_name(request.network)

        if _is_native_asset_ref(request.asset_ref, chain_spec=chain_spec):
            tx_hash = await execute_native_transfer(
                to=request.recipient,
                amount_native=request.amount,
                chain_name=request.network,
                sender=request.sender,
                sub_org_id=request.sub_org_id,
                idempotency_key=request.idempotency_key,
            )
            return _build_response(
                request=request,
                tx_hash=tx_hash,
                explorer_url=explorer_tx_url(chain.explorer_url, tx_hash),
            )

        gas_price_wei = int(await gas_price_cache.get_wei(chain_id=chain.chain_id))

        w3 = make_async_web3(chain.rpc_url)
        checksum_sender = w3.to_checksum_address(str(request.sender))
        checksum_recipient = w3.to_checksum_address(str(request.recipient))
        claim = await claim_transfer_idempotency(
            operation="evm_erc20_transfer",
            idempotency_key=request.idempotency_key,
            request_fields={
                "token_address": str(request.asset_ref),
                "recipient": checksum_recipient,
                "amount": str(request.amount),
                "chain": request.network,
                "sender": checksum_sender,
                "sub_org_id": request.sub_org_id,
                "decimals": request.decimals if request.decimals is not None else 18,
            },
        )
        if claim is not None and claim.reused and claim.tx_hash:
            return _build_response(
                request=request,
                tx_hash=claim.tx_hash,
                explorer_url=explorer_tx_url(chain.explorer_url, claim.tx_hash),
            )

        async with wallet_lock(checksum_sender, chain.chain_id) as lock:
            max_fee_per_gas, max_priority_fee = await estimate_eip1559_fees(
                w3, gas_price_wei
            )

            nonce_manager = await get_async_nonce_manager()
            nonce = await nonce_manager.allocate_safe(
                checksum_sender, chain.chain_id, w3
            )

            decimals_value = parse_decimal_field(
                request.decimals if request.decimals is not None else 18,
                field="decimals",
                min_value=Decimal("0"),
                max_value=Decimal("36"),
                invalid_recovery="use token decimals as a whole number between 0 and 36",
            )
            if decimals_value != decimals_value.to_integral_value():
                raise ValueError(
                    format_with_recovery(
                        "'decimals' must be a whole number",
                        "use token decimals as an integer between 0 and 36",
                    )
                )
            decimals = int(decimals_value)

            unsigned_tx = build_erc20_tx(
                token_address=str(request.asset_ref),
                to=request.recipient,
                amount=request.amount,
                decimals=decimals,
                chain_id=chain.chain_id,
                gas=_ERC20_TRANSFER_GAS_LIMIT,
                max_fee_per_gas=max_fee_per_gas,
                max_priority_fee_per_gas=max_priority_fee,
                nonce=nonce,
            )

            try:
                signed_tx = await sign_transaction_async(
                    str(request.sub_org_id),
                    unsigned_tx,
                    checksum_sender,
                )
            except Exception as exc:
                await rollback_after_signing_error_async(
                    checksum_sender, chain.chain_id, nonce, w3
                )
                await mark_transfer_failed(claim, error=str(exc))
                raise
            await lock.ensure_held()
            try:
                tx_hash = await async_broadcast_evm(w3, signed_tx)
            except Exception as exc:
                await reset_on_error_async(exc, checksum_sender, chain.chain_id, w3)
                await gas_price_cache.invalidate_async(chain_id=chain.chain_id)
                await mark_transfer_failed(claim, error=str(exc))
                raise RuntimeError(
                    format_with_recovery(
                        "We couldn't broadcast the transfer transaction",
                        "retry in a moment; if it keeps failing, refresh gas and try again",
                    )
                ) from exc
            await mark_transfer_inflight(claim, tx_hash=tx_hash)
            await mark_transfer_success(
                claim,
                tx_hash=tx_hash,
                result={"tx_hash": tx_hash, "status": "submitted"},
            )

        return _build_response(
            request=request,
            tx_hash=tx_hash,
            explorer_url=explorer_tx_url(chain.explorer_url, tx_hash),
        )


EVM_TRANSFER_HANDLER = EvmTransferHandler()
