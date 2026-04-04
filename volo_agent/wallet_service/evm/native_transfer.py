from __future__ import annotations

from decimal import Decimal, InvalidOperation

from config.chains import get_chain_by_name
from core.utils.evm_async import async_broadcast_evm, get_shared_async_web3
from wallet_service.common.messages import (
    format_with_recovery,
    require_non_empty_str,
)
from wallet_service.common.transfer_idempotency import (
    claim_transfer_idempotency,
    mark_transfer_failed,
    mark_transfer_inflight,
    mark_transfer_success,
)
from wallet_service.common.wallet_lock import wallet_lock
from wallet_service.evm.gas_price import (
    estimate_eip1559_fees,
    gas_price_cache,
)
from wallet_service.evm.nonce_manager import (
    get_async_nonce_manager,
    reset_on_error_async,
    rollback_after_signing_error_async,
)
from wallet_service.evm.sign_tx import sign_transaction_async

# Gas limit for a plain native-token transfer (ETH/MATIC/BNB/etc.).
# EIP-2028 mandates exactly 21 000 gas for a zero-data value transfer;
# this will never be higher so no buffer is needed.
_GAS_LIMIT: int = 21_000


def _to_wei_amount(amount_native: Decimal) -> int:
    try:
        value = Decimal(str(amount_native))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(
            format_with_recovery(
                "Invalid native transfer amount",
                "provide amount_native as a positive numeric value and retry",
            )
        ) from exc
    if not value.is_finite() or value <= 0:
        raise ValueError(
            format_with_recovery(
                "Invalid native transfer amount",
                "provide amount_native as a positive numeric value and retry",
            )
        )
    wei_value = int(value * Decimal(10**18))
    if wei_value <= 0:
        raise ValueError(
            format_with_recovery(
                "Transfer amount is too small after wei conversion",
                "increase amount_native and retry",
            )
        )
    return wei_value


async def execute_native_transfer(
    to: str,
    amount_native: Decimal,
    chain_name: str,
    sender: str,
    sub_org_id: str,
    idempotency_key: str | None = None,
) -> str:
    recipient = require_non_empty_str(to, field="to")
    sender_address = require_non_empty_str(sender, field="sender")
    sub_org = require_non_empty_str(sub_org_id, field="sub_org_id")
    chain = get_chain_by_name(require_non_empty_str(chain_name, field="chain_name"))
    # Reuse the shared AsyncWeb3 HTTP transport so broadcast-heavy flows keep
    # connections warm instead of building one session per transfer.
    w3 = await get_shared_async_web3(chain.rpc_url)

    try:
        checksum_sender = w3.to_checksum_address(sender_address)
        checksum_to = w3.to_checksum_address(recipient)
    except Exception as exc:
        raise ValueError(
            format_with_recovery(
                "Invalid sender or recipient Ethereum address",
                "provide valid hex addresses for 'sender' and 'to', then retry",
            )
        ) from exc

    claim = await claim_transfer_idempotency(
        operation="evm_native_transfer",
        idempotency_key=idempotency_key,
        request_fields={
            "to": checksum_to,
            "amount_native": str(amount_native),
            "chain_name": chain.name,
            "sender": checksum_sender,
            "sub_org_id": sub_org,
        },
    )
    if claim is not None and claim.reused and claim.tx_hash:
        return claim.tx_hash

    async with wallet_lock(checksum_sender, chain.chain_id) as lock:
        gas_price_wei = int(await gas_price_cache.get_wei(chain_id=chain.chain_id))
        max_fee_per_gas, max_priority_fee = await estimate_eip1559_fees(
            w3, gas_price_wei
        )
        nonce_manager = await get_async_nonce_manager()
        nonce = await nonce_manager.allocate_safe(checksum_sender, chain.chain_id, w3)

        # Convert human-readable amount → wei (18 decimal places for all native tokens).
        value_wei = _to_wei_amount(amount_native)

        unsigned_tx = {
            "to": checksum_to,
            "value": value_wei,
            "data": "0x",
            "nonce": nonce,
            "gas": _GAS_LIMIT,
            "maxFeePerGas": max_fee_per_gas,
            "maxPriorityFeePerGas": max_priority_fee,
            "type": "0x2",
            "chainId": chain.chain_id,
        }

        try:
            signed_tx = await sign_transaction_async(
                sub_org, unsigned_tx, checksum_sender
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
            await reset_on_error_async(exc, sender_address, chain.chain_id, w3)
            await gas_price_cache.invalidate_async(chain_id=chain.chain_id)
            await mark_transfer_failed(claim, error=str(exc))
            raise RuntimeError(
                format_with_recovery(
                    "We couldn't broadcast the transfer transaction",
                    "retry in a moment; if it repeats, refresh nonce and gas estimates, then retry",
                )
            ) from exc
        await mark_transfer_inflight(claim, tx_hash=tx_hash)
        await mark_transfer_success(
            claim,
            tx_hash=tx_hash,
            result={"tx_hash": tx_hash, "status": "submitted"},
        )
        return tx_hash
