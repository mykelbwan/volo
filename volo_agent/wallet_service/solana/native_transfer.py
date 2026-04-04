from __future__ import annotations

import base64
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from typing import Any

from config.solana_chains import get_solana_chain
from wallet_service.common.messages import format_with_recovery, require_non_empty_str
from wallet_service.common.transfer_idempotency import (
    claim_transfer_idempotency,
    mark_transfer_failed,
    mark_transfer_inflight,
    mark_transfer_success,
)
from wallet_service.solana.cdp_utils import (
    normalize_solana_network,
    send_solana_transaction_async,
)
from wallet_service.solana.rpc_client import (
    get_cached_latest_blockhash,
    get_shared_solana_client,
    invalidate_cached_blockhash,
)
from wallet_service.solana.sign_tx import sign_transaction_async
from wallet_service.solana.transaction_security import (
    build_replay_protection_memo,
    create_replay_protection_memo_instruction,
)


@lru_cache(maxsize=1)
def _solana_primitives() -> tuple[Any, Any, Any, Any, Any, Any]:
    from solders.hash import Hash
    from solders.message import Message
    from solders.pubkey import Pubkey
    from solders.system_program import TransferParams
    from solders.system_program import transfer as solders_transfer
    from solders.transaction import Transaction

    return Hash, Message, Pubkey, TransferParams, solders_transfer, Transaction


def _parse_amount(amount_native: Decimal | str | float | int) -> int:
    try:
        value = Decimal(str(amount_native))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(
            format_with_recovery(
                "Invalid amount_native value",
                "provide amount_native as a positive numeric SOL amount and retry",
            )
        ) from exc
    if value <= 0:
        raise ValueError(
            format_with_recovery(
                "Invalid amount_native value",
                "provide amount_native greater than zero and retry",
            )
        )
    lamports = int(value * Decimal(10**9))
    if lamports <= 0:
        raise ValueError(
            format_with_recovery(
                "amount_native is too small after conversion to lamports",
                "increase amount_native and retry",
            )
        )
    return lamports


async def _build_native_transfer_tx_async(
    sender: str,
    recipient: str,
    lamports: int,
    *,
    rpc_url: str,
    memo_text: str,
):
    Hash, Message, Pubkey, TransferParams, solders_transfer, Transaction = (
        _solana_primitives()
    )
    try:
        sender_pubkey = Pubkey.from_string(sender)
    except Exception as exc:
        raise ValueError(
            format_with_recovery(
                f"Invalid sender address: {sender!r}",
                "provide a valid sender base58 address and retry",
            )
        ) from exc
    try:
        recipient_pubkey = Pubkey.from_string(recipient)
    except Exception as exc:
        raise ValueError(
            format_with_recovery(
                f"Invalid recipient address: {recipient!r}",
                "provide a valid recipient base58 address and retry",
            )
        ) from exc

    transfer_params = TransferParams(
        from_pubkey=sender_pubkey,
        to_pubkey=recipient_pubkey,
        lamports=lamports,
    )
    instruction = solders_transfer(transfer_params)
    memo_instruction = create_replay_protection_memo_instruction(
        sender_pubkey,
        memo_text=memo_text,
    )
    # Warm, shared AsyncClient instances avoid rebuilding the Solana HTTP
    # transport for each transfer.
    await get_shared_solana_client(rpc_url)
    recent_blockhash = await get_cached_latest_blockhash(rpc_url)
    if recent_blockhash is None:
        raise RuntimeError(
            format_with_recovery(
                "Failed to fetch a recent Solana blockhash",
                "retry in a moment; if it persists, verify Solana RPC health",
            )
        )
    if not isinstance(recent_blockhash, Hash):
        recent_blockhash = Hash.from_string(str(recent_blockhash))
    message = Message.new_with_blockhash(
        [instruction, memo_instruction], sender_pubkey, recent_blockhash
    )
    return Transaction.new_unsigned(message)


async def execute_native_transfer(
    *,
    sender: str,
    sub_org_id: str,
    recipient: str,
    amount_native: Decimal | str | float | int,
    network: str | None = None,
    idempotency_key: str | None = None,
) -> str:
    sender_address = require_non_empty_str(sender, field="sender")
    solana_sub_org_id = require_non_empty_str(sub_org_id, field="sub_org_id")
    recipient_address = require_non_empty_str(recipient, field="recipient")
    lamports = _parse_amount(amount_native)
    normalized_network = normalize_solana_network(network)
    rpc_url = get_solana_chain(normalized_network).rpc_url
    claim = await claim_transfer_idempotency(
        operation="solana_native_transfer",
        idempotency_key=idempotency_key,
        request_fields={
            "sub_org_id": solana_sub_org_id,
            "sender": sender_address,
            "recipient": recipient_address,
            "amount_native": str(amount_native),
            "network": normalized_network,
        },
    )
    if claim is not None and claim.reused and claim.tx_hash:
        return claim.tx_hash
    memo_text = build_replay_protection_memo(idempotency_key)
    tx = await _build_native_transfer_tx_async(
        sender_address,
        recipient_address,
        lamports,
        rpc_url=rpc_url,
        memo_text=memo_text,
    )
    serialized = base64.b64encode(bytes(tx)).decode("utf-8")
    try:
        signed = await sign_transaction_async(
            solana_sub_org_id,
            serialized,
            sign_with=sender_address,
        )
    except Exception as exc:
        await mark_transfer_failed(claim, error=str(exc))
        raise
    try:
        signature = await send_solana_transaction_async(
            signed,
            network=normalized_network,
        )
    except Exception as exc:
        invalidate_cached_blockhash(rpc_url)
        await mark_transfer_failed(claim, error=str(exc))
        raise
    await mark_transfer_inflight(claim, tx_hash=signature)
    await mark_transfer_success(
        claim,
        tx_hash=signature,
        result={"tx_hash": signature, "status": "submitted"},
    )
    return signature
