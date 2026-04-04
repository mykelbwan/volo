from __future__ import annotations

import asyncio
import base64
import threading
from concurrent.futures import Future
from decimal import Decimal, InvalidOperation
from typing import Any

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

_TOKEN_DECIMALS_CACHE: dict[str, int] = {}
_TOKEN_DECIMALS_CACHE_LOCK = threading.Lock()
_TOKEN_DECIMALS_IN_FLIGHT: dict[str, Future[int]] = {}


async def _decode_mint_decimals_async(mint_info: Any) -> int:
    """Decode SPL mint decimals from raw account data."""
    data = getattr(mint_info, "value", None)
    if data is None:
        raise ValueError("Mint account info missing value")
    raw_data = getattr(data, "data", None)
    if raw_data is None:
        raise ValueError("Mint account info missing data")

    if isinstance(raw_data, (bytes, bytearray)):
        if len(raw_data) <= 44:
            raise ValueError("Mint account data is too short")
        return int(raw_data[44])
    if isinstance(raw_data, list) and raw_data and isinstance(raw_data[0], str):
        import base64 as _b64

        decoded = _b64.b64decode(raw_data[0])
        if len(decoded) <= 44:
            raise ValueError("Mint account data is too short")
        return int(decoded[44])
    raise ValueError("Unsupported mint account data format")


def _parse_amount(amount: Decimal | str | float | int, decimals: int) -> int:
    """Convert a human-readable SPL amount into raw base units."""
    try:
        value = Decimal(str(amount))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(
            format_with_recovery(
                "Invalid SPL transfer amount",
                "provide amount as a positive numeric value and retry",
            )
        ) from exc
    if value <= 0:
        raise ValueError(
            format_with_recovery(
                "Invalid SPL transfer amount",
                "provide amount greater than zero and retry",
            )
        )
    raw = int(value * (Decimal(10) ** decimals))
    if raw <= 0:
        raise ValueError(
            format_with_recovery(
                "Amount is too small after conversion to base units",
                "increase amount or verify token decimals and retry",
            )
        )
    return raw


def _resolve_solana_network(rpc_url: str, network: str | None) -> str:
    if network:
        return normalize_solana_network(network)
    rpc = rpc_url.strip().lower()
    if "devnet" in rpc or "testnet" in rpc:
        return "solana-devnet"
    return "solana"


async def _get_cached_mint_decimals(
    client: Any, mint_pubkey: Any, mint_address: str
) -> int:
    """Cache mint decimals indefinitely because token metadata is immutable."""
    cache_key = str(mint_address).strip()
    with _TOKEN_DECIMALS_CACHE_LOCK:
        cached = _TOKEN_DECIMALS_CACHE.get(cache_key)
        if cached is not None:
            return cached
        in_flight = _TOKEN_DECIMALS_IN_FLIGHT.get(cache_key)
        if in_flight is None:
            # Share one mint-info RPC per token so parallel builders do not
            # fan out identical metadata reads before the cache is filled.
            in_flight = Future()
            _TOKEN_DECIMALS_IN_FLIGHT[cache_key] = in_flight
            is_owner = True
        else:
            is_owner = False

    if not is_owner:
        return await asyncio.shield(asyncio.wrap_future(in_flight))

    try:
        mint_info = await client.get_account_info(mint_pubkey)
        if mint_info is None:
            raise ValueError("Failed to fetch mint info")
        decimals = await _decode_mint_decimals_async(mint_info)
        with _TOKEN_DECIMALS_CACHE_LOCK:
            _TOKEN_DECIMALS_CACHE[cache_key] = decimals
        if not in_flight.done():
            in_flight.set_result(decimals)
        return decimals
    except BaseException as exc:
        if not in_flight.done():
            in_flight.set_exception(exc)
        raise
    finally:
        with _TOKEN_DECIMALS_CACHE_LOCK:
            if _TOKEN_DECIMALS_IN_FLIGHT.get(cache_key) is in_flight:
                _TOKEN_DECIMALS_IN_FLIGHT.pop(cache_key, None)


async def _build_spl_transfer_tx_async(
    *,
    rpc_url: str,
    sender: str,
    recipient: str,
    mint_address: str,
    amount: Decimal | str | float | int,
    decimals: int | None,
    memo_text: str,
):
    from solders.hash import Hash
    from solders.message import Message
    from solders.pubkey import Pubkey
    from solders.transaction import Transaction
    from spl.token.constants import TOKEN_PROGRAM_ID
    from spl.token.instructions import (
        TransferCheckedParams,
        create_idempotent_associated_token_account,
        get_associated_token_address,
        transfer_checked,
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
    try:
        mint_pubkey = Pubkey.from_string(mint_address)
    except Exception as exc:
        raise ValueError(
            format_with_recovery(
                f"Invalid mint address: {mint_address!r}",
                "provide a valid mint base58 address and retry",
            )
        ) from exc

    client = await get_shared_solana_client(rpc_url)
    if decimals is None:
        decimals = await _get_cached_mint_decimals(client, mint_pubkey, mint_address)

    if not isinstance(decimals, int) or decimals < 0:
        raise ValueError(
            format_with_recovery(
                "Invalid token decimals",
                "provide decimals as a non-negative integer and retry",
            )
        )

    raw_amount = _parse_amount(amount, decimals)

    source_ata = get_associated_token_address(sender_pubkey, mint_pubkey)
    destination_ata = get_associated_token_address(recipient_pubkey, mint_pubkey)

    instructions = [
        # Use the idempotent ATA instruction so concurrent transfers do not
        # race each other when the recipient token account is created.
        create_idempotent_associated_token_account(
            payer=sender_pubkey,
            owner=recipient_pubkey,
            mint=mint_pubkey,
        )
    ]

    instructions.append(
        transfer_checked(
            params=TransferCheckedParams(
                program_id=TOKEN_PROGRAM_ID,
                amount=raw_amount,
                decimals=decimals,
                dest=destination_ata,
                owner=sender_pubkey,
                source=source_ata,
                mint=mint_pubkey,
            ),
        )
    )
    instructions.append(
        create_replay_protection_memo_instruction(
            sender_pubkey,
            memo_text=memo_text,
        )
    )

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
    message = Message.new_with_blockhash(instructions, sender_pubkey, recent_blockhash)
    return Transaction.new_unsigned(message)


async def execute_spl_transfer(
    *,
    sender: str,
    sub_org_id: str,
    recipient: str,
    mint_address: str,
    amount: Decimal | str | float | int,
    rpc_url: str,
    network: str | None = None,
    decimals: int | None = None,
    idempotency_key: str | None = None,
) -> str:
    """
    Asynchronously transfer SPL tokens via CDP using an async Solana client.
    """
    rpc = require_non_empty_str(rpc_url, field="rpc_url")
    normalized_network = _resolve_solana_network(rpc, network)
    sender_value = require_non_empty_str(sender, field="sender")
    solana_sub_org_id = require_non_empty_str(sub_org_id, field="sub_org_id")
    recipient_value = require_non_empty_str(recipient, field="recipient")
    mint_value = require_non_empty_str(mint_address, field="mint_address")
    claim = await claim_transfer_idempotency(
        operation="solana_spl_transfer",
        idempotency_key=idempotency_key,
        request_fields={
            "sub_org_id": solana_sub_org_id,
            "sender": sender_value,
            "recipient": recipient_value,
            "mint_address": mint_value,
            "amount": str(amount),
            "rpc_url": rpc,
            "network": normalized_network,
            "decimals": decimals,
        },
    )
    if claim is not None and claim.reused and claim.tx_hash:
        return claim.tx_hash
    memo_text = build_replay_protection_memo(idempotency_key)

    tx = await _build_spl_transfer_tx_async(
        rpc_url=rpc,
        sender=sender_value,
        recipient=recipient_value,
        mint_address=mint_value,
        amount=amount,
        decimals=decimals,
        memo_text=memo_text,
    )
    serialized = base64.b64encode(bytes(tx)).decode("utf-8")
    try:
        signed = await sign_transaction_async(
            solana_sub_org_id,
            serialized,
            sign_with=sender_value,
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
        invalidate_cached_blockhash(rpc)
        await mark_transfer_failed(claim, error=str(exc))
        raise
    await mark_transfer_inflight(claim, tx_hash=signature)
    await mark_transfer_success(
        claim,
        tx_hash=signature,
        result={"tx_hash": signature, "status": "submitted"},
    )
    return signature
