from decimal import Decimal, InvalidOperation
from typing import Any, Dict

from wallet_service.common.messages import format_with_recovery, require_non_empty_str

_HEX_CHARS = set("0123456789abcdef")


def encode_transfer(recipient: str, amount: int) -> str:
    # keccak256("transfer(address,uint256)")[0:4]
    method_id = "a9059cbb"

    # Normalize: strip 0x prefix if present, then lowercase
    recipient_value = require_non_empty_str(recipient, field="recipient")
    clean_recipient = recipient_value.lower().removeprefix("0x")
    if len(clean_recipient) != 40 or not all(
        ch in _HEX_CHARS for ch in clean_recipient
    ):
        raise ValueError(
            format_with_recovery(
                f"Invalid Ethereum recipient address: {recipient!r}",
                "provide a 20-byte hex address and retry",
            )
        )
    if not isinstance(amount, int) or amount <= 0:
        raise ValueError(
            format_with_recovery(
                "Invalid ERC-20 transfer amount",
                "provide a positive token amount and retry",
            )
        )

    # ABI encoding: address is left-padded to 32 bytes (64 hex chars)
    padded_recipient = clean_recipient.zfill(64)
    # ABI encoding: uint256 is left-padded to 32 bytes (64 hex chars)
    padded_amount = hex(amount)[2:].zfill(64)

    return "0x" + method_id + padded_recipient + padded_amount


def to_raw_amount(human_amount: Decimal | str | float | int, decimals: int) -> int:
    if not isinstance(decimals, int) or decimals < 0:
        raise ValueError(
            format_with_recovery(
                "Invalid ERC-20 decimals value",
                "provide decimals as a non-negative integer and retry",
            )
        )
    try:
        value = Decimal(str(human_amount))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(
            format_with_recovery(
                "Invalid token amount",
                "provide a numeric amount greater than zero and retry",
            )
        ) from exc
    if not value.is_finite() or value <= 0:
        raise ValueError(
            format_with_recovery(
                "Invalid token amount",
                "provide a numeric amount greater than zero and retry",
            )
        )
    raw = int(value * (Decimal(10) ** decimals))
    if raw <= 0:
        raise ValueError(
            format_with_recovery(
                "Token amount is too small for the token decimals",
                "increase amount or verify token decimals and retry",
            )
        )
    return raw


def build_erc20_tx(
    token_address: str,
    to: str,
    amount: Decimal | str | float | int,
    decimals: int,
    chain_id: int,
    gas: int,
    max_fee_per_gas: int,
    max_priority_fee_per_gas: int,
    nonce: int,
) -> Dict[str, Any]:
    raw_amount = to_raw_amount(amount, decimals)
    return {
        "to": token_address,
        "value": 0,
        "data": encode_transfer(to, raw_amount),
        "chainId": chain_id,
        "gas": gas,
        "maxFeePerGas": max_fee_per_gas,
        "maxPriorityFeePerGas": max_priority_fee_per_gas,
        "type": "0x2",
        "nonce": nonce,
    }
