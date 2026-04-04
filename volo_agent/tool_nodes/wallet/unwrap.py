from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict

from config.abi import ERC20_ABI, WETH_MINIMAL_ABI
from config.chains import get_chain_by_name
from core.utils.evm_async import async_broadcast_evm, make_async_web3
from core.utils.tx_links import explorer_tx_url
from core.utils.web3_helpers import encode_contract_call
from tool_nodes.common.input_utils import (
    format_with_recovery,
    parse_decimal_field,
    require_fields,
)
from wallet_service.common.wallet_lock import wallet_lock
from wallet_service.evm.gas_price import estimate_eip1559_fees, gas_price_cache
from wallet_service.evm.nonce_manager import (
    get_async_nonce_manager,
    reset_on_error_async,
    rollback_after_signing_error_async,
)
from wallet_service.evm.sign_tx import sign_transaction_async

_TOKEN_DECIMALS = 18
_UNWRAP_GAS_FALLBACK = 120_000
_UNWRAP_GAS_BUFFER = 1.2


def _format_amount(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _to_raw_18(amount: Decimal) -> int:
    scaled = amount * (Decimal(10) ** _TOKEN_DECIMALS)
    return int(scaled.to_integral_value(rounding=ROUND_DOWN))


def _wrapped_symbol(native_symbol: str) -> str:
    symbol = str(native_symbol or "").strip().upper()
    if not symbol:
        return "WRAPPED_NATIVE"
    if symbol.startswith("W"):
        return symbol
    return f"W{symbol}"


def _insufficient_wrapped_balance_error(
    *,
    requested: Decimal | None,
    available: Decimal,
    wrapped_symbol: str,
) -> ValueError:
    available_text = _format_amount(available)
    if requested is None:
        reason = (
            f"No wrapped balance available. Your wrapped balance is 0 {wrapped_symbol}"
        )
        recovery = (
            "fund your wrapped balance first, then retry the unwrap"
        )
        return ValueError(format_with_recovery(reason, recovery))

    requested_text = _format_amount(requested)
    reason = (
        f"Requested unwrap amount exceeds wrapped balance "
        f"(requested {requested_text} {wrapped_symbol}, available {available_text} {wrapped_symbol})"
    )
    recovery = (
        f"use an amount up to {available_text} {wrapped_symbol}, "
        "or omit the amount to unwrap your full wrapped balance"
    )
    return ValueError(format_with_recovery(reason, recovery))


async def unwrap_token(parameters: Dict[str, Any]) -> Dict[str, Any]:
    require_fields(
        parameters,
        ["token_symbol", "token_address", "chain", "sub_org_id", "sender"],
        context="unwrap",
    )

    token_symbol = str(parameters.get("token_symbol") or "").strip().upper()
    wrapped_token = str(parameters.get("token_address") or "").strip()
    chain_name = str(parameters.get("chain") or "").strip().lower()
    sub_org_id = str(parameters.get("sub_org_id") or "").strip()
    sender = str(parameters.get("sender") or "").strip()
    wrapped_symbol = _wrapped_symbol(token_symbol)

    amount_arg = parameters.get("amount")
    amount_requested: Decimal | None = None
    if amount_arg is not None and str(amount_arg).strip() != "":
        amount_requested = parse_decimal_field(
            amount_arg,
            field="amount",
            positive=True,
            invalid_recovery="use a positive unwrap amount (for example, 0.1)",
        )

    chain = get_chain_by_name(chain_name)
    w3 = make_async_web3(chain.rpc_url)
    try:
        checksum_sender = w3.to_checksum_address(sender)
        checksum_token = w3.to_checksum_address(wrapped_token)
    except Exception as exc:
        raise ValueError(
            format_with_recovery(
                "Invalid sender or wrapped token address",
                "provide valid EVM addresses and retry",
            )
        ) from exc

    erc20_contract = w3.eth.contract(address=checksum_token, abi=ERC20_ABI)
    wrapped_balance_raw = int(
        await erc20_contract.functions.balanceOf(checksum_sender).call()
    )
    wrapped_balance = Decimal(wrapped_balance_raw) / (Decimal(10) ** _TOKEN_DECIMALS)

    if amount_requested is None:
        if wrapped_balance_raw <= 0:
            raise _insufficient_wrapped_balance_error(
                requested=None,
                available=wrapped_balance,
                wrapped_symbol=wrapped_symbol,
            )
        amount_raw = wrapped_balance_raw
    else:
        amount_raw = _to_raw_18(amount_requested)
        if amount_raw <= 0:
            raise ValueError(
                format_with_recovery(
                    "Unwrap amount is below the minimum on-chain precision",
                    "increase the amount and retry",
                )
            )
        if amount_raw > wrapped_balance_raw:
            raise _insufficient_wrapped_balance_error(
                requested=amount_requested,
                available=wrapped_balance,
                wrapped_symbol=wrapped_symbol,
            )

    amount_dec = Decimal(amount_raw) / (Decimal(10) ** _TOKEN_DECIMALS)
    withdraw_contract = w3.eth.contract(address=checksum_token, abi=WETH_MINIMAL_ABI)
    data = encode_contract_call(withdraw_contract, "withdraw", [amount_raw])

    gas_price_wei = int(await gas_price_cache.get_wei(chain_id=chain.chain_id))
    max_fee_per_gas, max_priority_fee = await estimate_eip1559_fees(w3, gas_price_wei)
    nonce_manager = await get_async_nonce_manager()

    async with wallet_lock(checksum_sender, chain.chain_id) as lock:
        nonce = await nonce_manager.allocate_safe(checksum_sender, chain.chain_id, w3)
        try:
            gas_estimate = await w3.eth.estimate_gas(
                {
                    "from": checksum_sender,
                    "to": checksum_token,
                    "value": 0,
                    "data": data,
                }
            )
            gas_limit = int(gas_estimate * _UNWRAP_GAS_BUFFER)
        except Exception:
            gas_limit = _UNWRAP_GAS_FALLBACK

        unsigned_tx = {
            "to": checksum_token,
            "value": 0,
            "data": data,
            "nonce": nonce,
            "gas": gas_limit,
            "maxFeePerGas": max_fee_per_gas,
            "maxPriorityFeePerGas": max_priority_fee,
            "type": "0x2",
            "chainId": int(chain.chain_id),
        }

        try:
            signed_tx = await sign_transaction_async(
                sub_org_id,
                unsigned_tx,
                checksum_sender,
            )
        except Exception as exc:
            await rollback_after_signing_error_async(
                checksum_sender,
                chain.chain_id,
                nonce,
                w3,
            )
            raise exc

        await lock.ensure_held()
        try:
            tx_hash = await async_broadcast_evm(w3, signed_tx)
        except Exception as exc:
            await reset_on_error_async(exc, checksum_sender, chain.chain_id, w3)
            await gas_price_cache.invalidate_async(chain_id=chain.chain_id)
            raise RuntimeError(
                format_with_recovery(
                    "We couldn't broadcast the unwrap transaction",
                    "retry in a moment; if it keeps failing, refresh gas and try again",
                )
            ) from exc

    amount_text = _format_amount(amount_dec)
    chain_text = chain.name.strip().lower()
    tx_url = explorer_tx_url(chain.explorer_url, tx_hash)
    message = (
        f"Unwrap submitted: {amount_text} {wrapped_symbol} to {token_symbol} "
        f"on {chain_text}. tx: {tx_hash}"
    )
    if tx_url:
        message += f" ({tx_url})"

    return {
        "status": "success",
        "tx_hash": tx_hash,
        "amount": amount_text,
        "token_symbol": token_symbol,
        "wrapped_token_symbol": wrapped_symbol,
        "token_address": wrapped_token,
        "network": chain_text,
        "chain": chain_text,
        "message": message,
    }
