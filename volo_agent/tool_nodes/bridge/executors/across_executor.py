from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal

from web3 import Web3

from config.abi import ACROSS_SPOKE_POOL_ABI, ERC20_ABI
from config.chains import get_chain_by_id
from core.utils.evm_async import (
    async_await_evm_receipt,
    async_broadcast_evm,
    async_get_allowance,
    async_get_gas_price,
    make_async_web3,
)
from core.utils.http import (
    ExternalServiceError,
    async_raise_for_status,
    async_request_json,
)
from core.utils.web3_helpers import encode_contract_call
from tool_nodes.bridge.executors.evm_utils import to_raw
from tool_nodes.bridge.simulators.across_simulator import AcrossBridgeQuote
from tool_nodes.bridge.utils import format_fill_time
from wallet_service.evm.gas_price import to_eip1559_fees
from wallet_service.evm.nonce_manager import (
    get_async_nonce_manager,
    reset_on_error_async,
)
from wallet_service.evm.sign_tx import sign_transaction_async

_MAX_UINT256 = 2**256 - 1
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
_DEPOSIT_GAS_LIMIT = 300_000
_APPROVE_GAS_LIMIT = 100_000
_STATUS_ENDPOINT = "/deposit/status"
_STATUS_POLL_INTERVAL_SECONDS = 6.0
_STATUS_TIMEOUT_SECONDS = 300.0
_MAX_QUOTE_AGE_SECONDS = 600
_MAX_QUOTE_FUTURE_SKEW_SECONDS = 60

_ACROSS_SUCCESS_STATUSES = {"filled"}
_ACROSS_FAILURE_STATUSES = {"expired", "refunded"}

async def _fetch_deposit_status(api_base_url: str, deposit_tx_hash: str) -> dict:
    response = await async_request_json(
        "GET",
        f"{api_base_url}{_STATUS_ENDPOINT}",
        params={"depositTxnRef": deposit_tx_hash},
        service="across",
    )
    await async_raise_for_status(response, "across")
    return response.json()


async def _poll_deposit_status(
    api_base_url: str,
    deposit_tx_hash: str,
    timeout_seconds: float = _STATUS_TIMEOUT_SECONDS,
    poll_interval: float = _STATUS_POLL_INTERVAL_SECONDS,
) -> dict:
    deadline = time.time() + timeout_seconds
    last_payload: dict = {}
    while time.time() < deadline:
        try:
            payload = await _fetch_deposit_status(api_base_url, deposit_tx_hash)
        except ExternalServiceError:
            payload = {}
        except Exception:
            payload = {}

        if payload:
            last_payload = payload
            status = str(payload.get("status", "")).lower()
            if status in _ACROSS_SUCCESS_STATUSES | _ACROSS_FAILURE_STATUSES:
                return payload

        await asyncio.sleep(poll_interval)

    return last_payload

@dataclass
class AcrossBridgeResult:
    tx_hash: str
    approve_hash: str | None
    source_chain_id: int
    dest_chain_id: int
    source_chain_name: str
    dest_chain_name: str
    token_symbol: str
    input_amount: Decimal
    output_amount: Decimal
    recipient: str
    spoke_pool: str
    fill_deadline: int
    estimated_fill_time: str
    status: str | None = None
    protocol: str = "across"
    nonce: int | None = None
    raw_tx: str | None = None
    tx_payload: dict | None = None


def _build_approve_tx(
    w3: Web3,
    token_address: str,
    spender: str,
    nonce: int,
    max_fee_per_gas: int,
    max_priority_fee_per_gas: int,
    chain_id: int,
) -> dict:
    contract = w3.eth.contract(
        address=w3.to_checksum_address(token_address),
        abi=ERC20_ABI,
    )
    data = encode_contract_call(
        contract,
        "approve",
        [w3.to_checksum_address(spender), _MAX_UINT256],
    )
    return {
        "to": w3.to_checksum_address(token_address),
        "value": 0,
        "data": data,
        "nonce": nonce,
        "gas": hex(_APPROVE_GAS_LIMIT),
        "maxFeePerGas": hex(max_fee_per_gas),
        "maxPriorityFeePerGas": hex(max_priority_fee_per_gas),
        "type": "0x2",
        "chainId": chain_id,
    }


def _coerce_tx_numeric_fields(tx: dict) -> dict:
    normalized = dict(tx)
    for key in (
        "gas",
        "value",
        "nonce",
        "chainId",
        "maxFeePerGas",
        "maxPriorityFeePerGas",
    ):
        value = normalized.get(key)
        if isinstance(value, str) and value.startswith("0x"):
            try:
                normalized[key] = int(value, 16)
            except Exception:
                pass
    return normalized


def _build_deposit_tx(
    w3: Web3,
    quote: AcrossBridgeQuote,
    sender: str,
    recipient: str,
    nonce: int,
    max_fee_per_gas: int,
    max_priority_fee_per_gas: int,
    input_decimals: int,
    output_decimals: int,
) -> dict:
    spoke_pool = w3.eth.contract(
        address=w3.to_checksum_address(quote.spoke_pool),
        abi=ACROSS_SPOKE_POOL_ABI,
    )

    checksum_sender = w3.to_checksum_address(sender)
    checksum_recipient = w3.to_checksum_address(recipient)
    checksum_input_token = w3.to_checksum_address(quote.input_token)
    checksum_output_token = w3.to_checksum_address(quote.output_token)
    checksum_relayer = w3.to_checksum_address(quote.exclusive_relayer)

    input_amount_raw = to_raw(quote.input_amount, input_decimals)
    output_amount_raw = to_raw(quote.output_amount, output_decimals)

    # Native token deposits send ETH as msg.value; ERC-20 deposits send 0.
    value = input_amount_raw if quote.is_native_input else 0

    data = encode_contract_call(
        spoke_pool,
        "depositV3",
        [
            checksum_sender,  # depositor
            checksum_recipient,  # recipient
            checksum_input_token,  # inputToken
            checksum_output_token,  # outputToken
            input_amount_raw,  # inputAmount
            output_amount_raw,  # outputAmount
            quote.dest_chain_id,  # destinationChainId
            checksum_relayer,  # exclusiveRelayer
            quote.quote_timestamp,  # quoteTimestamp  (uint32)
            quote.fill_deadline,  # fillDeadline    (uint32)
            quote.exclusivity_deadline,  # exclusivityDeadline (uint32)
            b"",  # message (empty — no cross-chain call)
        ],
    )

    return {
        "to": w3.to_checksum_address(quote.spoke_pool),
        "value": value,
        "data": data,
        "nonce": nonce,
        "gas": _DEPOSIT_GAS_LIMIT,
        "maxFeePerGas": max_fee_per_gas,
        "maxPriorityFeePerGas": max_priority_fee_per_gas,
        "type": "0x2",
        "chainId": quote.source_chain_id,
    }


async def execute_across_bridge(
    quote: AcrossBridgeQuote,
    sub_org_id: str,
    sender: str,
    recipient: str | None = None,
    gas_price_gwei: float | None = None,
) -> AcrossBridgeResult:
    if int(time.time()) > quote.fill_deadline:
        raise ValueError(
            f"Across quote has expired (fill_deadline={quote.fill_deadline}, "
            f"now={int(time.time())}). Please re-simulate to get a fresh quote."
        )
    now = int(time.time())
    if quote.quote_timestamp:
        age = now - int(quote.quote_timestamp)
        if age > _MAX_QUOTE_AGE_SECONDS or age < -_MAX_QUOTE_FUTURE_SKEW_SECONDS:
            raise ValueError(
                "Across quote is stale or has an invalid timestamp "
                f"(quote_timestamp={quote.quote_timestamp}, now={now}). "
                "Please re-simulate to get a fresh quote."
            )

    recipient = recipient or sender
    source_chain = get_chain_by_id(quote.source_chain_id)
    w3 = make_async_web3(source_chain.rpc_url)

    checksum_sender = w3.to_checksum_address(sender)
    checksum_recipient = w3.to_checksum_address(recipient)
    nonce_manager = await get_async_nonce_manager()

    if gas_price_gwei is not None:
        gas_price = Web3.to_wei(gas_price_gwei, "gwei")
    else:
        gas_price = await async_get_gas_price(w3, chain_id=quote.source_chain_id)
    max_fee_per_gas, max_priority_fee = to_eip1559_fees(gas_price)

    approve_hash: str | None = None

    if not quote.is_native_input:
        current_allowance = await async_get_allowance(
            w3, quote.input_token, checksum_sender, quote.spoke_pool
        )
        input_amount_raw = to_raw(quote.input_amount, quote.input_decimals)

        if current_allowance < input_amount_raw:
            nonce = await nonce_manager.allocate_safe(
                checksum_sender, quote.source_chain_id, w3
            )
            unsigned_approve = _build_approve_tx(
                w3=w3,
                token_address=quote.input_token,
                spender=quote.spoke_pool,
                nonce=nonce,
                max_fee_per_gas=max_fee_per_gas,
                max_priority_fee_per_gas=max_priority_fee,
                chain_id=quote.source_chain_id,
            )
            unsigned_approve = _coerce_tx_numeric_fields(unsigned_approve)
            signed_approve = await sign_transaction_async(
                sub_org_id, unsigned_approve, checksum_sender
            )
            try:
                approve_hash = await async_broadcast_evm(w3, signed_approve)
            except Exception as exc:
                await reset_on_error_async(
                    exc, checksum_sender, quote.source_chain_id, w3
                )
                raise RuntimeError(
                    "We couldn't broadcast the approval transaction. "
                    "Please try again in a moment."
                ) from exc

            # Wait for approval to mine before depositing
            await async_await_evm_receipt(w3, approve_hash, timeout=120)

    nonce = await nonce_manager.allocate_safe(
        checksum_sender, quote.source_chain_id, w3
    )
    unsigned_deposit = _build_deposit_tx(
        w3=w3,
        quote=quote,
        sender=checksum_sender,
        recipient=checksum_recipient,
        nonce=nonce,
        max_fee_per_gas=max_fee_per_gas,
        max_priority_fee_per_gas=max_priority_fee,
        input_decimals=quote.input_decimals,
        output_decimals=quote.output_decimals,
    )

    signed_deposit = await sign_transaction_async(
        sub_org_id, unsigned_deposit, checksum_sender
    )
    try:
        tx_hash = await async_broadcast_evm(w3, signed_deposit)
    except Exception as exc:
        await reset_on_error_async(exc, checksum_sender, quote.source_chain_id, w3)
        raise RuntimeError(
            "We couldn't broadcast the bridge transaction. "
            "Please try again in a moment."
        ) from exc

    status = "pending"

    return AcrossBridgeResult(
        tx_hash=tx_hash,
        approve_hash=approve_hash,
        source_chain_id=quote.source_chain_id,
        dest_chain_id=quote.dest_chain_id,
        source_chain_name=quote.source_chain_name,
        dest_chain_name=quote.dest_chain_name,
        token_symbol=quote.token_symbol,
        input_amount=quote.input_amount,
        output_amount=quote.output_amount,
        recipient=checksum_recipient,
        spoke_pool=quote.spoke_pool,
        fill_deadline=quote.fill_deadline,
        estimated_fill_time=format_fill_time(quote.avg_fill_time_seconds),
        status=status,
        protocol="across",
        nonce=nonce,
        raw_tx=signed_deposit,
        tx_payload=unsigned_deposit,
    )
