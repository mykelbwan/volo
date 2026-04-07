from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

from config.abi import ERC20_ABI
from config.chains import get_chain_by_id
from core.utils.errors import NonRetryableError
from wallet_service.evm.gas_price import to_eip1559_fees
from wallet_service.evm.nonce_manager import (
    get_async_nonce_manager,
    reset_on_error_async,
)
from wallet_service.evm.sign_tx import sign_transaction_async

from core.utils.evm_async import (
    async_await_evm_receipt,
    async_broadcast_evm,
    async_get_allowance,
    async_get_gas_price,
    make_async_web3,
)
from tool_nodes.bridge.executors.evm_utils import safe_int
from wallet_service.solana.sign_tx import sign_transaction_async as sign_solana_transaction
from wallet_service.solana.cdp_utils import send_solana_transaction_async

_LOGGER = logging.getLogger("volo.bridge.lifi")

_ZERO = "0x0000000000000000000000000000000000000000"
_GAS_FALLBACK = 300_000
_GAS_BUFFER = 1.2


@dataclass
class LiFiBridgeResult:
    tx_hash: str
    approve_hash: Optional[str]
    protocol: str
    token_symbol: str
    input_amount: Decimal
    output_amount: Decimal
    source_chain_name: str
    dest_chain_name: str
    recipient: str
    status: str = "pending"
    nonce: Optional[int] = None
    raw_tx: Optional[str] = None
    tx_payload: Optional[dict] = None
    bridge: Optional[str] = None
    from_chain_id: Optional[int] = None
    to_chain_id: Optional[int] = None


def _extract_step_list(tool_data: Dict[str, Any]) -> list[dict]:
    steps = tool_data.get("steps") or tool_data.get("includedSteps") or []
    if isinstance(steps, list):
        return [s for s in steps if isinstance(s, dict)]
    return []


def _extract_approval_address(tool_data: Dict[str, Any]) -> Optional[str]:
    for step in _extract_step_list(tool_data):
        estimate = step.get("estimate") or {}
        approval = estimate.get("approvalAddress") or estimate.get("approvalAddress")
        if approval:
            return str(approval)
    return None


def _extract_from_token(step: Dict[str, Any]) -> Optional[str]:
    action = step.get("action") or {}
    token = action.get("fromToken") or {}
    address = token.get("address")
    return str(address) if address else None


def _extract_from_amount(step: Dict[str, Any]) -> Optional[int]:
    action = step.get("action") or {}
    raw = action.get("fromAmount")
    return safe_int(raw, 0) if raw is not None else None


def _extract_from_decimals(step: Dict[str, Any]) -> Optional[int]:
    action = step.get("action") or {}
    token = action.get("fromToken") or {}
    dec = token.get("decimals")
    if dec is None:
        return None
    return safe_int(dec, 0)


def _extract_bridge_name(tool_data: Dict[str, Any]) -> Optional[str]:
    bridge = tool_data.get("bridge")
    if bridge:
        return str(bridge)
    for step in _extract_step_list(tool_data):
        tool_details = step.get("toolDetails") or {}
        name = tool_details.get("name") or tool_details.get("key")
        if name:
            return str(name)
    return None


def _extract_chain_ids(tool_data: Dict[str, Any]) -> tuple[Optional[int], Optional[int]]:
    for step in _extract_step_list(tool_data):
        action = step.get("action") or {}
        from_chain_id = action.get("fromChainId")
        to_chain_id = action.get("toChainId")
        if from_chain_id or to_chain_id:
            return (
                safe_int(from_chain_id) if from_chain_id else None,
                safe_int(to_chain_id) if to_chain_id else None,
            )
    return None, None


def _is_hex_data(data: str) -> bool:
    if not isinstance(data, str):
        return False
    if not data.startswith("0x"):
        return False
    try:
        int(data[2:] or "0", 16)
        return True
    except Exception:
        return False


async def _execute_solana(
    *,
    tx_request: Dict[str, Any],
    sub_org_id: str,
    sender: str,
    solana_network: Optional[str],
) -> str:
    data_b64 = tx_request.get("data")
    if not isinstance(data_b64, str) or not data_b64.strip():
        raise NonRetryableError(
            "The LiFi Solana route is missing its base64 transaction payload. "
            "Please request a fresh quote."
        )
    try:
        signed = await sign_solana_transaction(sub_org_id, data_b64, sign_with=sender)
        signature = await send_solana_transaction_async(
            signed, network=solana_network
        )
        return signature
    except Exception as exc:
        raise RuntimeError(
            "We couldn't submit the Solana transaction. "
            "Please try again in a moment."
        ) from exc


async def _maybe_approve(
    *,
    w3: Any,
    sub_org_id: str,
    sender: str,
    token_address: Optional[str],
    approval_address: Optional[str],
    amount_raw: Optional[int],
    chain_id: int,
) -> Optional[str]:
    if not token_address or not approval_address:
        return None
    if token_address.strip().lower() == _ZERO:
        return None
    if amount_raw is None or amount_raw <= 0:
        return None

    checksum_sender = w3.to_checksum_address(sender)
    checksum_token = w3.to_checksum_address(token_address)
    checksum_spender = w3.to_checksum_address(approval_address)

    token_contract = w3.eth.contract(address=checksum_token, abi=ERC20_ABI)
    try:
        allowance = await async_get_allowance(
            w3, checksum_token, checksum_sender, checksum_spender
        )
    except Exception:
        allowance = 0

    if allowance >= amount_raw:
        return None

    _LOGGER.info("[lifi] approval needed for token=%s", token_address[:10])

    gas_price = await async_get_gas_price(w3, chain_id=chain_id)
    max_fee, max_priority = to_eip1559_fees(gas_price)
    nonce_manager = await get_async_nonce_manager()
    nonce = await nonce_manager.allocate_safe(checksum_sender, chain_id, w3)

    approve_data: str = token_contract.encode_abi(
        fn_name="approve",
        args=[checksum_spender, 2**256 - 1],
    )

    approve_tx = {
        "to": checksum_token,
        "data": approve_data,
        "value": 0,
        "nonce": nonce,
        "gas": 100_000,
        "maxFeePerGas": max_fee,
        "maxPriorityFeePerGas": max_priority,
        "chainId": chain_id,
        "type": "0x2",
    }

    signed = await sign_transaction_async(sub_org_id, approve_tx, checksum_sender)
    try:
        approve_hash = await async_broadcast_evm(w3, signed)
    except Exception as exc:
        await reset_on_error_async(exc, checksum_sender, chain_id, w3)
        raise RuntimeError(
            "We couldn't broadcast the approval transaction. "
            "Please try again in a moment."
        ) from exc
    await async_await_evm_receipt(w3, approve_hash, timeout=180)
    return approve_hash


async def execute_lifi_bridge(
    *,
    route_meta: Dict[str, Any],
    token_symbol: str,
    source_chain_id: int,
    dest_chain_id: int,
    source_chain_name: str,
    dest_chain_name: str,
    input_amount: Decimal,
    output_amount: Decimal,
    sub_org_id: str,
    sender: str,
    recipient: str,
    solana_network: Optional[str] = None,
) -> LiFiBridgeResult:
    tool_data = route_meta.get("tool_data") or {}
    tx_request = tool_data.get("transactionRequest") or {}

    if not isinstance(tx_request, dict):
        raise NonRetryableError(
            "The LiFi quote is missing a valid transactionRequest object. "
            "Please request a fresh quote."
        )

    to_addr = tx_request.get("to")
    raw_data = tx_request.get("data")
    calldata = raw_data or "0x"
    value_raw = tx_request.get("value") or "0x0"
    gas_limit_raw = tx_request.get("gasLimit") or tx_request.get("gas")
    chain_id_raw = tx_request.get("chainId") or source_chain_id
    tool_from_chain_id, tool_to_chain_id = _extract_chain_ids(tool_data)

    if tool_from_chain_id is not None and tool_from_chain_id != source_chain_id:
        raise NonRetryableError(
            "The LiFi quote source chain does not match the request "
            f"(quote={tool_from_chain_id}, requested={source_chain_id}). "
            "Please request a fresh quote."
        )
    if tool_to_chain_id is not None and tool_to_chain_id != dest_chain_id:
        raise NonRetryableError(
            "The LiFi quote destination chain does not match the request "
            f"(quote={tool_to_chain_id}, requested={dest_chain_id}). "
            "Please request a fresh quote."
        )

    if (not to_addr) and isinstance(calldata, str) and not _is_hex_data(calldata):
        # Solana route: transactionRequest contains only base64 data.
        tx_hash = await _execute_solana(
            tx_request=tx_request,
            sub_org_id=sub_org_id,
            sender=sender,
            solana_network=solana_network,
        )
        from_chain_id, to_chain_id = _extract_chain_ids(tool_data)
        bridge_name = _extract_bridge_name(tool_data)
        return LiFiBridgeResult(
            tx_hash=tx_hash,
            approve_hash=None,
            protocol="lifi",
            token_symbol=token_symbol,
            input_amount=input_amount,
            output_amount=output_amount,
            source_chain_name=source_chain_name,
            dest_chain_name=dest_chain_name,
            recipient=recipient,
            tx_payload=tx_request,
            bridge=bridge_name,
            from_chain_id=from_chain_id,
            to_chain_id=to_chain_id,
        )

    if not to_addr and (raw_data is None or str(raw_data).strip() == ""):
        raise NonRetryableError(
            "The LiFi quote is missing executable transaction data: "
            "neither transactionRequest.to (EVM) nor transactionRequest.data (Solana) is present. "
            "Please request a fresh quote."
        )

    if not to_addr:
        raise NonRetryableError(
            "The LiFi EVM transaction is missing transactionRequest.to (destination contract). "
            "Please request a fresh quote."
        )

    chain_id = safe_int(chain_id_raw, source_chain_id)
    if chain_id != source_chain_id:
        raise NonRetryableError(
            "The LiFi transaction targets the wrong source chain "
            f"(tx={chain_id}, requested={source_chain_id}). "
            "Please request a fresh quote."
        )
    value_wei = safe_int(value_raw)
    gas_limit = safe_int(gas_limit_raw, _GAS_FALLBACK)

    chain = get_chain_by_id(chain_id)
    try:
        w3 = make_async_web3(chain.rpc_url)
    except RuntimeError as exc:
        raise RuntimeError(
            "Async EVM client is not available. Please try again later."
        ) from exc
    checksum_sender = w3.to_checksum_address(sender)
    checksum_to = w3.to_checksum_address(str(to_addr))

    # Best-effort approval if Li.Fi gave us the details.
    approve_hash: Optional[str] = None
    approval_address = _extract_approval_address(tool_data)
    steps = _extract_step_list(tool_data)
    step0 = steps[0] if steps else {}
    from_token = _extract_from_token(step0)
    from_amount_raw = _extract_from_amount(step0)
    if from_amount_raw is None:
        dec = _extract_from_decimals(step0)
        if dec and dec > 0:
            try:
                from_amount_raw = int(input_amount * Decimal(10**dec))
            except Exception:
                from_amount_raw = None

    if approval_address and from_token and from_amount_raw:
        approve_hash = await _maybe_approve(
            w3=w3,
            sub_org_id=sub_org_id,
            sender=sender,
            token_address=from_token,
            approval_address=approval_address,
            amount_raw=from_amount_raw,
            chain_id=chain_id,
        )

    # Estimate gas if Li.Fi didn't provide it.
    if not gas_limit_raw:
        try:
            estimate = await w3.eth.estimate_gas(
                {
                    "from": checksum_sender,
                    "to": checksum_to,
                    "value": value_wei,
                    "data": calldata,
                }
            )
            gas_limit = int(estimate * _GAS_BUFFER)
        except Exception:
            gas_limit = _GAS_FALLBACK

    gas_price = await async_get_gas_price(w3, chain_id=chain_id)
    max_fee, max_priority = to_eip1559_fees(gas_price)
    nonce_manager = await get_async_nonce_manager()
    nonce = await nonce_manager.allocate_safe(checksum_sender, chain_id, w3)

    bridge_tx = {
        "to": checksum_to,
        "data": calldata,
        "value": value_wei,
        "nonce": nonce,
        "gas": gas_limit,
        "maxFeePerGas": max_fee,
        "maxPriorityFeePerGas": max_priority,
        "chainId": chain_id,
        "type": "0x2",
    }

    signed_tx = await sign_transaction_async(sub_org_id, bridge_tx, checksum_sender)
    try:
        tx_hash = await async_broadcast_evm(w3, signed_tx)
    except Exception as exc:
        await reset_on_error_async(exc, checksum_sender, chain_id, w3)
        raise RuntimeError(
            "We couldn't broadcast the bridge transaction. "
            "Please try again in a moment."
        ) from exc

    await async_await_evm_receipt(w3, tx_hash, timeout=240)

    bridge_name = _extract_bridge_name(tool_data)
    from_chain_id, to_chain_id = _extract_chain_ids(tool_data)
    _LOGGER.info(
        "[lifi] bridge submitted tx=%s bridge=%s chain=%s",
        tx_hash[:16],
        bridge_name or "unknown",
        chain.name,
    )

    return LiFiBridgeResult(
        tx_hash=tx_hash,
        approve_hash=approve_hash,
        protocol="lifi",
        token_symbol=token_symbol,
        input_amount=input_amount,
        output_amount=output_amount,
        source_chain_name=source_chain_name,
        dest_chain_name=dest_chain_name,
        recipient=recipient,
        nonce=nonce,
        tx_payload=tx_request,
        bridge=bridge_name,
        from_chain_id=from_chain_id,
        to_chain_id=to_chain_id,
    )
