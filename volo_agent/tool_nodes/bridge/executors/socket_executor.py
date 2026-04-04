from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

from config.abi import ERC20_ABI
from config.chains import get_chain_by_id
from core.utils.errors import NonRetryableError
from core.utils.evm_async import (
    async_await_evm_receipt,
    async_broadcast_evm,
    async_get_allowance,
    async_get_gas_price,
    make_async_web3,
)
from tool_nodes.bridge.executors.evm_utils import safe_int
from wallet_service.evm.gas_price import to_eip1559_fees
from wallet_service.evm.nonce_manager import (
    get_async_nonce_manager,
    reset_on_error_async,
)
from wallet_service.evm.sign_tx import sign_transaction_async

_LOGGER = logging.getLogger("volo.bridge.socket")

_ZERO = "0x0000000000000000000000000000000000000000"
_NATIVE_SENTINEL = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeeeE"
_GAS_FALLBACK = 300_000
_GAS_BUFFER = 1.2


@dataclass
class SocketBridgeResult:
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


def _is_native_token(address: Optional[str]) -> bool:
    if not address:
        return False
    addr = address.strip().lower()
    return addr in {_ZERO.lower(), _NATIVE_SENTINEL.lower()}


def _extract_bridge_name(route: Dict[str, Any]) -> Optional[str]:
    used = route.get("usedBridgeNames")
    if isinstance(used, list) and used:
        return str(used[0])
    return None


def _extract_chain_ids(route: Dict[str, Any]) -> tuple[Optional[int], Optional[int]]:
    from_chain_id = route.get("fromChainId")
    to_chain_id = route.get("toChainId")
    return (
        safe_int(from_chain_id) if from_chain_id is not None else None,
        safe_int(to_chain_id) if to_chain_id is not None else None,
    )


def _extract_approval_data(
    build_tx: Dict[str, Any], route: Dict[str, Any]
) -> Dict[str, Any]:
    approval = build_tx.get("approvalData")
    if isinstance(approval, dict):
        return approval
    approval = route.get("approvalData")
    if isinstance(approval, dict):
        return approval

    user_txs = route.get("userTxs") or []
    for tx in user_txs if isinstance(user_txs, list) else []:
        if not isinstance(tx, dict):
            continue
        tx_type = str(tx.get("userTxType") or tx.get("txType") or "").lower()
        if tx_type in {"approve", "approval"}:
            data = tx.get("approvalData")
            if isinstance(data, dict):
                return data
            return tx
    return {}


async def _maybe_approve(
    *,
    w3: Any,
    sub_org_id: str,
    sender: str,
    approval: Dict[str, Any],
    chain_id: int,
) -> Optional[str]:
    token_address = approval.get("tokenAddress") or approval.get("token")
    spender = (
        approval.get("spender")
        or approval.get("allowanceTarget")
        or approval.get("approvalAddress")
    )
    amount_raw = approval.get("amount")

    if not token_address or not spender:
        return None
    if _is_native_token(token_address):
        return None

    checksum_sender = w3.to_checksum_address(sender)
    checksum_token = w3.to_checksum_address(str(token_address))
    checksum_spender = w3.to_checksum_address(str(spender))
    token_contract = w3.eth.contract(address=checksum_token, abi=ERC20_ABI)

    try:
        allowance = await async_get_allowance(
            w3, checksum_token, checksum_sender, checksum_spender
        )
    except Exception:
        allowance = 0

    amount = safe_int(amount_raw, 0)
    if amount <= 0:
        # Use max approval if amount is missing.
        amount = 2**256 - 1

    if allowance >= amount:
        return None

    gas_price = await async_get_gas_price(w3, chain_id=chain_id)
    max_fee, max_priority = to_eip1559_fees(gas_price)
    nonce_manager = await get_async_nonce_manager()
    nonce = await nonce_manager.allocate_safe(checksum_sender, chain_id, w3)

    approve_data: str = token_contract.encode_abi(
        fn_name="approve",
        args=[checksum_spender, amount],
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


async def execute_socket_bridge(
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
) -> SocketBridgeResult:
    tool_data = route_meta.get("tool_data") or {}
    build_tx = tool_data.get("buildTxResult") or {}
    route = tool_data.get("route") or {}

    if not isinstance(build_tx, dict):
        raise NonRetryableError(
            "The bridge quote is missing the transaction data. Please try again."
        )
    if not isinstance(route, dict):
        route = {}

    to_addr = build_tx.get("txTarget")
    calldata = build_tx.get("txData") or "0x"
    value_raw = build_tx.get("value") or "0x0"
    gas_limit_raw = build_tx.get("gasLimit") or build_tx.get("gas")
    chain_id_raw = build_tx.get("chainId") or source_chain_id
    route_from_chain_id, route_to_chain_id = _extract_chain_ids(route)

    if route_from_chain_id is not None and route_from_chain_id != source_chain_id:
        raise NonRetryableError(
            "The bridge quote chain does not match the requested source chain. Please request a fresh quote."
        )
    if route_to_chain_id is not None and route_to_chain_id != dest_chain_id:
        raise NonRetryableError(
            "The bridge quote destination chain does not match the request. Please request a fresh quote."
        )

    if not to_addr:
        raise NonRetryableError(
            "The bridge quote is missing a destination contract. Please try again."
        )

    chain_id = safe_int(chain_id_raw, source_chain_id)
    if chain_id != source_chain_id:
        raise NonRetryableError(
            "The bridge transaction targets the wrong source chain. Please request a fresh quote."
        )
    value_wei = safe_int(value_raw)
    gas_limit = safe_int(gas_limit_raw, _GAS_FALLBACK)

    chain = get_chain_by_id(chain_id)
    w3 = make_async_web3(chain.rpc_url)
    checksum_sender = w3.to_checksum_address(sender)
    checksum_to = w3.to_checksum_address(str(to_addr))

    approve_hash: Optional[str] = None
    approval_data = _extract_approval_data(build_tx, route)
    if isinstance(approval_data, dict) and approval_data:
        approve_hash = await _maybe_approve(
            w3=w3,
            sub_org_id=sub_org_id,
            sender=sender,
            approval=approval_data,
            chain_id=chain_id,
        )

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

    bridge_name = _extract_bridge_name(route)
    from_chain_id, to_chain_id = _extract_chain_ids(route)
    _LOGGER.info(
        "[socket] bridge submitted tx=%s bridge=%s chain=%s",
        tx_hash[:16],
        bridge_name or "unknown",
        chain.name,
    )

    return SocketBridgeResult(
        tx_hash=tx_hash,
        approve_hash=approve_hash,
        protocol="socket",
        token_symbol=token_symbol,
        input_amount=input_amount,
        output_amount=output_amount,
        source_chain_name=source_chain_name,
        dest_chain_name=dest_chain_name,
        recipient=recipient,
        nonce=nonce,
        tx_payload=build_tx,
        bridge=bridge_name,
        from_chain_id=from_chain_id,
        to_chain_id=to_chain_id,
    )
