from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from config.bridge_registry import RELAY, BridgeProtocolConfig
from config.chains import get_chain_by_id
from core.utils.errors import NonRetryableError
from core.utils.http import (
    ExternalServiceError,
    async_raise_for_status,
    async_request_json,
)
from core.utils.evm_async import (
    async_await_evm_receipt,
    async_broadcast_evm,
    async_get_gas_price,
    make_async_web3,
)
from tool_nodes.bridge.simulators.relay_simulator import RelayBridgeQuote
from wallet_service.evm.nonce_manager import (
    get_async_nonce_manager,
    reset_on_error_async,
)
from wallet_service.evm.sign_tx import sign_transaction_async

_STATUS_ENDPOINT = "/intents/status/v3"

_GAS_FALLBACK = 300_000
_GAS_BUFFER = 1.2
_STATUS_POLL_INTERVAL_SECONDS = 4.0
_STATUS_TIMEOUT_SECONDS = 240.0

_TERMINAL_STATUSES = {"success", "failure", "refund", "refunded"}
_SUCCESS_STATUSES = {"success"}
_FAILURE_STATUSES = {"failure", "refund", "refunded"}

_LOGGER = logging.getLogger("volo.bridge")


@dataclass
class RelayBridgeResult:
    protocol: str
    tx_hash: str
    tx_hashes: list[str]
    request_id: Optional[str]
    token_symbol: str
    input_amount: Decimal
    output_amount: Decimal
    source_chain_name: str
    dest_chain_name: str
    recipient: str
    status: Optional[str]
    relay_status: Optional[str]
    nonce: Optional[int] = None
    raw_tx: Optional[str] = None
    tx_payload: Optional[dict] = None


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.startswith("0x"):
            return int(s, 16)
        return int(s)
    except Exception:
        return None


async def _fetch_intent_status(
    protocol: BridgeProtocolConfig,
    base_url: str,
    request_id: Optional[str],
) -> Optional[str]:
    if not request_id:
        return None
    headers = None
    if protocol.api_key:
        headers = {"x-api-key": protocol.api_key}
    response = await async_request_json(
        "GET",
        f"{base_url}{_STATUS_ENDPOINT}",
        params={"requestId": request_id},
        headers=headers,
        service="relay",
    )
    try:
        await async_raise_for_status(response, "relay")
    except ExternalServiceError:
        return None
    try:
        data = response.json()
    except Exception:
        return None
    return data.get("status") or data.get("data", {}).get("status")


def _normalize_status(status: Optional[str]) -> Optional[str]:
    if status is None:
        return None
    return str(status).strip().lower()


def _extract_request_id(steps: list[dict[str, Any]]) -> Optional[str]:
    for step in steps:
        request_id = step.get("requestId")
        if request_id:
            return str(request_id)
        check = step.get("check") or {}
        if isinstance(check, dict):
            request_id = check.get("requestId")
            if request_id:
                return str(request_id)
            endpoint = check.get("endpoint") or check.get("url")
            if endpoint:
                try:
                    query = parse_qs(urlparse(endpoint).query)
                    rid = query.get("requestId") or query.get("requestid")
                    if rid:
                        return str(rid[0])
                except Exception:
                    pass
    return None


async def _poll_intent_status(
    protocol: BridgeProtocolConfig,
    base_url: str,
    request_id: Optional[str],
    *,
    timeout_seconds: float = _STATUS_TIMEOUT_SECONDS,
    poll_interval: float = _STATUS_POLL_INTERVAL_SECONDS,
) -> Optional[str]:
    if not request_id:
        return None

    deadline = time.time() + timeout_seconds
    last_status: Optional[str] = None

    while time.time() < deadline:
        status = _normalize_status(
            await _fetch_intent_status(protocol, base_url, request_id)
        )
        if status and status != last_status:
            _LOGGER.info("relay_status request_id=%s status=%s", request_id, status)
            last_status = status
        if status in _TERMINAL_STATUSES:
            return status
        await asyncio.sleep(poll_interval)

    return last_status


async def execute_relay_bridge(
    quote: RelayBridgeQuote,
    sub_org_id: str,
    sender: str,
    recipient: str,
    protocol: BridgeProtocolConfig = RELAY,
    timeout: float = 120.0,
    status_timeout: float = _STATUS_TIMEOUT_SECONDS,
) -> RelayBridgeResult:
    if not quote.steps:
        raise RuntimeError("Relay quote did not include executable steps.")

    tx_hashes: list[str] = []
    w3_cache: Dict[int, Any] = {}
    nonce_manager = await get_async_nonce_manager()

    last_nonce: Optional[int] = None
    last_signed_tx: Optional[str] = None
    last_payload: Optional[dict] = None
    for step in quote.steps:
        items = step.get("items") or []
        for item in items:
            data = item.get("data") or {}
            if not isinstance(data, dict):
                continue

            chain_id = _as_int(
                data.get("chainId") or step.get("chainId") or quote.source_chain_id
            )
            if chain_id is None:
                continue

            chain = get_chain_by_id(chain_id)
            w3 = w3_cache.get(chain_id)
            if w3 is None:
                try:
                    w3 = make_async_web3(chain.rpc_url)
                except RuntimeError as exc:
                    raise RuntimeError(
                        "Async EVM client is not available. Please try again later."
                    ) from exc
                w3_cache[chain_id] = w3

            sender_addr = data.get("from") or sender
            checksum_sender = w3.to_checksum_address(sender_addr)

            nonce = await nonce_manager.allocate_safe(checksum_sender, chain_id, w3)
            last_nonce = nonce

            to_addr = data.get("to")
            if not to_addr:
                continue

            tx: Dict[str, Any] = {
                "to": w3.to_checksum_address(to_addr),
                "data": data.get("data") or "0x",
                "value": _as_int(data.get("value")) or 0,
                "nonce": nonce,
                "chainId": chain_id,
                "from": checksum_sender,
            }

            gas = _as_int(data.get("gas") or data.get("gasLimit"))
            if gas is None:
                try:
                    estimate = await w3.eth.estimate_gas(
                        {
                            "from": checksum_sender,
                            "to": tx["to"],
                            "value": tx["value"],
                            "data": tx["data"],
                        }
                    )
                    gas = int(estimate * _GAS_BUFFER)
                except Exception:
                    gas = _GAS_FALLBACK
            tx["gas"] = gas

            max_fee = _as_int(data.get("maxFeePerGas"))
            max_priority = _as_int(data.get("maxPriorityFeePerGas"))
            gas_price = _as_int(data.get("gasPrice"))

            if max_fee is not None and max_priority is not None:
                tx["maxFeePerGas"] = max_fee
                tx["maxPriorityFeePerGas"] = max_priority
            elif gas_price is not None:
                tx["maxFeePerGas"] = gas_price
                tx["maxPriorityFeePerGas"] = gas_price
            else:
                try:
                    fallback = await async_get_gas_price(w3, chain_id=chain_id)
                    tx["maxFeePerGas"] = fallback
                    tx["maxPriorityFeePerGas"] = fallback
                except Exception:
                    pass

            if "maxFeePerGas" in tx and "maxPriorityFeePerGas" in tx:
                tx["type"] = "0x2"

            tx_to_sign = dict(tx)
            tx_to_sign.pop("from", None)
            signed_tx = await sign_transaction_async(
                sub_org_id, tx_to_sign, checksum_sender
            )
            last_signed_tx = signed_tx
            last_payload = tx_to_sign
            try:
                tx_hash = await async_broadcast_evm(w3, signed_tx)
            except Exception as exc:
                await reset_on_error_async(exc, checksum_sender, chain_id, w3)
                raise RuntimeError(
                    "We couldn't broadcast the bridge transaction. "
                    "Please try again in a moment."
                ) from exc
            try:
                await async_await_evm_receipt(w3, tx_hash, timeout=int(timeout))
            except NonRetryableError as exc:
                if "pending beyond" in str(exc).lower():
                    await nonce_manager.reset(checksum_sender, chain_id, w3)
                raise
            tx_hashes.append(tx_hash)

    if not tx_hashes:
        raise RuntimeError("Relay quote did not produce executable transactions.")

    request_id = quote.request_id or _extract_request_id(quote.steps)
    if request_id:
        _LOGGER.info("relay_request request_id=%s txs=%d", request_id, len(tx_hashes))
    relay_status = "pending"

    return RelayBridgeResult(
        protocol="relay",
        tx_hash=tx_hashes[-1],
        tx_hashes=tx_hashes,
        request_id=request_id,
        token_symbol=quote.token_symbol,
        input_amount=quote.input_amount,
        output_amount=quote.output_amount,
        source_chain_name=quote.source_chain_name,
        dest_chain_name=quote.dest_chain_name,
        recipient=recipient,
        status=relay_status,
        relay_status=relay_status,
        nonce=last_nonce,
        raw_tx=last_signed_tx,
        tx_payload=last_payload,
    )
