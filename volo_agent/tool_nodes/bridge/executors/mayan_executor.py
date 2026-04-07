from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, cast

from web3.types import TxParams

from core.routing.models import BridgeRouteQuote
from core.utils.errors import NonRetryableError
from core.utils.http import async_request_json

_LOGGER = logging.getLogger("volo.bridge.mayan")
_DEFAULT_SWAP_API = "https://swap-api.mayan.finance/v3"

# Mayan quote hashes are valid for ~2 minutes.
_QUOTE_MAX_AGE_SECONDS: float = 100.0

_GAS_FALLBACK: int = 300_000
_GAS_BUFFER: float = 1.2
_BROADCAST_TIMEOUT: int = 120

@dataclass
class MayanBridgeResult:
    tx_hash: str
    approve_hash: Optional[str]
    protocol: str
    route_type: str
    token_symbol: str
    input_amount: Decimal
    output_amount: Decimal
    source_chain_name: str
    dest_chain_name: str
    recipient: str
    status: str = "pending"


def _swap_api_base() -> str:
    import os

    return os.getenv("MAYAN_SWAP_API_URL", _DEFAULT_SWAP_API).rstrip("/")


async def _await_mayan_receipt(w3: Any, tx_hash: str) -> None:
    from core.utils.evm_async import async_await_evm_receipt

    await async_await_evm_receipt(w3, tx_hash, timeout=_BROADCAST_TIMEOUT)


async def _fetch_swap_transaction_async(
    quote_id: str,
    from_address: str,
    to_address: str,
    slippage: float,
    timeout: float,
) -> Dict[str, Any]:
    url = f"{_swap_api_base()}/swap/trx/"
    params = {
        "quoteId": quote_id,
        "fromAddress": from_address,
        "toAddress": to_address,
        "slippage": slippage,
    }

    try:
        resp = await async_request_json(
            "GET",
            url,
            params=params,
            timeout=timeout,
            service="mayan-swap-trx",
        )
    except Exception as exc:
        err = str(exc).lower()
        if "timeout" in err:
            raise RuntimeError(
                "Mayan took too long to respond. Please try again in a moment."
            )
        raise RuntimeError(f"Could not reach Mayan's bridge service: {exc}")

    if resp.status_code == 400:
        body = resp.text[:200]
        raise RuntimeError(
            f"The bridge quote is no longer valid — it may have expired. "
            f"Please try again. (detail: {body})"
        )

    if resp.status_code == 429:
        raise RuntimeError(
            "Mayan is busy right now. Please wait a moment and try again."
        )

    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(
            f"Mayan returned an unexpected error ({resp.status_code}). "
            "Please try again."
        )

    try:
        return resp.json()
    except Exception:
        raise RuntimeError("Mayan returned an unreadable response. Please try again.")


async def _execute_evm_to_solana(
    quote: BridgeRouteQuote,
    sub_org_id: str,
    sender: str,
    solana_recipient: str,
    slippage: float,
) -> MayanBridgeResult:
    from config.abi import ERC20_ABI
    from config.chains import get_chain_by_id
    from core.utils.evm_async import (
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

    tool_data: Dict[str, Any] = quote.tool_data or {}
    quote_id: str = str(tool_data.get("quoteId", "")).strip()
    route_type: str = tool_data.get("routeType", "WH")
    from_token: str = tool_data.get("fromToken", "")

    if not quote_id:
        raise NonRetryableError(
            "The bridge quote is missing required information. Please try again."
        )

    _LOGGER.info("[mayan] fetching EVM transaction for quoteId=%s", quote_id[:16])
    tx_data = await _fetch_swap_transaction_async(
        quote_id=quote_id,
        from_address=sender,
        to_address=solana_recipient,
        slippage=slippage,
        timeout=15.0,
    )

    to_addr = tx_data.get("to")
    calldata = tx_data.get("data") or "0x"
    value_raw = tx_data.get("value") or tx_data.get("amount") or "0x0"
    gas_limit_raw = tx_data.get("gasLimit") or tx_data.get("gas")
    chain_id_raw = tx_data.get("chainId") or quote.source_chain_id

    if not to_addr:
        raise RuntimeError(
            "Mayan did not return a valid transaction. Please try again."
        )

    chain_id = safe_int(chain_id_raw, quote.source_chain_id)
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
    checksum_to = w3.to_checksum_address(to_addr)

    nonce_manager = await get_async_nonce_manager()
    gas_price = await async_get_gas_price(w3, chain_id=chain_id)
    max_fee, max_priority = to_eip1559_fees(gas_price)

    _ZERO = "0x0000000000000000000000000000000000000000"
    approve_hash: Optional[str] = None
    is_native = from_token.lower() == _ZERO.lower() or not from_token

    if not is_native and from_token:
        token_contract = w3.eth.contract(
            address=w3.to_checksum_address(from_token),
            abi=ERC20_ABI,
        )
        amount_raw = int(quote.input_amount * Decimal(10**18))

        try:
            allowance = await async_get_allowance(
                w3, from_token, checksum_sender, checksum_to
            )
        except Exception:
            allowance = 0

        if allowance < amount_raw:
            _LOGGER.info("[mayan] ERC-20 approval needed for %s", from_token[:10])
            nonce = await nonce_manager.allocate_safe(checksum_sender, chain_id, w3)
            approve_data: str = token_contract.encode_abi(
                fn_name="approve",
                args=[checksum_to, 2**256 - 1],
            )
            approve_tx: TxParams = cast(
                TxParams,
                {
                    "to": w3.to_checksum_address(from_token),
                    "data": approve_data,
                    "value": 0,
                    "nonce": nonce,
                    "gas": 100_000,
                    "maxFeePerGas": max_fee,
                    "maxPriorityFeePerGas": max_priority,
                    "chainId": chain_id,
                    "type": "0x2",
                },
            )
            signed_approve = await sign_transaction_async(
                sub_org_id, approve_tx, checksum_sender
            )
            try:
                approve_hash = await async_broadcast_evm(w3, signed_approve)
            except Exception as exc:
                await reset_on_error_async(exc, checksum_sender, chain_id, w3)
                raise RuntimeError(
                    "We couldn't broadcast the approval transaction. "
                    "Please try again in a moment."
                ) from exc
            await _await_mayan_receipt(w3, approve_hash)

    if not gas_limit_raw:
        try:
            estimate = await w3.eth.estimate_gas(
                cast(
                    TxParams,
                    {
                        "from": checksum_sender,
                        "to": checksum_to,
                        "value": value_wei,
                        "data": calldata,
                    },
                )
            )
            gas_limit = int(estimate * _GAS_BUFFER)
        except Exception:
            gas_limit = _GAS_FALLBACK

    nonce = await nonce_manager.allocate_safe(checksum_sender, chain_id, w3)

    bridge_tx: TxParams = {
        "to": checksum_to,
        "data": calldata,
        "value": value_wei,  # type: ignore[typeddict-item]
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

    await _await_mayan_receipt(w3, tx_hash)

    _LOGGER.info(
        "[mayan] EVM→Solana bridge submitted  tx=%s  route=%s  chain=%s",
        tx_hash[:16],
        route_type,
        chain.name,
    )

    return MayanBridgeResult(
        tx_hash=tx_hash,
        approve_hash=approve_hash,
        protocol="mayan",
        route_type=route_type,
        token_symbol=quote.token_symbol,
        input_amount=quote.input_amount,
        output_amount=quote.output_amount,
        source_chain_name=quote.source_chain_name,
        dest_chain_name=quote.dest_chain_name,
        recipient=solana_recipient,
        status="pending",
    )

async def _execute_solana_to_evm(
    quote: BridgeRouteQuote,
    sub_org_id: str,
    solana_sender: str,
    evm_recipient: str,
    slippage: float,
) -> MayanBridgeResult:
    from wallet_service.solana.cdp_utils import send_solana_transaction_async
    from wallet_service.solana.sign_tx import sign_transaction_async as solana_sign

    tool_data: Dict[str, Any] = quote.tool_data or {}
    quote_id: str = str(tool_data.get("quoteId", "")).strip()
    route_type: str = tool_data.get("routeType", "WH")

    if not quote_id:
        raise NonRetryableError(
            "The bridge quote is missing required information. Please try again."
        )

    _LOGGER.info(
        "[mayan] fetching Solana transaction for quoteId=%s", quote_id[:16]
    )
    tx_data = await _fetch_swap_transaction_async(
        quote_id=quote_id,
        from_address=solana_sender,
        to_address=evm_recipient,
        slippage=slippage,
        timeout=15.0,
    )

    swap_transaction: Optional[str] = (
        tx_data.get("transaction")
        or tx_data.get("swapTransaction")
        or tx_data.get("tx")
    )

    if not swap_transaction:
        raise RuntimeError(
            "Mayan didn't return a valid Solana transaction. Please try again."
        )

    try:
        signed_tx = await solana_sign(
            sub_org_id, swap_transaction, sign_with=solana_sender
        )
        network = "solana"
        signature = await send_solana_transaction_async(signed_tx, network=network)
    except Exception as exc:
        raise RuntimeError(
            "We couldn't submit the Solana transaction. Please try again in a moment."
        ) from exc

    _LOGGER.info(
        "[mayan] Solana→EVM bridge submitted  sig=%s  route=%s",
        signature[:16],
        route_type,
    )

    return MayanBridgeResult(
        tx_hash=signature,
        approve_hash=None,
        protocol="mayan",
        route_type=route_type,
        token_symbol=quote.token_symbol,
        input_amount=quote.input_amount,
        output_amount=quote.output_amount,
        source_chain_name=quote.source_chain_name,
        dest_chain_name=quote.dest_chain_name,
        recipient=evm_recipient,
        status="pending",
    )

async def execute_mayan_bridge(
    quote: BridgeRouteQuote,
    sub_org_id: str,
    sender: str,
    recipient: str,
) -> MayanBridgeResult:
    tool_data: Dict[str, Any] = quote.tool_data or {}
    src_is_sol: bool = bool(tool_data.get("srcIsSolana", False))
    dst_is_sol: bool = bool(tool_data.get("dstIsSolana", False))
    slippage: float = float(tool_data.get("slippage", 0.01))

    # Validate quote freshness — Mayan quote hashes expire quickly.
    fetched_at: float = float(tool_data.get("rawRoute", {}).get("fetchedAt", 0) or 0)
    if fetched_at > 0:
        age = time.time() - fetched_at
        if age > _QUOTE_MAX_AGE_SECONDS:
            raise NonRetryableError(
                "The bridge quote expired before the transaction could be sent. "
                "Please try again — a new quote will be fetched automatically."
            )

    if not src_is_sol and not dst_is_sol:
        raise NonRetryableError(
            "Mayan only supports routes that involve Solana. "
            "Please use a different bridge for EVM ↔ EVM transfers."
        )

    if src_is_sol:
        # Solana → EVM: sender is Solana public key, recipient is EVM address.
        return await _execute_solana_to_evm(
            quote,
            sub_org_id,
            sender,
            recipient,
            slippage,
        )
    else:
        # EVM → Solana: sender is EVM address, recipient is Solana public key.
        return await _execute_evm_to_solana(
            quote,
            sub_org_id,
            sender,
            recipient,
            slippage,
        )
