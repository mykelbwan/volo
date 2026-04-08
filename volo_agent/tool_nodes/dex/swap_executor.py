from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Awaitable, Callable, Union, cast

from web3 import Web3
from web3.exceptions import ContractLogicError
from web3.types import TxParams

from config.abi import (
    ERC20_ABI,
    UNISWAP_V2_ROUTER_ABI,
    UNISWAP_V3_ROUTER_ABI,
    WETH_MINIMAL_ABI,
)
from config.chains import get_chain_by_id, get_chain_by_name
from core.chain_capabilities import get_router_capabilities, set_router_capabilities
from core.utils.errors import NonRetryableError
from core.utils.evm_async import (
    async_await_evm_receipt,
    async_broadcast_evm,
    async_get_allowance,
    make_async_web3,
)
from core.utils.web3_helpers import encode_contract_call
from tool_nodes.bridge.executors.evm_utils import to_raw
from tool_nodes.dex.swap_simulator_v2 import SwapQuoteV2
from tool_nodes.dex.swap_simulator_v3 import SwapQuoteV3
from wallet_service.common.wallet_lock import wallet_lock
from wallet_service.evm.gas_price import to_eip1559_fees
from wallet_service.evm.nonce_manager import (
    AsyncUpstashNonceManager,
    get_async_nonce_manager,
    reset_on_error_async,
)
from wallet_service.evm.sign_tx import sign_transaction_async

_DEADLINE_BUFFER_SECONDS = 20 * 60
# Use max uint256 as the approval amount so the user only ever needs to approve once.
_MAX_UINT256 = 2**256 - 1
_WRAP_GAS_FALLBACK = 200_000
_WRAP_GAS_BUFFER = 1.2
_APPROVE_GAS_FALLBACK = 100_000
_APPROVE_GAS_BUFFER = 1.2
_SWAP_GAS_BUFFER = 1.2
_SWAP_GAS_FALLBACK_EXTRA = 50_000
# Keep 1inch disabled until API/KYC is ready.
_PRECOMPUTED_SWAP_ALLOWLIST = frozenset({"0x", "paraswap"})
_NATIVE = "0x0000000000000000000000000000000000000000"


@dataclass
class SwapResult:
    tx_hash: str
    approve_hash: str | None
    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out_minimum: Decimal
    chain_id: int
    chain_name: str
    protocol: str
    router: str
    wrap_hash: str | None = None
    unwrap_hash: str | None = None


def _deadline() -> int:
    return int(time.time()) + _DEADLINE_BUFFER_SECONDS


def _is_native(address: str, chain) -> bool:
    _ = chain
    return address.lower() == _NATIVE


def _is_wrapped_native(address: str, chain) -> bool:
    wrapped = getattr(chain, "wrapped_native", None)
    return bool(wrapped) and address.lower() == str(wrapped).lower()


def _supports_native_swaps(chain, override: bool | None = None) -> bool:
    if override is not None:
        return bool(override)
    return bool(getattr(chain, "supports_native_swaps", True))


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    raw = str(value).strip()
    if not raw:
        return default
    try:
        return int(raw, 0)
    except Exception:
        try:
            return int(raw)
        except Exception:
            return default


def _safe_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


async def _resolve_erc20_decimals(w3: Any, token_address: str) -> int:
    try:
        token_contract = w3.eth.contract(
            address=w3.to_checksum_address(token_address),
            abi=ERC20_ABI,
        )
        decimals = await token_contract.functions.decimals().call()
        return int(decimals)
    except Exception:
        return 18


def _normalize_execution_state(
    execution_state: dict[str, Any] | None,
) -> dict[str, Any]:
    state = execution_state if isinstance(execution_state, dict) else {}
    steps = state.get("steps")
    metadata = state.get("metadata")
    return {
        "current_step": str(state.get("current_step") or "").strip() or None,
        "completion_status": str(state.get("completion_status") or "pending").strip()
        or "pending",
        "steps": dict(steps) if isinstance(steps, dict) else {},
        "metadata": dict(metadata) if isinstance(metadata, dict) else {},
    }


def _copy_execution_state(execution_state: dict[str, Any]) -> dict[str, Any]:
    return {
        "current_step": execution_state.get("current_step"),
        "completion_status": execution_state.get("completion_status"),
        "steps": {
            step: dict(payload)
            for step, payload in dict(execution_state.get("steps") or {}).items()
            if isinstance(payload, dict)
        },
        "metadata": dict(execution_state.get("metadata") or {}),
    }


def _swap_step_fingerprint(
    *,
    quote: SwapQuoteV2 | SwapQuoteV3,
    protocol: str,
    router_address: str,
    sender: str,
) -> str:
    payload = {
        "step": "swap",
        "sender": str(sender).strip().lower(),
        "protocol": str(protocol),
        "router_address": str(router_address).strip().lower(),
        "chain_id": int(quote.chain_id),
        "token_in": str(quote.token_in).strip().lower(),
        "token_out": str(quote.token_out).strip().lower(),
        "amount_in": str(quote.amount_in),
        "amount_out_minimum": str(quote.amount_out_minimum),
        "path": [
            str(item).strip().lower() for item in list(getattr(quote, "path", []) or [])
        ],
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _get_recorded_step(
    execution_state: dict[str, Any],
    step_name: str,
) -> dict[str, Any]:
    steps = execution_state.get("steps")
    if not isinstance(steps, dict):
        return {}
    step = steps.get(step_name)
    return dict(step) if isinstance(step, dict) else {}


def _legacy_claim_matches_swap_tx(
    execution_state: dict[str, Any],
    tx_hash: str | None,
) -> bool:
    if not tx_hash:
        return False
    metadata = execution_state.get("metadata")
    if not isinstance(metadata, dict):
        return False
    legacy_claim_tx_hash = str(metadata.get("legacy_claim_tx_hash") or "").strip()
    return bool(legacy_claim_tx_hash and legacy_claim_tx_hash == tx_hash)


async def _persist_execution_state_snapshot(
    execution_state: dict[str, Any],
    persist_execution_state: Callable[[dict[str, Any]], Awaitable[None]] | None,
) -> None:
    if persist_execution_state is None:
        return
    await persist_execution_state(_copy_execution_state(execution_state))


async def _record_step_state(
    execution_state: dict[str, Any],
    *,
    step_name: str,
    status: str,
    tx_hash: str | None = None,
    fingerprint: str | None = None,
    persist_execution_state: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
) -> None:
    steps = execution_state.setdefault("steps", {})
    assert isinstance(steps, dict)
    steps[step_name] = {
        "status": status,
        "tx_hash": str(tx_hash or "").strip() or None,
    }
    if fingerprint is not None:
        steps[step_name]["fingerprint"] = str(fingerprint)
    execution_state["current_step"] = step_name
    execution_state["completion_status"] = "pending"
    await _persist_execution_state_snapshot(execution_state, persist_execution_state)


async def _build_approve_tx(
    w3: Any,
    token_address: str,
    spender: str,
    owner: str,
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
    try:
        gas_estimate = await w3.eth.estimate_gas(
            cast(
                TxParams,
                {
                    "from": w3.to_checksum_address(owner),
                    "to": w3.to_checksum_address(token_address),
                    "value": 0,
                    "data": data,
                },
            )
        )
        gas_limit = int(gas_estimate * _APPROVE_GAS_BUFFER)
    except Exception:
        gas_limit = _APPROVE_GAS_FALLBACK
    return {
        "to": w3.to_checksum_address(token_address),
        "value": 0,
        "data": data,
        "nonce": nonce,
        "gas": gas_limit,
        "maxFeePerGas": max_fee_per_gas,
        "maxPriorityFeePerGas": max_priority_fee_per_gas,
        "type": "0x2",
        "chainId": chain_id,
    }


async def _build_wrap_tx(
    w3: Any,
    wrapped_address: str,
    sender: str,
    amount_raw: int,
    nonce: int,
    max_fee_per_gas: int,
    max_priority_fee_per_gas: int,
    chain_id: int,
) -> dict:
    contract = w3.eth.contract(
        address=w3.to_checksum_address(wrapped_address),
        abi=WETH_MINIMAL_ABI,
    )
    data = encode_contract_call(contract, "deposit", [])
    try:
        gas_estimate = await w3.eth.estimate_gas(
            cast(
                TxParams,
                {
                    "from": sender,
                    "to": wrapped_address,
                    "value": amount_raw,
                    "data": data,
                },
            )
        )
        gas_limit = int(gas_estimate * _WRAP_GAS_BUFFER)
    except Exception:
        gas_limit = _WRAP_GAS_FALLBACK
    return {
        "to": w3.to_checksum_address(wrapped_address),
        "value": amount_raw,
        "data": data,
        "nonce": nonce,
        "gas": gas_limit,
        "maxFeePerGas": max_fee_per_gas,
        "maxPriorityFeePerGas": max_priority_fee_per_gas,
        "type": "0x2",
        "chainId": chain_id,
    }


async def _build_unwrap_tx(
    w3: Any,
    wrapped_address: str,
    sender: str,
    amount_raw: int,
    nonce: int,
    max_fee_per_gas: int,
    max_priority_fee_per_gas: int,
    chain_id: int,
) -> dict:
    contract = w3.eth.contract(
        address=w3.to_checksum_address(wrapped_address),
        abi=WETH_MINIMAL_ABI,
    )
    data = encode_contract_call(contract, "withdraw", [amount_raw])
    try:
        gas_estimate = await w3.eth.estimate_gas(
            cast(
                TxParams,
                {
                    "from": sender,
                    "to": wrapped_address,
                    "value": 0,
                    "data": data,
                },
            )
        )
        gas_limit = int(gas_estimate * _WRAP_GAS_BUFFER)
    except Exception:
        gas_limit = _WRAP_GAS_FALLBACK
    return {
        "to": w3.to_checksum_address(wrapped_address),
        "value": 0,
        "data": data,
        "nonce": nonce,
        "gas": gas_limit,
        "maxFeePerGas": max_fee_per_gas,
        "maxPriorityFeePerGas": max_priority_fee_per_gas,
        "type": "0x2",
        "chainId": chain_id,
    }


async def _maybe_approve(
    w3: Any,
    quote: SwapQuoteV2 | SwapQuoteV3,
    router_address: str,
    sub_org_id: str,
    sender: str,
    chain,
    nonce_manager: AsyncUpstashNonceManager,
    max_fee_per_gas: int | None = None,
    gas_price: int | None = None,
    supports_native: bool | None = None,
    wallet_execution_lock: Any | None = None,
    persist_step_submission: Callable[[str], Awaitable[None]] | None = None,
) -> str | None:
    native_in = _is_native(quote.token_in, chain)
    supports_native = _supports_native_swaps(chain, supports_native)

    if getattr(quote, "needs_approval", None) is False:
        return None

    # If native swaps are supported and the input is native, no approval needed.
    if native_in and supports_native:
        return None

    token_for_allowance = (
        chain.wrapped_native if (native_in and not supports_native) else quote.token_in
    )
    if not token_for_allowance:
        return None

    amount_in_raw = to_raw(quote.amount_in, quote.decimals_in)

    try:
        allowance = await async_get_allowance(
            w3,
            token_for_allowance,
            sender,
            router_address,
        )
        needs_approval = allowance < amount_in_raw
    except Exception:
        needs_approval = True

    if not needs_approval:
        return None

    effective_max_fee = max_fee_per_gas if max_fee_per_gas is not None else gas_price
    if effective_max_fee is None:
        raise ValueError("max_fee_per_gas or gas_price must be provided for approvals")

    nonce = await nonce_manager.allocate_safe(sender, chain.chain_id, w3)
    unsigned_approve = await _build_approve_tx(
        w3=w3,
        token_address=token_for_allowance,
        spender=router_address,
        owner=sender,
        nonce=nonce,
        max_fee_per_gas=effective_max_fee,
        max_priority_fee_per_gas=effective_max_fee,
        chain_id=quote.chain_id,
    )
    signed_approve = await sign_transaction_async(sub_org_id, unsigned_approve, sender)
    if wallet_execution_lock is not None:
        await wallet_execution_lock.ensure_held()
    try:
        approve_hash = await async_broadcast_evm(w3, signed_approve)
    except Exception as exc:
        await reset_on_error_async(exc, sender, quote.chain_id, w3)
        raise RuntimeError(
            "We couldn't broadcast the approval transaction. "
            "Please try again in a moment."
        ) from exc
    if persist_step_submission is not None:
        await persist_step_submission(approve_hash)

    # Wait for the approval to be mined before proceeding with the swap.
    await async_await_evm_receipt(w3, approve_hash, timeout=120)
    try:
        allowance_after = await async_get_allowance(
            w3,
            token_for_allowance,
            sender,
            router_address,
        )
    except Exception:
        allowance_after = 0
    if allowance_after < amount_in_raw:
        if getattr(quote, "needs_approval", None) is True:
            return approve_hash
        raise NonRetryableError(
            "Approval mined but allowance still insufficient "
            f"(needed {amount_in_raw}, got {allowance_after}). "
            f"Approval tx {approve_hash}"
        )

    return approve_hash


async def _build_v2_swap_tx(
    w3: Any,
    quote: SwapQuoteV2,
    router_address: str,
    sender: str,
    nonce: int,
    max_fee_per_gas: int,
    max_priority_fee_per_gas: int,
    chain,
    supports_native: bool | None = None,
) -> dict:
    router = w3.eth.contract(
        address=w3.to_checksum_address(router_address),
        abi=UNISWAP_V2_ROUTER_ABI,
    )

    supports_native = _supports_native_swaps(chain, supports_native)
    native_in = _is_native(quote.token_in, chain) and supports_native
    native_out = _is_native(quote.token_out, chain) and supports_native
    deadline = _deadline()

    decimals_in = quote.decimals_in
    decimals_out = quote.decimals_out

    amount_in_raw = to_raw(quote.amount_in, decimals_in)
    amount_out_min_raw = to_raw(quote.amount_out_minimum, decimals_out)
    checksum_sender = w3.to_checksum_address(sender)

    if native_in:
        data = encode_contract_call(
            router,
            "swapExactETHForTokens",
            [amount_out_min_raw, quote.path, checksum_sender, deadline],
        )
        value = amount_in_raw
    elif native_out:
        data = encode_contract_call(
            router,
            "swapExactTokensForETH",
            [
                amount_in_raw,
                amount_out_min_raw,
                quote.path,
                checksum_sender,
                deadline,
            ],
        )
        value = 0
    else:
        data = encode_contract_call(
            router,
            "swapExactTokensForTokens",
            [
                amount_in_raw,
                amount_out_min_raw,
                quote.path,
                checksum_sender,
                deadline,
            ],
        )
        value = 0

    # Try to estimate gas for the exact tx and apply a buffer.
    try:
        estimate = await w3.eth.estimate_gas(
            cast(
                TxParams,
                {
                    "from": checksum_sender,
                    "to": w3.to_checksum_address(router_address),
                    "value": value,
                    "data": data,
                },
            )
        )
        gas_limit = int(max(estimate, quote.gas_estimate) * _SWAP_GAS_BUFFER)
    except Exception:
        gas_limit = quote.gas_estimate + _SWAP_GAS_FALLBACK_EXTRA

    return {
        "to": w3.to_checksum_address(router_address),
        "value": value,
        "data": data,
        "nonce": nonce,
        "gas": gas_limit,
        "maxFeePerGas": max_fee_per_gas,
        "maxPriorityFeePerGas": max_priority_fee_per_gas,
        "type": "0x2",
        "chainId": quote.chain_id,
    }


def _encode_v3_path(path: list[str], fee_tiers: list[int]) -> bytes:
    result = bytes.fromhex(path[0][2:])
    for fee, token in zip(fee_tiers, path[1:]):
        result += fee.to_bytes(3, byteorder="big")
        result += bytes.fromhex(token[2:])
    return result


async def _build_v3_swap_tx(
    w3: Any,
    quote: SwapQuoteV3,
    router_address: str,
    sender: str,
    nonce: int,
    max_fee_per_gas: int,
    max_priority_fee_per_gas: int,
    chain,
    supports_native: bool | None = None,
) -> dict:
    router = w3.eth.contract(
        address=w3.to_checksum_address(router_address),
        abi=UNISWAP_V3_ROUTER_ABI,
    )

    supports_native = _supports_native_swaps(chain, supports_native)
    native_in = _is_native(quote.token_in, chain) and supports_native
    checksum_sender = w3.to_checksum_address(sender)

    token_in_addr = quote.path[0]
    token_out_addr = quote.path[-1]

    decimals_in = quote.decimals_in
    decimals_out = quote.decimals_out

    amount_in_raw = to_raw(quote.amount_in, decimals_in)
    amount_out_min_raw = to_raw(quote.amount_out_minimum, decimals_out)
    value = amount_in_raw if native_in else 0

    if quote.route == "single-hop":
        fee_tier = quote.fee_tiers[0]
        data = encode_contract_call(
            router,
            "exactInputSingle",
            [
                (
                    w3.to_checksum_address(token_in_addr),  
                    w3.to_checksum_address(token_out_addr),  
                    fee_tier,  
                    checksum_sender,  
                    amount_in_raw,  
                    amount_out_min_raw,  
                    0,  # sqrtPriceLimitX96 (no limit)
                )
            ],
        )
    else:
        # Multi-hop: encode the full path bytes and call exactInput
        encoded_path = _encode_v3_path(quote.path, quote.fee_tiers)
        data = encode_contract_call(
            router,
            "exactInput",
            [
                (
                    encoded_path,  # path
                    checksum_sender,  # recipient
                    amount_in_raw,  # amountIn
                    amount_out_min_raw,  # amountOutMinimum
                )
            ],
        )

    # Try to estimate gas for the exact tx and apply a buffer.
    try:
        estimate = await w3.eth.estimate_gas(
            cast(
                TxParams,
                {
                    "from": checksum_sender,
                    "to": w3.to_checksum_address(router_address),
                    "value": value,
                    "data": data,
                },
            )
        )
        gas_limit = int(max(estimate, quote.gas_estimate) * _SWAP_GAS_BUFFER)
    except Exception:
        gas_limit = quote.gas_estimate + _SWAP_GAS_FALLBACK_EXTRA

    return {
        "to": w3.to_checksum_address(router_address),
        "value": value,
        "data": data,
        "nonce": nonce,
        "gas": gas_limit,
        "maxFeePerGas": max_fee_per_gas,
        "maxPriorityFeePerGas": max_priority_fee_per_gas,
        "type": "0x2",
        "chainId": quote.chain_id,
    }


async def _build_probe_tx(
    w3: Any,
    quote: SwapQuoteV2 | SwapQuoteV3,
    router_address: str,
    sender: str,
    nonce: int,
    max_fee_per_gas: int,
    max_priority_fee_per_gas: int,
    chain,
) -> dict:
    probe_quote = replace(quote, amount_out_minimum=Decimal("0"))
    if isinstance(probe_quote, SwapQuoteV3):
        return await _build_v3_swap_tx(
            w3=w3,
            quote=probe_quote,
            router_address=router_address,
            sender=sender,
            nonce=nonce,
            max_fee_per_gas=max_fee_per_gas,
            max_priority_fee_per_gas=max_priority_fee_per_gas,
            chain=chain,
            supports_native=True,
        )
    return await _build_v2_swap_tx(
        w3=w3,
        quote=probe_quote,
        router_address=router_address,
        sender=sender,
        nonce=nonce,
        max_fee_per_gas=max_fee_per_gas,
        max_priority_fee_per_gas=max_priority_fee_per_gas,
        chain=chain,
        supports_native=True,
    )


def _should_probe_native_support(last_checked: str | None) -> bool:
    if not last_checked:
        return True
    try:
        ts = datetime.fromisoformat(last_checked.replace("Z", ""))
    except Exception:
        return True
    return datetime.utcnow() - ts > timedelta(hours=24)


async def _probe_native_swap_support(
    w3: Any,
    quote: SwapQuoteV2 | SwapQuoteV3,
    router_address: str,
    sender: str,
    nonce: int,
    max_fee_per_gas: int,
    max_priority_fee_per_gas: int,
    chain,
) -> bool:
    try:
        unsigned = await _build_probe_tx(
            w3=w3,
            quote=quote,
            router_address=router_address,
            sender=sender,
            nonce=nonce,
            max_fee_per_gas=max_fee_per_gas,
            max_priority_fee_per_gas=max_priority_fee_per_gas,
            chain=chain,
        )
        call_tx = cast(
            TxParams,
            {
                "from": sender,
                "to": unsigned["to"],
                "value": unsigned.get("value", 0),
                "data": unsigned["data"],
                "gas": unsigned["gas"],
            },
        )
        await w3.eth.call(call_tx)
        return True
    except (ContractLogicError, ValueError):
        return False


async def execute_precomputed_swap_route(
    *,
    route_meta: dict[str, Any],
    amount_in: Decimal,
    chain_name: str,
    sub_org_id: str,
    sender: str,
    gas_price_gwei: float = 1.0,
    persist_broadcast: Callable[[SwapResult], Awaitable[None]] | None = None,
) -> SwapResult:
    if not isinstance(route_meta, dict) or not route_meta:
        raise NonRetryableError("Missing planned swap route metadata.")

    aggregator = str(route_meta.get("aggregator") or "").strip().lower()
    if aggregator not in _PRECOMPUTED_SWAP_ALLOWLIST:
        raise NonRetryableError(
            f"Planned {aggregator or 'unknown'} swap routes are not executable."
        )

    calldata = str(route_meta.get("calldata") or "").strip()
    to_address = str(route_meta.get("to") or "").strip()
    if not calldata or not calldata.startswith("0x") or not to_address:
        raise NonRetryableError(
            "Planned swap route metadata is missing executable calldata."
        )

    chain = get_chain_by_name(chain_name)
    route_chain_id = route_meta.get("chain_id")
    if route_chain_id is not None and int(route_chain_id) != int(chain.chain_id):
        raise NonRetryableError(
            "Planned swap route metadata targets a different chain."
        )

    route_amount_in = route_meta.get("amount_in")
    if route_amount_in is not None and _safe_decimal(route_amount_in) != amount_in:
        raise NonRetryableError(
            "Planned swap route metadata does not match the requested amount."
        )

    token_in = str(route_meta.get("token_in") or "").strip() or _NATIVE
    token_out = str(route_meta.get("token_out") or "").strip() or _NATIVE
    amount_out_minimum = _safe_decimal(
        route_meta.get("amount_out_min"),
        _safe_decimal(
            route_meta.get("min_output"),
            _safe_decimal(
                route_meta.get("expected_output"),
                _safe_decimal(route_meta.get("amount_out"), Decimal("0")),
            ),
        ),
    )

    execution_meta = route_meta.get("execution")
    execution = execution_meta if isinstance(execution_meta, dict) else {}
    value_wei = _safe_int(route_meta.get("value"), _safe_int(execution.get("value"), 0))

    w3 = make_async_web3(chain.rpc_url)
    checksum_sender = w3.to_checksum_address(sender)
    checksum_to = w3.to_checksum_address(to_address)

    gas_price = Web3.to_wei(gas_price_gwei, "gwei")
    max_fee_per_gas, max_priority_fee = to_eip1559_fees(gas_price)

    async with wallet_lock(checksum_sender, chain.chain_id) as execution_lock:
        nonce_manager = await get_async_nonce_manager()

        approve_hash: str | None = None
        approval_address = str(route_meta.get("approval_address") or "").strip()
        if approval_address and token_in.lower() != _NATIVE:
            decimals_in = _safe_int(route_meta.get("token_in_decimals"), 0)
            if decimals_in <= 0:
                decimals_in = _safe_int(execution.get("decimals_in"), 0)
            if decimals_in <= 0:
                decimals_in = await _resolve_erc20_decimals(w3, token_in)
            precomputed_quote = cast(
                Any,
                type(
                    "_PrecomputedQuote",
                    (),
                    {
                        "token_in": token_in,
                        "amount_in": amount_in,
                        "decimals_in": decimals_in,
                    },
                )(),
            )
            approve_hash = await _maybe_approve(
                w3=w3,
                quote=precomputed_quote,
                router_address=approval_address,
                sub_org_id=sub_org_id,
                sender=checksum_sender,
                chain=chain,
                nonce_manager=nonce_manager,
                max_fee_per_gas=max_fee_per_gas,
                gas_price=gas_price,
                supports_native=_supports_native_swaps(chain),
                wallet_execution_lock=execution_lock,
                persist_step_submission=None,
            )

        nonce = await nonce_manager.allocate_safe(checksum_sender, chain.chain_id, w3)
        route_gas_estimate = _safe_int(route_meta.get("gas_estimate"), 0)
        if route_gas_estimate > 0:
            gas_limit = (
                int(route_gas_estimate * _SWAP_GAS_BUFFER) + _SWAP_GAS_FALLBACK_EXTRA
            )
        else:
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
                gas_limit = int(estimate * _SWAP_GAS_BUFFER)
            except Exception:
                gas_limit = 350_000

        unsigned_tx: TxParams = {
            "to": checksum_to,
            "data": calldata,
            "value": value_wei,  # type: ignore[typeddict-item]
            "nonce": nonce,
            "gas": gas_limit,
            "maxFeePerGas": max_fee_per_gas,
            "maxPriorityFeePerGas": max_priority_fee,
            "chainId": chain.chain_id,
            "type": "0x2",
        }
        signed_tx = await sign_transaction_async(
            sub_org_id, unsigned_tx, checksum_sender
        )

        await execution_lock.ensure_held()
        try:
            tx_hash = await async_broadcast_evm(w3, signed_tx)
        except Exception as exc:
            await reset_on_error_async(exc, checksum_sender, chain.chain_id, w3)
            raise RuntimeError(
                "We couldn't broadcast the planned swap transaction. "
                "Please try again in a moment."
            ) from exc

        result = SwapResult(
            tx_hash=tx_hash,
            approve_hash=approve_hash,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out_minimum=amount_out_minimum,
            chain_id=chain.chain_id,
            chain_name=chain.name,
            protocol=aggregator,
            router=checksum_to,
        )
        if persist_broadcast is not None:
            await persist_broadcast(result)
        await async_await_evm_receipt(w3, tx_hash, timeout=120)
        return result


async def execute_swap(
    quote: Union[SwapQuoteV2, SwapQuoteV3],
    sub_org_id: str,
    sender: str,
    gas_price_gwei: float = 1.0,
    persist_broadcast: Callable[[SwapResult], Awaitable[None]] | None = None,
    execution_state: dict[str, Any] | None = None,
    persist_execution_state: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
) -> SwapResult:
    chain = get_chain_by_id(quote.chain_id)
    w3 = make_async_web3(chain.rpc_url)
    supports_native = _supports_native_swaps(chain)

    # Determine protocol and router address
    if isinstance(quote, SwapQuoteV3):
        protocol = "v3"
        router_address = chain.v3_router
        if not router_address:
            raise ValueError(f"Chain {chain.name!r} has no V3 router configured.")
    else:
        protocol = "v2"
        router_address = chain.v2_router
        if not router_address:
            raise ValueError(f"Chain {chain.name!r} has no V2 router configured.")

    caps = get_router_capabilities(
        chain.chain_id,
        protocol,
        router_address,
        getattr(chain, "supports_native_swaps", True),
    )
    supports_native = caps.supports_native_swaps

    checksum_sender = w3.to_checksum_address(sender)
    gas_price = Web3.to_wei(gas_price_gwei, "gwei")
    max_fee_per_gas, max_priority_fee = to_eip1559_fees(gas_price)
    ledger = _normalize_execution_state(execution_state)

    async with wallet_lock(checksum_sender, quote.chain_id) as execution_lock:
        nonce_manager = await get_async_nonce_manager()
        pending_nonce = await nonce_manager.pending(checksum_sender, quote.chain_id, w3)

        native_in = _is_native(quote.token_in, chain)
        native_out = _is_native(quote.token_out, chain)
        wrapped_out = _is_wrapped_native(quote.token_out, chain)
        should_unwrap_output = native_out or wrapped_out

        if (
            supports_native
            and (native_in or native_out)
            and _should_probe_native_support(caps.last_checked)
        ):
            probe_ok = await _probe_native_swap_support(
                w3=w3,
                quote=quote,
                router_address=router_address,
                sender=checksum_sender,
                nonce=pending_nonce,
                max_fee_per_gas=max_fee_per_gas,
                max_priority_fee_per_gas=max_priority_fee,
                chain=chain,
            )
            if not probe_ok:
                set_router_capabilities(
                    chain.chain_id,
                    protocol,
                    router_address,
                    supports_native_swaps=False,
                )
                supports_native = False
            else:
                set_router_capabilities(
                    chain.chain_id,
                    protocol,
                    router_address,
                    supports_native_swaps=True,
                )

        wrap_hash = None
        unwrap_hash = None
        wrapped_balance_before = None

        # The wallet lock spans wrap/approve/swap/unwrap so nothing else can
        # interleave new nonces or mutate allowance/balance mid-route.
        if native_in and not supports_native:
            if not chain.wrapped_native:
                raise ValueError(
                    f"Chain {chain.name!r} has no wrapped native token configured."
                )
            amount_in_raw = to_raw(quote.amount_in, quote.decimals_in)
            wrapped_contract = w3.eth.contract(
                address=w3.to_checksum_address(chain.wrapped_native),
                abi=ERC20_ABI,
            )
            wrapped_balance = await wrapped_contract.functions.balanceOf(
                checksum_sender
            ).call()
            wrap_amount = max(0, amount_in_raw - wrapped_balance)
            if wrap_amount > 0:
                wrap_step = _get_recorded_step(ledger, "wrap")
                wrap_hash = str(wrap_step.get("tx_hash") or "").strip() or None
                wrap_status = str(wrap_step.get("status") or "").strip().lower()
                if wrap_hash and wrap_status == "submitted":
                    await async_await_evm_receipt(w3, wrap_hash, timeout=120)
                    await _record_step_state(
                        ledger,
                        step_name="wrap",
                        status="completed",
                        tx_hash=wrap_hash,
                        persist_execution_state=persist_execution_state,
                    )
                elif not (wrap_hash and wrap_status == "completed"):
                    nonce = await nonce_manager.allocate_safe(
                        checksum_sender, quote.chain_id, w3
                    )
                    unsigned_wrap = await _build_wrap_tx(
                        w3=w3,
                        wrapped_address=chain.wrapped_native,
                        sender=checksum_sender,
                        amount_raw=wrap_amount,
                        nonce=nonce,
                        max_fee_per_gas=max_fee_per_gas,
                        max_priority_fee_per_gas=max_priority_fee,
                        chain_id=quote.chain_id,
                    )
                    signed_wrap = await sign_transaction_async(
                        sub_org_id, unsigned_wrap, checksum_sender
                    )
                    await execution_lock.ensure_held()
                    try:
                        wrap_hash = await async_broadcast_evm(w3, signed_wrap)
                    except Exception as exc:
                        await reset_on_error_async(
                            exc, checksum_sender, quote.chain_id, w3
                        )
                        raise RuntimeError(
                            "We couldn't broadcast the wrap transaction. "
                            "Please try again in a moment."
                        ) from exc
                    await _record_step_state(
                        ledger,
                        step_name="wrap",
                        status="submitted",
                        tx_hash=wrap_hash,
                        persist_execution_state=persist_execution_state,
                    )
                    await async_await_evm_receipt(w3, wrap_hash, timeout=120)
                    await _record_step_state(
                        ledger,
                        step_name="wrap",
                        status="completed",
                        tx_hash=wrap_hash,
                        persist_execution_state=persist_execution_state,
                    )

        if should_unwrap_output:
            if not chain.wrapped_native:
                raise ValueError(
                    f"Chain {chain.name!r} has no wrapped native token configured."
                )
            wrapped_contract = w3.eth.contract(
                address=w3.to_checksum_address(chain.wrapped_native),
                abi=ERC20_ABI,
            )
            metadata = ledger.setdefault("metadata", {})
            assert isinstance(metadata, dict)
            if "wrapped_balance_before" in metadata:
                wrapped_balance_before = int(metadata["wrapped_balance_before"])
            else:
                wrapped_balance_before = await wrapped_contract.functions.balanceOf(
                    checksum_sender
                ).call()
                metadata["wrapped_balance_before"] = int(wrapped_balance_before)
                await _persist_execution_state_snapshot(ledger, persist_execution_state)

        approve_step = _get_recorded_step(ledger, "approve")
        approve_hash = str(approve_step.get("tx_hash") or "").strip() or None
        approve_status = str(approve_step.get("status") or "").strip().lower()
        if approve_hash and approve_status == "submitted":
            await async_await_evm_receipt(w3, approve_hash, timeout=120)
            await _record_step_state(
                ledger,
                step_name="approve",
                status="completed",
                tx_hash=approve_hash,
                persist_execution_state=persist_execution_state,
            )
        elif not (approve_hash and approve_status == "completed"):
            approve_hash = await _maybe_approve(
                w3=w3,
                quote=quote,
                router_address=router_address,
                sub_org_id=sub_org_id,
                sender=checksum_sender,
                max_fee_per_gas=max_fee_per_gas,
                chain=chain,
                nonce_manager=nonce_manager,
                supports_native=supports_native,
                wallet_execution_lock=execution_lock,
                persist_step_submission=lambda tx_hash: _record_step_state(
                    ledger,
                    step_name="approve",
                    status="submitted",
                    tx_hash=tx_hash,
                    persist_execution_state=persist_execution_state,
                ),
            )
            if approve_hash:
                await _record_step_state(
                    ledger,
                    step_name="approve",
                    status="completed",
                    tx_hash=approve_hash,
                    persist_execution_state=persist_execution_state,
                )

        swap_step = _get_recorded_step(ledger, "swap")
        tx_hash = str(swap_step.get("tx_hash") or "").strip() or None
        swap_status = str(swap_step.get("status") or "").strip().lower()
        swap_fingerprint = _swap_step_fingerprint(
            quote=quote,
            protocol=protocol,
            router_address=router_address,
            sender=checksum_sender,
        )
        if tx_hash and swap_status in {"submitted", "completed"}:
            recorded_fingerprint = str(swap_step.get("fingerprint") or "").strip()
            if recorded_fingerprint != swap_fingerprint:
                if not recorded_fingerprint and _legacy_claim_matches_swap_tx(
                    ledger, tx_hash
                ):
                    await _record_step_state(
                        ledger,
                        step_name="swap",
                        status=swap_status,
                        tx_hash=tx_hash,
                        fingerprint=swap_fingerprint,
                        persist_execution_state=persist_execution_state,
                    )
                else:
                    raise NonRetryableError(
                        "Tampered swap execution state detected. Recovery: clear the stored swap state and request a fresh quote."
                    )
        if tx_hash and swap_status == "submitted":
            result = SwapResult(
                tx_hash=tx_hash,
                approve_hash=approve_hash,
                token_in=quote.token_in,
                token_out=quote.token_out,
                amount_in=quote.amount_in,
                amount_out_minimum=quote.amount_out_minimum,
                chain_id=quote.chain_id,
                chain_name=quote.chain_name,
                protocol=protocol,
                router=router_address,
                wrap_hash=wrap_hash,
                unwrap_hash=unwrap_hash,
            )
            await async_await_evm_receipt(w3, tx_hash, timeout=120)
            await _record_step_state(
                ledger,
                step_name="swap",
                status="completed",
                tx_hash=tx_hash,
                fingerprint=swap_fingerprint,
                persist_execution_state=persist_execution_state,
            )
        elif not (tx_hash and swap_status == "completed"):
            nonce = await nonce_manager.allocate_safe(
                checksum_sender, quote.chain_id, w3
            )
            if isinstance(quote, SwapQuoteV3):
                unsigned_tx = await _build_v3_swap_tx(
                    w3=w3,
                    quote=quote,
                    router_address=router_address,
                    sender=checksum_sender,
                    nonce=nonce,
                    max_fee_per_gas=max_fee_per_gas,
                    max_priority_fee_per_gas=max_priority_fee,
                    chain=chain,
                    supports_native=supports_native,
                )
            else:
                unsigned_tx = await _build_v2_swap_tx(
                    w3=w3,
                    quote=quote,
                    router_address=router_address,
                    sender=checksum_sender,
                    nonce=nonce,
                    max_fee_per_gas=max_fee_per_gas,
                    max_priority_fee_per_gas=max_priority_fee,
                    chain=chain,
                    supports_native=supports_native,
                )

            signed_tx = await sign_transaction_async(
                sub_org_id, unsigned_tx, checksum_sender
            )

            await execution_lock.ensure_held()
            try:
                tx_hash = await async_broadcast_evm(w3, signed_tx)
            except Exception as exc:
                await reset_on_error_async(exc, checksum_sender, quote.chain_id, w3)
                raise RuntimeError(
                    "We couldn't broadcast the swap transaction. "
                    "Please try again in a moment."
                ) from exc
            await _record_step_state(
                ledger,
                step_name="swap",
                status="submitted",
                tx_hash=tx_hash,
                fingerprint=swap_fingerprint,
                persist_execution_state=persist_execution_state,
            )

            result = SwapResult(
                tx_hash=tx_hash,
                approve_hash=approve_hash,
                token_in=quote.token_in,
                token_out=quote.token_out,
                amount_in=quote.amount_in,
                amount_out_minimum=quote.amount_out_minimum,
                chain_id=quote.chain_id,
                chain_name=quote.chain_name,
                protocol=protocol,
                router=router_address,
                wrap_hash=wrap_hash,
                unwrap_hash=unwrap_hash,
            )
            if persist_broadcast is not None:
                await persist_broadcast(result)
            await async_await_evm_receipt(w3, tx_hash, timeout=120)
            await _record_step_state(
                ledger,
                step_name="swap",
                status="completed",
                tx_hash=tx_hash,
                fingerprint=swap_fingerprint,
                persist_execution_state=persist_execution_state,
            )

        result = SwapResult(
            tx_hash=tx_hash,
            approve_hash=approve_hash,
            token_in=quote.token_in,
            token_out=quote.token_out,
            amount_in=quote.amount_in,
            amount_out_minimum=quote.amount_out_minimum,
            chain_id=quote.chain_id,
            chain_name=quote.chain_name,
            protocol=protocol,
            router=router_address,
            wrap_hash=wrap_hash,
            unwrap_hash=unwrap_hash,
        )

        if should_unwrap_output and wrapped_balance_before is not None:
            wrapped_contract = w3.eth.contract(
                address=w3.to_checksum_address(chain.wrapped_native),
                abi=ERC20_ABI,
            )
            wrapped_balance_after = await wrapped_contract.functions.balanceOf(
                checksum_sender
            ).call()
            delta = max(0, wrapped_balance_after - wrapped_balance_before)
            if delta > 0:
                unwrap_step = _get_recorded_step(ledger, "unwrap")
                unwrap_hash = str(unwrap_step.get("tx_hash") or "").strip() or None
                unwrap_status = str(unwrap_step.get("status") or "").strip().lower()
                if unwrap_hash and unwrap_status == "submitted":
                    await async_await_evm_receipt(w3, unwrap_hash, timeout=120)
                    await _record_step_state(
                        ledger,
                        step_name="unwrap",
                        status="completed",
                        tx_hash=unwrap_hash,
                        persist_execution_state=persist_execution_state,
                    )
                    result = replace(result, unwrap_hash=unwrap_hash)
                elif not (unwrap_hash and unwrap_status == "completed"):
                    nonce = await nonce_manager.allocate_safe(
                        checksum_sender, quote.chain_id, w3
                    )
                    unsigned_unwrap = await _build_unwrap_tx(
                        w3=w3,
                        wrapped_address=chain.wrapped_native,
                        sender=checksum_sender,
                        amount_raw=delta,
                        nonce=nonce,
                        max_fee_per_gas=max_fee_per_gas,
                        max_priority_fee_per_gas=max_priority_fee,
                        chain_id=quote.chain_id,
                    )
                    signed_unwrap = await sign_transaction_async(
                        sub_org_id, unsigned_unwrap, checksum_sender
                    )
                    await execution_lock.ensure_held()
                    try:
                        unwrap_hash = await async_broadcast_evm(w3, signed_unwrap)
                    except Exception as exc:
                        await reset_on_error_async(
                            exc, checksum_sender, quote.chain_id, w3
                        )
                        raise RuntimeError(
                            "We couldn't broadcast the unwrap transaction. "
                            "Please try again in a moment."
                        ) from exc
                    await _record_step_state(
                        ledger,
                        step_name="unwrap",
                        status="submitted",
                        tx_hash=unwrap_hash,
                        persist_execution_state=persist_execution_state,
                    )
                    await async_await_evm_receipt(w3, unwrap_hash, timeout=120)
                    await _record_step_state(
                        ledger,
                        step_name="unwrap",
                        status="completed",
                        tx_hash=unwrap_hash,
                        persist_execution_state=persist_execution_state,
                    )
                    result = replace(result, unwrap_hash=unwrap_hash)

        return result
