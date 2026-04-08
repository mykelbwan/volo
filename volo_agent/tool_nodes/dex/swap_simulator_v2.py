from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

from web3 import Web3
from web3.exceptions import ContractLogicError

from config.abi import UNISWAP_V2_FACTORY_ABI, UNISWAP_V2_ROUTER_ABI
from config.chains import ChainConfig
from core.chain_capabilities import get_router_capabilities
from tool_nodes.common.input_utils import safe_decimal
from tool_nodes.dex.swap_simulator_common import (
    NATIVE_TOKEN_ADDRESS,
    _get_allowance,
    _get_token_decimals,
    _get_web3,
    _is_native,
    _is_zero_native,
    _resolve_chain,
)


@dataclass
class SwapQuoteV2:
    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    amount_out_minimum: Decimal
    decimals_in: int
    decimals_out: int
    slippage_pct: Decimal
    price_impact_pct: Decimal
    path: list[str]
    gas_estimate: int
    needs_approval: bool
    allowance: int
    chain_id: int
    chain_name: str
    dex_name: str


@dataclass
class SimulationErrorV2:
    reason: str
    message: str


def _resolve_to_wrapped(address: str, chain: ChainConfig) -> str:
    if _is_zero_native(address):
        if not chain.wrapped_native:
            raise ValueError(
                f"Chain {chain.name!r} has no wrapped native token configured. "
                "Cannot simulate a swap involving the native token."
            )
        return chain.wrapped_native
    return address


async def _pair_exists(
    w3: Any, factory_address: str, token_a: str, token_b: str
) -> bool:
    factory = w3.eth.contract(
        address=w3.to_checksum_address(factory_address),
        abi=UNISWAP_V2_FACTORY_ABI,
    )
    pair_address = await factory.functions.getPair(
        w3.to_checksum_address(token_a),
        w3.to_checksum_address(token_b),
    ).call()
    return pair_address != NATIVE_TOKEN_ADDRESS


async def _resolve_path(
    w3: Any,
    factory_address: str,
    token_in: str,
    token_out: str,
    wrapped_native: str,
) -> Optional[list[str]]:
    checksum_in = w3.to_checksum_address(token_in)
    checksum_out = w3.to_checksum_address(token_out)
    checksum_wrapped = (
        w3.to_checksum_address(wrapped_native) if wrapped_native else None
    )

    # Direct pair
    if await _pair_exists(w3, factory_address, checksum_in, checksum_out):
        return [checksum_in, checksum_out]

    # Route through wrapped native if it's not already one of the tokens
    if (
        checksum_wrapped
        and checksum_wrapped != checksum_in
        and checksum_wrapped != checksum_out
        and await _pair_exists(w3, factory_address, checksum_in, checksum_wrapped)
        and await _pair_exists(w3, factory_address, checksum_wrapped, checksum_out)
    ):
        return [checksum_in, checksum_wrapped, checksum_out]

    return None


async def _get_amounts_out(
    router,
    amount_in_raw: int,
    path: list[str],
) -> Optional[list[int]]:
    try:
        return await router.functions.getAmountsOut(amount_in_raw, path).call()
    except (ContractLogicError, Exception):
        return None


async def _estimate_gas(
    w3: Any,
    router_address: str,
    router,
    path: list[str],
    amount_in_raw: int,
    amount_out_min_raw: int,
    sender: str,
    native_in: bool,
    native_out: bool,
) -> int:
    deadline = 2**256 - 1  # far-future deadline for estimation purposes
    try:
        if native_in:
            return await w3.eth.estimate_gas(
                {
                    "from": sender,
                    "to": router_address,
                    "value": Web3.to_wei(amount_in_raw, "wei"),
                    "data": router.encodeABI(
                        fn_name="swapExactETHForTokens",
                        args=[amount_out_min_raw, path, sender, deadline],
                    ),
                }
            )
        elif native_out:
            return await w3.eth.estimate_gas(
                {
                    "from": sender,
                    "to": router_address,
                    "data": router.encodeABI(
                        fn_name="swapExactTokensForETH",
                        args=[
                            amount_in_raw,
                            amount_out_min_raw,
                            path,
                            sender,
                            deadline,
                        ],
                    ),
                }
            )
        else:
            return await w3.eth.estimate_gas(
                {
                    "from": sender,
                    "to": router_address,
                    "data": router.encodeABI(
                        fn_name="swapExactTokensForTokens",
                        args=[
                            amount_in_raw,
                            amount_out_min_raw,
                            path,
                            sender,
                            deadline,
                        ],
                    ),
                }
            )
    except Exception:
        # Gas estimation can fail if the sender has no balance for the simulation.
        # Return a standard V2 swap gas cost as a safe fallback.
        if len(path) == 2:
            return 130_000  # direct pair
        return 180_000  # one intermediate hop


async def simulate_swap_v2(
    token_in: str,
    token_out: str,
    amount_in: float | Decimal,
    sender: str,
    slippage_pct: float | Decimal = Decimal("0.5"),
    chain_id: Optional[int] = None,
    chain_name: Optional[str] = None,
    dex_name: str = "Uniswap V2",
) -> SwapQuoteV2 | SimulationErrorV2:
    chain = _resolve_chain(chain_id, chain_name)

    if not chain.v2_router:
        return SimulationErrorV2(
            reason="NO_ROUTER",
            message=(
                f"Chain {chain.name!r} does not have a Uniswap V2 Router configured."
            ),
        )

    if not chain.v2_factory:
        return SimulationErrorV2(
            reason="NO_FACTORY",
            message=(
                f"Chain {chain.name!r} does not have a Uniswap V2 Factory configured."
            ),
        )

    try:
        w3 = _get_web3(chain)
    except RuntimeError as exc:
        return SimulationErrorV2(
            reason="ASYNC_WEB3_UNAVAILABLE",
            message=str(exc),
        )

    try:
        checksum_sender = w3.to_checksum_address(sender)
    except Exception:
        raise ValueError(f"Invalid sender address: {sender!r}")

    token_in_raw = token_in.strip()
    token_out_raw = token_out.strip()

    native_in = _is_native(token_in_raw, chain)
    native_out = _is_native(token_out_raw, chain)
    supports_native_swaps = get_router_capabilities(
        chain.chain_id,
        "v2",
        chain.v2_router,
        getattr(chain, "supports_native_swaps", True),
    ).supports_native_swaps

    # Resolve to wrapped addresses for path-finding and quoting
    resolved_in = w3.to_checksum_address(_resolve_to_wrapped(token_in_raw, chain))
    resolved_out = w3.to_checksum_address(_resolve_to_wrapped(token_out_raw, chain))

    if resolved_in == resolved_out:
        return SimulationErrorV2(
            reason="SAME_TOKEN",
            message="token_in and token_out resolve to the same address.",
        )
    try:
        decimals_in = await _get_token_decimals(w3, resolved_in, chain.chain_id)
        decimals_out = await _get_token_decimals(w3, resolved_out, chain.chain_id)
    except Exception as e:
        return SimulationErrorV2(
            reason="DECIMALS_FETCH_FAILED",
            message=f"Could not fetch token decimals: {e}",
        )

    amount_in_decimal = safe_decimal(amount_in)
    if amount_in_decimal is None:
        return SimulationErrorV2(
            reason="INVALID_AMOUNT",
            message="amount_in must be a valid number.",
        )
    slippage_decimal = safe_decimal(slippage_pct)
    if slippage_decimal is None:
        return SimulationErrorV2(
            reason="INVALID_SLIPPAGE",
            message="slippage_pct must be a valid number.",
        )
    if slippage_decimal < 0 or slippage_decimal > 100:
        return SimulationErrorV2(
            reason="INVALID_SLIPPAGE",
            message="slippage_pct must be between 0 and 100.",
        )
    amount_in_raw = int(amount_in_decimal * Decimal(10**decimals_in))

    if amount_in_raw <= 0:
        return SimulationErrorV2(
            reason="ZERO_AMOUNT",
            message="amount_in must be greater than zero.",
        )

    path = await _resolve_path(
        w3,
        chain.v2_factory,
        resolved_in,
        resolved_out,
        chain.wrapped_native,
    )

    if path is None:
        return SimulationErrorV2(
            reason="NO_LIQUIDITY",
            message=(
                f"No liquidity pair found for {token_in} -> {token_out} "
                f"on {chain.name} ({dex_name}). "
                "Neither a direct pair nor a route through the wrapped native "
                "token exists on this DEX."
            ),
        )

    router = w3.eth.contract(
        address=w3.to_checksum_address(chain.v2_router),
        abi=UNISWAP_V2_ROUTER_ABI,
    )

    amounts = await _get_amounts_out(router, amount_in_raw, path)

    if amounts is None:
        return SimulationErrorV2(
            reason="QUOTE_FAILED",
            message=(
                f"getAmountsOut reverted for {token_in} -> {token_out} "
                f"on {chain.name} ({dex_name}). "
                "The pool may exist but have insufficient liquidity for this amount."
            ),
        )

    amount_out_raw = amounts[-1]
    amount_out_decimal = Decimal(amount_out_raw) / Decimal(10**decimals_out)
    slippage_multiplier = Decimal(1) - (slippage_decimal / Decimal(100))
    amount_out_minimum = amount_out_decimal * slippage_multiplier
    amount_out_minimum_raw = int(amount_out_minimum * Decimal(10**decimals_out))

    # Compare a 1-unit reference quote (scaled to the swap size) against the
    # actual output. The difference is the price impact from pool depth.
    price_impact_pct = Decimal(0)
    one_unit_raw = int(Decimal(10**decimals_in))
    if one_unit_raw > 0 and one_unit_raw != amount_in_raw:
        ref_amounts = await _get_amounts_out(router, one_unit_raw, path)
        if ref_amounts is not None:
            ref_out_per_unit = Decimal(ref_amounts[-1]) / Decimal(10**decimals_out)
            expected_out = ref_out_per_unit * amount_in_decimal
            if expected_out > 0:
                price_impact_pct = (
                    (expected_out - amount_out_decimal) / expected_out * Decimal(100)
                ).quantize(Decimal("0.0001"))

    gas_estimate = await _estimate_gas(
        w3=w3,
        router_address=chain.v2_router,
        router=router,
        path=path,
        amount_in_raw=amount_in_raw,
        amount_out_min_raw=amount_out_minimum_raw,
        sender=checksum_sender,
        native_in=native_in and supports_native_swaps,
        native_out=native_out and supports_native_swaps,
    )

    needs_approval = False
    current_allowance = 0

    # If the chain router does not support native swaps, we must treat native
    # inputs as wrapped ERC-20s for approval purposes.
    requires_erc20_approval = (not native_in) or (
        native_in and not supports_native_swaps
    )
    if requires_erc20_approval:
        try:
            current_allowance = await _get_allowance(
                w3, resolved_in, checksum_sender, chain.v2_router
            )
            needs_approval = current_allowance < amount_in_raw
        except Exception:
            # Non-fatal: allowance check failure doesn't invalidate the quote.
            needs_approval = True

    return SwapQuoteV2(
        token_in=token_in_raw,
        token_out=token_out_raw,
        amount_in=amount_in_decimal,
        amount_out=amount_out_decimal,
        amount_out_minimum=amount_out_minimum,
        decimals_in=decimals_in,
        decimals_out=decimals_out,
        slippage_pct=slippage_decimal,
        price_impact_pct=price_impact_pct,
        path=path,
        gas_estimate=gas_estimate,
        needs_approval=needs_approval,
        allowance=current_allowance,
        chain_id=chain.chain_id,
        chain_name=chain.name,
        dex_name=dex_name,
    )
