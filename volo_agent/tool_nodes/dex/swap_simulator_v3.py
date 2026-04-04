from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from web3.exceptions import ContractLogicError

from config.abi import UNISWAP_V3_QUOTER_ABI
from config.chains import ChainConfig
from core.chain_capabilities import get_router_capabilities
from tool_nodes.common.input_utils import safe_decimal
from tool_nodes.dex.swap_simulator_common import (
    _get_allowance,
    _get_token_decimals,
    _get_web3,
    _is_native,
    _is_zero_native,
    _resolve_chain,
)

# ---------------------------------------------------------------------------
# Pool fee tiers supported by Uniswap V3 (in hundredths of a bip)
# 100   = 0.01%  — stable pairs (e.g. USDC/USDT)
# 500   = 0.05%  — stable-ish pairs (e.g. ETH/USDC)
# 3000  = 0.30%  — standard pairs
# 10000 = 1.00%  — exotic / low-liquidity pairs
# ---------------------------------------------------------------------------
FEE_TIERS = [100, 500, 3000, 10000]


@dataclass
class SwapQuoteV3:
    """
    The result of a simulated Uniswap V3 swap.

    Attributes:
        token_in:           Address of the input token (or zero address for native).
        token_out:          Address of the output token (or zero address for native).
        amount_in:          Exact input amount in human-readable units.
        amount_out:         Expected output amount in human-readable units.
        amount_out_minimum: Minimum acceptable output after slippage is applied.
        decimals_in:        Decimals for the input token.
        decimals_out:       Decimals for the output token.
        slippage_pct:       Slippage tolerance used (e.g. Decimal('0.5') = 0.5%).
        price_impact_pct:   Estimated price impact of the swap.
        fee_tiers:          List of fee tiers used per hop.
                            Single-hop: [3000]
                            Multi-hop:  [500, 3000]  (token_in->WETH fee, WETH->token_out fee)
        gas_estimate:       Gas units estimated by the quoter contract.
        needs_approval:     True if the router does not yet have sufficient allowance.
        allowance:          Current router allowance in raw token units.
        chain_id:           EIP-155 chain ID this quote is for.
        chain_name:         Human-readable chain name.
        route:              "single-hop" or "multi-hop".
        path:               Human-readable token address path used for the swap.
                            Single-hop: ["0xTokenIn", "0xTokenOut"]
                            Multi-hop:  ["0xTokenIn", "0xWETH", "0xTokenOut"]
    """

    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    amount_out_minimum: Decimal
    decimals_in: int
    decimals_out: int
    slippage_pct: Decimal
    price_impact_pct: Decimal
    fee_tiers: list[int]
    gas_estimate: int
    needs_approval: bool
    allowance: int
    chain_id: int
    chain_name: str
    route: str
    path: list[str] = field(default_factory=list)


@dataclass
class SimulationError:
    """
    Returned when the simulation fails for a known, user-facing reason.

    Attributes:
        reason:  Short machine-readable reason code.
        message: Human-readable explanation.
    """

    reason: str
    message: str


def _resolve_token_address(address: str, chain: ChainConfig) -> str:
    """
    If address is the native token placeholder, return the chain's wrapped
    native address (WETH, WMATIC, etc.) so the quoter can work with it.
    The router handles the native -> wrapped wrapping transparently.
    """
    if _is_zero_native(address):
        if not chain.wrapped_native:
            raise ValueError(
                f"Chain {chain.name!r} has no wrapped native token configured. "
                "Cannot simulate a swap involving the native token."
            )
        return chain.wrapped_native
    return address


def _encode_multihop_path(
    token_in: str,
    fee_in: int,
    hop: str,
    fee_out: int,
    token_out: str,
) -> bytes:
    return (
        bytes.fromhex(token_in[2:])
        + fee_in.to_bytes(3, byteorder="big")
        + bytes.fromhex(hop[2:])
        + fee_out.to_bytes(3, byteorder="big")
        + bytes.fromhex(token_out[2:])
    )


async def _try_quote_single(
    quoter,
    token_in: str,
    token_out: str,
    amount_in_raw: int,
    fee: int,
) -> Optional[tuple[int, int]]:
    try:
        result = await quoter.functions.quoteExactInputSingle(
            (token_in, token_out, amount_in_raw, fee, 0)
        ).call()
        # QuoterV2 returns (amountOut, sqrtPriceX96After, initializedTicksCrossed, gasEstimate)
        return result[0], result[3]
    except (ContractLogicError, Exception):
        return None


async def _try_quote_multihop(
    quoter,
    token_in: str,
    hop: str,
    token_out: str,
    amount_in_raw: int,
    fee_in: int,
    fee_out: int,
) -> Optional[tuple[int, int, int, int]]:
    try:
        path = _encode_multihop_path(token_in, fee_in, hop, fee_out, token_out)
        result = await quoter.functions.quoteExactInput(path, amount_in_raw).call()
        # QuoterV2 quoteExactInput returns:
        # (amountOut, sqrtPriceX96AfterList, initializedTicksCrossedList, gasEstimate)
        return result[0], result[3], fee_in, fee_out
    except (ContractLogicError, Exception):
        return None


async def simulate_swap(
    token_in: str,
    token_out: str,
    amount_in: float | Decimal,
    sender: str,
    slippage_pct: float | Decimal = Decimal("0.5"),
    chain_id: Optional[int] = None,
    chain_name: Optional[str] = None,
    fee_tier: Optional[int] = None,
) -> SwapQuoteV3 | SimulationError:
    chain = _resolve_chain(chain_id, chain_name)

    if not chain.v3_quoter:
        return SimulationError(
            reason="NO_QUOTER",
            message=(
                f"Chain {chain.name!r} does not have a Uniswap V3 Quoter configured. "
                "Swap simulation is not supported on this chain yet."
            ),
        )

    if not chain.v3_router:
        return SimulationError(
            reason="NO_ROUTER",
            message=(
                f"Chain {chain.name!r} does not have a Uniswap V3 Router configured."
            ),
        )
    try:
        w3 = _get_web3(chain)
    except RuntimeError as exc:
        return SimulationError(
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
    supports_native_swaps = get_router_capabilities(
        chain.chain_id,
        "v3",
        chain.v3_router,
        getattr(chain, "supports_native_swaps", True),
    ).supports_native_swaps

    # Resolve to wrapped addresses for the quoter
    resolved_in = w3.to_checksum_address(_resolve_token_address(token_in_raw, chain))
    resolved_out = w3.to_checksum_address(_resolve_token_address(token_out_raw, chain))

    if resolved_in == resolved_out:
        return SimulationError(
            reason="SAME_TOKEN",
            message="token_in and token_out resolve to the same address.",
        )

    try:
        decimals_in = await _get_token_decimals(w3, resolved_in, chain.chain_id)
        decimals_out = await _get_token_decimals(w3, resolved_out, chain.chain_id)
    except Exception as e:
        return SimulationError(
            reason="DECIMALS_FETCH_FAILED",
            message=f"Could not fetch token decimals: {e}",
        )
    amount_in_decimal = safe_decimal(amount_in)
    if amount_in_decimal is None:
        return SimulationError(
            reason="INVALID_AMOUNT",
            message="amount_in must be a valid number.",
        )
    slippage_decimal = safe_decimal(slippage_pct)
    
    if slippage_decimal is None:
        return SimulationError(
            reason="INVALID_SLIPPAGE",
            message="slippage_pct must be a valid number.",
        )
    if slippage_decimal < 0 or slippage_decimal > 100:
        return SimulationError(
            reason="INVALID_SLIPPAGE",
            message="slippage_pct must be between 0 and 100.",
        )
    amount_in_raw = int(amount_in_decimal * Decimal(10**decimals_in))

    if amount_in_raw <= 0:
        return SimulationError(
            reason="ZERO_AMOUNT",
            message="amount_in must be greater than zero.",
        )

    quoter = w3.eth.contract(
        address=w3.to_checksum_address(chain.v3_quoter),
        abi=UNISWAP_V3_QUOTER_ABI,
    )

    tiers_to_try = [fee_tier] if fee_tier is not None else FEE_TIERS
    best_single: Optional[tuple[int, int, int]] = None  # (amount_out, gas, fee_tier)

    for tier in tiers_to_try:
        result = await _try_quote_single(
            quoter, resolved_in, resolved_out, amount_in_raw, tier
        )
        if result is not None:
            amount_out_raw, gas = result
            if best_single is None or amount_out_raw > best_single[0]:
                best_single = (amount_out_raw, gas, tier)

    best_multi: Optional[tuple[int, int, int, int]] = (
        None  # (amount_out, gas, fee_in, fee_out)
    )

    if best_single is None and chain.wrapped_native:
        wrapped = w3.to_checksum_address(chain.wrapped_native)

        # Only attempt multi-hop if neither resolved token is already the wrapped native
        # (avoids a redundant WETH->WETH->token or token->WETH->WETH path)
        if wrapped != resolved_in and wrapped != resolved_out:
            for fee_in in FEE_TIERS:
                for fee_out in FEE_TIERS:
                    result = await _try_quote_multihop(
                        quoter,
                        resolved_in,
                        wrapped,
                        resolved_out,
                        amount_in_raw,
                        fee_in,
                        fee_out,
                    )
                    if result is not None:
                        amount_out_raw, gas, fi, fo = result
                        if best_multi is None or amount_out_raw > best_multi[0]:
                            best_multi = (amount_out_raw, gas, fi, fo)

    if best_single is None and best_multi is None:
        return SimulationError(
            reason="NO_LIQUIDITY",
            message=(
                f"No liquidity found for {token_in} -> {token_out} on {chain.name}. "
                "Tried all fee tiers for a direct pool and a two-hop route via "
                f"{chain.wrapped_native or 'wrapped native'} — no pool exists or "
                "has insufficient liquidity."
            ),
        )

    # Prefer single-hop; only use multi-hop if single gave no result or multi is better
    if best_single is not None and (
        best_multi is None or best_single[0] >= best_multi[0]
    ):
        amount_out_raw, gas_estimate, used_fee = best_single
        used_fee_tiers = [used_fee]
        route = "single-hop"
        resolved_path: list[str] = [resolved_in, resolved_out]
    else:
        assert best_multi is not None
        amount_out_raw, gas_estimate, fee_in, fee_out = best_multi
        used_fee_tiers = [fee_in, fee_out]
        route = "multi-hop"
        resolved_path = [
            resolved_in,
            w3.to_checksum_address(chain.wrapped_native),
            resolved_out,
        ]

    amount_out_decimal = Decimal(amount_out_raw) / Decimal(10**decimals_out)

    slippage_multiplier = Decimal(1) - (slippage_decimal / Decimal(100))
    amount_out_minimum = amount_out_decimal * slippage_multiplier

    # Get a reference quote for 1 unit to establish a "fair" price,
    # then compare it against the actual quote scaled to the same units.
    # We reuse the same route (single or multi) for a fair comparison.
    price_impact_pct = Decimal(0)
    one_unit_raw = int(Decimal(10**decimals_in))

    if one_unit_raw > 0 and one_unit_raw != amount_in_raw:
        if route == "single-hop":
            ref_result = await _try_quote_single(
                quoter, resolved_in, resolved_out, one_unit_raw, used_fee_tiers[0]
            )
            ref_out_raw = ref_result[0] if ref_result is not None else None
        else:
            ref_result = await _try_quote_multihop(
                quoter,
                resolved_in,
                w3.to_checksum_address(chain.wrapped_native),
                resolved_out,
                one_unit_raw,
                used_fee_tiers[0],
                used_fee_tiers[1],
            )
            ref_out_raw = ref_result[0] if ref_result is not None else None

        if ref_out_raw is not None:
            ref_out_per_unit = Decimal(ref_out_raw) / Decimal(10**decimals_out)
            expected_out = ref_out_per_unit * amount_in_decimal
            if expected_out > 0:
                price_impact_pct = (
                    (expected_out - amount_out_decimal) / expected_out * Decimal(100)
                ).quantize(Decimal("0.0001"))

    needs_approval = False
    current_allowance = 0

    requires_erc20_approval = (not native_in) or (
        native_in and not supports_native_swaps
    )
    if requires_erc20_approval:
        try:
            current_allowance = await _get_allowance(
                w3, resolved_in, checksum_sender, chain.v3_router
            )
            needs_approval = current_allowance < amount_in_raw
        except Exception:
            # Non-fatal: allowance check failure doesn't invalidate the quote.
            needs_approval = True

    return SwapQuoteV3(
        token_in=token_in_raw,
        token_out=token_out_raw,
        amount_in=amount_in_decimal,
        amount_out=amount_out_decimal,
        amount_out_minimum=amount_out_minimum,
        decimals_in=decimals_in,
        decimals_out=decimals_out,
        slippage_pct=slippage_decimal,
        price_impact_pct=price_impact_pct,
        fee_tiers=used_fee_tiers,
        gas_estimate=gas_estimate,
        needs_approval=needs_approval,
        allowance=current_allowance,
        chain_id=chain.chain_id,
        chain_name=chain.name,
        route=route,
        path=resolved_path,
    )
