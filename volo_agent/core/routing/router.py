from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from config.chains import get_chain_by_id, get_chain_by_name
from config.solana_chains import (
    fetch_solana_token_decimals,
    get_solana_chain,
    is_solana_chain_id,
    is_solana_network,
)
from core.memory.ledger import PerformanceLedger
from core.observer.price_observer import price_cache
from core.routing.bridge.base import BridgeAggregator
from core.routing.bridge.lifi import LiFiAggregator
from core.routing.bridge.mayan import MayanAggregator
from core.routing.bridge.token_resolver import resolve_bridge_token
from core.routing.models import (
    BridgeRouteQuote,
    RouteDecision,
    SolanaSwapRouteQuote,
    SwapRouteQuote,
)
from core.routing.scorer import pick_best_bridge, pick_best_solana_swap, pick_best_swap
from core.routing.solana.base import SolanaSwapAggregator
from core.routing.solana.jupiter import JupiterAggregator
from core.routing.solana.raydium import RaydiumAggregator
from core.routing.swap.base import SwapAggregator
from core.routing.swap.paraswap import ParaSwapAggregator
from core.routing.swap.zerox import ZeroXAggregator
from core.utils.async_tools import run_blocking
from wallet_service.evm.gas_price import gas_price_cache

_LOGGER = logging.getLogger("volo.routing.router")
# Timeout for each internal simulator call (blocking, runs in thread pool).
_INTERNAL_SIMULATOR_TIMEOUT: float = 60.0


@dataclass(frozen=True)
class _TimedCallOutcome:
    value: Optional[Any]
    error: Optional[str] = None

def _convert_v3_quote(
    v3_result: Any,
    chain_id: int,
    token_in: str,
    token_out: str,
    amount_in: Decimal,
) -> Optional[SwapRouteQuote]:
    from tool_nodes.dex.swap_simulator_v3 import SimulationError

    if isinstance(v3_result, SimulationError):
        return None

    try:
        amount_out = Decimal(str(v3_result.amount_out))
        amount_out_min = Decimal(str(v3_result.amount_out_minimum))
        price_impact = Decimal(str(getattr(v3_result, "price_impact_pct", 0) or 0))
        gas_estimate = int(getattr(v3_result, "gas_estimate", 200_000) or 200_000)
    except Exception as exc:
        _LOGGER.debug("v3 quote conversion failed: %s", exc)
        return None

    return SwapRouteQuote(
        aggregator="uniswap_v3",
        chain_id=chain_id,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        amount_out=amount_out,
        amount_out_min=amount_out_min,
        gas_estimate=gas_estimate,
        gas_cost_usd=None,
        price_impact_pct=price_impact,
        calldata=None,  # No pre-built calldata for internal routes.
        to=None,  # The swap_token tool knows the router address.
        approval_address=None,
        raw={
            "execution": {
                "protocol": "v3",
                "route": getattr(v3_result, "route", "single-hop"),
                "path": list(getattr(v3_result, "path", []) or []),
                "fee_tiers": list(getattr(v3_result, "fee_tiers", []) or []),
                "decimals_in": int(getattr(v3_result, "decimals_in", 18) or 18),
                "decimals_out": int(getattr(v3_result, "decimals_out", 18) or 18),
                "needs_approval": bool(getattr(v3_result, "needs_approval", False)),
                "allowance": int(getattr(v3_result, "allowance", 0) or 0),
                "chain_name": str(getattr(v3_result, "chain_name", "") or ""),
            }
        },
    )


def _convert_v2_quote(
    v2_result: Any,
    chain_id: int,
    token_in: str,
    token_out: str,
    amount_in: Decimal,
) -> Optional[SwapRouteQuote]:
    from tool_nodes.dex.swap_simulator_v2 import SimulationErrorV2

    if isinstance(v2_result, SimulationErrorV2):
        return None

    try:
        amount_out = Decimal(str(v2_result.amount_out))
        amount_out_min = Decimal(str(v2_result.amount_out_minimum))
        price_impact = Decimal(str(getattr(v2_result, "price_impact_pct", 0) or 0))
        gas_estimate = int(getattr(v2_result, "gas_estimate", 150_000) or 150_000)
    except Exception as exc:
        _LOGGER.debug("v2 quote conversion failed: %s", exc)
        return None

    return SwapRouteQuote(
        aggregator="uniswap_v2",
        chain_id=chain_id,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        amount_out=amount_out,
        amount_out_min=amount_out_min,
        gas_estimate=gas_estimate,
        gas_cost_usd=None,
        price_impact_pct=price_impact,
        calldata=None,
        to=None,
        approval_address=None,
        raw={
            "execution": {
                "protocol": "v2",
                "path": list(getattr(v2_result, "path", []) or []),
                "decimals_in": int(getattr(v2_result, "decimals_in", 18) or 18),
                "decimals_out": int(getattr(v2_result, "decimals_out", 18) or 18),
                "needs_approval": bool(getattr(v2_result, "needs_approval", False)),
                "allowance": int(getattr(v2_result, "allowance", 0) or 0),
                "dex_name": str(getattr(v2_result, "dex_name", "") or ""),
                "chain_name": str(getattr(v2_result, "chain_name", "") or ""),
            }
        },
    )


def _convert_across_quote(
    across_result: Any,
    source_chain_name: str,
    dest_chain_name: str,
    source_chain_id: int,
    dest_chain_id: int,
    token_symbol: str,
    amount: Decimal,
) -> Optional[BridgeRouteQuote]:
    from tool_nodes.bridge.simulators.across_simulator import AcrossSimulationError

    if isinstance(across_result, AcrossSimulationError):
        return None

    try:
        output_amount = Decimal(str(across_result.output_amount))
        total_fee = Decimal(str(across_result.total_fee))
        total_fee_pct = Decimal(str(across_result.total_fee_pct))
        fill_time = int(getattr(across_result, "avg_fill_time_seconds", 120) or 120)
    except Exception as exc:
        _LOGGER.debug("across quote conversion failed: %s", exc)
        return None

    return BridgeRouteQuote(
        aggregator="across",
        token_symbol=token_symbol,
        source_chain_id=source_chain_id,
        dest_chain_id=dest_chain_id,
        source_chain_name=source_chain_name,
        dest_chain_name=dest_chain_name,
        input_amount=amount,
        output_amount=output_amount,
        total_fee=total_fee,
        total_fee_pct=total_fee_pct,
        estimated_fill_time_seconds=fill_time,
        gas_cost_source=None,
        calldata=None,  # Across calldata is built by the existing executor.
        to=None,
        tool_data={
            "planned_quote": {
                "protocol": "across",
                "token_symbol": across_result.token_symbol,
                "input_token": across_result.input_token,
                "output_token": across_result.output_token,
                "source_chain_id": across_result.source_chain_id,
                "dest_chain_id": across_result.dest_chain_id,
                "source_chain_name": across_result.source_chain_name,
                "dest_chain_name": across_result.dest_chain_name,
                "input_amount": str(across_result.input_amount),
                "output_amount": str(across_result.output_amount),
                "total_fee": str(across_result.total_fee),
                "total_fee_pct": str(across_result.total_fee_pct),
                "lp_fee": str(across_result.lp_fee),
                "relayer_fee": str(across_result.relayer_fee),
                "gas_fee": str(across_result.gas_fee),
                "input_decimals": across_result.input_decimals,
                "output_decimals": across_result.output_decimals,
                "quote_timestamp": across_result.quote_timestamp,
                "fill_deadline": across_result.fill_deadline,
                "exclusivity_deadline": across_result.exclusivity_deadline,
                "exclusive_relayer": across_result.exclusive_relayer,
                "spoke_pool": across_result.spoke_pool,
                "is_native_input": across_result.is_native_input,
                "avg_fill_time_seconds": across_result.avg_fill_time_seconds,
            }
        },
    )


def _convert_relay_quote(
    relay_result: Any,
    source_chain_name: str,
    dest_chain_name: str,
    source_chain_id: int,
    dest_chain_id: int,
    token_symbol: str,
    amount: Decimal,
) -> Optional[BridgeRouteQuote]:
    from tool_nodes.bridge.simulators.relay_simulator import RelaySimulationError

    if isinstance(relay_result, RelaySimulationError):
        return None

    try:
        output_amount = Decimal(str(relay_result.output_amount))
        total_fee = Decimal(str(relay_result.total_fee))
        total_fee_pct = Decimal(str(relay_result.total_fee_pct))
        fill_time = int(getattr(relay_result, "avg_fill_time_seconds", 180) or 180)
    except Exception as exc:
        _LOGGER.debug("relay quote conversion failed: %s", exc)
        return None

    return BridgeRouteQuote(
        aggregator="relay",
        token_symbol=token_symbol,
        source_chain_id=source_chain_id,
        dest_chain_id=dest_chain_id,
        source_chain_name=source_chain_name,
        dest_chain_name=dest_chain_name,
        input_amount=amount,
        output_amount=output_amount,
        total_fee=total_fee,
        total_fee_pct=total_fee_pct,
        estimated_fill_time_seconds=fill_time,
        gas_cost_source=None,
        calldata=None,
        to=None,
        tool_data={
            "planned_quote": {
                "protocol": relay_result.protocol,
                "token_symbol": relay_result.token_symbol,
                "source_chain_id": relay_result.source_chain_id,
                "dest_chain_id": relay_result.dest_chain_id,
                "source_chain_name": relay_result.source_chain_name,
                "dest_chain_name": relay_result.dest_chain_name,
                "input_amount": str(relay_result.input_amount),
                "output_amount": str(relay_result.output_amount),
                "total_fee": str(relay_result.total_fee),
                "total_fee_pct": str(relay_result.total_fee_pct),
                "fees": relay_result.fees,
                "steps": relay_result.steps,
                "request_id": relay_result.request_id,
                "avg_fill_time_seconds": relay_result.avg_fill_time_seconds,
                "api_base_url": relay_result.api_base_url,
            }
        },
    )

async def _timed_aggregator_call(
    coro,
    source_name: str,
    timeout: float,
) -> Optional[Any]:
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        _LOGGER.warning("[router] %s timed out after %.1fs", source_name, timeout)
        return None
    except Exception as exc:
        _LOGGER.warning(
            "[router] %s raised %s: %s", source_name, type(exc).__name__, exc
        )
        return None


async def _timed_aggregator_call_with_outcome(
    coro,
    source_name: str,
    timeout: float,
) -> _TimedCallOutcome:
    try:
        value = await asyncio.wait_for(coro, timeout=timeout)
        return _TimedCallOutcome(value=value, error=None)
    except asyncio.TimeoutError:
        _LOGGER.warning("[router] %s timed out after %.1fs", source_name, timeout)
        return _TimedCallOutcome(
            value=None,
            error=f"timed out after {timeout:.1f}s",
        )
    except Exception as exc:
        _LOGGER.warning(
            "[router] %s raised %s: %s", source_name, type(exc).__name__, exc
        )
        return _TimedCallOutcome(
            value=None,
            error=f"{type(exc).__name__}: {exc}",
        )


async def _get_gas_cost_usd_fallback(
    chain_id: int,
    gas_estimate: Optional[int] = None,
    native_amount: Optional[Decimal] = None,
) -> Optional[Decimal]:
    try:
        # Resolve chain config to get native token symbol
        chain_cfg = get_chain_by_id(chain_id)
        native_symbol = chain_cfg.native_symbol

        # Fetch native token price in USD
        native_price_usd = await price_cache.get(native_symbol)
        if native_price_usd is None:
            if native_symbol == "ETH":
                native_price_usd = 2500.0
            else:
                return None

        # If native_amount is provided, just multiply by price
        if native_amount is not None:
            return native_amount * Decimal(str(native_price_usd))

        # If gas_estimate is provided, fetch gas price and calculate
        if gas_estimate is not None and gas_estimate > 0:
            gas_price_wei = await gas_price_cache.get_wei(chain_id)
            if not gas_price_wei:
                return None

            cost_native = (Decimal(gas_estimate) * Decimal(gas_price_wei)) / Decimal(
                10**18
            )
            return cost_native * Decimal(str(native_price_usd))

        return None
    except Exception as exc:
        _LOGGER.debug("[router] gas fallback calculation failed: %s", exc)
        return None

class RoutePlanner:
    def __init__(
        self,
        swap_aggregators: Optional[List[SwapAggregator]] = None,
        bridge_aggregators: Optional[List[BridgeAggregator]] = None,
        solana_aggregators: Optional[List[SolanaSwapAggregator]] = None,
    ) -> None:
        # EVM swap aggregators
        self.swap_aggregators: List[SwapAggregator] = swap_aggregators or [
            # OneInchAggregator(), # uncomment when 1inch API key is available
            ZeroXAggregator(),
            ParaSwapAggregator(),
        ]
        # Bridge aggregators
        self.bridge_aggregators: List[BridgeAggregator] = bridge_aggregators or [
            LiFiAggregator(),
            MayanAggregator(),
        ]
        # Solana DEX aggregators — stateless singletons, safe to share.
        self.solana_aggregators: List[SolanaSwapAggregator] = solana_aggregators or [
            JupiterAggregator(),
            RaydiumAggregator(),
        ]

    async def get_best_swap_route(
        self,
        node_args: Dict[str, Any],
        sender: str,
        ledger: PerformanceLedger,
    ) -> Optional[RouteDecision]:
        token_in = node_args.get("token_in_address", "")
        token_out = node_args.get("token_out_address", "")
        chain_name = node_args.get("chain", "")

        # amount_in may be a float, int, str, or a dynamic marker.
        raw_amount = node_args.get("amount_in")
        if raw_amount is None or (isinstance(raw_amount, str) and "{{" in raw_amount):
            _LOGGER.debug(
                "[router:swap] amount_in is a dynamic marker — skipping routing"
            )
            return None

        try:
            amount_in = Decimal(str(raw_amount))
        except Exception:
            _LOGGER.debug(
                "[router:swap] could not parse amount_in %r — skipping", raw_amount
            )
            return None

        if not token_in or not token_out or not chain_name or amount_in <= 0:
            _LOGGER.debug("[router:swap] missing required args — skipping routing")
            return None

        # Resolve actual sender from args (may be a literal address or a marker).
        effective_sender = node_args.get("sender", sender) or sender
        if "{{" in effective_sender:
            effective_sender = sender

        slippage_pct = float(node_args.get("slippage", 0.5) or 0.5)

        try:
            chain_cfg = get_chain_by_name(chain_name)
        except KeyError as exc:
            _LOGGER.debug("[router:swap] unknown chain %r: %s", chain_name, exc)
            return None

        chain_id = chain_cfg.chain_id
        chain_name_canonical = chain_cfg.name

        # Internal V3/V2 execution is intentionally deferred to swap_tool
        # fallback, so route discovery remains non-blocking and lightweight.
        tasks: List[Tuple[str, Any]] = [] 

        # External aggregators
        for agg in self.swap_aggregators:
            coro = agg.get_quote(
                chain_id=chain_id,
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                slippage_pct=slippage_pct,
                sender=effective_sender,
            )
            tasks.append(
                (
                    agg.name,
                    _timed_aggregator_call_with_outcome(
                        coro,
                        agg.name,
                        agg.TIMEOUT_SECONDS,
                    ),
                )
            )

        if not tasks:
            _LOGGER.debug(
                "[router:swap] no sources available for chain %s", chain_name_canonical
            )
            return None

        # Execute all sources in parallel 
        source_names = [name for name, _ in tasks]
        coros = [coro for _, coro in tasks]
        raw_results = await asyncio.gather(*coros)

        # Collect valid SwapRouteQuote objects 
        quotes: List[SwapRouteQuote] = []
        source_failures: Dict[str, str] = {}
        for name, outcome in zip(source_names, raw_results):
            if isinstance(outcome, _TimedCallOutcome):
                result = outcome.value
                failure = outcome.error
            else:
                result = outcome
                failure = None

            if isinstance(result, SwapRouteQuote):
                #  Normalize gas cost if missing 
                if result.gas_cost_usd is None and result.gas_estimate > 0:
                    result.gas_cost_usd = await _get_gas_cost_usd_fallback(
                        result.chain_id, result.gas_estimate
                    )

                quotes.append(result)
                _LOGGER.debug(
                    "[router:swap] %s: out=%s impact=%s%% gas=%d cost_usd=%s",
                    name,
                    result.amount_out,
                    result.price_impact_pct,
                    result.gas_estimate,
                    result.gas_cost_usd,
                )
            else:
                source_failures[name] = failure or "no quote returned"
                _LOGGER.debug(
                    "[router:swap] %s returned no quote (%s)",
                    name,
                    source_failures[name],
                )

        if not quotes:
            failure_summary = "; ".join(
                f"{name}={reason}" for name, reason in source_failures.items()
            ) or "no quote returned"
            _LOGGER.warning(
                "[router:swap] all sources failed for %s→%s on %s (%s)",
                token_in[:10],
                token_out[:10],
                chain_name_canonical,
                failure_summary,
            )
            raise RuntimeError(
                "Swap route discovery failed for "
                f"{token_in[:10]}→{token_out[:10]} on {chain_name_canonical}. "
                f"Source failures: {failure_summary}."
            )

        # Score and select best quote 
        best_result = pick_best_swap(quotes, ledger, chain_name_canonical)
        if best_result is None:
            return None

        best_quote, best_score = best_result

        # Rank all quotes descending so they are stored in RouteDecision.
        from core.routing.models import AnyRouteQuote
        from core.routing.scorer import rank_swap_quotes

        ranked = rank_swap_quotes(quotes, ledger, chain_name_canonical)
        all_sorted: List[AnyRouteQuote] = [q for q, _ in ranked]

        _LOGGER.info(
            "[router:swap] selected %s for %s→%s on %s "
            "(score=%.4f, out=%s, impact=%s%%)",
            best_quote.aggregator,
            token_in[:10],
            token_out[:10],
            chain_name_canonical,
            best_score,
            best_quote.amount_out,
            best_quote.price_impact_pct,
        )

        return RouteDecision(
            node_id="",  # Filled in by route_planner_node.
            intent_type="swap",
            selected=best_quote,
            all_quotes=all_sorted,
            score=best_score,
        )

    async def get_best_solana_swap_route(
        self,
        node_args: Dict[str, Any],
        sender: str,
        ledger: PerformanceLedger,
    ) -> Optional[RouteDecision]:
        # Extract and validate args 
        token_in_mint = str(node_args.get("token_in_mint") or "").strip()
        token_out_mint = str(node_args.get("token_out_mint") or "").strip()
        network = str(node_args.get("network") or "solana").strip().lower()

        raw_amount = node_args.get("amount_in")
        if raw_amount is None or (isinstance(raw_amount, str) and "{{" in raw_amount):
            _LOGGER.debug("[router:solana] amount_in is a dynamic marker — skipping")
            return None

        try:
            amount_in = Decimal(str(raw_amount))
        except Exception:
            _LOGGER.debug(
                "[router:solana] could not parse amount_in %r — skipping", raw_amount
            )
            return None

        if not token_in_mint or not token_out_mint or amount_in <= 0:
            _LOGGER.debug("[router:solana] missing required args — skipping routing")
            return None

        effective_sender = node_args.get("sender", sender) or sender
        if "{{" in str(effective_sender):
            effective_sender = sender

        slippage_pct = float(node_args.get("slippage", 0.5) or 0.5)

        # Resolve Solana chain config 
        try:
            chain = get_solana_chain(network)
        except KeyError as exc:
            _LOGGER.debug("[router:solana] unknown network %r: %s", network, exc)
            return None

        # Fetch token decimals (parallel, non-blocking) 
        # These are needed to convert the human-readable amount_in to the
        # raw lamport integer that Jupiter and Raydium expect.
        try:
            input_decimals, output_decimals = await asyncio.gather(
                fetch_solana_token_decimals(token_in_mint, chain.rpc_url),
                fetch_solana_token_decimals(token_out_mint, chain.rpc_url),
            )
        except Exception as exc:
            _LOGGER.warning(
                "[router:solana] token decimal lookup failed for %s→%s on %s: %s",
                token_in_mint[:8],
                token_out_mint[:8],
                chain.network,
                exc,
            )
            return None

        # Build coroutines for all Solana aggregators 
        tasks: List[Tuple[str, Any]] = []
        for agg in self.solana_aggregators:
            coro = agg.get_quote(
                network=chain.network,
                rpc_url=chain.rpc_url,
                input_mint=token_in_mint,
                output_mint=token_out_mint,
                amount_in=amount_in,
                input_decimals=input_decimals,
                output_decimals=output_decimals,
                slippage_pct=slippage_pct,
                sender=effective_sender,
            )
            tasks.append(
                (
                    agg.name,
                    _timed_aggregator_call(coro, agg.name, agg.TIMEOUT_SECONDS),
                )
            )

        if not tasks:
            return None

        # Execute all aggregators in parallel 
        source_names = [name for name, _ in tasks]
        coros = [coro for _, coro in tasks]
        raw_results = await asyncio.gather(*coros)

        #  Collect valid SolanaSwapRouteQuote objects 
        quotes: List[SolanaSwapRouteQuote] = []
        for name, result in zip(source_names, raw_results):
            if isinstance(result, SolanaSwapRouteQuote):
                quotes.append(result)
                _LOGGER.debug(
                    "[router:solana] %s: out=%s impact=%s%%  tx=%s",
                    name,
                    result.amount_out,
                    result.price_impact_pct,
                    "yes" if result.swap_transaction else "no",
                )
            else:
                _LOGGER.debug("[router:solana] %s returned no quote", name)

        if not quotes:
            _LOGGER.warning(
                "[router:solana] all aggregators failed for %s→%s on %s",
                token_in_mint[:8],
                token_out_mint[:8],
                network,
            )
            return None

        # Score and select best quote 
        best_result = pick_best_solana_swap(quotes, ledger)
        if best_result is None:
            return None

        best_quote, best_score = best_result

        # Rank all quotes for storage in RouteDecision.
        from core.routing.scorer import rank_solana_swap_quotes

        ranked = rank_solana_swap_quotes(quotes, ledger)
        all_sorted = [q for q, _ in ranked]

        _LOGGER.info(
            "[router:solana] selected %s for %s→%s on %s "
            "(score=%.4f, out=%s, impact=%s%%)",
            best_quote.aggregator,
            token_in_mint[:8],
            token_out_mint[:8],
            network,
            best_score,
            best_quote.amount_out,
            best_quote.price_impact_pct,
        )

        return RouteDecision(
            node_id="",  # Filled in by route_planner_node.
            intent_type="solana_swap",
            selected=best_quote,
            all_quotes=list(all_sorted),  # type: ignore[arg-type]
            score=best_score,
        )

    async def get_best_bridge_route(
        self,
        node_args: Dict[str, Any],
        sender: str,
        ledger: PerformanceLedger,
        *,
        solana_sender: Optional[str] = None,
    ) -> Optional[RouteDecision]:
        # Extract and validate args
        token_symbol = str(node_args.get("token_symbol", "")).strip().upper()
        source_chain_name = str(node_args.get("source_chain", "")).strip()
        dest_chain_name = str(node_args.get("target_chain", "")).strip()

        raw_amount = node_args.get("amount")
        if raw_amount is None or (isinstance(raw_amount, str) and "{{" in raw_amount):
            _LOGGER.debug(
                "[router:bridge] amount is a dynamic marker — skipping routing"
            )
            return None

        try:
            amount = Decimal(str(raw_amount))
        except Exception:
            _LOGGER.debug(
                "[router:bridge] could not parse amount %r — skipping", raw_amount
            )
            return None

        if (
            not token_symbol
            or not source_chain_name
            or not dest_chain_name
            or amount <= 0
        ):
            _LOGGER.debug("[router:bridge] missing required args — skipping routing")
            return None

        # Resolve actual sender / recipient.
        source_is_solana = is_solana_network(source_chain_name)
        dest_is_solana = is_solana_network(dest_chain_name)

        # Default fallback for sender based on source chain.
        sender_fallback = solana_sender if source_is_solana else sender
        effective_sender = node_args.get("sender", sender_fallback) or sender_fallback
        if effective_sender and "{{" in str(effective_sender):
            effective_sender = sender_fallback

        # Default fallback for recipient based on destination chain.
        recipient_fallback = solana_sender if dest_is_solana else sender
        effective_recipient = (
            node_args.get("recipient", recipient_fallback) or recipient_fallback
        )
        if effective_recipient and "{{" in str(effective_recipient):
            effective_recipient = recipient_fallback

        # If we still have no addresses for an EVM/Solana route, we can't get a real quote.
        if not effective_sender or not effective_recipient:
            _LOGGER.debug(
                "[router:bridge] missing sender/recipient for %s→%s",
                source_chain_name,
                dest_chain_name,
            )
            return None
        #  Resolve chain configs 
        # Solana chain IDs now live in config/solana_chains.py, so bridge
        # routing can treat them like any other registered chain ID.
        source_chain_id: int
        dest_chain_id: int
        source_chain_name_canonical: str
        dest_chain_name_canonical: str
        try:
            source_cfg = get_chain_by_name(source_chain_name)
            source_chain_id = source_cfg.chain_id
            source_chain_name_canonical = source_cfg.name
        except KeyError:
            if is_solana_network(source_chain_name):
                source_cfg = get_solana_chain(source_chain_name)
                source_chain_id = source_cfg.chain_id
                source_chain_name_canonical = source_cfg.name
            else:
                _LOGGER.debug(
                    "[router:bridge] unknown source chain: %s", source_chain_name
                )
                return None

        try:
            dest_cfg = get_chain_by_name(dest_chain_name)
            dest_chain_id = dest_cfg.chain_id
            dest_chain_name_canonical = dest_cfg.name
        except KeyError:
            if is_solana_network(dest_chain_name):
                dest_cfg = get_solana_chain(dest_chain_name)
                dest_chain_id = dest_cfg.chain_id
                dest_chain_name_canonical = dest_cfg.name
            else:
                _LOGGER.debug("[router:bridge] unknown dest chain: %s", dest_chain_name)
                return None

        # Build coroutines for all sources 
        tasks: List[Tuple[str, Any]] = []

        # External aggregators (Li.Fi, Mayan for Solana routes)
        for agg in self.bridge_aggregators:
            coro = agg.get_quote(
                token_symbol=token_symbol,
                source_chain_id=source_chain_id,
                dest_chain_id=dest_chain_id,
                source_chain_name=source_chain_name_canonical,
                dest_chain_name=dest_chain_name_canonical,
                amount=amount,
                sender=effective_sender,
                recipient=effective_recipient,
            )
            tasks.append(
                (
                    agg.name,
                    _timed_aggregator_call(coro, agg.name, agg.TIMEOUT_SECONDS),
                )
            )

        # Internal Across and Relay simulators are EVM-only.
        # Skip them when either chain is Solana to avoid spurious errors.
        solana_route = is_solana_chain_id(source_chain_id) or is_solana_chain_id(
            dest_chain_id
        )

        if not solana_route:
            tasks.append(
                (
                    "across",
                    _timed_aggregator_call(
                        self._run_across_simulator(
                            token_symbol=token_symbol,
                            source_chain_id=source_chain_id,
                            dest_chain_id=dest_chain_id,
                            source_chain_name=source_chain_name_canonical,
                            dest_chain_name=dest_chain_name_canonical,
                            amount=amount,
                            sender=effective_sender,
                            recipient=effective_recipient,
                        ),
                        "across",
                        _INTERNAL_SIMULATOR_TIMEOUT,
                    ),
                )
            )

            tasks.append(
                (
                    "relay",
                    _timed_aggregator_call(
                        self._run_relay_simulator(
                            token_symbol=token_symbol,
                            source_chain_id=source_chain_id,
                            dest_chain_id=dest_chain_id,
                            source_chain_name=source_chain_name_canonical,
                            dest_chain_name=dest_chain_name_canonical,
                            amount=amount,
                            sender=effective_sender,
                            recipient=effective_recipient,
                        ),
                        "relay",
                        _INTERNAL_SIMULATOR_TIMEOUT,
                    ),
                )
            )

        if not tasks:
            return None

        # Execute all sources in parallel 
        source_names = [name for name, _ in tasks]
        coros = [coro for _, coro in tasks]
        raw_results = await asyncio.gather(*coros)

        # Collect valid BridgeRouteQuote objects 
        quotes: List[BridgeRouteQuote] = []
        for name, result in zip(source_names, raw_results):
            if isinstance(result, BridgeRouteQuote):
                # Normalize gas cost if missing 
                if result.gas_cost_usd is None:
                    # Case 1: Fetch directly from aggregator tool_data
                    raw_usd = (
                        result.tool_data.get("gasCostUsd") if result.tool_data else None
                    )
                    if raw_usd:
                        try:
                            result.gas_cost_usd = Decimal(str(raw_usd))
                        except Exception:
                            pass

                    # Estimate from gas_cost_source (Native units)
                    if result.gas_cost_usd is None and result.gas_cost_source:
                        result.gas_cost_usd = await _get_gas_cost_usd_fallback(
                            result.source_chain_id, native_amount=result.gas_cost_source
                        )

                quotes.append(result)
                _LOGGER.debug(
                    "[router:bridge] %s: out=%s fee=%s%% fill=%ds cost_usd=%s",
                    name,
                    result.output_amount,
                    result.total_fee_pct,
                    result.estimated_fill_time_seconds,
                    result.gas_cost_usd,
                )
            else:
                _LOGGER.debug("[router:bridge] %s returned no quote", name)

        if not quotes:
            _LOGGER.warning(
                "[router:bridge] all sources failed for %s %s→%s",
                token_symbol,
                source_chain_name_canonical,
                dest_chain_name_canonical,
            )
            return None

        # Score and select best quote 
        best_result = pick_best_bridge(quotes, ledger)
        if best_result is None:
            return None

        best_quote, best_score = best_result

        from core.routing.models import AnyRouteQuote
        from core.routing.scorer import rank_bridge_quotes

        ranked = rank_bridge_quotes(quotes, ledger)
        all_sorted: List[AnyRouteQuote] = [q for q, _ in ranked]

        _LOGGER.info(
            "[router:bridge] selected %s for %s %s→%s "
            "(score=%.4f, out=%s, fee=%s%%, fill=%ds)",
            best_quote.aggregator,
            token_symbol,
            source_chain_name_canonical,
            dest_chain_name_canonical,
            best_score,
            best_quote.output_amount,
            best_quote.total_fee_pct,
            best_quote.estimated_fill_time_seconds,
        )

        return RouteDecision(
            node_id="",  # Filled in by route_planner_node.
            intent_type="bridge",
            selected=best_quote,
            all_quotes=all_sorted,
            score=best_score,
        )

    async def _run_v3_simulator(
        self,
        *,
        token_in: str,
        token_out: str,
        amount_in: float,
        sender: str,
        slippage_pct: float,
        chain_name: str,
        chain_id: int,
    ) -> Optional[SwapRouteQuote]:
        """Wrap the internal Uniswap V3 simulator as an async quote source."""
        from tool_nodes.dex.swap_simulator_v3 import simulate_swap

        try:
            result = await simulate_swap(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                sender=sender,
                slippage_pct=slippage_pct,
                chain_name=chain_name,
            )
        except Exception as exc:
            _LOGGER.debug("[router:uniswap_v3] simulator error: %s", exc)
            return None

        return _convert_v3_quote(
            result, chain_id, token_in, token_out, Decimal(str(amount_in))
        )

    async def _run_v2_simulator(
        self,
        *,
        token_in: str,
        token_out: str,
        amount_in: float,
        sender: str,
        slippage_pct: float,
        chain_name: str,
        chain_id: int,
    ) -> Optional[SwapRouteQuote]:
        """Wrap the internal Uniswap V2 simulator as an async quote source."""
        from tool_nodes.dex.swap_simulator_v2 import simulate_swap_v2

        try:
            result = await simulate_swap_v2(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                sender=sender,
                slippage_pct=slippage_pct,
                chain_name=chain_name,
            )
        except Exception as exc:
            _LOGGER.debug("[router:uniswap_v2] simulator error: %s", exc)
            return None

        return _convert_v2_quote(
            result, chain_id, token_in, token_out, Decimal(str(amount_in))
        )

    async def _run_across_simulator(
        self,
        *,
        token_symbol: str,
        source_chain_id: int,
        dest_chain_id: int,
        source_chain_name: str,
        dest_chain_name: str,
        amount: Decimal,
        sender: str,
        recipient: str,
    ) -> Optional[BridgeRouteQuote]:
        from config.bridge_registry import ACROSS

        from tool_nodes.bridge.simulators.across_simulator import (
            fetch_across_available_routes,
            simulate_across_bridge,
        )

        symbol = token_symbol.strip().upper()

        async def _resolve_symbol_decimals(
            chain_id: int, chain_name: str
        ) -> Optional[int]:
            resolved = await resolve_bridge_token(
                symbol,
                chain_id=chain_id,
                chain_name=chain_name,
            )
            if resolved is not None:
                return int(resolved.decimals)
            return None

        src_decimals, dst_decimals = await asyncio.gather(
            _resolve_symbol_decimals(source_chain_id, source_chain_name),
            _resolve_symbol_decimals(dest_chain_id, dest_chain_name),
        )
        if src_decimals is None or dst_decimals is None:
            _LOGGER.debug(
                "[router:across] unresolved decimals for %s (%s->%s): src=%s dst=%s",
                symbol,
                source_chain_name,
                dest_chain_name,
                src_decimals,
                dst_decimals,
            )
            return None

        try:
            dynamic_routes = await run_blocking(
                fetch_across_available_routes,
                source_chain_id,
                dest_chain_id,
                token_symbol,
                ACROSS,
                timeout=8.0,
            )
        except Exception as exc:
            _LOGGER.debug("[router:across] available-routes lookup failed: %s", exc)
            return None

        if not dynamic_routes:
            return None

        # Simulate the first available route (Across routes are fungible for
        # fee estimation purposes — different routes for the same token/chain
        # pair have the same fee model).
        route = dynamic_routes[0]
        try:
            result = await run_blocking(
                simulate_across_bridge,
                route,
                amount,
                sender,
                recipient,
                src_decimals,
                dst_decimals,
                ACROSS,
            )
        except Exception as exc:
            _LOGGER.debug("[router:across] simulation error: %s", exc)
            return None

        return _convert_across_quote(
            result,
            source_chain_name,
            dest_chain_name,
            source_chain_id,
            dest_chain_id,
            token_symbol,
            amount,
        )

    async def _run_relay_simulator(
        self,
        *,
        token_symbol: str,
        source_chain_id: int,
        dest_chain_id: int,
        source_chain_name: str,
        dest_chain_name: str,
        amount: Decimal,
        sender: str,
        recipient: str,
    ) -> Optional[BridgeRouteQuote]:
        """Wrap the internal Relay simulator as an async quote source."""
        from config.bridge_registry import RELAY, relay_api_base_url
        from tool_nodes.bridge.simulators.relay_simulator import simulate_relay_bridge

        base_url = relay_api_base_url(source_chain_id, dest_chain_id)
        if not base_url or not base_url.startswith("http"):
            _LOGGER.debug("[router:relay] API base URL not configured — skipping")
            return None

        try:
            result = await run_blocking(
                simulate_relay_bridge,
                token_symbol,
                source_chain_id,
                dest_chain_id,
                amount,
                sender,
                recipient,
                RELAY,
            )
        except Exception as exc:
            _LOGGER.debug("[router:relay] simulation error: %s", exc)
            return None

        return _convert_relay_quote(
            result,
            source_chain_name,
            dest_chain_name,
            source_chain_id,
            dest_chain_id,
            token_symbol,
            amount,
        )
