from __future__ import annotations

from decimal import Decimal
from typing import List, Optional, Tuple

from core.memory.ledger import PerformanceLedger
from core.routing.models import BridgeRouteQuote, SolanaSwapRouteQuote, SwapRouteQuote

# Minimum number of recorded runs before historical success-rate data is
# considered reliable.  Below this threshold we return the optimistic prior.
MIN_SAMPLE_SIZE: int = 5

# Optimistic prior used when there is not enough historical data.
OPTIMISTIC_PRIOR: float = 1.0

# Fill-time baseline in seconds.  Routes faster than this earn a small bonus;
# routes slower than this earn nothing (we don't penalise slow bridges,
# because the user's primary goal is maximising output).
BRIDGE_FILL_TIME_BASELINE_SECONDS: int = 300  # 5 minutes

# Maximum fill-time bonus contribution to the score.  Kept small so it can
# only break ties — it should never overcome a meaningful output difference.
BRIDGE_FILL_TIME_MAX_BONUS: float = 0.001

# Conservative gas penalty fallback (USD) used when both the aggregator and
# our internal fallback estimator fail.  Prevents "free" gas bias.
GAS_PENALTY_FALLBACK_USD: Decimal = Decimal("1.0")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_success_rate(
    ledger: PerformanceLedger,
    aggregator: str,
    chain: str,
) -> float:
    key = f"{aggregator}:{chain.strip().lower()}"
    stats = ledger.get_stats(key)
    if not stats:
        return OPTIMISTIC_PRIOR

    total = stats.get("total_runs", 0)
    if total < MIN_SAMPLE_SIZE:
        return OPTIMISTIC_PRIOR

    successes = stats.get("successes", 0)
    return float(successes) / float(total)


def _success_rate_weight(success_rate: float) -> float:
    clamped = max(0.0, min(1.0, success_rate))
    return clamped**2


def _fill_time_bonus(fill_time_seconds: int) -> float:
    if fill_time_seconds >= BRIDGE_FILL_TIME_BASELINE_SECONDS:
        return 0.0
    fraction = (BRIDGE_FILL_TIME_BASELINE_SECONDS - fill_time_seconds) / float(
        BRIDGE_FILL_TIME_BASELINE_SECONDS
    )
    return fraction * BRIDGE_FILL_TIME_MAX_BONUS

def score_swap_quote(
    quote: "SwapRouteQuote",
    ledger: PerformanceLedger,
    chain_name: str,
) -> float:
    sr = _get_success_rate(ledger, quote.aggregator, chain_name)
    weight = _success_rate_weight(sr)

    output = float(quote.amount_out)

    # Use normalized gas cost.  If both aggregator and fallback failed,
    # apply a conservative 1.0 USD penalty to prevent bias.
    gas_penalty = float(quote.gas_cost_usd if quote.gas_cost_usd is not None else GAS_PENALTY_FALLBACK_USD)

    return output * weight - gas_penalty


def score_bridge_quote(
    quote: "BridgeRouteQuote",
    ledger: PerformanceLedger,
) -> float:
    sr = _get_success_rate(ledger, quote.aggregator, quote.source_chain_name)
    weight = _success_rate_weight(sr)

    output = float(quote.output_amount)
    fill_bonus = _fill_time_bonus(quote.estimated_fill_time_seconds)

    # Bridge gas costs are now normalized in the router.
    gas_penalty = float(quote.gas_cost_usd if quote.gas_cost_usd is not None else GAS_PENALTY_FALLBACK_USD)

    return output * weight + fill_bonus - gas_penalty


def rank_swap_quotes(
    quotes: List["SwapRouteQuote"],
    ledger: PerformanceLedger,
    chain_name: str,
) -> List[Tuple["SwapRouteQuote", float]]:
    scored = [(q, score_swap_quote(q, ledger, chain_name)) for q in quotes]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


def rank_bridge_quotes(
    quotes: List["BridgeRouteQuote"],
    ledger: PerformanceLedger,
) -> List[Tuple["BridgeRouteQuote", float]]:
    scored = [(q, score_bridge_quote(q, ledger)) for q in quotes]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


def pick_best_swap(
    quotes: List["SwapRouteQuote"],
    ledger: PerformanceLedger,
    chain_name: str,
) -> Optional[Tuple["SwapRouteQuote", float]]:
    """
    Return ``(best_quote, score)`` or ``None`` if *quotes* is empty.
    """
    ranked = rank_swap_quotes(quotes, ledger, chain_name)
    return ranked[0] if ranked else None


def pick_best_bridge(
    quotes: List["BridgeRouteQuote"],
    ledger: PerformanceLedger,
) -> Optional[Tuple["BridgeRouteQuote", float]]:
    """
    Return ``(best_quote, score)`` or ``None`` if *quotes* is empty.
    """
    ranked = rank_bridge_quotes(quotes, ledger)
    return ranked[0] if ranked else None

def score_solana_swap_quote(
    quote: "SolanaSwapRouteQuote",
    ledger: PerformanceLedger,
) -> float:
    sr = _get_success_rate(ledger, quote.aggregator, quote.network)
    weight = _success_rate_weight(sr)

    output = float(quote.amount_out)

    # Small penalty for high price impact so that a route with a slightly
    # better output but much worse market impact does not win blindly.
    # Impact is expressed as a percentage (e.g. 0.5 = 0.5 %), so we scale
    # it relative to the output to keep units consistent.
    impact_penalty = float(quote.price_impact_pct) * 0.01 * output

    return output * weight - impact_penalty


def rank_solana_swap_quotes(
    quotes: List["SolanaSwapRouteQuote"],
    ledger: PerformanceLedger,
) -> List[Tuple["SolanaSwapRouteQuote", float]]:
    scored = [(q, score_solana_swap_quote(q, ledger)) for q in quotes]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


def pick_best_solana_swap(
    quotes: List["SolanaSwapRouteQuote"],
    ledger: PerformanceLedger,
) -> Optional[Tuple["SolanaSwapRouteQuote", float]]:
    ranked = rank_solana_swap_quotes(quotes, ledger)
    return ranked[0] if ranked else None
