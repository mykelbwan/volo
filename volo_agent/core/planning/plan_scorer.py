from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from config.planner_config import (
    MIN_TOTAL_RUNS_FOR_RELIABILITY,
    PLAN_SCORE_GAS_WEIGHT,
    PLAN_SCORE_LATENCY_WEIGHT,
    PLAN_SCORE_OUTPUT_WEIGHT,
    PLAN_SCORE_RISK_WEIGHT,
)
from core.planning.vws_execution import CandidatePlanSimulationResult

_OPTIMISTIC_SUCCESS_RATE = 0.98


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


@dataclass(frozen=True)
class PlanScoreWeights:
    output_weight: float = PLAN_SCORE_OUTPUT_WEIGHT
    gas_weight: float = PLAN_SCORE_GAS_WEIGHT
    risk_weight: float = PLAN_SCORE_RISK_WEIGHT
    latency_weight: float = PLAN_SCORE_LATENCY_WEIGHT

    @classmethod
    def from_env(cls) -> "PlanScoreWeights":
        return cls(
            output_weight=_env_float(
                "PLAN_SCORE_OUTPUT_WEIGHT",
                PLAN_SCORE_OUTPUT_WEIGHT,
            ),
            gas_weight=_env_float(
                "PLAN_SCORE_GAS_WEIGHT",
                PLAN_SCORE_GAS_WEIGHT,
            ),
            risk_weight=_env_float(
                "PLAN_SCORE_RISK_WEIGHT",
                PLAN_SCORE_RISK_WEIGHT,
            ),
            latency_weight=_env_float(
                "PLAN_SCORE_LATENCY_WEIGHT",
                PLAN_SCORE_LATENCY_WEIGHT,
            ),
        )


@dataclass
class PlanScore:
    label: str
    total_score: float
    output_value: float
    gas_cost: float
    failure_risk: float
    latency_seconds: float
    plan: Any
    breakdown: Dict[str, float] = field(default_factory=dict)
    estimated_gas_cost_usd: float = 0.0
    estimated_bridge_fee_usd: float = 0.0
    reliability_bonus_usd: float = 0.0


def _label_for_plan(plan: Any) -> str:
    return str(getattr(plan, "metadata", {}).get("vws_label") or f"plan_v{plan.version}")


def _success_rate(ledger: Any, *, aggregator: str, chain: str) -> float:
    if ledger is None or not aggregator or not chain:
        return _OPTIMISTIC_SUCCESS_RATE
    key = f"{aggregator}:{chain.strip().lower()}"
    try:
        stats = ledger.get_stats(key)
    except Exception:
        stats = None
    if not stats:
        return _OPTIMISTIC_SUCCESS_RATE

    total = int(stats.get("total_runs", 0) or 0)
    if total < MIN_TOTAL_RUNS_FOR_RELIABILITY:
        return _OPTIMISTIC_SUCCESS_RATE

    successes = float(stats.get("successes", 0) or 0)
    if total <= 0:
        return _OPTIMISTIC_SUCCESS_RATE
    return max(0.0, min(1.0, successes / float(total)))


def failure_risk_for_plan(plan: Any, ledger: Any = None) -> float:
    node_rates: List[float] = []
    for node in getattr(plan, "nodes", {}).values():
        route_meta = (getattr(node, "metadata", {}) or {}).get("route") or {}
        aggregator = str(route_meta.get("aggregator") or "").strip().lower()
        chain = str(
            route_meta.get("source_chain")
            or route_meta.get("chain")
            or node.args.get("source_chain")
            or node.args.get("chain")
            or node.args.get("network")
            or ""
        ).strip().lower()
        node_rates.append(_success_rate(ledger, aggregator=aggregator, chain=chain))

    if not node_rates:
        return 1.0 - _OPTIMISTIC_SUCCESS_RATE
    average_success = sum(node_rates) / float(len(node_rates))
    return max(0.0, min(1.0, 1.0 - average_success))


def score_simulation_result(
    result: CandidatePlanSimulationResult,
    *,
    ledger: Any = None,
    weights: PlanScoreWeights | None = None,
) -> PlanScore:
    weights = weights or PlanScoreWeights.from_env()
    output_value = float(result.output_value)
    gas_cost = float(result.total_gas_cost_native)
    failure_risk = failure_risk_for_plan(result.plan, ledger=ledger)
    latency_seconds = float(result.latency_estimate_seconds)

    total_score = (
        (output_value * float(weights.output_weight))
        - (gas_cost * float(weights.gas_weight))
        - (failure_risk * float(weights.risk_weight))
        - (latency_seconds * float(weights.latency_weight))
    )

    return PlanScore(
        label=result.label,
        total_score=total_score,
        output_value=output_value,
        gas_cost=gas_cost,
        failure_risk=failure_risk,
        latency_seconds=latency_seconds,
        plan=result.plan,
        breakdown={
            "output_value": output_value,
            "gas_cost": gas_cost,
            "failure_risk": failure_risk,
            "latency_seconds": latency_seconds,
            "output_component": output_value * float(weights.output_weight),
            "gas_penalty": gas_cost * float(weights.gas_weight),
            "risk_penalty": failure_risk * float(weights.risk_weight),
            "latency_penalty": latency_seconds * float(weights.latency_weight),
        },
    )


def prune_dominated_results(
    results: List[CandidatePlanSimulationResult],
    *,
    ledger: Any = None,
) -> Tuple[List[CandidatePlanSimulationResult], List[str]]:
    valid_results = [result for result in results if result.valid]
    if len(valid_results) <= 1:
        return valid_results, []

    risk_by_label = {
        result.label: failure_risk_for_plan(result.plan, ledger=ledger)
        for result in valid_results
    }
    kept: List[CandidatePlanSimulationResult] = []
    pruned: List[str] = []

    for candidate in valid_results:
        dominated = False
        for other in valid_results:
            if other.label == candidate.label:
                continue

            better_or_equal = (
                other.output_value >= candidate.output_value
                and other.total_gas_cost_native <= candidate.total_gas_cost_native
                and risk_by_label[other.label] <= risk_by_label[candidate.label]
                and other.latency_estimate_seconds <= candidate.latency_estimate_seconds
            )
            strictly_better = (
                other.output_value > candidate.output_value
                or other.total_gas_cost_native < candidate.total_gas_cost_native
                or risk_by_label[other.label] < risk_by_label[candidate.label]
                or other.latency_estimate_seconds < candidate.latency_estimate_seconds
            )
            if better_or_equal and strictly_better:
                dominated = True
                break

        if dominated:
            pruned.append(candidate.label)
        else:
            kept.append(candidate)

    return kept, pruned


def pick_best_simulation(
    results: List[CandidatePlanSimulationResult],
    *,
    ledger: Any = None,
    weights: PlanScoreWeights | None = None,
) -> Tuple[CandidatePlanSimulationResult | None, List[PlanScore]]:
    scored = [
        score_simulation_result(result, ledger=ledger, weights=weights)
        for result in results
        if result.valid
    ]
    scored.sort(
        key=lambda score: (
            -score.total_score,
            -score.output_value,
            score.gas_cost,
            score.failure_risk,
            score.latency_seconds,
            score.label,
        )
    )
    if not scored:
        return None, []

    winner_label = scored[0].label
    winner = next((result for result in results if result.label == winner_label), None)
    return winner, scored


def pick_best(plans: List[Any], ledger: Any = None) -> Tuple[Any, List[PlanScore]]:
    pseudo_results: List[CandidatePlanSimulationResult] = []
    for plan in plans:
        meta = getattr(plan, "metadata", {}) or {}
        gas_cost = Decimal(str(meta.get("estimated_gas_cost_usd") or 0))
        output_value = Decimal(str(meta.get("estimated_output_value") or 0))
        pseudo_results.append(
            CandidatePlanSimulationResult(
                plan=plan,
                label=_label_for_plan(plan),
                simulation=type("Sim", (), {"valid": True, "failure": None})(),
                final_balances={},
                per_step_state_transitions={},
                total_gas_cost_native=gas_cost,
                gas_cost_by_step={},
                latency_estimate_seconds=int(meta.get("estimated_latency_seconds") or 0),
                output_value=output_value,
            )
        )

    winner, scores = pick_best_simulation(pseudo_results, ledger=ledger)
    return (winner.plan if winner is not None else None), scores
