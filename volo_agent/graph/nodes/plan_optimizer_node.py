from __future__ import annotations

import logging
import time
from copy import deepcopy
from typing import Any, Dict, List

from core.memory.ledger import get_ledger
from core.planning.execution_plan import ExecutionPlan, ExecutionState, NodeState, StepStatus
from core.planning.plan_scorer import pick_best_simulation, prune_dominated_results
from core.planning.vws_execution import (
    CandidatePlanSimulationResult,
    simulate_many_execution_plans,
)
from graph.agent_state import AgentState

_LOGGER = logging.getLogger("volo.plan_optimizer")
_WARN_THRESHOLD_SECONDS = 2.0


def _initial_state_for_plan(plan: ExecutionPlan) -> ExecutionState:
    return ExecutionState(
        node_states={node_id: NodeState(node_id=node_id) for node_id in plan.nodes}
    )


def _candidate_route_decisions(
    plan: ExecutionPlan,
    fallback: Dict[str, Dict[str, Any]] | None,
) -> Dict[str, Dict[str, Any]]:
    decisions = plan.metadata.get("candidate_route_decisions")
    if isinstance(decisions, dict):
        return dict(decisions)
    return dict(fallback or {})


def _candidate_preflight_estimates(
    plan: ExecutionPlan,
    fallback: Dict[str, Dict[str, Any]] | None,
) -> Dict[str, Dict[str, Any]]:
    preflight = plan.metadata.get("candidate_preflight_estimates")
    if isinstance(preflight, dict):
        return dict(preflight)
    return dict(fallback or {})


def _result_sort_key(result: CandidatePlanSimulationResult) -> tuple[int, str]:
    return (len(result.per_step_state_transitions), result.label)


def _recovery_result(
    results: List[CandidatePlanSimulationResult],
) -> CandidatePlanSimulationResult | None:
    if not results:
        return None
    return sorted(results, key=_result_sort_key, reverse=True)[0]


def _failure_state_for_result(
    plan: ExecutionPlan,
    result: CandidatePlanSimulationResult,
) -> ExecutionState:
    state = _initial_state_for_plan(plan)
    failure = result.failure
    if failure and failure.node_id in state.node_states:
        state.node_states[failure.node_id] = NodeState(
            node_id=failure.node_id,
            status=StepStatus.FAILED,
            error=failure.reason,
            error_category=failure.category,
        )
    return state


def _serialize_result(result: CandidatePlanSimulationResult) -> Dict[str, Any]:
    failure = result.failure
    return {
        "label": result.label,
        "valid": result.valid,
        "output_value": str(result.output_value),
        "total_gas_cost_native": str(result.total_gas_cost_native),
        "latency_estimate_seconds": result.latency_estimate_seconds,
        "balance_validation_skipped": result.balance_validation_skipped,
        "shared_prefix_hits": result.shared_prefix_hits,
        "failure": (
            {
                "node_id": failure.node_id,
                "tool": failure.tool,
                "category": failure.category,
                "reason": failure.reason,
                "path": failure.path,
            }
            if failure is not None
            else None
        ),
    }


def _build_logs(
    *,
    results: List[CandidatePlanSimulationResult],
    pruned_labels: List[str],
    scores: List[Any],
    selected_label: str | None,
    elapsed_ms: float,
) -> List[str]:
    logs = [
        f"[OPT] Simulated {len(results)} candidate plan(s) in {elapsed_ms:.1f} ms."
    ]
    for result in results:
        if result.valid:
            logs.append(
                f"[OPT] {result.label}: valid "
                f"output={result.output_value} "
                f"gas={result.total_gas_cost_native} "
                f"latency={result.latency_estimate_seconds}s "
                f"prefix_hits={result.shared_prefix_hits}"
            )
        else:
            failure = result.failure
            reason = failure.reason if failure is not None else "simulation failed"
            category = failure.category if failure is not None else "unknown"
            logs.append(
                f"[OPT] {result.label}: rejected "
                f"{category} — {reason}"
            )
    if pruned_labels:
        logs.append(f"[OPT] Pruned dominated candidates: {', '.join(pruned_labels)}")
    for score in scores:
        logs.append(
            f"[OPT] score {score.label}: total={score.total_score:.6f} "
            f"output={score.output_value:.6f} "
            f"gas={score.gas_cost:.6f} "
            f"risk={score.failure_risk:.6f} "
            f"latency={score.latency_seconds:.2f}s"
        )
    if selected_label:
        logs.append(f"[OPT] Selected {selected_label}.")
    return logs


async def plan_optimizer_node(state: AgentState) -> Dict[str, Any]:
    start = time.perf_counter()
    history = state.get("plan_history") or []
    if not history:
        return {}

    original_plan = history[-1]
    if not original_plan or not original_plan.nodes:
        return {}

    execution_state = state.get("execution_state")
    if execution_state and execution_state.node_states:
        any_started = any(
            node_state.status in {StepStatus.RUNNING, StepStatus.SUCCESS}
            for node_state in execution_state.node_states.values()
        )
        if any_started:
            return {}

    candidate_plans = list(state.get("candidate_plans") or [original_plan])
    ledger = get_ledger()
    results = simulate_many_execution_plans(
        plans=candidate_plans,
        balance_snapshot=state.get("balance_snapshot") or {},
        execution_state=execution_state,
        context=state.get("artifacts"),
        default_preflight_estimates=state.get("preflight_estimates") or {},
    )

    valid_results = [result for result in results if result.valid]
    pruned_results, pruned_labels = prune_dominated_results(valid_results, ledger=ledger)
    scored_pool = pruned_results or valid_results
    winner, scores = pick_best_simulation(scored_pool, ledger=ledger)
    elapsed_ms = (time.perf_counter() - start) * 1000

    if elapsed_ms > (_WARN_THRESHOLD_SECONDS * 1000):
        _LOGGER.warning(
            "[plan_optimizer] slow optimization run: %.1f ms across %d candidates",
            elapsed_ms,
            len(candidate_plans),
        )

    debug_payload = {
        "elapsed_ms": round(elapsed_ms, 3),
        "candidate_count": len(candidate_plans),
        "results": [_serialize_result(result) for result in results],
        "pruned_labels": pruned_labels,
        "scores": [
            {
                "label": score.label,
                "total_score": round(score.total_score, 8),
                "output_value": round(score.output_value, 8),
                "gas_cost": round(score.gas_cost, 8),
                "failure_risk": round(score.failure_risk, 8),
                "latency_seconds": round(score.latency_seconds, 3),
                "breakdown": {
                    key: round(value, 8) for key, value in score.breakdown.items()
                },
            }
            for score in scores
        ],
        "selected_label": winner.label if winner is not None else None,
    }

    if not valid_results or winner is None:
        recovery = _recovery_result(results)
        if recovery is None:
            return {
                "route_decision": "PLAN_RETRY",
                "plan_optimizer_debug": {**debug_payload, "all_failed": True},
                "reasoning_logs": [
                    f"[OPT] No candidate plans available after optimization ({elapsed_ms:.1f} ms)."
                ],
            }

        recovery_plan = deepcopy(recovery.plan)
        recovery_plan.version = max(1, int(original_plan.version)) + 1
        recovery_plan.metadata["plan_optimizer"] = {
            "selected": None,
            "all_failed": True,
            "recovery_candidate": recovery.label,
        }
        return {
            "plan_history": [recovery_plan],
            "candidate_plans": candidate_plans,
            "execution_state": _failure_state_for_result(recovery_plan, recovery),
            "route_decision": "PLAN_RETRY",
            "route_decisions": _candidate_route_decisions(
                recovery_plan,
                state.get("route_decisions"),
            ),
            "preflight_estimates": _candidate_preflight_estimates(
                recovery_plan,
                state.get("preflight_estimates"),
            ),
            "plan_optimizer_debug": {**debug_payload, "all_failed": True},
            "reasoning_logs": _build_logs(
                results=results,
                pruned_labels=pruned_labels,
                scores=scores,
                selected_label=None,
                elapsed_ms=elapsed_ms,
            )
            + ["[OPT] All candidates failed simulation; handing off to planner_node for recovery."],
        }

    winning_plan = deepcopy(winner.plan)
    winning_plan.version = max(1, int(original_plan.version)) + 1
    winning_plan.metadata["plan_optimizer"] = {
        "selected": winner.label,
        "rejected": [result.label for result in results if not result.valid],
        "pruned": pruned_labels,
        "score_count": len(scores),
        "elapsed_ms": round(elapsed_ms, 3),
    }

    return {
        "plan_history": [winning_plan],
        "candidate_plans": candidate_plans,
        "execution_state": _initial_state_for_plan(winning_plan),
        "route_decisions": _candidate_route_decisions(
            winning_plan,
            state.get("route_decisions"),
        ),
        "preflight_estimates": _candidate_preflight_estimates(
            winning_plan,
            state.get("preflight_estimates"),
        ),
        "plan_optimizer_debug": debug_payload,
        "reasoning_logs": _build_logs(
            results=results,
            pruned_labels=pruned_labels,
            scores=scores,
            selected_label=winner.label,
            elapsed_ms=elapsed_ms,
        ),
    }
