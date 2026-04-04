from __future__ import annotations

from decimal import Decimal

from core.planning.execution_plan import ExecutionPlan, PlanNode
from core.planning.plan_scorer import (
    failure_risk_for_plan,
    pick_best_simulation,
    prune_dominated_results,
)
from core.planning.vws_execution import CandidatePlanSimulationResult, VWSPlanSimulation


class _LedgerStub:
    def __init__(self, stats_map: dict[str, dict[str, int]]) -> None:
        self._stats_map = stats_map

    def get_stats(self, key: str):
        return self._stats_map.get(key)


def _make_plan(label: str, aggregator: str) -> ExecutionPlan:
    return ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={"chain": "base"},
                metadata={"route": {"aggregator": aggregator}},
            )
        },
        metadata={"vws_label": label},
    )


def _make_result(
    label: str,
    aggregator: str,
    *,
    output: str,
    gas: str,
    latency: int,
) -> CandidatePlanSimulationResult:
    plan = _make_plan(label, aggregator)
    return CandidatePlanSimulationResult(
        plan=plan,
        label=label,
        simulation=VWSPlanSimulation(valid=True),
        final_balances={},
        per_step_state_transitions={},
        total_gas_cost_native=Decimal(gas),
        gas_cost_by_step={"step_0": Decimal(gas)},
        latency_estimate_seconds=latency,
        output_value=Decimal(output),
    )


def test_failure_risk_uses_aggregator_history():
    ledger = _LedgerStub({"agg-a:base": {"successes": 8, "total_runs": 10}})
    plan = _make_plan("plan-a", "agg-a")

    risk = failure_risk_for_plan(plan, ledger=ledger)

    assert abs(risk - 0.2) < 1e-9


def test_pick_best_simulation_prefers_higher_output_after_penalties():
    ledger = _LedgerStub(
        {
            "agg-a:base": {"successes": 9, "total_runs": 10},
            "agg-b:base": {"successes": 5, "total_runs": 10},
        }
    )
    result_a = _make_result("plan-a", "agg-a", output="100", gas="2", latency=60)
    result_b = _make_result("plan-b", "agg-b", output="102", gas="8", latency=120)

    winner, scores = pick_best_simulation([result_a, result_b], ledger=ledger)

    assert winner is not None
    assert winner.label == "plan-a"
    assert [score.label for score in scores] == ["plan-a", "plan-b"]


def test_prune_dominated_results_discards_worse_candidate():
    ledger = _LedgerStub({"agg-a:base": {"successes": 9, "total_runs": 10}})
    strong = _make_result("strong", "agg-a", output="100", gas="1", latency=60)
    weak = _make_result("weak", "agg-a", output="99", gas="2", latency=120)

    kept, pruned = prune_dominated_results([strong, weak], ledger=ledger)

    assert [result.label for result in kept] == ["strong"]
    assert pruned == ["weak"]
