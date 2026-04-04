import asyncio
from decimal import Decimal

from core.planning.execution_plan import ExecutionPlan, PlanNode
from graph.nodes.plan_optimizer_node import plan_optimizer_node


class _LedgerStub:
    def __init__(self, stats_map):
        self._stats_map = stats_map

    def get_stats(self, key: str):
        return self._stats_map.get(key)


def _base_state(candidate_plans):
    return {
        "user_id": "user-1",
        "provider": "discord",
        "username": "alice",
        "user_info": {"wallet_address": "0xabc"},
        "intents": [],
        "plans": [],
        "goal_parameters": {},
        "plan_history": [candidate_plans[0]],
        "candidate_plans": candidate_plans,
        "execution_state": None,
        "artifacts": {},
        "context": {},
        "route_decision": None,
        "confirmation_status": None,
        "pending_transactions": [],
        "reasoning_logs": [],
        "messages": [],
        "fee_quotes": [],
        "balance_snapshot": {},
        "resource_snapshots": {},
        "native_requirements": {},
        "reservation_requirements": {},
        "projected_deltas": {},
        "preflight_estimates": {},
        "vws_simulation": {},
        "vws_failure": None,
        "route_decisions": {},
        "plan_optimizer_debug": None,
        "trigger_id": None,
        "is_triggered_execution": None,
    }


def _candidate(label: str, aggregator: str, amount_out: str) -> ExecutionPlan:
    return ExecutionPlan(
        goal="swap",
        nodes={
            "step_0": PlanNode(
                id="step_0",
                tool="swap",
                args={
                    "sender": "0xabc",
                    "chain": "Base",
                    "token_in_address": "0xusdc",
                    "token_out_address": "0xeth",
                    "amount_in": "1",
                },
                metadata={
                    "route": {
                        "aggregator": aggregator,
                        "amount_out": amount_out,
                        "amount_out_min": amount_out,
                        "gas_estimate": 200000,
                    }
                },
                depends_on=[],
                approval_required=True,
            )
        },
        metadata={
            "vws_label": label,
            "candidate_preflight_estimates": {
                "step_0": {"amount_out": amount_out, "gas_estimate": 200000}
            },
            "candidate_route_decisions": {
                "step_0": {
                    "node_id": "step_0",
                    "intent_type": "swap",
                    "selected": {
                        "aggregator": aggregator,
                        "chain_id": 8453,
                        "token_in": "0xusdc",
                        "token_out": "0xeth",
                        "amount_in": "1",
                        "amount_out": amount_out,
                        "amount_out_min": amount_out,
                        "gas_estimate": 200000,
                        "gas_cost_usd": None,
                        "price_impact_pct": "0.1",
                        "calldata": None,
                        "to": None,
                        "approval_address": None,
                        "fetched_at": 1.0,
                    },
                    "all_quotes": [],
                    "score": 1.0,
                    "decided_at": 1.0,
                }
            },
        },
    )


def test_plan_optimizer_selects_best_candidate(monkeypatch):
    low_risk = _candidate("candidate_a", "agg-a", "100")
    high_output = _candidate("candidate_b", "agg-b", "101")
    state = _base_state([low_risk, high_output])
    monkeypatch.setattr(
        "graph.nodes.plan_optimizer_node.get_ledger",
        lambda: _LedgerStub(
            {
                "agg-a:base": {"successes": 10, "total_runs": 10},
                "agg-b:base": {"successes": 5, "total_runs": 10},
            }
        ),
    )

    result = asyncio.run(plan_optimizer_node(state))

    assert result["plan_history"][0].metadata["vws_label"] == "candidate_a"
    assert result["route_decisions"]["step_0"]["selected"]["aggregator"] == "agg-a"
    assert result["plan_optimizer_debug"]["selected_label"] == "candidate_a"


def test_plan_optimizer_hands_off_to_planner_when_all_candidates_fail(monkeypatch):
    candidate = _candidate("candidate_fail", "agg-a", "100")
    state = _base_state([candidate])
    state["balance_snapshot"] = {
        "0xabc|base|0x0000000000000000000000000000000000000000": "0",
        "0xabc|base|0xusdc": "0",
    }
    monkeypatch.setattr(
        "graph.nodes.plan_optimizer_node.get_ledger",
        lambda: _LedgerStub({}),
    )

    result = asyncio.run(plan_optimizer_node(state))

    assert result["route_decision"] == "PLAN_RETRY"
    assert result["execution_state"].node_states["step_0"].status == "failed"
    assert result["plan_optimizer_debug"]["all_failed"] is True
