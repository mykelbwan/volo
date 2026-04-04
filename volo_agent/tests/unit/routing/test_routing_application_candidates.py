import asyncio
from decimal import Decimal

from core.planning.execution_plan import ExecutionPlan, PlanNode
from core.routing.application import RoutingApplicationService
from core.routing.models import RouteDecision, SwapRouteQuote


class _LedgerStub:
    def get_stats(self, key: str):  # noqa: ARG002
        return None


class _PlannerStub:
    async def get_best_swap_route(self, *, node_args, sender, ledger):  # noqa: ARG002
        best = SwapRouteQuote(
            aggregator="agg-best",
            chain_id=8453,
            token_in=node_args["token_in_address"],
            token_out=node_args["token_out_address"],
            amount_in=Decimal("1"),
            amount_out=Decimal("100"),
            amount_out_min=Decimal("99"),
            gas_estimate=150000,
            gas_cost_usd=None,
            price_impact_pct=Decimal("0.1"),
        )
        alt = SwapRouteQuote(
            aggregator="agg-alt",
            chain_id=8453,
            token_in=node_args["token_in_address"],
            token_out=node_args["token_out_address"],
            amount_in=Decimal("1"),
            amount_out=Decimal("101"),
            amount_out_min=Decimal("100"),
            gas_estimate=170000,
            gas_cost_usd=None,
            price_impact_pct=Decimal("0.2"),
        )
        return RouteDecision(
            node_id="step_0",
            intent_type="swap",
            selected=best,
            all_quotes=[best, alt],
            score=100.0,
        )


def test_route_plan_emits_bounded_candidate_plans(monkeypatch):
    plan = ExecutionPlan(
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
                depends_on=[],
                approval_required=True,
            )
        },
    )
    monkeypatch.setattr(
        "core.routing.application.get_ledger",
        lambda: _LedgerStub(),
    )
    monkeypatch.setattr(
        "core.routing.application.generate_candidates",
        lambda current_plan: asyncio.sleep(0, result=([current_plan], None)),
    )

    service = RoutingApplicationService(
        route_planner=_PlannerStub(),
        max_candidate_plans=5,
        max_route_alternatives_per_node=1,
    )
    state = {
        "plan_history": [plan],
        "execution_state": None,
        "user_info": {"wallet_address": "0xabc"},
        "route_decisions": {},
        "preflight_estimates": {},
    }

    result = asyncio.run(service.route_plan(state))

    assert len(result["candidate_plans"]) == 2
    labels = [candidate.metadata["vws_label"] for candidate in result["candidate_plans"]]
    assert labels == ["plan_v1", "plan_v1__step_0_agg-alt_1"]
    assert result["route_decisions"]["step_0"]["selected"]["aggregator"] == "agg-best"
