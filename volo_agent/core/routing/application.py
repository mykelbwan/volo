from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from config.planner_config import (
    MAX_CANDIDATE_PLANS,
    MAX_ROUTE_ALTERNATIVES_PER_NODE,
)
from core.memory.ledger import get_ledger
from core.planning.execution_plan import ExecutionPlan, StepStatus
from core.planning.plan_generator import generate_candidates
from core.routing.models import (
    AnyRouteQuote,
    BridgeRouteQuote,
    RouteDecision,
    SolanaSwapRouteQuote,
    SwapRouteQuote,
)
from core.routing.router import RoutePlanner
from core.routing.scorer import (
    score_bridge_quote,
    score_solana_swap_quote,
    score_swap_quote,
)

_LOGGER = logging.getLogger("volo.routing.application")
_SWAP_ROUTE_TTL_SECONDS = 45
_SOLANA_ROUTE_TTL_SECONDS = 30
_BRIDGE_ROUTE_TTL_SECONDS = 45


def build_swap_preflight(quote: SwapRouteQuote) -> Dict[str, Any]:
    return {
        "protocol": quote.aggregator,
        "amount_out": str(quote.amount_out),
        "amount_out_min": str(quote.amount_out_min),
        "price_impact_pct": str(quote.price_impact_pct),
        "gas_estimate": quote.gas_estimate,
        "gas_cost_usd": str(quote.gas_cost_usd) if quote.gas_cost_usd else None,
        "fetched_at": quote.fetched_at,
        "routed_by": "route_planner",
    }


def build_bridge_preflight(quote: BridgeRouteQuote) -> Dict[str, Any]:
    return {
        "protocol": quote.aggregator,
        "output_amount": str(quote.output_amount),
        "total_fee": str(quote.total_fee),
        "total_fee_pct": str(quote.total_fee_pct),
        "estimated_fill_time_seconds": quote.estimated_fill_time_seconds,
        "gas_cost_source": str(quote.gas_cost_source)
        if quote.gas_cost_source
        else None,
        "gas_cost_usd": str(quote.gas_cost_usd)
        if quote.gas_cost_usd
        else None,
        "source_chain": quote.source_chain_name,
        "target_chain": quote.dest_chain_name,
        "fetched_at": quote.fetched_at,
        "routed_by": "route_planner",
    }


def build_swap_route_metadata(
    quote: SwapRouteQuote,
    score: float,
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "aggregator": quote.aggregator,
        "provider": quote.aggregator,
        "chain_id": quote.chain_id,
        "token_in": quote.token_in,
        "token_out": quote.token_out,
        "amount_in": str(quote.amount_in),
        "expected_output": str(quote.amount_out),
        "min_output": str(quote.amount_out_min),
        "amount_out": str(quote.amount_out),
        "amount_out_min": str(quote.amount_out_min),
        "gas_estimate": quote.gas_estimate,
        "price_impact_pct": str(quote.price_impact_pct),
        "score": score,
        "fetched_at": quote.fetched_at,
        "expiry_timestamp": int(quote.fetched_at + _SWAP_ROUTE_TTL_SECONDS),
    }
    execution = quote.raw.get("execution") if isinstance(quote.raw, dict) else None
    if isinstance(execution, dict) and execution:
        meta["execution"] = execution
    if quote.calldata:
        meta["calldata"] = quote.calldata
    if quote.to:
        meta["to"] = quote.to
    if quote.approval_address:
        meta["approval_address"] = quote.approval_address
    if isinstance(quote.raw, dict):
        tx_obj = quote.raw.get("transaction")
        if not isinstance(tx_obj, dict):
            tx_obj = quote.raw.get("tx")
        if isinstance(tx_obj, dict) and tx_obj.get("value") not in (None, ""):
            meta["value"] = tx_obj.get("value")
    return meta


def build_bridge_route_metadata(
    quote: BridgeRouteQuote,
    score: float,
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "aggregator": quote.aggregator,
        "provider": quote.aggregator,
        "token_in": quote.token_symbol,
        "token_out": quote.token_symbol,
        "token_symbol": quote.token_symbol,
        "source_chain_id": quote.source_chain_id,
        "dest_chain_id": quote.dest_chain_id,
        "source_chain": quote.source_chain_name,
        "target_chain": quote.dest_chain_name,
        "amount_in": str(quote.input_amount),
        "input_amount": str(quote.input_amount),
        "expected_output": str(quote.output_amount),
        "min_output": str(quote.output_amount),
        "output_amount": str(quote.output_amount),
        "total_fee": str(quote.total_fee),
        "total_fee_pct": str(quote.total_fee_pct),
        "gas_estimate": 0,
        "gas_cost_usd": str(quote.gas_cost_usd) if quote.gas_cost_usd else None,
        "fill_time_seconds": quote.estimated_fill_time_seconds,
        "score": score,
        "fetched_at": quote.fetched_at,
        "expiry_timestamp": int(quote.fetched_at + _BRIDGE_ROUTE_TTL_SECONDS),
    }
    if quote.calldata:
        meta["calldata"] = quote.calldata
    if quote.to:
        meta["to"] = quote.to
    if quote.tool_data:
        meta["tool_data"] = quote.tool_data
    return meta


def build_solana_swap_preflight(quote: SolanaSwapRouteQuote) -> Dict[str, Any]:
    return {
        "protocol": quote.aggregator,
        "amount_out": str(quote.amount_out),
        "amount_out_min": str(quote.amount_out_min),
        "price_impact_pct": str(quote.price_impact_pct),
        "gas_estimate": 0,
        "gas_cost_usd": None,
        "fetched_at": quote.fetched_at,
        "routed_by": "route_planner",
        "network": quote.network,
    }


def build_solana_swap_route_metadata(
    quote: SolanaSwapRouteQuote,
    score: float,
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "aggregator": quote.aggregator,
        "provider": quote.aggregator,
        "input_mint": quote.input_mint,
        "output_mint": quote.output_mint,
        "amount_in": str(quote.amount_in),
        "amount_in_lamports": quote.amount_in_lamports,
        "expected_output": str(quote.amount_out),
        "min_output": str(quote.amount_out_min),
        "amount_out": str(quote.amount_out),
        "amount_out_min": str(quote.amount_out_min),
        "amount_out_lamports": quote.amount_out_lamports,
        "price_impact_pct": str(quote.price_impact_pct),
        "gas_estimate": 0,
        "score": score,
        "fetched_at": quote.fetched_at,
        "network": quote.network,
        "expiry_timestamp": int(quote.fetched_at + _SOLANA_ROUTE_TTL_SECONDS),
    }
    if quote.swap_transaction:
        meta["swap_transaction"] = quote.swap_transaction
    return meta


def log_decision(
    node_id: str,
    intent_type: str,
    decision: RouteDecision,
    all_count: int,
) -> str:
    selected = decision.selected
    if intent_type == "swap":
        assert isinstance(selected, SwapRouteQuote)
        return (
            f"[ROUTE] {node_id}: selected {selected.aggregator} from "
            f"{all_count} quote(s) — "
            f"out={selected.amount_out:.6f}, "
            f"impact={selected.price_impact_pct:.3f}%, "
            f"gas={selected.gas_estimate}, "
            f"score={decision.score:.4f}, "
            f"calldata={'yes' if selected.calldata else 'no (structured route)'}"
        )
    if intent_type == "solana_swap":
        assert isinstance(selected, SolanaSwapRouteQuote)
        return (
            f"[ROUTE] {node_id}: selected {selected.aggregator} from "
            f"{all_count} Solana quote(s) — "
            f"out={selected.amount_out:.6f}, "
            f"impact={selected.price_impact_pct:.3f}%, "
            f"score={decision.score:.4f}, "
            f"tx={'yes' if selected.swap_transaction else 'no (route invalid)'}"
        )
    assert isinstance(selected, BridgeRouteQuote)
    return (
        f"[ROUTE] {node_id}: selected {selected.aggregator} from "
        f"{all_count} quote(s) — "
        f"out={selected.output_amount:.6f}, "
        f"fee={selected.total_fee_pct:.2f}%, "
        f"fill≈{selected.estimated_fill_time_seconds}s, "
        f"score={decision.score:.4f}, "
        f"calldata={'yes' if selected.calldata else 'no (structured route)'}"
    )


def log_fallback(node_id: str, reason: str) -> str:
    return (
        f"[ROUTE] {node_id}: route planner could not produce a quote ({reason}). "
        "Execution will continue with runtime fallback routing."
    )


def _quote_score(
    *,
    intent_type: str,
    quote: AnyRouteQuote,
    ledger: Any,
    node_args: Dict[str, Any],
) -> float:
    if intent_type == "swap":
        assert isinstance(quote, SwapRouteQuote)
        chain_name = str(node_args.get("chain") or "").strip().lower()
        return score_swap_quote(quote, ledger, chain_name)
    if intent_type == "solana_swap":
        assert isinstance(quote, SolanaSwapRouteQuote)
        return score_solana_swap_quote(quote, ledger)
    assert isinstance(quote, BridgeRouteQuote)
    return score_bridge_quote(quote, ledger)


def _route_metadata_for_quote(
    *,
    intent_type: str,
    quote: AnyRouteQuote,
    score: float,
) -> Dict[str, Any]:
    if intent_type == "swap":
        assert isinstance(quote, SwapRouteQuote)
        return build_swap_route_metadata(quote, score)
    if intent_type == "solana_swap":
        assert isinstance(quote, SolanaSwapRouteQuote)
        return build_solana_swap_route_metadata(quote, score)
    assert isinstance(quote, BridgeRouteQuote)
    return build_bridge_route_metadata(quote, score)


def _preflight_for_quote(
    *,
    intent_type: str,
    quote: AnyRouteQuote,
) -> Dict[str, Any]:
    if intent_type == "swap":
        assert isinstance(quote, SwapRouteQuote)
        return build_swap_preflight(quote)
    if intent_type == "solana_swap":
        assert isinstance(quote, SolanaSwapRouteQuote)
        return build_solana_swap_preflight(quote)
    assert isinstance(quote, BridgeRouteQuote)
    return build_bridge_preflight(quote)


def _route_signature(plan: ExecutionPlan) -> Tuple[str, ...]:
    signature: List[str] = []
    for node_id in sorted(plan.nodes):
        node = plan.nodes[node_id]
        route_meta = plan.nodes[node_id].metadata.get("route") or {}
        signature.append(
            "|".join(
                [
                    node_id,
                    str(node.tool),
                    str(sorted(node.depends_on)),
                    str(sorted((node.args or {}).items())),
                    str(route_meta.get("aggregator") or ""),
                    str(route_meta.get("expected_output") or route_meta.get("output_amount") or ""),
                    str(route_meta.get("fetched_at") or ""),
                ]
            )
        )
    return tuple(signature)


def _candidate_label(plan: ExecutionPlan, route_variant: str) -> str:
    topology_label = str(plan.metadata.get("vws_label") or f"plan_v{plan.version}")
    if route_variant == "best_routes":
        return topology_label
    return f"{topology_label}__{route_variant}"


@dataclass
class _RoutedPlanBundle:
    plan: ExecutionPlan
    route_decisions: Dict[str, Dict[str, Any]]
    preflight_estimates: Dict[str, Dict[str, Any]]
    decision_objects: Dict[str, RouteDecision]
    reasoning_logs: List[str]
    routed_nodes: int
    routable_nodes: int
    timed_out: bool = False


@dataclass
class _CandidatePlanBundle:
    plan: ExecutionPlan
    route_decisions: Dict[str, Dict[str, Any]]
    preflight_estimates: Dict[str, Dict[str, Any]]
    reasoning_logs: List[str]


class PreflightEstimateService:
    def __init__(self, route_planner_cache_ttl_seconds: float = 120.0) -> None:
        self._route_planner_cache_ttl_seconds = route_planner_cache_ttl_seconds

    def inspect_cached_estimate(
        self,
        preflight_estimates: Dict[str, Dict[str, Any]],
        node_id: str,
    ) -> tuple[Dict[str, Any], float, bool]:
        cached = preflight_estimates.get(node_id) or {}
        age_seconds = time.time() - float(cached.get("fetched_at", 0.0))
        use_route_planner_cache = (
            cached.get("routed_by") == "route_planner"
            and age_seconds < self._route_planner_cache_ttl_seconds
        )
        return cached, age_seconds, use_route_planner_cache

    @staticmethod
    def cached_swap_quote(cached: Dict[str, Any]) -> Dict[str, Any] | None:
        if not cached.get("amount_out"):
            return None
        return {
            "protocol": cached.get("protocol"),
            "amount_out": cached.get("amount_out"),
            "amount_out_minimum": cached.get("amount_out_min"),
            "price_impact_pct": cached.get("price_impact_pct"),
            "gas_estimate": cached.get("gas_estimate"),
        }

    @staticmethod
    def cached_bridge_quote(cached: Dict[str, Any]) -> Dict[str, Any] | None:
        if not cached.get("output_amount"):
            return None
        return {
            "protocol": cached.get("protocol"),
            "output_amount": cached.get("output_amount"),
            "total_fee": cached.get("total_fee"),
            "total_fee_pct": cached.get("total_fee_pct"),
            "source_chain": cached.get("source_chain"),
            "target_chain": cached.get("target_chain"),
            "estimated_fill_time_seconds": cached.get(
                "estimated_fill_time_seconds"
            ),
        }


class RoutingApplicationService:
    def __init__(
        self,
        *,
        route_planner: RoutePlanner | None = None,
        global_timeout_seconds: float = 12.0,
        routable_tools: frozenset[str] | None = None,
        max_candidate_plans: int = MAX_CANDIDATE_PLANS,
        max_route_alternatives_per_node: int = MAX_ROUTE_ALTERNATIVES_PER_NODE,
    ) -> None:
        self._route_planner = route_planner or RoutePlanner()
        self._global_timeout_seconds = global_timeout_seconds
        self._routable_tools = routable_tools or frozenset(
            {"swap", "bridge", "solana_swap"}
        )
        self._max_candidate_plans = max(1, int(max_candidate_plans))
        self._max_route_alternatives_per_node = max(
            0,
            int(max_route_alternatives_per_node),
        )

    def _routable_nodes(
        self,
        *,
        plan: ExecutionPlan,
        execution_state: Any,
    ) -> List[Tuple[str, str]]:
        routable: List[Tuple[str, str]] = []
        for node_id, node in plan.nodes.items():
            if node.tool not in self._routable_tools:
                continue
            if node.metadata.get("route"):
                fetched_at = node.metadata["route"].get("fetched_at", 0.0)
                age = time.time() - float(fetched_at)
                if age <= RouteDecision._DEFAULT_TTL:
                    continue
            if execution_state:
                node_state = execution_state.node_states.get(node_id)
                if node_state and node_state.status in {
                    StepStatus.SUCCESS,
                    StepStatus.RUNNING,
                }:
                    continue
            routable.append((node_id, node.tool))
        return routable

    async def _route_single_plan(
        self,
        *,
        plan: ExecutionPlan,
        execution_state: Any,
        sender: str,
        ledger: Any,
    ) -> _RoutedPlanBundle:
        node_start = time.time()
        routable = self._routable_nodes(plan=plan, execution_state=execution_state)
        if not routable:
            enriched_plan = plan.model_copy(deep=True)
            enriched_plan.metadata["route_planner"] = {
                "applied": True,
                "elapsed_seconds": 0.0,
                "routable_nodes": 0,
                "routed_nodes": 0,
                "unrouted_nodes": 0,
            }
            return _RoutedPlanBundle(
                plan=enriched_plan,
                route_decisions={},
                preflight_estimates={},
                decision_objects={},
                reasoning_logs=[],
                routed_nodes=0,
                routable_nodes=0,
            )

        routing_coros = []
        for node_id, tool in routable:
            node = plan.nodes[node_id]
            if tool == "swap":
                routing_coros.append(
                    self._route_planner.get_best_swap_route(
                        node_args=node.args,
                        sender=sender,
                        ledger=ledger,
                    )
                )
            elif tool == "solana_swap":
                routing_coros.append(
                    self._route_planner.get_best_solana_swap_route(
                        node_args=node.args,
                        sender=sender,
                        ledger=ledger,
                    )
                )
            else:
                routing_coros.append(
                    self._route_planner.get_best_bridge_route(
                        node_args=node.args,
                        sender=sender,
                        ledger=ledger,
                    )
                )

        try:
            results = list(
                await asyncio.wait_for(
                    asyncio.gather(*routing_coros, return_exceptions=True),
                    timeout=self._global_timeout_seconds,
                )
            )
        except asyncio.TimeoutError:
            elapsed = time.time() - node_start
            enriched_plan = plan.model_copy(deep=True)
            enriched_plan.metadata["route_planner"] = {
                "applied": True,
                "timed_out": True,
                "elapsed_seconds": elapsed,
                "routable_nodes": len(routable),
                "routed_nodes": 0,
                "unrouted_nodes": len(routable),
            }
            return _RoutedPlanBundle(
                plan=enriched_plan,
                route_decisions={},
                preflight_estimates={},
                decision_objects={},
                reasoning_logs=[
                    f"[ROUTE] Global routing timeout after {elapsed:.1f}s — "
                    "candidate preserved without route metadata"
                ],
                routed_nodes=0,
                routable_nodes=len(routable),
                timed_out=True,
            )

        enriched_plan = plan.model_copy(deep=True)
        route_decisions: Dict[str, Dict[str, Any]] = {}
        preflight_estimates: Dict[str, Dict[str, Any]] = {}
        decision_objects: Dict[str, RouteDecision] = {}
        reasoning_logs: List[str] = []

        for (node_id, intent_type), result in zip(routable, results):
            if isinstance(result, BaseException):
                reasoning_logs.append(
                    log_fallback(node_id, f"{type(result).__name__}: {result}")
                )
                continue
            if result is None:
                reasoning_logs.append(log_fallback(node_id, "no quotes returned"))
                continue

            decision: RouteDecision = result
            decision.node_id = node_id
            decision_objects[node_id] = decision
            selected = decision.selected

            enriched_plan.nodes[node_id].metadata["route"] = _route_metadata_for_quote(
                intent_type=intent_type,
                quote=selected,
                score=decision.score,
            )
            preflight_estimates[node_id] = _preflight_for_quote(
                intent_type=intent_type,
                quote=selected,
            )
            try:
                route_decisions[node_id] = decision.to_dict()
            except Exception as exc:
                _LOGGER.warning(
                    "[route_planner] could not serialise RouteDecision for %s: %s",
                    node_id,
                    exc,
                )

            reasoning_logs.append(
                log_decision(node_id, intent_type, decision, len(decision.all_quotes))
            )

        elapsed = time.time() - node_start
        enriched_plan.metadata["route_planner"] = {
            "applied": True,
            "elapsed_seconds": elapsed,
            "routable_nodes": len(routable),
            "routed_nodes": len(route_decisions),
            "unrouted_nodes": len(routable) - len(route_decisions),
        }

        return _RoutedPlanBundle(
            plan=enriched_plan,
            route_decisions=route_decisions,
            preflight_estimates=preflight_estimates,
            decision_objects=decision_objects,
            reasoning_logs=reasoning_logs,
            routed_nodes=len(route_decisions),
            routable_nodes=len(routable),
        )

    def _materialize_candidate(
        self,
        *,
        base_bundle: _RoutedPlanBundle,
        route_variant: str,
        overrides: Optional[Dict[str, AnyRouteQuote]],
        ledger: Any,
    ) -> _CandidatePlanBundle:
        plan = base_bundle.plan.model_copy(deep=True)
        route_decisions = dict(base_bundle.route_decisions)
        preflight_estimates = dict(base_bundle.preflight_estimates)
        reasoning_logs = list(base_bundle.reasoning_logs)
        label = _candidate_label(plan, route_variant)

        for node_id, quote in (overrides or {}).items():
            node = plan.nodes[node_id]
            intent_type = str(node.tool)
            score = _quote_score(
                intent_type=intent_type,
                quote=quote,
                ledger=ledger,
                node_args=node.args,
            )
            node.metadata["route"] = _route_metadata_for_quote(
                intent_type=intent_type,
                quote=quote,
                score=score,
            )
            preflight_estimates[node_id] = _preflight_for_quote(
                intent_type=intent_type,
                quote=quote,
            )
            route_decisions[node_id] = RouteDecision(
                node_id=node_id,
                intent_type=intent_type,
                selected=quote,
                all_quotes=[quote],
                score=score,
            ).to_dict()
            reasoning_logs.append(
                f"[ROUTE] {label}: override {node_id} with "
                f"{quote.aggregator} (score={score:.4f})"
            )

        plan.metadata["vws_topology_label"] = str(
            plan.metadata.get("vws_label") or f"plan_v{plan.version}"
        )
        plan.metadata["vws_label"] = label
        plan.metadata["candidate_route_decisions"] = route_decisions
        plan.metadata["candidate_preflight_estimates"] = preflight_estimates
        plan.metadata["candidate_reasoning_logs"] = reasoning_logs
        plan.metadata["candidate_signature"] = list(_route_signature(plan))

        return _CandidatePlanBundle(
            plan=plan,
            route_decisions=route_decisions,
            preflight_estimates=preflight_estimates,
            reasoning_logs=reasoning_logs,
        )

    def _candidate_variants(
        self,
        *,
        base_bundle: _RoutedPlanBundle,
        ledger: Any,
    ) -> List[_CandidatePlanBundle]:
        candidates = [
            self._materialize_candidate(
                base_bundle=base_bundle,
                route_variant="best_routes",
                overrides=None,
                ledger=ledger,
            )
        ]

        for node_id in sorted(base_bundle.decision_objects):
            decision = base_bundle.decision_objects[node_id]
            runner_ups = list(decision.all_quotes[1 : self._max_route_alternatives_per_node + 1])
            for idx, quote in enumerate(runner_ups, start=1):
                candidates.append(
                    self._materialize_candidate(
                        base_bundle=base_bundle,
                        route_variant=f"{node_id}_{quote.aggregator}_{idx}",
                        overrides={node_id: quote},
                        ledger=ledger,
                    )
                )
        return candidates

    async def route_plan(self, state: Dict[str, Any]) -> Dict[str, Any]:
        history = state.get("plan_history") or []
        if not history:
            return {}

        original_plan = history[-1]
        if not original_plan or not original_plan.nodes:
            return {}

        execution_state = state.get("execution_state")
        user_info = state.get("user_info") or {}
        sender = ""
        if isinstance(user_info, dict):
            sender = (
                user_info.get("wallet_address")
                or user_info.get("address")
                or user_info.get("sender")
                or ""
            )

        ledger = get_ledger()
        topology_candidates, skip_reason = await generate_candidates(original_plan)
        if not topology_candidates:
            topology_candidates = [original_plan]

        candidate_bundles: List[_CandidatePlanBundle] = []
        baseline_bundle: Optional[_CandidatePlanBundle] = None
        reasoning_logs: List[str] = []

        for topology_idx, topology_plan in enumerate(topology_candidates):
            routed = await self._route_single_plan(
                plan=topology_plan,
                execution_state=execution_state,
                sender=sender,
                ledger=ledger,
            )
            variants = self._candidate_variants(base_bundle=routed, ledger=ledger)
            if topology_idx == 0 and variants:
                baseline_bundle = variants[0]
            candidate_bundles.extend(variants)
            reasoning_logs.extend(routed.reasoning_logs)

        if skip_reason:
            reasoning_logs.append(f"[ROUTE] {skip_reason}")

        deduped: List[_CandidatePlanBundle] = []
        seen_signatures: set[Tuple[str, ...]] = set()
        for bundle in candidate_bundles:
            signature = _route_signature(bundle.plan)
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            deduped.append(bundle)
            if len(deduped) >= self._max_candidate_plans:
                break

        if not deduped:
            return {}

        if baseline_bundle is None:
            baseline_bundle = deduped[0]

        baseline_plan = baseline_bundle.plan.model_copy(deep=True)
        baseline_plan.metadata["route_planner"] = {
            **dict(baseline_plan.metadata.get("route_planner") or {}),
            "candidate_count": len(deduped),
        }

        existing_route_decisions = state.get("route_decisions") or {}
        existing_preflight = state.get("preflight_estimates") or {}

        reasoning_logs.append(
            f"[ROUTE] Produced {len(deduped)} candidate plan(s) "
            f"from {len(topology_candidates)} topology variant(s)."
        )

        return {
            "plan_history": [baseline_plan],
            "candidate_plans": [bundle.plan for bundle in deduped],
            "route_decisions": {
                **existing_route_decisions,
                **baseline_bundle.route_decisions,
            },
            "preflight_estimates": {
                **existing_preflight,
                **baseline_bundle.preflight_estimates,
            },
            "reasoning_logs": reasoning_logs,
        }
