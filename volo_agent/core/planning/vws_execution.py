from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from config.chains import get_chain_by_name
from core.fees.chains import is_native_token, resolve_fee_chain
from core.planning.execution_plan import (
    ExecutionPlan,
    ExecutionState,
    NodeState,
    StepStatus,
    resolve_dynamic_args,
)
from core.planning.vws import (
    FALLBACK_GAS_PRICE_WEI,
    GAS_UNITS,
    NATIVE_ADDRESS,
    StepResult,
    VirtualWalletState,
)
from core.routing.route_meta import (
    canonicalize_route_meta,
    is_route_expired,
    log_execution_comparison,
    log_route_expiry,
    log_route_validation,
    preflight_from_route_meta,
    route_meta_strictly_enforced,
    validate_route_meta,
)
from core.transfers.planning import (
    TransferPlanningMetadata,
    resolve_transfer_planning_metadata,
)

_SOLANA_NETWORK_FEE_RESERVE = Decimal("0.00001")
_SOLANA_NATIVE_TRANSFER_RESERVE = _SOLANA_NETWORK_FEE_RESERVE
_SOLANA_TOKEN_TRANSFER_RESERVE = _SOLANA_NETWORK_FEE_RESERVE
_EVM_GAS_BUFFER = Decimal("1.5")
_DEFAULT_GAS_UNITS = 200_000
_LOGGER = logging.getLogger("volo.planning.vws_execution")


def _to_decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _resource_key(sender: str, chain_name: str, token_ref: str) -> str:
    return (
        f"{str(sender).strip().lower()}|"
        f"{str(chain_name).strip().lower()}|"
        f"{str(token_ref).strip().lower()}"
    )


def _topological_order(plan: ExecutionPlan) -> List[str]:
    in_degree: Dict[str, int] = {node_id: 0 for node_id in plan.nodes}
    children: Dict[str, List[str]] = {node_id: [] for node_id in plan.nodes}
    for node_id, node in plan.nodes.items():
        for dep_id in node.depends_on:
            if dep_id not in in_degree:
                continue
            in_degree[node_id] += 1
            children.setdefault(dep_id, []).append(node_id)

    queue = deque(
        sorted(node_id for node_id, degree in in_degree.items() if degree == 0)
    )
    ordered: List[str] = []
    while queue:
        node_id = queue.popleft()
        ordered.append(node_id)
        for child_id in sorted(children.get(node_id) or []):
            in_degree[child_id] = max(0, in_degree.get(child_id, 0) - 1)
            if in_degree[child_id] == 0:
                queue.append(child_id)

    for node_id in plan.nodes:
        if node_id not in ordered:
            ordered.append(node_id)
    return ordered


def _normalize_token_ref(token_ref: str | None, node_args: Dict[str, Any], tool: str | None = None) -> str:
    raw = str(token_ref or "").strip().lower()
    if not raw:
        return raw
    fee_chain = resolve_fee_chain(node_args, tool=tool)
    if fee_chain is None:
        return raw
    if fee_chain.family == "evm":
        try:
            chain = get_chain_by_name(fee_chain.name)
            wrapped = str(getattr(chain, "wrapped_native", "") or "").strip().lower()
            if raw == NATIVE_ADDRESS or (wrapped and raw == wrapped):
                return NATIVE_ADDRESS
        except Exception:
            if raw == NATIVE_ADDRESS:
                return NATIVE_ADDRESS
    if is_native_token(raw, fee_chain):
        return str(fee_chain.native_token_ref).strip().lower()
    return raw


def _native_token_ref(node_args: Dict[str, Any], tool: str | None = None) -> str:
    fee_chain = resolve_fee_chain(node_args, tool=tool)
    if fee_chain is None:
        return NATIVE_ADDRESS
    return str(fee_chain.native_token_ref).strip().lower()


def _node_chain_name(node_tool: str, node_args: Dict[str, Any]) -> str:
    if node_tool == "bridge":
        return str(
            node_args.get("source_chain") or node_args.get("chain") or ""
        ).strip()
    return str(node_args.get("chain") or node_args.get("network") or "").strip()


def _node_native_chain_name(node_tool: str, node_args: Dict[str, Any]) -> str:
    return _node_chain_name(node_tool, node_args)


def _node_family(node_args: Dict[str, Any], tool: str | None = None) -> str:
    fee_chain = resolve_fee_chain(node_args, tool=tool)
    return fee_chain.family if fee_chain is not None else "evm"


def _transfer_gas_profile(transfer_meta: TransferPlanningMetadata) -> str:
    if transfer_meta.family == "evm":
        if transfer_meta.asset_kind == "native":
            return "evm_native_transfer"
        return "evm_token_transfer"
    if transfer_meta.family == "solana":
        if transfer_meta.asset_kind == "native":
            return "solana_native_transfer"
        return "solana_token_transfer"
    return "transfer"


def _estimate_native_gas_cost(
    *,
    node_tool: str,
    node_args: Dict[str, Any],
    preflight: Dict[str, Any],
    family: str | None = None,
    gas_profile: str | None = None,
) -> Decimal:
    family = str(family or _node_family(node_args, tool=node_tool)).strip().lower()
    gas_profile = str(gas_profile or node_tool).strip().lower()
    if family == "solana":
        if gas_profile == "solana_native_transfer":
            return _SOLANA_NATIVE_TRANSFER_RESERVE
        if gas_profile == "solana_token_transfer":
            return _SOLANA_TOKEN_TRANSFER_RESERVE
        return _SOLANA_NETWORK_FEE_RESERVE

    if node_tool == "bridge":
        gas_cost = _to_decimal(preflight.get("gas_cost_source"))
        if gas_cost is not None and gas_cost >= 0:
            return gas_cost * _EVM_GAS_BUFFER

    gas_units_raw = preflight.get("gas_estimate")
    try:
        if gas_units_raw is not None:
            gas_units = int(gas_units_raw)
        else:
            gas_units = GAS_UNITS.get(gas_profile, _DEFAULT_GAS_UNITS)
    except Exception:
        gas_units = GAS_UNITS.get(gas_profile, _DEFAULT_GAS_UNITS)

    if gas_units <= 0:
        return Decimal("0")
    return (
        Decimal(gas_units) * Decimal(FALLBACK_GAS_PRICE_WEI) * _EVM_GAS_BUFFER
    ) / Decimal(10**18)


def _extract_simulated_output(
    *,
    node_tool: str,
    node_args: Dict[str, Any],
    preflight: Dict[str, Any],
) -> tuple[str | None, Decimal | None]:
    if node_tool == "swap":
        return (
            _normalize_token_ref(node_args.get("token_out_address"), node_args),
            _to_decimal(preflight.get("amount_out") or preflight.get("amount_out_min")),
        )
    if node_tool == "solana_swap":
        return (
            _normalize_token_ref(node_args.get("token_out_mint"), node_args),
            _to_decimal(preflight.get("amount_out") or preflight.get("amount_out_min")),
        )
    if node_tool == "bridge":
        return (
            _normalize_token_ref(
                node_args.get("target_address")
                or node_args.get("dest_token_address")
                or node_args.get("source_address"),
                {"chain": node_args.get("target_chain")},
            ),
            _to_decimal(preflight.get("output_amount")),
        )
    return None, None


def _build_node_output_artifact(
    *,
    node_tool: str,
    amount_out: Decimal | None,
) -> Dict[str, Any]:
    if amount_out is None:
        return {}
    if node_tool == "bridge":
        return {
            "output_amount": str(amount_out),
            "details": {"output_amount": str(amount_out)},
        }
    return {
        "amount_out": str(amount_out),
        "output_amount": str(amount_out),
        "details": {"output_amount": str(amount_out)},
    }


def _extract_actual_output_for_vws(
    *,
    node_tool: str,
    output_artifact: Dict[str, Any],
) -> Decimal | None:
    if node_tool == "bridge":
        return _to_decimal(output_artifact.get("output_amount"))
    return _to_decimal(
        output_artifact.get("amount_out")
        or output_artifact.get("output_amount")
        or output_artifact.get("amount_out_minimum")
    )


def _find_unresolved_marker(value: Any, *, path: str = "args") -> str | None:
    if isinstance(value, str) and "{{" in value and "}}" in value:
        return path
    if isinstance(value, dict):
        for key, nested in value.items():
            found = _find_unresolved_marker(nested, path=f"{path}.{key}")
            if found:
                return found
        return None
    if isinstance(value, (list, tuple)):
        for idx, nested in enumerate(value):
            found = _find_unresolved_marker(nested, path=f"{path}[{idx}]")
            if found:
                return found
        return None
    return None


def _clone_json_like(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _clone_json_like(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clone_json_like(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_clone_json_like(item) for item in value)
    return value


def _clone_execution_state(state: ExecutionState) -> ExecutionState:
    cloned_artifacts = dict(state.artifacts)
    outputs = state.artifacts.get("outputs")
    if isinstance(outputs, dict):
        # Prefix checkpoints only contain simple JSON-like artifacts; cloning
        # them directly is cheaper than a full generic deepcopy of the state.
        cloned_artifacts["outputs"] = {
            key: _clone_json_like(value) for key, value in outputs.items()
        }
    return ExecutionState(
        node_states=dict(state.node_states),
        artifacts=cloned_artifacts,
        completed=bool(state.completed),
    )


def _clone_node_metadata(
    node_metadata: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    return {
        node_id: _clone_json_like(metadata)
        for node_id, metadata in node_metadata.items()
    }


@dataclass(frozen=True)
class VWSRequirement:
    sender: str
    chain: str
    token_ref: str
    required: Decimal
    kind: str


@dataclass(frozen=True)
class VWSFailure:
    node_id: str
    tool: str
    category: str
    reason: str
    path: str | None = None


@dataclass
class VWSPlanSimulation:
    valid: bool
    projected_deltas: Dict[str, Decimal] = field(default_factory=dict)
    native_requirements: Dict[str, Decimal] = field(default_factory=dict)
    reservation_requirements: Dict[str, List[VWSRequirement]] = field(
        default_factory=dict
    )
    node_metadata: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    failure: Optional[VWSFailure] = None
    ending_balances: Dict[str, str] = field(default_factory=dict)


@dataclass
class CandidatePlanSimulationResult:
    plan: ExecutionPlan
    label: str
    simulation: VWSPlanSimulation
    final_balances: Dict[str, str]
    per_step_state_transitions: Dict[str, Dict[str, Any]]
    total_gas_cost_native: Decimal
    gas_cost_by_step: Dict[str, Decimal]
    latency_estimate_seconds: int
    output_value: Decimal
    balance_validation_skipped: bool = False
    shared_prefix_hits: int = 0

    @property
    def valid(self) -> bool:
        return bool(self.simulation.valid)

    @property
    def failure(self) -> Optional[VWSFailure]:
        return self.simulation.failure


@dataclass
class _SimulationCheckpoint:
    current_state: ExecutionState
    vws_by_sender: Dict[str, VirtualWalletState]
    projected_deltas: Dict[str, Decimal]
    native_requirements: Dict[str, Decimal]
    reservation_requirements: Dict[str, List[VWSRequirement]]
    node_metadata: Dict[str, Dict[str, Any]]


def simulate_execution_plan(
    *,
    plan: ExecutionPlan,
    balance_snapshot: Dict[str, str],
    execution_state: ExecutionState | None,
    context: Dict[str, Any] | None,
    preflight_estimates: Dict[str, Dict[str, Any]] | None,
    platform_fee_native_by_node: Dict[str, Decimal] | None = None,
    prefix_cache: Dict[str, _SimulationCheckpoint] | None = None,
    cache_stats: Dict[str, int] | None = None,
) -> VWSPlanSimulation:
    preflight_estimates = preflight_estimates or {}
    platform_fee_native_by_node = platform_fee_native_by_node or {}
    route_planner_meta = getattr(plan, "metadata", {}) or {}
    route_meta_enforced = route_meta_strictly_enforced(route_planner_meta)
    current_state = ExecutionState(
        node_states=dict(execution_state.node_states) if execution_state else {},
        artifacts=dict(execution_state.artifacts) if execution_state else {},
        completed=bool(execution_state.completed) if execution_state else False,
    )
    outputs = current_state.artifacts.get("outputs")
    if not isinstance(outputs, dict):
        outputs = {}
        current_state.artifacts["outputs"] = outputs

    vws_by_sender: Dict[str, VirtualWalletState] = {}
    initial_balances_by_sender: Dict[str, Dict[Tuple[str, str], Decimal]] = {}
    for raw_key, raw_value in (balance_snapshot or {}).items():
        parts = str(raw_key).split("|")
        if len(parts) != 3:
            continue
        sender, chain_name, token_ref = parts
        amount = _to_decimal(raw_value)
        if amount is None:
            continue
        sender_key = sender.strip().lower()
        chain_key = chain_name.strip().lower()
        token_key = token_ref.strip().lower()
        initial_balances_by_sender.setdefault(sender_key, {})[
            (chain_key, token_key)
        ] = amount

    for sender, balances in initial_balances_by_sender.items():
        vws_by_sender[sender] = VirtualWalletState(balances=balances)

    projected_deltas: Dict[str, Decimal] = {}
    native_requirements: Dict[str, Decimal] = {}
    reservation_requirements: Dict[str, List[VWSRequirement]] = {}
    node_metadata: Dict[str, Dict[str, Any]] = {}
    route_now = int((context or {}).get("route_meta_now") or time.time())
    prefix_key = "root"

    for node_id in _topological_order(plan):
        node = plan.nodes[node_id]
        existing = current_state.node_states.get(node_id)
        if existing and existing.status in {StepStatus.SUCCESS, StepStatus.SKIPPED}:
            node_metadata[node_id] = {
                "status": str(existing.status),
                "skipped_simulation": True,
            }
            continue

        if node.tool == "check_balance":
            node_metadata[node_id] = {
                "status": "success",
                "read_only": True,
                "gas_cost_native": "0",
                "balance_deltas": {},
            }
            continue

        resolved_args = resolve_dynamic_args(
            node.args,
            current_state,
            context=context,
        )
        unresolved_path = _find_unresolved_marker(resolved_args)
        if unresolved_path:
            return VWSPlanSimulation(
                valid=False,
                projected_deltas=projected_deltas,
                native_requirements=native_requirements,
                reservation_requirements=reservation_requirements,
                node_metadata=node_metadata,
                failure=VWSFailure(
                    node_id=node_id,
                    tool=node.tool,
                    category="dependency_resolution_failure",
                    reason="unresolved dynamic dependency during VWS simulation",
                    path=unresolved_path,
                ),
            )

        sender = str(resolved_args.get("sender") or "").strip().lower()
        if not sender:
            return VWSPlanSimulation(
                valid=False,
                projected_deltas=projected_deltas,
                native_requirements=native_requirements,
                reservation_requirements=reservation_requirements,
                node_metadata=node_metadata,
                failure=VWSFailure(
                    node_id=node_id,
                    tool=node.tool,
                    category="dependency_resolution_failure",
                    reason="missing sender for actionable step",
                ),
            )

        vws = vws_by_sender.setdefault(
            sender,
            VirtualWalletState(balances=initial_balances_by_sender.get(sender, {})),
        )
        route_meta = node.metadata.get("route") if node.metadata else None
        route_validation = validate_route_meta(
            tool=node.tool,
            resolved_args=resolved_args,
            route_meta=route_meta,
            strict_missing=route_meta_enforced,
        )
        if not route_validation.valid:
            _LOGGER.warning(
                "route_validation %s",
                {
                    "event": "route_validation",
                    "node_id": node_id,
                    "tool": node.tool,
                    "valid": False,
                    "provider": None,
                    "token_in": None,
                    "token_out": None,
                    "amount_in": None,
                    "expected_output": None,
                    "min_output": None,
                    "error": route_validation.reason or "invalid route metadata",
                },
            )
            return VWSPlanSimulation(
                valid=False,
                projected_deltas=projected_deltas,
                native_requirements=native_requirements,
                reservation_requirements=reservation_requirements,
                node_metadata=node_metadata,
                failure=VWSFailure(
                    node_id=node_id,
                    tool=node.tool,
                    category="route_meta_validation_failure",
                    reason=route_validation.reason or "invalid route metadata",
                ),
            )

        canonical_route_meta = None
        preflight = dict(preflight_estimates.get(node_id) or {})
        if route_validation.should_use_route_meta and isinstance(route_meta, dict):
            try:
                validate_route_meta(route_meta)
                canonical_route_meta = canonicalize_route_meta(
                    route_meta, tool=node.tool
                )
            except Exception as exc:
                _LOGGER.warning(
                    "route_validation %s",
                    {
                        "event": "route_validation",
                        "node_id": node_id,
                        "tool": node.tool,
                        "valid": False,
                        "provider": None,
                        "token_in": None,
                        "token_out": None,
                        "amount_in": None,
                        "expected_output": None,
                        "min_output": None,
                        "error": str(exc),
                    },
                )
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="route_meta_validation_failure",
                        reason=str(exc),
                    ),
                )
            validation_payload = log_route_validation(
                route_meta=canonical_route_meta,
                valid=True,
                tool=node.tool,
            )
            validation_payload["node_id"] = node_id
            _LOGGER.info("route_validation %s", validation_payload)
            expiry_payload = log_route_expiry(
                route_meta=canonical_route_meta, now=route_now
            )
            expiry_payload["node_id"] = node_id
            if is_route_expired(canonical_route_meta, route_now):
                _LOGGER.warning("route_expiry %s", expiry_payload)
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="route_expired",
                        reason=(
                            f"planned route expired at "
                            f"{canonical_route_meta.expiry_timestamp}"
                        ),
                    ),
                )
            _LOGGER.info("route_expiry %s", expiry_payload)
            preflight = preflight_from_route_meta(node.tool, route_meta)
        elif route_validation.should_use_route_meta:
            preflight = {}

        step_cache_key = _step_prefix_key(
            prefix_key=prefix_key,
            node_id=node_id,
            tool=node.tool,
            resolved_args=resolved_args,
            preflight=preflight,
            route_meta=route_meta,
        )
        if prefix_cache and step_cache_key in prefix_cache:
            checkpoint = _clone_checkpoint(prefix_cache[step_cache_key])
            current_state = checkpoint.current_state
            vws_by_sender = checkpoint.vws_by_sender
            projected_deltas = checkpoint.projected_deltas
            native_requirements = checkpoint.native_requirements
            reservation_requirements = checkpoint.reservation_requirements
            node_metadata = checkpoint.node_metadata
            outputs = current_state.artifacts.get("outputs")
            if not isinstance(outputs, dict):
                outputs = {}
                current_state.artifacts["outputs"] = outputs
            prefix_key = step_cache_key
            if cache_stats is not None:
                cache_stats["hits"] = int(cache_stats.get("hits", 0)) + 1
            continue
        before = vws.snapshot()
        step_result: StepResult

        if node.tool in {"swap", "solana_swap"}:
            chain_name = _node_chain_name(node.tool, resolved_args)
            token_in_ref = _normalize_token_ref(
                resolved_args.get("token_in_address")
                or resolved_args.get("token_in_mint"),
                resolved_args,
            )
            amount_in = _to_decimal(resolved_args.get("amount_in"))
            if (
                not chain_name
                or not token_in_ref
                or amount_in is None
                or amount_in <= 0
            ):
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="dependency_resolution_failure",
                        reason="swap step is missing chain, token, or amount",
                    ),
                )
            token_out_ref, amount_out = _extract_simulated_output(
                node_tool=node.tool,
                node_args=resolved_args,
                preflight=preflight,
            )
            if (
                canonical_route_meta is not None
                and amount_out is not None
                and amount_out < canonical_route_meta.min_output
            ):
                payload = log_execution_comparison(
                    route_meta=canonical_route_meta,
                    node_id=node_id,
                    tool=node.tool,
                    actual_output=amount_out,
                )
                _LOGGER.warning("route_execution_output %s", payload)
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="slippage_exceeded",
                        reason=(
                            f"simulated output {amount_out} is below minimum "
                            f"output {canonical_route_meta.min_output}"
                        ),
                    ),
                )
            gas_cost = _estimate_native_gas_cost(
                node_tool=node.tool,
                node_args=resolved_args,
                preflight=preflight,
            )
            step_result = vws.simulate_swap(
                chain=chain_name,
                chain_id=None,
                token_in_address=token_in_ref,
                amount_in=amount_in,
                native_address=_native_token_ref(resolved_args),
                token_out_address=token_out_ref,
                amount_out=amount_out,
                gas_cost_native_override=gas_cost,
                tool_name=node.tool,
            )
            if step_result.success:
                reservation_requirements.setdefault(node_id, []).append(
                    VWSRequirement(
                        sender=sender,
                        chain=chain_name,
                        token_ref=token_in_ref,
                        required=amount_in,
                        kind="token_spend",
                    )
                )
                if gas_cost > 0:
                    native_requirements[node_id] = gas_cost
                    reservation_requirements.setdefault(node_id, []).append(
                        VWSRequirement(
                            sender=sender,
                            chain=chain_name,
                            token_ref=_native_token_ref(resolved_args),
                            required=gas_cost,
                            kind="native_reserve",
                        )
                    )
                outputs[node_id] = _build_node_output_artifact(
                    node_tool=node.tool,
                    amount_out=amount_out,
                )

        elif node.tool == "bridge":
            source_chain = _node_chain_name(node.tool, resolved_args)
            target_chain = str(resolved_args.get("target_chain") or "").strip()
            token_in_ref = _normalize_token_ref(
                resolved_args.get("source_address")
                or resolved_args.get("token_address"),
                resolved_args,
            )
            dest_token_ref, output_amount = _extract_simulated_output(
                node_tool=node.tool,
                node_args=resolved_args,
                preflight=preflight,
            )
            if (
                canonical_route_meta is not None
                and output_amount is not None
                and output_amount < canonical_route_meta.min_output
            ):
                payload = log_execution_comparison(
                    route_meta=canonical_route_meta,
                    node_id=node_id,
                    tool=node.tool,
                    actual_output=output_amount,
                )
                _LOGGER.warning("route_execution_output %s", payload)
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="slippage_exceeded",
                        reason=(
                            f"simulated output {output_amount} is below minimum "
                            f"output {canonical_route_meta.min_output}"
                        ),
                    ),
                )
            amount = _to_decimal(resolved_args.get("amount"))
            if (
                not source_chain
                or not target_chain
                or not token_in_ref
                or amount is None
                or amount <= 0
            ):
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="dependency_resolution_failure",
                        reason="bridge step is missing chain, token, or amount",
                    ),
                )
            gas_cost = _estimate_native_gas_cost(
                node_tool=node.tool,
                node_args=resolved_args,
                preflight=preflight,
            )
            protocol = (
                str(
                    preflight.get("protocol")
                    or (route_meta or {}).get("aggregator")
                    or "across"
                )
                .strip()
                .lower()
            )
            step_result = vws.simulate_bridge(
                source_chain=source_chain,
                source_chain_id=None,
                dest_chain=target_chain,
                token_address=token_in_ref,
                dest_token_address=dest_token_ref or token_in_ref,
                amount=amount,
                protocol=protocol,
                native_address=_native_token_ref(resolved_args),
                arrival_amount=output_amount,
                gas_cost_native_override=gas_cost,
            )
            if step_result.success:
                reservation_requirements.setdefault(node_id, []).append(
                    VWSRequirement(
                        sender=sender,
                        chain=source_chain,
                        token_ref=token_in_ref,
                        required=amount,
                        kind="token_spend",
                    )
                )
                if gas_cost > 0:
                    native_requirements[node_id] = gas_cost
                    reservation_requirements.setdefault(node_id, []).append(
                        VWSRequirement(
                            sender=sender,
                            chain=source_chain,
                            token_ref=_native_token_ref(resolved_args),
                            required=gas_cost,
                            kind="native_reserve",
                        )
                    )
                outputs[node_id] = _build_node_output_artifact(
                    node_tool=node.tool,
                    amount_out=output_amount,
                )

        elif node.tool == "transfer":
            try:
                transfer_meta = resolve_transfer_planning_metadata(resolved_args)
            except ValueError as exc:
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="dependency_resolution_failure",
                        reason=str(exc),
                    ),
                )
            chain_name = transfer_meta.network
            token_ref = transfer_meta.asset_ref
            amount = _to_decimal(resolved_args.get("amount"))
            if amount is None or amount <= 0:
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="dependency_resolution_failure",
                        reason="transfer step is missing amount",
                    ),
                )
            gas_profile = _transfer_gas_profile(transfer_meta)
            gas_cost = _estimate_native_gas_cost(
                node_tool=node.tool,
                node_args=resolved_args,
                preflight=preflight,
                family=transfer_meta.family,
                gas_profile=gas_profile,
            )
            step_result = vws.simulate_transfer(
                network=chain_name,
                chain_id=None,
                asset_ref=token_ref,
                amount=amount,
                native_asset_ref=transfer_meta.native_asset_ref,
                gas_profile=gas_profile,
                gas_cost_native_override=gas_cost,
            )
            if step_result.success:
                reservation_requirements.setdefault(node_id, []).append(
                    VWSRequirement(
                        sender=sender,
                        chain=chain_name,
                        token_ref=token_ref,
                        required=amount,
                        kind="token_spend",
                    )
                )
                if gas_cost > 0:
                    native_requirements[node_id] = gas_cost
                    reservation_requirements.setdefault(node_id, []).append(
                        VWSRequirement(
                            sender=sender,
                            chain=chain_name,
                            token_ref=transfer_meta.native_asset_ref,
                            required=gas_cost,
                            kind="native_reserve",
                        )
                    )
                outputs[node_id] = {}

        elif node.tool == "unwrap":
            chain_name = _node_chain_name(node.tool, resolved_args)
            wrapped_ref = (
                str(
                    resolved_args.get("token_address")
                    or resolved_args.get("token_in_address")
                    or ""
                )
                .strip()
                .lower()
            )
            if not chain_name or not wrapped_ref:
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="dependency_resolution_failure",
                        reason="unwrap step is missing chain or wrapped token address",
                    ),
                )

            amount = _to_decimal(resolved_args.get("amount"))
            available_wrapped = vws.get_balance(chain_name, wrapped_ref)
            if amount is None:
                amount = available_wrapped

            if amount is None or amount <= 0:
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="insufficient_funds",
                        reason=(
                            "wrapped balance is 0 "
                            f"(available {available_wrapped:.6f}) on {chain_name}"
                        ),
                    ),
                )

            gas_cost = _estimate_native_gas_cost(
                node_tool=node.tool,
                node_args=resolved_args,
                preflight=preflight,
            )
            step_result = vws.simulate_unwrap(
                chain=chain_name,
                chain_id=None,
                wrapped_token_address=wrapped_ref,
                amount_wrapped=amount,
                native_address=_native_token_ref(resolved_args),
                gas_cost_native_override=gas_cost,
            )
            if step_result.success:
                reservation_requirements.setdefault(node_id, []).append(
                    VWSRequirement(
                        sender=sender,
                        chain=chain_name,
                        token_ref=wrapped_ref,
                        required=amount,
                        kind="token_spend",
                    )
                )
                if gas_cost > 0:
                    native_requirements[node_id] = gas_cost
                    reservation_requirements.setdefault(node_id, []).append(
                        VWSRequirement(
                            sender=sender,
                            chain=chain_name,
                            token_ref=_native_token_ref(resolved_args),
                            required=gas_cost,
                            kind="native_reserve",
                        )
                    )
                outputs[node_id] = {
                    "amount_out": str(amount),
                    "output_amount": str(amount),
                    "amount": str(amount),
                    "token_symbol": resolved_args.get("token_symbol"),
                    "wrapped_token_symbol": resolved_args.get("wrapped_token_symbol"),
                }

        else:
            node_metadata[node_id] = {
                "status": "skipped",
                "unsupported_tool": True,
            }
            continue

        if not step_result.success:
            category = "gas_shortfall"
            if "not enough gas" not in step_result.rejection_reason.lower():
                category = "insufficient_funds"
            return VWSPlanSimulation(
                valid=False,
                projected_deltas=projected_deltas,
                native_requirements=native_requirements,
                reservation_requirements=reservation_requirements,
                node_metadata=node_metadata,
                failure=VWSFailure(
                    node_id=node_id,
                    tool=node.tool,
                    category=category,
                    reason=step_result.rejection_reason,
                ),
            )

        platform_fee_native = _to_decimal(platform_fee_native_by_node.get(node_id))
        if platform_fee_native is None or platform_fee_native < 0:
            platform_fee_native = Decimal("0")
        if node.tool == "unwrap":
            platform_fee_native = Decimal("0")
        if platform_fee_native > 0:
            if node.tool in {"transfer", "unwrap"}:
                try:
                    if node.tool == "transfer":
                        transfer_meta = resolve_transfer_planning_metadata(
                            resolved_args
                        )
                        native_chain = transfer_meta.network
                        native_token_ref = transfer_meta.native_asset_ref
                    else:
                        native_chain = _node_native_chain_name(node.tool, resolved_args)
                        native_token_ref = _native_token_ref(resolved_args)
                except ValueError as exc:
                    return VWSPlanSimulation(
                        valid=False,
                        projected_deltas=projected_deltas,
                        native_requirements=native_requirements,
                        reservation_requirements=reservation_requirements,
                        node_metadata=node_metadata,
                        failure=VWSFailure(
                            node_id=node_id,
                            tool=node.tool,
                            category="dependency_resolution_failure",
                            reason=str(exc),
                        ),
                    )
            else:
                native_chain = _node_native_chain_name(node.tool, resolved_args)
                native_token_ref = _native_token_ref(resolved_args)
            ok, reason = vws.reserve_balance(
                chain=native_chain,
                token_address=native_token_ref,
                amount=platform_fee_native,
                label="platform fee",
            )
            if not ok:
                return VWSPlanSimulation(
                    valid=False,
                    projected_deltas=projected_deltas,
                    native_requirements=native_requirements,
                    reservation_requirements=reservation_requirements,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="insufficient_funds",
                        reason=reason or "platform fee shortfall",
                    ),
                )
            native_requirements[node_id] = (
                native_requirements.get(
                    node_id,
                    Decimal("0"),
                )
                + platform_fee_native
            )
            reservation_requirements.setdefault(node_id, []).append(
                VWSRequirement(
                    sender=sender,
                    chain=native_chain,
                    token_ref=native_token_ref,
                    required=platform_fee_native,
                    kind="native_reserve",
                )
            )

        current_state.node_states[node_id] = NodeState(
            node_id=node_id,
            status=StepStatus.SUCCESS,
            result=outputs.get(node_id) or {},
        )

        if canonical_route_meta is not None:
            simulated_output = _extract_actual_output_for_vws(
                node_tool=node.tool,
                output_artifact=outputs.get(node_id) or {},
            )
            payload = log_execution_comparison(
                route_meta=canonical_route_meta,
                node_id=node_id,
                tool=node.tool,
                actual_output=simulated_output,
            )
            _LOGGER.info("route_execution_output %s", payload)

        after = vws.snapshot()
        delta_map: Dict[str, str] = {}
        ending_map: Dict[str, str] = {}
        all_keys = set(before) | set(after)
        for chain_key, token_key in sorted(all_keys):
            before_amount = before.get((chain_key, token_key), Decimal("0"))
            after_amount = after.get((chain_key, token_key), Decimal("0"))
            delta = after_amount - before_amount
            if delta != 0:
                serialized_key = _resource_key(sender, chain_key, token_key)
                projected_deltas[serialized_key] = (
                    projected_deltas.get(
                        serialized_key,
                        Decimal("0"),
                    )
                    + delta
                )
                delta_map[serialized_key] = str(delta)
            ending_map[_resource_key(sender, chain_key, token_key)] = str(after_amount)

        node_metadata[node_id] = {
            "status": "success",
            "gas_cost_native": str(step_result.gas_cost_native),
            "platform_fee_native": str(platform_fee_native),
            "native_requirement_total": str(
                native_requirements.get(node_id, Decimal("0"))
            ),
            "balance_deltas": delta_map,
            "ending_balances": ending_map,
            "resolved_args": resolved_args,
            "route_meta_used": bool(route_validation.should_use_route_meta),
            "route_meta_fallback": bool(
                route_validation.fallback_policy.allow_fallback
            ),
            "route_meta_reason": route_validation.reason,
        }
        if prefix_cache is not None:
            prefix_cache[step_cache_key] = _SimulationCheckpoint(
                current_state=_clone_execution_state(current_state),
                vws_by_sender={
                    key: value.clone() for key, value in vws_by_sender.items()
                },
                projected_deltas=dict(projected_deltas),
                native_requirements=dict(native_requirements),
                reservation_requirements={
                    key: list(value) for key, value in reservation_requirements.items()
                },
                node_metadata=_clone_node_metadata(node_metadata),
            )
        prefix_key = step_cache_key

    ending_balances: Dict[str, str] = {}
    for sender, vws in vws_by_sender.items():
        for (chain_key, token_key), amount in sorted(vws.snapshot().items()):
            ending_balances[_resource_key(sender, chain_key, token_key)] = str(amount)

    return VWSPlanSimulation(
        valid=True,
        projected_deltas=projected_deltas,
        native_requirements=native_requirements,
        reservation_requirements=reservation_requirements,
        node_metadata=node_metadata,
        ending_balances=ending_balances,
    )


def _clone_checkpoint(checkpoint: _SimulationCheckpoint) -> _SimulationCheckpoint:
    return _SimulationCheckpoint(
        current_state=_clone_execution_state(checkpoint.current_state),
        vws_by_sender={
            key: value.clone() for key, value in checkpoint.vws_by_sender.items()
        },
        projected_deltas=dict(checkpoint.projected_deltas),
        native_requirements=dict(checkpoint.native_requirements),
        reservation_requirements={
            key: list(value)
            for key, value in checkpoint.reservation_requirements.items()
        },
        node_metadata=_clone_node_metadata(checkpoint.node_metadata),
    )


def _step_prefix_key(
    *,
    prefix_key: str,
    node_id: str,
    tool: str,
    resolved_args: Dict[str, Any],
    preflight: Dict[str, Any],
    route_meta: Dict[str, Any] | None,
) -> str:
    payload = {
        "prefix": prefix_key,
        "node_id": node_id,
        "tool": tool,
        "resolved_args": resolved_args,
        "preflight": preflight,
        "route_meta": route_meta or {},
    }
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def _simulation_label(plan: ExecutionPlan) -> str:
    return str(plan.metadata.get("vws_label") or f"plan_v{plan.version}")


def _gas_costs_by_step(simulation: VWSPlanSimulation) -> Dict[str, Decimal]:
    gas_costs: Dict[str, Decimal] = {}
    for node_id, metadata in simulation.node_metadata.items():
        gas_cost = _to_decimal(metadata.get("gas_cost_native"))
        if gas_cost is None:
            continue
        gas_costs[node_id] = gas_cost
    return gas_costs


def _latency_estimate_seconds(
    *,
    plan: ExecutionPlan,
    preflight_estimates: Dict[str, Dict[str, Any]],
) -> int:
    total = 0
    for node_id in _topological_order(plan):
        node = plan.nodes[node_id]
        preflight = preflight_estimates.get(node_id) or {}
        raw = (
            preflight.get("estimated_fill_time_seconds")
            or (node.metadata.get("route") or {}).get("fill_time_seconds")
            or 0
        )
        try:
            total += max(0, int(raw))
        except Exception:
            continue
    return total


def _output_value(
    *,
    plan: ExecutionPlan,
    simulation: VWSPlanSimulation,
    preflight_estimates: Dict[str, Dict[str, Any]],
) -> Decimal:
    depended_on: set[str] = set()
    for node in plan.nodes.values():
        depended_on.update(node.depends_on)

    total = Decimal("0")
    for node_id in _topological_order(plan):
        if node_id in depended_on:
            continue
        node = plan.nodes[node_id]
        preflight = preflight_estimates.get(node_id) or {}
        if node.tool in {"swap", "solana_swap"}:
            amount = _to_decimal(
                preflight.get("amount_out") or preflight.get("amount_out_min")
            )
        elif node.tool == "bridge":
            amount = _to_decimal(preflight.get("output_amount"))
        else:
            amount = None
        if amount is not None and amount > 0:
            total += amount

    if total > 0:
        return total

    for value in simulation.projected_deltas.values():
        if value > 0:
            total += value
    return total


def _project_execution_plan(
    *,
    plan: ExecutionPlan,
    execution_state: ExecutionState | None,
    context: Dict[str, Any] | None,
    preflight_estimates: Dict[str, Dict[str, Any]] | None,
) -> VWSPlanSimulation:
    preflight_estimates = preflight_estimates or {}
    current_state = ExecutionState(
        node_states=dict(execution_state.node_states) if execution_state else {},
        artifacts=dict(execution_state.artifacts) if execution_state else {},
        completed=bool(execution_state.completed) if execution_state else False,
    )
    outputs = current_state.artifacts.get("outputs")
    if not isinstance(outputs, dict):
        outputs = {}
        current_state.artifacts["outputs"] = outputs

    node_metadata: Dict[str, Dict[str, Any]] = {}

    for node_id in _topological_order(plan):
        node = plan.nodes[node_id]
        existing = current_state.node_states.get(node_id)
        if existing and existing.status in {StepStatus.SUCCESS, StepStatus.SKIPPED}:
            node_metadata[node_id] = {
                "status": str(existing.status),
                "skipped_projection": True,
            }
            continue

        resolved_args = resolve_dynamic_args(
            node.args,
            current_state,
            context=context,
        )
        unresolved_path = _find_unresolved_marker(resolved_args)
        if unresolved_path:
            return VWSPlanSimulation(
                valid=False,
                node_metadata=node_metadata,
                failure=VWSFailure(
                    node_id=node_id,
                    tool=node.tool,
                    category="dependency_resolution_failure",
                    reason="unresolved dynamic dependency during projected simulation",
                    path=unresolved_path,
                ),
            )

        preflight = dict(preflight_estimates.get(node_id) or {})
        transfer_meta: TransferPlanningMetadata | None = None
        if node.tool == "transfer":
            try:
                transfer_meta = resolve_transfer_planning_metadata(resolved_args)
            except ValueError as exc:
                return VWSPlanSimulation(
                    valid=False,
                    node_metadata=node_metadata,
                    failure=VWSFailure(
                        node_id=node_id,
                        tool=node.tool,
                        category="dependency_resolution_failure",
                        reason=str(exc),
                    ),
                )
        gas_cost = _estimate_native_gas_cost(
            node_tool=node.tool,
            node_args=resolved_args,
            preflight=preflight,
            family=transfer_meta.family if transfer_meta is not None else None,
            gas_profile=(
                _transfer_gas_profile(transfer_meta)
                if transfer_meta is not None
                else None
            ),
        )
        token_out_ref, amount_out = _extract_simulated_output(
            node_tool=node.tool,
            node_args=resolved_args,
            preflight=preflight,
        )
        outputs[node_id] = _build_node_output_artifact(
            node_tool=node.tool,
            amount_out=amount_out,
        )
        current_state.node_states[node_id] = NodeState(
            node_id=node_id,
            status=StepStatus.SUCCESS,
            result=outputs.get(node_id) or {},
        )
        node_metadata[node_id] = {
            "status": "projected",
            "gas_cost_native": str(gas_cost),
            "resolved_args": resolved_args,
            "projected_output_token": token_out_ref,
            "projected_output_amount": str(amount_out)
            if amount_out is not None
            else None,
        }

    return VWSPlanSimulation(valid=True, node_metadata=node_metadata)


def simulate_many_execution_plans(
    *,
    plans: List[ExecutionPlan],
    balance_snapshot: Dict[str, str],
    execution_state: ExecutionState | None,
    context: Dict[str, Any] | None,
    default_preflight_estimates: Dict[str, Dict[str, Any]] | None = None,
    platform_fee_native_by_node: Dict[str, Decimal] | None = None,
) -> List[CandidatePlanSimulationResult]:
    results: List[CandidatePlanSimulationResult] = []
    prefix_cache: Dict[str, _SimulationCheckpoint] = {}
    cache_stats = {"hits": 0}
    balance_snapshot = balance_snapshot or {}
    default_preflight_estimates = default_preflight_estimates or {}

    for plan in plans:
        preflight_estimates = dict(
            plan.metadata.get("candidate_preflight_estimates")
            or default_preflight_estimates
            or {}
        )
        hits_before = int(cache_stats.get("hits", 0))
        if balance_snapshot:
            simulation = simulate_execution_plan(
                plan=plan,
                balance_snapshot=balance_snapshot,
                execution_state=execution_state,
                context=context,
                preflight_estimates=preflight_estimates,
                platform_fee_native_by_node=platform_fee_native_by_node,
                prefix_cache=prefix_cache,
                cache_stats=cache_stats,
            )
            balance_validation_skipped = False
        else:
            simulation = _project_execution_plan(
                plan=plan,
                execution_state=execution_state,
                context=context,
                preflight_estimates=preflight_estimates,
            )
            balance_validation_skipped = True

        gas_cost_by_step = _gas_costs_by_step(simulation)
        results.append(
            CandidatePlanSimulationResult(
                plan=plan,
                label=_simulation_label(plan),
                simulation=simulation,
                final_balances=dict(simulation.ending_balances),
                per_step_state_transitions=_clone_node_metadata(
                    simulation.node_metadata
                ),
                total_gas_cost_native=sum(gas_cost_by_step.values(), Decimal("0")),
                gas_cost_by_step=gas_cost_by_step,
                latency_estimate_seconds=_latency_estimate_seconds(
                    plan=plan,
                    preflight_estimates=preflight_estimates,
                ),
                output_value=_output_value(
                    plan=plan,
                    simulation=simulation,
                    preflight_estimates=preflight_estimates,
                ),
                balance_validation_skipped=balance_validation_skipped,
                shared_prefix_hits=int(cache_stats.get("hits", 0)) - hits_before,
            )
        )

    return results
