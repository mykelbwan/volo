from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class PlanNode(BaseModel):
    id: str
    tool: str
    args: Dict[str, Any]
    depends_on: List[str] = []
    approval_required: bool = True
    retry_policy: Dict[str, Any] = Field(
        default_factory=lambda: {"max_retries": 3, "backoff_factor": 2.0}
    )
    # Routing metadata set by route_planner_node.
    # Holds the selected aggregator, pre-fetched quote, calldata, and any
    # other protocol-specific execution data.  Kept separate from ``args``
    # so tool schemas (SwapArgs, BridgeArgs, etc.) stay clean and unchanged.
    # The executor reads ``metadata["route"]`` before deciding how to invoke
    # the tool, injecting the pre-built calldata fast-path when available.
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ExecutionPlan(BaseModel):
    goal: str
    nodes: Dict[str, PlanNode]
    version: int = 1
    metadata: Dict[str, Any] = {}


class NodeState(BaseModel):
    node_id: str
    status: StepStatus = StepStatus.PENDING
    retries: int = 0
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_category: Optional[str] = None
    user_message: Optional[str] = None
    mutated_args: Optional[Dict[str, Any]] = None


class ExecutionState(BaseModel):
    node_states: Dict[str, NodeState]
    artifacts: Dict[str, Any] = {}
    completed: bool = False

    def merge(self, other: "ExecutionState") -> "ExecutionState":
        """
        Deterministically merges another execution state into this one.
        Treats this object as immutable and returns a new merged one.
        """
        new_node_states = self.node_states.copy()
        new_node_states.update(other.node_states)

        new_artifacts = self.artifacts.copy()
        new_artifacts.update(other.artifacts)
        left_outputs = self.artifacts.get("outputs")
        right_outputs = other.artifacts.get("outputs")
        if isinstance(left_outputs, dict) and isinstance(right_outputs, dict):
            merged_outputs = left_outputs.copy()
            merged_outputs.update(right_outputs)
            new_artifacts["outputs"] = merged_outputs

        return ExecutionState(
            node_states=new_node_states,
            artifacts=new_artifacts,
            # If either says it's completed, it's completed
            completed=self.completed or other.completed,
        )


CandidatePlanSet = List[ExecutionPlan]


def get_ready_nodes(plan: ExecutionPlan, state: ExecutionState) -> List[PlanNode]:
    """
    Returns nodes where state.status == PENDING and all dependencies have SUCCESS status.
    """
    ready_nodes = []
    for node_id, node in plan.nodes.items():
        node_state = state.node_states.get(node_id)
        if not node_state or node_state.status != StepStatus.PENDING:
            continue

        # Check dependencies
        all_deps_success = True
        for dep_id in node.depends_on:
            dep_state = state.node_states.get(dep_id)
            if not dep_state or dep_state.status != StepStatus.SUCCESS:
                all_deps_success = False
                break

        if all_deps_success:
            ready_nodes.append(node)

    return ready_nodes


def create_node_running_state(node_id: str) -> ExecutionState:
    """Returns a state delta marking a node as RUNNING."""
    return ExecutionState(
        node_states={node_id: NodeState(node_id=node_id, status=StepStatus.RUNNING)}
    )


def create_node_success_state(node_id: str, result: Dict[str, Any]) -> ExecutionState:
    """Returns a state delta marking a node as SUCCESS."""
    return ExecutionState(
        node_states={
            node_id: NodeState(
                node_id=node_id, status=StepStatus.SUCCESS, result=result
            )
        }
    )


def create_node_reset_state(node_id: str) -> ExecutionState:
    """Returns a state delta marking a node as PENDING (reset)."""
    return ExecutionState(
        node_states={
            node_id: NodeState(
                node_id=node_id,
                status=StepStatus.PENDING,
                retries=0,
                error=None,
                error_category=None,
            )
        }
    )


def create_node_failure_state(
    node_id: str,
    error: str,
    retries: int = 0,
    category: Optional[str] = None,
    user_message: Optional[str] = None,
    mutated_args: Optional[Dict[str, Any]] = None,
) -> ExecutionState:
    """Returns a state delta marking a node as FAILED."""
    return ExecutionState(
        node_states={
            node_id: NodeState(
                node_id=node_id,
                status=StepStatus.FAILED,
                error=error,
                retries=retries,
                error_category=category,
                user_message=user_message,
                mutated_args=mutated_args,
            )
        }
    )


def check_plan_complete(plan: ExecutionPlan, state: ExecutionState) -> bool:
    """Return True if all nodes are SUCCESS or SKIPPED."""
    for node_id in plan.nodes:
        node_state = state.node_states.get(node_id)
        if not node_state or node_state.status not in [
            StepStatus.SUCCESS,
            StepStatus.SKIPPED,
        ]:
            return False
    return True


def _normalize_wallet_artifacts(
    context: Dict[str, Any] | None,
) -> Dict[str, Any]:
    normalized = dict(context or {})

    if "evm_address" not in normalized and "sender_address" in normalized:
        normalized["evm_address"] = normalized["sender_address"]
    if "evm_sub_org_id" not in normalized and "sub_org_id" in normalized:
        normalized["evm_sub_org_id"] = normalized["sub_org_id"]

    if "sender_address" not in normalized and "evm_address" in normalized:
        normalized["sender_address"] = normalized["evm_address"]
    if "sub_org_id" not in normalized and "evm_sub_org_id" in normalized:
        normalized["sub_org_id"] = normalized["evm_sub_org_id"]

    return normalized


def resolve_dynamic_args(
    node_args: Dict[str, Any],
    state: ExecutionState,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Pure function: Takes a dictionary of arguments and returns a NEW one
    where markers are resolved based on the current execution state and
    optional session context (database values).
    """
    from intent_hub.utils.amount import to_wei

    def _extract_amount(result: Dict[str, Any]) -> float | None:
        details = result.get("details") if isinstance(result, dict) else None
        candidate = None
        if isinstance(details, dict):
            for key in (
                "output_amount",
                "amount_out",
                "amount_out_minimum",
                "amount_in",
                "amount",
            ):
                if details.get(key) is not None:
                    candidate = details.get(key)
                    break
        if candidate is None:
            for key in (
                "output_amount",
                "amount_out",
                "amount_out_minimum",
                "amount_in",
                "amount",
            ):
                if result.get(key) is not None:
                    candidate = result.get(key)
                    break
        if candidate is None:
            return None
        try:
            return float(candidate)
        except (ValueError, TypeError):
            return None

    def _extract_balance_for_symbol(
        result: Dict[str, Any], symbol: str
    ) -> float | None:
        balances = result.get("balances") if isinstance(result, dict) else None
        if not isinstance(balances, list):
            return None
        target = symbol.strip().upper()
        for entry in balances:
            if not isinstance(entry, dict):
                continue
            entry_symbol = str(entry.get("symbol", "")).strip().upper()
            if entry_symbol != target:
                continue
            value = entry.get("balance_formatted") or entry.get("balance")
            if value is None:
                return None
            try:
                return float(str(value))
            except (ValueError, TypeError):
                return None
        return None

    def _get_output_artifact(node_id: str) -> Dict[str, Any] | None:
        outputs = (
            state.artifacts.get("outputs")
            if isinstance(state.artifacts, dict)
            else None
        )
        if not isinstance(outputs, dict):
            return None
        value = outputs.get(node_id)
        if isinstance(value, dict):
            return value
        return None

    # 1. Resolve Sequential Markers (from previous node results)
    total_available: float | None = None
    has_any_success = any(
        s.status == StepStatus.SUCCESS for s in state.node_states.values()
    )

    normalized_context = _normalize_wallet_artifacts(context)
    resolved_args = node_args.copy()

    for arg_key, arg_val in resolved_args.items():
        if not isinstance(arg_val, str):
            continue

        # ── Pass 1: Resolve Execution Markers ({{OUTPUT_OF:step_X}}) ────────
        if "{{OUTPUT_OF:" in arg_val:
            start = arg_val.find("{{OUTPUT_OF:")
            end = arg_val.find("}}", start)
            if end > start:
                ref_id = arg_val[start + len("{{OUTPUT_OF:") : end].strip()
                ref_state = state.node_states.get(ref_id)
                ref_amount = None
                if (
                    ref_state
                    and ref_state.status == StepStatus.SUCCESS
                    and ref_state.result
                ):
                    ref_amount = _extract_amount(ref_state.result)
                if ref_amount is None:
                    artifact = _get_output_artifact(ref_id)
                    if artifact:
                        ref_amount = _extract_amount(artifact)
                if ref_amount is not None and "amount" in arg_key.lower():
                    if "wei" in arg_key.lower():
                        resolved_args[arg_key] = str(to_wei(ref_amount, 18))
                    else:
                        resolved_args[arg_key] = ref_amount

        if "{{BALANCE_OF:" in arg_val:
            start = arg_val.find("{{BALANCE_OF:")
            end = arg_val.find("}}", start)
            if end > start:
                payload = arg_val[start + len("{{BALANCE_OF:") : end].strip()
                parts = payload.split(":")
                if len(parts) >= 2:
                    ref_id = parts[0].strip()
                    symbol = parts[1].strip()
                    ref_state = state.node_states.get(ref_id)
                    balance = None
                    if (
                        ref_state
                        and ref_state.status == StepStatus.SUCCESS
                        and ref_state.result
                    ):
                        balance = _extract_balance_for_symbol(ref_state.result, symbol)
                    if balance is None:
                        artifact = _get_output_artifact(ref_id)
                        if artifact:
                            balance = _extract_balance_for_symbol(artifact, symbol)
                    if balance is not None and "amount" in arg_key.lower():
                        if "wei" in arg_key.lower():
                            resolved_args[arg_key] = str(to_wei(balance, 18))
                        else:
                            resolved_args[arg_key] = balance

        # ── Pass 2: Resolve Execution Markers ({{TOTAL_BALANCE}}, etc.) ──────
        if (
            "{{SUM_FROM_PREVIOUS}}" in arg_val or "{{TOTAL_BALANCE}}" in arg_val
        ) and has_any_success:
            if total_available is None:
                total_available = 0.0
                for node_id, node_state in state.node_states.items():
                    if node_state.status == StepStatus.SUCCESS:
                        amount = None
                        if node_state.result:
                            amount = _extract_amount(node_state.result)
                        if amount is None:
                            artifact = _get_output_artifact(node_id)
                            if artifact:
                                amount = _extract_amount(artifact)
                        if amount is not None:
                            total_available += float(amount)

            if "amount" in arg_key.lower():
                if "wei" in arg_key.lower():
                    resolved_args[arg_key] = str(to_wei(total_available, 18))
                else:
                    resolved_args[arg_key] = total_available

        # ── Pass 3: Resolve Session Markers ({{SUB_ORG_ID}}, etc.) ────────────
        if normalized_context:
            # We look for markers that match keys in the context (uppercase)
            for ctx_key, ctx_val in normalized_context.items():
                marker = "{{" + str(ctx_key).upper() + "}}"
                if marker in arg_val:
                    # Replace marker with the actual value from database/context
                    resolved_args[arg_key] = arg_val.replace(marker, str(ctx_val))

    return resolved_args
