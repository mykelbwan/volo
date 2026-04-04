"""
Intent resolver node.

Parallelization flags (environment variables):
- ENABLE_SAME_CHAIN_PARALLEL_SWAPS: allow same-chain swaps to run in parallel.
- PARALLEL_SWAP_MAX_PER_CHAIN: cap same-chain parallel swaps per chain (default 2).
- PARALLEL_SWAP_MAX_SLIPPAGE: max slippage % allowed for same-chain parallel swaps (default 1.0).
"""

import os
import re
import uuid
from typing import Any, Dict, Set

from langchain_core.messages import AIMessage

from core.planning.execution_plan import (
    ExecutionPlan,
    ExecutionState,
    NodeState,
    PlanNode,
)
from core.utils.user_feedback import intent_resolution_failed, token_resolution_failed
from graph.agent_state import AgentState
from intent_hub.ontology.intent import Intent, IntentStatus
from intent_hub.resolver.intent_resolver import resolve_intent
from intent_hub.resolver.templates import apply_templates

_OUTPUT_MARKER_RE = re.compile(r"\{\{OUTPUT_OF:([^}]+)\}\}")
_BALANCE_MARKER_RE = re.compile(r"\{\{BALANCE_OF:([^:}]+):([^}]+)\}\}")
_RESOLVE_SWAP_RE = re.compile(
    r"^Could not resolve addresses for ([^/]+)/([^ ]+) on (.+)$"
)
_RESOLVE_BRIDGE_RE = re.compile(
    r"^Could not resolve addresses for ([^ ]+) on ([^/]+)/(.+)$"
)
_RESOLVE_TRANSFER_RE = re.compile(r"^Could not resolve addresses for ([^ ]+) on (.+)$")

_PARALLEL_TOOLS = {"transfer", "check_balance", "swap", "bridge", "unwrap"}


def _get_bool_env(key: str, default: bool = False) -> bool:
    raw = os.getenv(key, "")
    if raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_int_env(key: str, default: int) -> int:
    raw = os.getenv(key, "")
    if raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _get_float_env(key: str, default: float) -> float:
    raw = os.getenv(key, "")
    if raw == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _same_chain_parallel_swaps_enabled() -> bool:
    return _get_bool_env("ENABLE_SAME_CHAIN_PARALLEL_SWAPS", False)


def _max_parallel_swaps_per_chain() -> int:
    return _get_int_env("PARALLEL_SWAP_MAX_PER_CHAIN", 2)


def _max_parallel_swap_slippage() -> float:
    return _get_float_env("PARALLEL_SWAP_MAX_SLIPPAGE", 1.0)


def _extract_marker_dependencies(args: Dict[str, Any]) -> Set[str]:
    deps: set[str] = set()

    def _scan(value: Any) -> None:
        if isinstance(value, str):
            for match in _OUTPUT_MARKER_RE.finditer(value):
                deps.add(match.group(1).strip())
            for match in _BALANCE_MARKER_RE.finditer(value):
                deps.add(match.group(1).strip())
            return
        if isinstance(value, dict):
            for v in value.values():
                _scan(v)
            return
        if isinstance(value, (list, tuple, set)):
            for v in value:
                _scan(v)

    _scan(args)
    return deps


def _parallel_key(tool: str, args: Dict[str, Any]) -> str | None:
    if tool == "check_balance":
        chain = args.get("network") or args.get("chain")
        return f"balance:{str(chain).lower()}" if chain else None
    if tool == "transfer":
        chain = args.get("network") or args.get("chain")
        token_symbol = args.get("asset_symbol") or args.get("token_symbol")
        if not token_symbol:
            token_slot = args.get("token")
            if isinstance(token_slot, dict):
                token_symbol = token_slot.get("symbol")
        if not chain or not token_symbol:
            return None
        return f"transfer:{str(chain).lower()}:{str(token_symbol).upper()}"
    if tool == "swap":
        chain = args.get("chain")
        if not chain:
            return None
        return f"swap:{str(chain).lower()}"
    if tool == "bridge":
        source_chain = args.get("source_chain") or args.get("chain")
        if not source_chain:
            return None
        return f"bridge:{str(source_chain).lower()}"
    if tool == "unwrap":
        chain = args.get("network") or args.get("chain")
        if not chain:
            return None
        return f"unwrap:{str(chain).lower()}"
    return None


def _swap_pair_key(args: Dict[str, Any]) -> str | None:
    token_in = args.get("token_in_symbol")
    token_out = args.get("token_out_symbol")
    if not token_in or not token_out:
        return None
    return f"{str(token_in).upper()}->{str(token_out).upper()}"


def _extract_symbols(intent: Intent) -> tuple[list[str], str | None]:
    slots = intent.slots or {}
    chain = slots.get("chain")
    symbols: list[str] = []

    if intent.intent_type == "swap":
        token_in = slots.get("token_in")
        token_out = slots.get("token_out")
        if isinstance(token_in, dict):
            symbol = token_in.get("symbol")
        else:
            symbol = token_in
        if symbol:
            symbols.append(str(symbol))
        if isinstance(token_out, dict):
            symbol = token_out.get("symbol")
        else:
            symbol = token_out
        if symbol:
            symbols.append(str(symbol))

    if intent.intent_type == "bridge":
        token_in = slots.get("token_in")
        if isinstance(token_in, dict):
            symbol = token_in.get("symbol")
        else:
            symbol = token_in
        if symbol:
            symbols.append(str(symbol))

    if intent.intent_type == "transfer":
        token = slots.get("token")
        if isinstance(token, dict):
            symbol = token.get("symbol")
        else:
            symbol = token
        if symbol:
            symbols.append(str(symbol))

    if intent.intent_type == "unwrap":
        token = slots.get("token")
        if isinstance(token, dict):
            symbol = token.get("symbol")
        else:
            symbol = token
        if symbol:
            symbols.append(str(symbol))

    return symbols, str(chain) if chain is not None else None


def _symbol_is_invalid(symbol: str) -> bool:
    if not symbol or not str(symbol).strip():
        return True
    if re.search(r"\s", str(symbol)):
        return True
    return False


def _preflight_symbol_feedback(intent: Intent):
    symbols, chain = _extract_symbols(intent)
    if not symbols:
        return None
    invalid = [s for s in symbols if _symbol_is_invalid(s)]
    if not invalid:
        return None
    return token_resolution_failed(invalid, chain)


def _resolution_feedback_for_exception(exc: Exception):
    detail = str(exc)
    match = _RESOLVE_SWAP_RE.match(detail)
    if match:
        token_in, token_out, chain = match.groups()
        return token_resolution_failed([token_in, token_out], chain)
    match = _RESOLVE_BRIDGE_RE.match(detail)
    if match:
        token, source_chain, target_chain = match.groups()
        chain = f"{source_chain} or {target_chain}"
        return token_resolution_failed([token], chain)
    match = _RESOLVE_TRANSFER_RE.match(detail)
    if match:
        token, chain = match.groups()
        return token_resolution_failed([token], chain)
    return intent_resolution_failed()


def _swap_parallel_candidate(
    args: Dict[str, Any],
    swap_counts: Dict[str, int],
    swap_pairs: Dict[str, Set[str]],
) -> tuple[str | None, str | None, str | None, bool]:
    chain = args.get("chain")
    if not chain:
        return None, None, None, False
    chain_key = f"swap:{str(chain).lower()}"

    if not _same_chain_parallel_swaps_enabled():
        return chain_key, chain_key, None, False

    slippage = args.get("slippage")
    if slippage is not None:
        try:
            if float(slippage) > _max_parallel_swap_slippage():
                return chain_key, chain_key, None, False
        except Exception:
            return chain_key, chain_key, None, False

    pair = _swap_pair_key(args)
    if not pair:
        return chain_key, chain_key, None, False

    max_per_chain = _max_parallel_swaps_per_chain()
    current = swap_counts.get(chain_key, 0)
    if current >= max_per_chain:
        return chain_key, chain_key, None, False

    seen = swap_pairs.get(chain_key, set())
    if pair in seen:
        return chain_key, chain_key, None, False

    return f"{chain_key}:{pair}", chain_key, pair, True


async def intent_resolver_node(state: AgentState) -> Dict[str, Any]:
    """
    Resolves all complete Intents into an ExecutionPlan (DAG).
    """
    intent_data_list = state.get("intents", [])
    if not intent_data_list:
        raise ValueError(
            "No intents found in state. Recovery path: run intent_parser_node first or pass state['intents'] with at least one parsed intent."
        )

    intents = [Intent(**data) for data in intent_data_list]
    intents = apply_templates(intents)

    plans_data = []
    for intent in intents:
        if intent.status == IntentStatus.COMPLETE:
            preflight = _preflight_symbol_feedback(intent)
            if preflight:
                return {
                    "messages": [AIMessage(content=preflight.render())],
                    "route_decision": "end",
                }
            try:
                plan = await resolve_intent(intent)
            except Exception as exc:
                feedback = _resolution_feedback_for_exception(exc)
                return {
                    "messages": [AIMessage(content=feedback.render())],
                    "route_decision": "end",
                }
            plans_data.append(plan.model_dump())
        else:
            # If we hit an incomplete one, we stop resolving
            break

    if not plans_data:
        return {}

    # Convert linear plans to DAG
    nodes = {}
    node_states = {}
    prev_node_id = None
    barrier_node_id: str | None = None
    parallel_keys: set[str] = set()
    current_parallel_group: str | None = None
    group_anchor_id: str | None = None
    parallel_swap_counts: Dict[str, int] = {}
    parallel_swap_pairs: Dict[str, Set[str]] = {}
    goal_params = {}
    logs = []

    for i, plan_data in enumerate(plans_data):
        node_id = f"step_{i}"

        # Merge parameters into global goal_params for planner context
        goal_params.update(plan_data.get("parameters", {}))

        # Close out a parallel group when tool type changes.
        if (
            current_parallel_group
            and current_parallel_group != plan_data["intent_type"]
        ):
            barrier_node_id = prev_node_id
            current_parallel_group = None
            parallel_keys = set()
            group_anchor_id = None
            parallel_swap_counts = {}
            parallel_swap_pairs = {}

        # Merge constraints into args (e.g. slippage)
        node_args = {**plan_data.get("parameters", {}), "chain": plan_data.get("chain")}
        constraints = plan_data.get("constraints")

        if constraints:
            logs.append(f"[DEBUG] Found constraints for {node_id}: {constraints}")

        # Unified constraint processing (handle both dict and str)
        constraints_to_process = {}
        if isinstance(constraints, dict):
            constraints_to_process = constraints
        elif isinstance(constraints, str):
            # Try to find slippage in the string
            if "slippage" in constraints.lower():
                match = re.search(r"(\d+(\.\d+)?)", constraints)
                if match:
                    constraints_to_process["slippage"] = match.group(1)

        for k, v in constraints_to_process.items():
            if k == "slippage":
                # Clean up numeric values (remove % etc)
                if isinstance(v, str):
                    clean_v = v.replace("%", "").strip()
                    try:
                        node_args["slippage"] = float(clean_v)
                        logs.append(
                            f"[DEBUG] Normalized slippage to {node_args['slippage']}"
                        )
                    except ValueError:
                        node_args[k] = v
                else:
                    node_args[k] = v
            else:
                node_args[k] = v

        # Determine dependencies (allow safe parallelism)
        marker_deps = _extract_marker_dependencies(node_args)
        depends_on: list[str] = []

        if marker_deps:
            if barrier_node_id and barrier_node_id not in marker_deps:
                depends_on = sorted(set(marker_deps) | {barrier_node_id})
            else:
                depends_on = sorted(marker_deps)
            # Marker deps imply ordering; reset parallel group
            barrier_node_id = node_id
            parallel_keys = set()
            current_parallel_group = None
            group_anchor_id = None
            parallel_swap_counts = {}
            parallel_swap_pairs = {}
        else:
            if plan_data["intent_type"] in _PARALLEL_TOOLS:
                key = None
                chain_key = None
                pair = None
                record_swap = False
                if plan_data["intent_type"] == "swap":
                    key, chain_key, pair, record_swap = _swap_parallel_candidate(
                        node_args, parallel_swap_counts, parallel_swap_pairs
                    )
                else:
                    key = _parallel_key(plan_data["intent_type"], node_args)
                if key and key not in parallel_keys:
                    if current_parallel_group is None:
                        group_anchor_id = barrier_node_id
                        current_parallel_group = plan_data["intent_type"]
                        parallel_keys = set()
                    depends_on = [group_anchor_id] if group_anchor_id else []
                    parallel_keys.add(key)
                    if record_swap and chain_key and pair:
                        parallel_swap_counts[chain_key] = (
                            parallel_swap_counts.get(chain_key, 0) + 1
                        )
                        pairs = parallel_swap_pairs.get(chain_key)
                        if pairs is None:
                            pairs = set()
                            parallel_swap_pairs[chain_key] = pairs
                        pairs.add(pair)
                    # Keep barrier_node_id unchanged to allow parallel siblings
                else:
                    depends_on = [prev_node_id] if prev_node_id else []
                    barrier_node_id = node_id
                    parallel_keys = {key} if key else set()
                    current_parallel_group = None
                    group_anchor_id = None
                    parallel_swap_counts = {}
                    parallel_swap_pairs = {}
            else:
                depends_on = [prev_node_id] if prev_node_id else []
                barrier_node_id = node_id
                parallel_keys = set()
                current_parallel_group = None
                group_anchor_id = None
                parallel_swap_counts = {}
                parallel_swap_pairs = {}

        # Determine approval requirement
        approval_req = True
        if plan_data["intent_type"] in {"check_balance", "unwrap"}:
            approval_req = False

        node = PlanNode(
            id=node_id,
            tool=plan_data["intent_type"],
            args=node_args,
            depends_on=depends_on,
            approval_required=approval_req,
        )
        nodes[node_id] = node

        node_states[node_id] = NodeState(node_id=node_id)
        prev_node_id = node_id

    goal = f"Execute {len(plans_data)} intent(s)"
    if len(plans_data) == 1:
        p = plans_data[0]
        params = p.get("parameters", {})
        if p["intent_type"] == "swap":
            goal = f"Swap {params.get('amount_in')} {params.get('token_in_symbol')} for {params.get('token_out_symbol')}"
        elif p["intent_type"] == "bridge":
            goal = f"Bridge {params.get('amount')} {params.get('token_symbol')} to {params.get('target_chain')}"
        elif p["intent_type"] == "transfer":
            goal = (
                f"Transfer {params.get('amount')} "
                f"{params.get('asset_symbol') or params.get('token_symbol')} "
                f"to {params.get('recipient')}"
            )
        elif p["intent_type"] == "check_balance":
            goal = f"Check balances on {p.get('network') or p.get('chain')}"
        elif p["intent_type"] == "unwrap":
            goal = (
                f"Unwrap {params.get('amount') or 'all'} "
                f"{params.get('token_symbol')} on {params.get('chain') or p.get('chain')}"
            )

    execution_plan = ExecutionPlan(goal=goal, nodes=nodes)

    execution_state = ExecutionState(node_states=node_states)

    return {
        "route_decision": "resolve",
        "intents": [intent.model_dump() for intent in intents],
        "plans": plans_data,  # Keep for backward compatibility if needed
        "goal_parameters": goal_params,
        "plan_history": [execution_plan],
        "execution_state": execution_state,
        "reasoning_logs": logs,
        "execution_id": str(uuid.uuid4()),
    }
