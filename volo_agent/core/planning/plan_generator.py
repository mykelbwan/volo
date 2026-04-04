"""
core/planning/plan_generator.py
--------------------------------
Deterministic candidate plan generator for the VWS optimizer.

Responsibility
--------------
Given an existing ExecutionPlan (produced by intent_resolver_node), detect
whether it contains a pattern that has a meaningful alternative ordering,
and if so return a small set of candidate plans for VWS simulation and
scoring.

v1 scope
--------
Only one pattern generates alternatives: **swap + bridge**.

    User: "Swap my ETH to USDC and bridge to Base"

    Plan A  (swap_first  — the original plan, unchanged)
        step_0: swap  ETH → USDC  on Ethereum
        step_1: bridge USDC       Ethereum → Base

    Plan B  (bridge_first — alternative ordering)
        step_0: bridge ETH        Ethereum → Base
        step_1: swap  ETH → USDC  on Base

Both are valid orderings with different gas, fee, and liquidity profiles.
All other plan shapes (single step, transfer, check_balance, parallel swaps,
etc.) return the original plan unchanged as the sole candidate — the
optimizer passes it through transparently.

Extensibility
-------------
New patterns (stake+bridge, lend+swap, …) are added by implementing a new
``_detect_*`` + ``_generate_*`` pair and registering it in
``generate_candidates()``.  Callers should ``await`` the async
``generate_candidates()`` API.

Token address resolution for Plan B
------------------------------------
Plan B's swap step runs on the destination chain, so the token addresses
(token_in, token_out) must be destination-chain contracts / mints.  We use
the shared resolver (MongoDB registry → Dexscreener) so there is a single
source of truth.  If any address cannot be resolved, Plan B is skipped
gracefully and only Plan A is returned.  The optimizer never blocks
execution.
"""

from __future__ import annotations

import logging
import asyncio
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from config.chains import get_chain_by_name
from config.solana_chains import get_solana_chain, is_solana_network
from core.planning.execution_plan import ExecutionPlan, PlanNode
from core.routing.bridge.token_resolver import ResolvedBridgeToken, resolve_bridge_token

_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ZERO = "0x0000000000000000000000000000000000000000"

# Tools that represent a swap operation (EVM and Solana).
_SWAP_TOOLS = frozenset({"swap", "solana_swap"})

# Tools that represent a bridge operation.
_BRIDGE_TOOLS = frozenset({"bridge"})

# Plan type labels stored in plan metadata for observability.
_LABEL_SWAP_FIRST = "swap_first"
_LABEL_BRIDGE_FIRST = "bridge_first"


# ---------------------------------------------------------------------------
# Token address resolution
# ---------------------------------------------------------------------------


def _get_chain_id(chain_name: str) -> Optional[int]:
    """Return the registered chain ID for *chain_name*."""
    name = chain_name.strip().lower()
    if is_solana_network(name):
        try:
            return get_solana_chain(name).chain_id
        except Exception:
            return None
    try:
        return get_chain_by_name(name).chain_id
    except Exception:
        return None


def _is_testnet(chain_name: str) -> bool:
    name = chain_name.strip().lower()
    if is_solana_network(name):
        try:
            return get_solana_chain(name).is_testnet
        except Exception:
            return False
    try:
        return get_chain_by_name(name).is_testnet
    except Exception:
        return False


async def _resolve_token(
    symbol: str,
    chain_name: str,
    chain_id: Optional[int],
) -> Optional[ResolvedBridgeToken]:
    if chain_id is None:
        return None
    try:
        return await resolve_bridge_token(
            symbol,
            chain_id=chain_id,
            chain_name=chain_name,
        )
    except Exception as exc:
        _LOGGER.debug(
            "[plan_gen] token resolution failed for %s on %s: %s",
            symbol,
            chain_name,
            exc,
        )
        return None


# ---------------------------------------------------------------------------
# Pattern detection
# ---------------------------------------------------------------------------


def _find_swap_bridge_sequence(
    plan: ExecutionPlan,
) -> Optional[Tuple[PlanNode, PlanNode]]:
    """
    Detect a sequential ``swap → bridge`` (or ``bridge → swap``) pattern
    in the plan.

    Detection rules
    ---------------
    * Exactly one swap node and exactly one bridge node must exist.
    * The bridge node must depend on the swap node (directly or indirectly),
      forming a sequential dependency, **or** the swap node depends on the
      bridge node.
    * Parallel plans (swap and bridge with no mutual dependency) are NOT
      considered for reordering because they operate on independent assets.

    Returns
    -------
    ``(swap_node, bridge_node)`` when the pattern is detected,
    ``None`` otherwise.
    """
    swap_nodes = [n for n in plan.nodes.values() if n.tool in _SWAP_TOOLS]
    bridge_nodes = [n for n in plan.nodes.values() if n.tool in _BRIDGE_TOOLS]

    if len(swap_nodes) != 1 or len(bridge_nodes) != 1:
        return None

    swap_node = swap_nodes[0]
    bridge_node = bridge_nodes[0]

    # Check for sequential dependency in either direction.
    bridge_after_swap = swap_node.id in bridge_node.depends_on
    swap_after_bridge = bridge_node.id in swap_node.depends_on

    if not bridge_after_swap and not swap_after_bridge:
        _LOGGER.debug(
            "[plan_gen] swap+bridge nodes exist but are parallel — no reordering"
        )
        return None

    # Normalise: always return (swap_node, bridge_node) regardless of order
    # in the original plan.  The generator will build both Plan A and Plan B
    # from this canonical form.
    return swap_node, bridge_node


# ---------------------------------------------------------------------------
# Plan A — preserve original (swap_first)
# ---------------------------------------------------------------------------


def _build_plan_a(original: ExecutionPlan) -> ExecutionPlan:
    """
    Plan A is the original plan produced by intent_resolver_node, unchanged.

    We only stamp the ``vws_label`` into the plan's metadata so the
    optimizer's reasoning log can refer to it by name.
    """
    plan = deepcopy(original)
    plan.metadata["vws_label"] = _LABEL_SWAP_FIRST
    return plan


# ---------------------------------------------------------------------------
# Plan B — bridge_first alternative
# ---------------------------------------------------------------------------


async def _build_plan_b(
    swap_node: PlanNode,
    bridge_node: PlanNode,
    original: ExecutionPlan,
) -> Tuple[Optional[ExecutionPlan], Optional[str]]:
    """
    Build the bridge_first alternative plan.

    Step 0  bridge the SWAP INPUT TOKEN from the swap's source chain to the
            bridge's destination chain.

    Step 1  swap the bridged token → original output token on the destination
            chain.  The input amount is a dynamic marker ``{{OUTPUT_OF:step_0}}``
            so the executor uses the actual bridged amount at runtime.

    Returns ``(None, reason)`` when Plan B cannot be built.  The optimizer
    will fall back to Plan A and surface the reason in reasoning_logs.

    Parameters
    ----------
    swap_node:
        The swap ``PlanNode`` from the original plan.
    bridge_node:
        The bridge ``PlanNode`` from the original plan.
    original:
        The full original plan (used to preserve non-swap/bridge nodes).
    """
    swap_args = swap_node.args
    bridge_args = bridge_node.args

    # ── Extract key fields ────────────────────────────────────────────────
    # Source side (where the swap currently happens)
    if swap_node.tool == "solana_swap":
        source_chain = str(
            swap_args.get("network") or bridge_args.get("source_chain") or ""
        ).strip().lower()
    else:
        source_chain = str(
            swap_args.get("chain") or bridge_args.get("source_chain") or ""
        ).strip().lower()
    token_in_symbol: str = str(swap_args.get("token_in_symbol") or "").strip().upper()
    token_in_address: str = ""
    amount_in_raw = swap_args.get("amount_in") or "0"

    # Destination side (where the swap will move to in Plan B)
    dest_chain: str = str(bridge_args.get("target_chain") or "").strip().lower()
    token_out_symbol: str = str(swap_args.get("token_out_symbol") or "").strip().upper()

    if (
        not source_chain
        or not dest_chain
        or not token_in_symbol
        or not token_out_symbol
    ):
        _LOGGER.debug(
            "[plan_gen] Plan B skipped: missing required fields "
            "(source_chain=%r, dest_chain=%r, token_in=%r, token_out=%r)",
            source_chain,
            dest_chain,
            token_in_symbol,
            token_out_symbol,
        )
        return None, "Plan B skipped: missing required swap/bridge fields"

    # ── Resolve destination chain ID (for registry lookups) ───────────────
    # ── Guard: skip testnet reordering ───────────────────────────────────
    if _is_testnet(source_chain) or _is_testnet(dest_chain):
        return (
            None,
            "Plan B skipped: testnet chain detected (swap/bridge reordering disabled)",
        )

    source_chain_id = _get_chain_id(source_chain)
    dest_chain_id = _get_chain_id(dest_chain)
    if source_chain_id is None or dest_chain_id is None:
        return None, "Plan B skipped: unknown chain id"

    # ── Resolve token addresses on source/destination chains ─────────────
    source_token, dest_token_in, dest_token_out = await asyncio.gather(
        _resolve_token(token_in_symbol, source_chain, source_chain_id),
        _resolve_token(token_in_symbol, dest_chain, dest_chain_id),
        _resolve_token(token_out_symbol, dest_chain, dest_chain_id),
    )
    if source_token is None:
        return (
            None,
            f"Plan B skipped: cannot resolve {token_in_symbol} on {source_chain}",
        )
    if dest_token_in is None:
        return (
            None,
            f"Plan B skipped: cannot resolve {token_in_symbol} on {dest_chain}",
        )
    if dest_token_out is None:
        return (
            None,
            f"Plan B skipped: cannot resolve {token_out_symbol} on {dest_chain}",
        )

    # ── Resolve source token address for the bridge step ──────────────────
    source_is_solana = is_solana_network(source_chain)
    dest_is_solana = is_solana_network(dest_chain)

    token_in_address = ""
    if swap_node.tool == "solana_swap":
        token_in_address = str(swap_args.get("token_in_mint") or "").strip()
    else:
        token_in_address = str(swap_args.get("token_in_address") or "").strip()

    if not token_in_address:
        token_in_address = source_token.address

    if not source_is_solana and source_token.is_native:
        token_in_address = _ZERO

    if not token_in_address:
        return (
            None,
            f"Plan B skipped: missing {token_in_symbol} address on {source_chain}",
        )

    if source_is_solana or dest_is_solana:
        # Require decimals to be resolved for Solana routes.
        if (
            source_token.decimals is None
            or dest_token_in.decimals is None
            or dest_token_out.decimals is None
        ):
            return None, "Plan B skipped: Solana token decimals unresolved"

    # ── Build Step 0: bridge token_in from source → destination ──────────
    bridge_first_args: Dict[str, Any] = {
        # Inherit wallet fields from the original bridge node.
        "sub_org_id": bridge_args.get("sub_org_id") or swap_args.get("sub_org_id"),
        "sender": bridge_args.get("sender") or swap_args.get("sender"),
        "recipient": bridge_args.get("recipient") or swap_args.get("sender"),
        # Route: source chain → destination chain.
        "token_symbol": token_in_symbol,
        "source_chain": source_chain,
        "target_chain": dest_chain,
        "source_address": token_in_address,
        # Amount: same as the original swap input.
        "amount": amount_in_raw,
    }

    bridge_first_node = PlanNode(
        id="step_0",
        tool="bridge",
        args=bridge_first_args,
        depends_on=[],
        approval_required=bridge_node.approval_required,
        retry_policy=deepcopy(bridge_node.retry_policy),
    )

    # ── Build Step 1: swap token_in → token_out on destination chain ──────
    swap_dest_args: Dict[str, Any] = {
        # Wallet fields.
        "sub_org_id": swap_args.get("sub_org_id"),
        "sender": swap_args.get("sender"),
        # Amount resolved at runtime from the bridge output.
        "amount_in": "{{OUTPUT_OF:step_0}}",
        # Inherit slippage setting if present.
        "slippage": swap_args.get("slippage", 0.5),
    }

    swap_tool = "solana_swap" if dest_is_solana else "swap"
    dest_token_in_address = dest_token_in.address
    dest_token_out_address = dest_token_out.address
    if not dest_is_solana and dest_token_in.is_native:
        dest_token_in_address = _ZERO
    if not dest_is_solana and dest_token_out.is_native:
        dest_token_out_address = _ZERO
    if dest_is_solana:
        swap_dest_args.update(
            {
                "token_in_symbol": token_in_symbol,
                "token_out_symbol": token_out_symbol,
                "token_in_mint": dest_token_in_address,
                "token_out_mint": dest_token_out_address,
                "network": dest_chain,
            }
        )
    else:
        swap_dest_args.update(
            {
                "token_in_symbol": token_in_symbol,
                "token_in_address": dest_token_in_address,
                "token_out_symbol": token_out_symbol,
                "token_out_address": dest_token_out_address,
                "chain": dest_chain,
            }
        )

    swap_dest_node = PlanNode(
        id="step_1",
        tool=swap_tool,
        args=swap_dest_args,
        depends_on=["step_0"],
        approval_required=swap_node.approval_required,
        retry_policy=deepcopy(swap_node.retry_policy),
    )

    # ── Assemble Plan B ───────────────────────────────────────────────────
    # Start from a deep copy of the original to preserve any non-swap/bridge
    # nodes (e.g. check_balance steps), then replace the swap/bridge pair.
    plan_b = deepcopy(original)
    plan_b.nodes = {}

    # Re-insert any nodes that are neither the original swap nor the original
    # bridge (e.g. a check_balance step that precedes the main action).
    for node_id, node in original.nodes.items():
        if node.tool in _SWAP_TOOLS or node.tool in _BRIDGE_TOOLS:
            continue
        plan_b.nodes[node_id] = deepcopy(node)

    # Insert the reordered pair.
    plan_b.nodes["step_0"] = bridge_first_node
    plan_b.nodes["step_1"] = swap_dest_node

    plan_b.version = original.version  # version bump happens in optimizer
    plan_b.metadata["vws_label"] = _LABEL_BRIDGE_FIRST

    return plan_b, None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def generate_candidates(
    plan: ExecutionPlan,
) -> Tuple[List[ExecutionPlan], Optional[str]]:
    """
    Return a list of candidate ``ExecutionPlan`` objects derived from *plan*.

    Behaviour
    ---------
    * When the plan contains a detectable ``swap + bridge`` sequence, returns
      ``[plan_a, plan_b]`` — Plan A is the original ordering, Plan B is the
      flipped ordering.  Plan B is omitted if its token addresses cannot be
      resolved for the destination chain.
    * For all other plan shapes (single-step, parallel, transfer-only, etc.)
      returns ``[plan]`` unchanged.  The plan_optimizer_node will pass it
      through without calling VWS or the scorer.

    The returned list is always non-empty.  The first element is always the
    original plan (Plan A), preserving backward compatibility — if VWS and
    scoring are skipped or fail, the original plan is used unchanged.

    Parameters
    ----------
    plan:
        The ``ExecutionPlan`` produced by ``intent_resolver_node``.

    Returns
    -------
    list[ExecutionPlan], Optional[str]
        One or two candidate plans plus an optional skip reason when
        Plan B could not be built.
    """
    result = _find_swap_bridge_sequence(plan)
    if result is None:
        _LOGGER.debug("[plan_gen] no swap+bridge pattern — single candidate")
        return [plan], None

    swap_node, bridge_node = result
    _LOGGER.info(
        "[plan_gen] swap+bridge pattern detected "
        "(swap=%s, bridge=%s) — generating alternatives",
        swap_node.id,
        bridge_node.id,
    )

    plan_a = _build_plan_a(plan)

    plan_b, skip_reason = await _build_plan_b(swap_node, bridge_node, plan)
    if plan_b is None:
        reason = skip_reason or "Plan B skipped (address resolution failed)"
        _LOGGER.info("[plan_gen] %s — returning Plan A only", reason)
        return [plan_a], reason

    _LOGGER.info("[plan_gen] generated 2 candidates: swap_first, bridge_first")
    return [plan_a, plan_b], None
