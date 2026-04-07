from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from config.chains import get_chain_by_name
from config.solana_chains import get_solana_chain, is_solana_network
from core.planning.execution_plan import ExecutionPlan, PlanNode
from core.routing.bridge.token_resolver import ResolvedBridgeToken, resolve_bridge_token

_LOGGER = logging.getLogger(__name__)
_ZERO = "0x0000000000000000000000000000000000000000"
_SWAP_TOOLS = frozenset({"swap", "solana_swap"})
_BRIDGE_TOOLS = frozenset({"bridge"})
_LABEL_SWAP_FIRST = "swap_first"
_LABEL_BRIDGE_FIRST = "bridge_first"


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


def _find_swap_bridge_sequence(
    plan: ExecutionPlan,
) -> Optional[Tuple[PlanNode, PlanNode]]:
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


def _build_plan_a(original: ExecutionPlan) -> ExecutionPlan:
    plan = deepcopy(original)
    plan.metadata["vws_label"] = _LABEL_SWAP_FIRST
    return plan


async def _build_plan_b(
    swap_node: PlanNode,
    bridge_node: PlanNode,
    original: ExecutionPlan,
) -> Tuple[Optional[ExecutionPlan], Optional[str]]:
    swap_args = swap_node.args
    bridge_args = bridge_node.args
    # Source side (where the swap currently happens)
    if swap_node.tool == "solana_swap":
        source_chain = (
            str(swap_args.get("network") or bridge_args.get("source_chain") or "")
            .strip()
            .lower()
        )
    else:
        source_chain = (
            str(swap_args.get("chain") or bridge_args.get("source_chain") or "")
            .strip()
            .lower()
        )
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

    if _is_testnet(source_chain) or _is_testnet(dest_chain):
        return (
            None,
            "Plan B skipped: testnet chain detected (swap/bridge reordering disabled)",
        )

    source_chain_id = _get_chain_id(source_chain)
    dest_chain_id = _get_chain_id(dest_chain)
    if source_chain_id is None or dest_chain_id is None:
        return None, "Plan B skipped: unknown chain id"

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

    bridge_first_args: Dict[str, Any] = {
        "sub_org_id": bridge_args.get("sub_org_id") or swap_args.get("sub_org_id"),
        "sender": bridge_args.get("sender") or swap_args.get("sender"),
        "recipient": bridge_args.get("recipient") or swap_args.get("sender"),
        "token_symbol": token_in_symbol,
        "source_chain": source_chain,
        "target_chain": dest_chain,
        "source_address": token_in_address,
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

    swap_dest_args: Dict[str, Any] = {
        "sub_org_id": swap_args.get("sub_org_id"),
        "sender": swap_args.get("sender"),
        "amount_in": "{{OUTPUT_OF:step_0}}",
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

    plan_b = deepcopy(original)
    plan_b.nodes = {}
    
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

async def generate_candidates(
    plan: ExecutionPlan,
) -> Tuple[List[ExecutionPlan], Optional[str]]:
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
