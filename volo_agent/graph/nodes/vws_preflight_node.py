from __future__ import annotations

from typing import Any, Dict

from graph.agent_state import AgentState
from graph.nodes.balance_check_node import run_vws_preflight


async def vws_preflight_node(state: AgentState) -> Dict[str, Any]:
    """
    Deterministic VWS preflight stage.

    Runs the pure Virtual Wallet State simulation against the latest plan and
    current balance snapshot inputs, then writes balance
    and reservation metadata into AgentState for the downstream balance check.
    """
    return await run_vws_preflight(state)
