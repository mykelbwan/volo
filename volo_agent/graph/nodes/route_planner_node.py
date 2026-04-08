from __future__ import annotations

import logging
from typing import Any, Dict, cast

from graph.agent_state import AgentState
from graph.nodes.runtime_factories import build_routing_service

_LOGGER = logging.getLogger("volo.route_planner_node")
_GLOBAL_ROUTING_TIMEOUT_SECONDS: float = 12.0

_routing_service = build_routing_service(
    global_timeout_seconds=_GLOBAL_ROUTING_TIMEOUT_SECONDS
)

async def route_planner_node(state: AgentState) -> Dict[str, Any]:
    return await _routing_service.route_plan(cast(Dict[str, Any], state))
