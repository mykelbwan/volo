import os
from functools import lru_cache
from typing import Annotated, Any, Dict, List, Optional, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

from core.planning.execution_plan import ExecutionPlan, ExecutionState
from graph.replay_guard import observe_history_retention


def add_logs(left: List[str], right: List[str]) -> List[str]:
    if left is None:
        left = []
    if right is None:
        right = []
    return left + right


def merge_execution_state(
    left: Optional[ExecutionState], right: Optional[ExecutionState]
) -> ExecutionState:
    if left is None:
        return right or ExecutionState(node_states={})
    if right is None:
        return left
    return left.merge(right)


_MAX_MESSAGE_HISTORY_DEFAULT = 80


def _load_max_history() -> int:
    raw = os.getenv("VOLO_MAX_MESSAGE_HISTORY", "").strip()
    if not raw:
        return _MAX_MESSAGE_HISTORY_DEFAULT
    try:
        value = int(raw)
        return value if value >= 0 else _MAX_MESSAGE_HISTORY_DEFAULT
    except ValueError:
        return _MAX_MESSAGE_HISTORY_DEFAULT


# Cached for performance — avoid repeated os.getenv calls
# on every message addition in the graph.
@lru_cache(maxsize=1)
def _max_message_history() -> int:
    raw = os.getenv("VOLO_MAX_MESSAGE_HISTORY", "").strip()
    if not raw:
        return _MAX_MESSAGE_HISTORY_DEFAULT
    try:
        value = int(raw)
        return value if value >= 0 else _MAX_MESSAGE_HISTORY_DEFAULT
    except ValueError:
        return _MAX_MESSAGE_HISTORY_DEFAULT


def add_messages_bounded(left, right):
    merged = add_messages(left, right)
    max_history = _max_message_history()
    if max_history <= 0:
        return list(merged)
    merged_list = list(merged)
    if len(merged_list) <= max_history:
        return merged_list
    trimmed = merged_list[-max_history:]
    observe_history_retention(before_count=len(merged_list), after_count=len(trimmed))
    return trimmed


class AgentState(TypedDict):
    user_id: str  # Platform-specific ID (Telegram ID, Discord ID, etc)
    provider: str  # The platform provider (e.g. 'telegram', 'discord', 'web')
    username: Optional[str]  # Platform-specific username
    user_info: Optional[Dict[str, Any]]  # volo_user_id, wallet address, sub_org_id, etc
    intents: List[Dict[str, Any]]
    plans: List[Dict[str, Any]]
    goal_parameters: Dict[str, Any]  # Persistent context for the goal
    plan_history: Annotated[List[ExecutionPlan], add_logs]  # Historical DAGs
    candidate_plans: Optional[List[ExecutionPlan]]
    execution_state: Annotated[Optional[ExecutionState], merge_execution_state]
    artifacts: dict
    context: dict
    route_decision: Optional[str]
    confirmation_status: Optional[str]
    pending_transactions: List[dict]
    reasoning_logs: Annotated[List[str], add_logs]  # Internal system logs
    messages: Annotated[List[BaseMessage], add_messages_bounded]
    fee_quotes: Optional[List[Dict[str, Any]]]
    balance_snapshot: Optional[Dict[str, str]]
    resource_snapshots: Optional[Dict[str, Dict[str, Any]]]
    native_requirements: Optional[Dict[str, str]]
    reservation_requirements: Optional[Dict[str, List[Dict[str, Any]]]]
    projected_deltas: Optional[Dict[str, str]]
    preflight_estimates: Optional[Dict[str, Dict[str, Any]]]
    vws_simulation: Optional[Dict[str, Dict[str, Any]]]
    vws_failure: Optional[Dict[str, Any]]
    route_decisions: Optional[Dict[str, Dict[str, Any]]]
    plan_optimizer_debug: Optional[Dict[str, Any]]
    guardrail_policy: Optional[Dict[str, Any]]
    trigger_id: Optional[str]
    is_triggered_execution: Optional[bool]
    execution_id: Optional[str]
    trigger_fire_id: Optional[str]
    pending_cancel: Optional[Dict[str, Any]]
    pending_edit: Optional[Dict[str, Any]]
    pending_intent: Optional[Dict[str, Any]]
    pending_intent_queue: Optional[List[Dict[str, Any]]]
    pending_clarification: Optional[Dict[str, Any]]
    parse_scope: Optional[str]
    selected_task_number: Optional[int]
    waiting_for_funds: Optional[Dict[str, Any]]
    auto_resume_execution: Optional[bool]
