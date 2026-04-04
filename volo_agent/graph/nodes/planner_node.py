import json
import threading
import time
from typing import Any, Callable, Dict, Iterable, cast

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from core.memory.ledger import ErrorCategory, get_ledger
from core.planning.execution_plan import (
    ExecutionPlan,
    ExecutionState,
    NodeState,
    PlanNode,
    StepStatus,
    check_plan_complete,
    create_node_reset_state,
    get_ready_nodes,
    resolve_dynamic_args,
)
from core.tasks.updater import upsert_task_from_state
from core.utils.circuit_breaker import CircuitBreaker
from graph.agent_state import AgentState

# Tests patch this symbol directly. Keep it module-level, but load lazily.
planning_llm: Any | None = None
tools_registry: Any | None = None
_PLANNING_LLM_CACHE: Any | None = None
_TOOLS_REGISTRY_CACHE: Any | None = None
_PROMPT_CACHE_TTL_SECONDS = 5.0
_PROMPT_CACHE_LOCK = threading.Lock()
_LEDGER_SUMMARY_CACHE: Dict[str, Any] = {"value": None, "expires_at": 0.0}
_DISABLED_TOOLS_CACHE: Dict[str, Any] = {"value": None, "expires_at": 0.0}


def _get_planning_llm():
    global planning_llm, _PLANNING_LLM_CACHE
    if planning_llm is not None:
        return planning_llm
    if _PLANNING_LLM_CACHE is None:
        from llms.llms_init import planning_llm as _planning_llm

        _PLANNING_LLM_CACHE = _planning_llm
    return _PLANNING_LLM_CACHE


def _get_tools_registry():
    global tools_registry, _TOOLS_REGISTRY_CACHE
    if tools_registry is not None:
        return tools_registry
    if _TOOLS_REGISTRY_CACHE is None:
        from tools_registry.register import tools_registry as _tools_registry

        _TOOLS_REGISTRY_CACHE = _tools_registry
    return _TOOLS_REGISTRY_CACHE


def _get_ttl_cached(cache: Dict[str, Any], loader: Callable[[], Any]) -> Any:
    # Planner prompts are built frequently; a short TTL avoids repeated disk
    # reads while keeping reliability context fresh enough for replanning.
    now = time.monotonic()
    with _PROMPT_CACHE_LOCK:
        if now < float(cache.get("expires_at") or 0):
            return cache.get("value")

    value = loader()
    with _PROMPT_CACHE_LOCK:
        cache["value"] = value
        cache["expires_at"] = time.monotonic() + _PROMPT_CACHE_TTL_SECONDS
    return value


def _compact_json_for_prompt(
    value: Any,
    *,
    depth: int = 0,
    max_depth: int = 2,
    max_items: int = 5,
    max_string: int = 240,
) -> Any:
    if isinstance(value, str):
        if len(value) <= max_string:
            return value
        return f"{value[:max_string]}...<trimmed>"
    if isinstance(value, dict):
        items = list(value.items())
        if depth >= max_depth:
            compacted = {
                str(key): (
                    f"<{type(val).__name__}>"
                    if isinstance(val, (dict, list, tuple))
                    else _compact_json_for_prompt(
                        val,
                        depth=depth + 1,
                        max_depth=max_depth,
                        max_items=max_items,
                        max_string=max_string,
                    )
                )
                for key, val in items[:max_items]
            }
            return compacted | (
                {"_trimmed_keys": len(items) - max_items}
                if len(items) > max_items
                else {}
            )
        compacted = {
            str(key): _compact_json_for_prompt(
                val,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
                max_string=max_string,
            )
            for key, val in items[:max_items]
        }
        if len(items) > max_items:
            compacted["_trimmed_keys"] = len(items) - max_items
        return compacted
    if isinstance(value, list):
        if depth >= max_depth:
            compacted = [
                (
                    f"<{type(item).__name__}>"
                    if isinstance(item, (dict, list, tuple))
                    else _compact_json_for_prompt(
                        item,
                        depth=depth + 1,
                        max_depth=max_depth,
                        max_items=max_items,
                        max_string=max_string,
                    )
                )
                for item in value[:max_items]
            ]
            if len(value) > max_items:
                compacted.append(f"...<{len(value) - max_items} more items>")
            return compacted
        compacted = [
            _compact_json_for_prompt(
                item,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
                max_string=max_string,
            )
            for item in value[:max_items]
        ]
        if len(value) > max_items:
            compacted.append(f"...<{len(value) - max_items} more items>")
        return compacted
    return value


def _compact_outputs_for_prompt(outputs: Dict[str, Any]) -> Dict[str, Any]:
    compacted: Dict[str, Any] = {}
    for node_id, output in outputs.items():
        if not isinstance(output, dict):
            compacted[node_id] = output
            continue
        compact_output = {
            key: output[key]
            for key in (
                "output_amount",
                "amount_out",
                "amount_out_minimum",
                "amount_in",
                "amount",
            )
            if output.get(key) is not None
        }
        details = output.get("details")
        if isinstance(details, dict):
            compact_details = {
                key: details[key]
                for key in (
                    "output_amount",
                    "amount_out",
                    "amount_out_minimum",
                    "amount_in",
                    "amount",
                )
                if details.get(key) is not None
            }
            if compact_details:
                compact_output["details"] = compact_details
        if not compact_output:
            # Keep the high-signal fields and trim bulky metadata from tool outputs.
            compact_output = _compact_json_for_prompt(output, max_depth=1, max_items=4)
        compacted[node_id] = compact_output
    return compacted


def _compact_history_for_prompt(
    *,
    current_plan: ExecutionPlan,
    execution_state: ExecutionState,
    context: Dict[str, Any] | None,
    max_entries: int = 5,
    recent_entries: int = 3,
) -> list[Dict[str, Any]]:
    history_data: list[Dict[str, Any]] = []
    for node_id, node_state in execution_state.node_states.items():
        node = current_plan.nodes.get(node_id)
        if node:
            resolved_args = resolve_dynamic_args(
                node.args,
                execution_state,
                context=context,
            )
            tool_name = node.tool
        else:
            resolved_args = {}
            tool_name = None

        history_data.append(
            {
                "node_id": node_id,
                "tool": tool_name,
                "status": node_state.status,
                "result": _compact_json_for_prompt(
                    node_state.result,
                    max_depth=1,
                    max_items=4,
                ),
                "error": node_state.error,
                "error_category": node_state.error_category,
                "resolved_args": _compact_json_for_prompt(resolved_args, max_depth=2),
            }
        )

    if len(history_data) <= max_entries:
        return history_data

    selected: list[Dict[str, Any]] = []
    seen: set[str] = set()
    # Keep the freshest context plus failures so the planner sees the current
    # blocking issues without paying to replay the entire execution history.
    failed_entries = [
        entry
        for entry in history_data
        if str(entry.get("status") or "") == StepStatus.FAILED.value
    ]
    recent_history = history_data[-recent_entries:]
    for entry in failed_entries + recent_history:
        node_id = str(entry.get("node_id") or "")
        if not node_id or node_id in seen:
            continue
        seen.add(node_id)
        selected.append(entry)

    return selected[-max_entries:]


def _tool_docs_for_prompt(
    *,
    current_plan: ExecutionPlan,
    disabled_tools: Iterable[str],
) -> list[Dict[str, Any]]:
    registry = _get_tools_registry()
    # Limit schemas to the tools the current plan is actively using or routing
    # around, which trims prompt size without changing the planner API.
    relevant_tools = {node.tool for node in current_plan.nodes.values()}
    relevant_tools.update(
        str(item).split(":", 1)[0].strip()
        for item in disabled_tools
        if str(item).strip()
    )
    if not relevant_tools:
        relevant_tools = set(registry.tools.keys())

    return [
        {
            "name": name,
            "description": tool.description,
            "args_schema": tool.args_schema.model_json_schema()
            if tool.args_schema
            else None,
        }
        for name, tool in registry.tools.items()
        if name in relevant_tools
    ]


PLANNER_SYSTEM_PROMPT = """
You are the Volo Agent Planner. Your goal is to manage a Directed Acyclic Graph (DAG) execution plan for crypto transactions.

Current Goal: {goal}

Goal Parameters (Fixed Context):
{goal_params}

Structured Outputs (Normalized):
{outputs}

System Performance Context (Global Tool Reliability):
{performance_ledger}

Currently DISABLED Tools (Due to Open Circuits):
{disabled_tools}

Relevant Tool Schemas:
{tools}

Execution History (Current Attempt Status):
{history}
...
### Self-Healing Strategy:
1. **Slippage Errors**: Increase slippage tolerance or try a different DEX/route.
2. **Liquidity Errors**: Switch to a different bridge or aggregator.
3. **Gas Errors**: If gas is high, suggest waiting or using a different chain. For transfers, retry with a fresh gas price.
4. **Network Errors**: If a tool has high network failure rates or is listed as DISABLED, you MUST choose an alternative route or fail.
5. **Security Errors**: DO NOT retry. Stop immediately.

Your task:
...
1. Analyze the execution history, global performance data, and circuit status.
2. Determine if the goal has been achieved.
3. If not achieved, and more steps are needed, propose the NEXT steps.
4. AVOID tools that are currently DISABLED for the specific chain. If no alternative path exists to reach the goal, return "status": "FAILED" with reasoning.
5. If an error occurred, apply the Self-Healing Strategy.


Format your response as a JSON object:
{{
    "status": "CONTINUE" | "FINISHED" | "FAILED",
    "new_nodes": [
        {{
            "id": "unique_node_id",
            "tool": "tool_name",
            "args": {{ ... }},
            "depends_on": ["prev_node_id"],
            "approval_required": true
        }}
    ],
    "reasoning": "A concise summary of what you decided and why. Mention if you are applying a recovery strategy based on error categories."
}}

IMPORTANT:
- DO NOT propose nodes that are ALREADY in the execution history or plan with IDENTICAL parameters.
- To FIX a failed step (e.g., higher slippage, different bridge), REUSE its 'id' and update the 'args'.
- Only add nodes that are necessary to achieve the goal given the CURRENT state.
- Ensure 'depends_on' correctly references existing or new node IDs.
- 'args' must match the tool's schema.
- If an argument is a dynamic placeholder like "{{TOTAL_BALANCE}}" or "{{SUM_FROM_PREVIOUS}}", keep it as is.
"""


def _balance_completion_message(
    current_plan: ExecutionPlan, execution_state: ExecutionState
) -> str | None:
    if not current_plan.nodes:
        return None
    if any(node.tool != "check_balance" for node in current_plan.nodes.values()):
        return None

    messages: list[str] = []
    for node_id in sorted(current_plan.nodes.keys()):
        node_state = execution_state.node_states.get(node_id)
        if not node_state or node_state.status != StepStatus.SUCCESS:
            continue
        result = node_state.result if isinstance(node_state.result, dict) else {}
        message = str(result.get("message") or "").strip()
        if message:
            messages.append(message)

    if not messages:
        return None
    return "\n\n".join(messages)


def _has_identical_ai_message(messages: Any, content: str) -> bool:
    target = str(content or "").strip()
    if not target:
        return False
    if not isinstance(messages, list):
        return False
    for message in reversed(messages):
        if not isinstance(message, AIMessage):
            continue
        text = str(message.content or "").strip()
        if text == target:
            return True
    return False


async def planner_node(state: AgentState) -> Dict[str, Any]:
    """
    The ReAct Planner Node. Analyzes execution state and mutates the plan if needed.
    """
    history = state.get("plan_history", [])
    if not history:
        return {}

    # Always work with the LATEST plan
    current_plan = history[-1]
    execution_state = state.get("execution_state")

    if not current_plan or not execution_state:
        return {}

    # Hard stop on non-retryable failures (avoid LLM-driven retries).
    for node_id, node_state in execution_state.node_states.items():
        if (
            node_state.status == StepStatus.FAILED
            and node_state.error_category == ErrorCategory.NON_RETRYABLE.value
        ):
            err_msg = node_state.error or "Step failed with a non-retryable error."
            log_msg = f"[THOUGHT] {node_id} failed with a non-retryable error."
            await upsert_task_from_state(
                cast(Dict[str, Any], state),
                title=str(getattr(current_plan, "goal", "") or "Task"),
                status="FAILED",
                latest_summary=err_msg,
                tool=(
                    next(iter(current_plan.nodes.values())).tool
                    if getattr(current_plan, "nodes", None)
                    else None
                ),
                error_category=ErrorCategory.NON_RETRYABLE.value,
            )
            return {
                "route_decision": "FAILED",
                "reasoning_logs": [log_msg],
                "messages": [
                    AIMessage(content=f"Error executing {node_id}: {err_msg}")
                ],
            }

    # 1. Resolve any dynamic markers in the existing plan nodes
    # (Note: we don't return the resolved markers here as the executor will do it anyway,
    # but we do it locally so the LLM prompt has the latest data)
    history_data = _compact_history_for_prompt(
        current_plan=current_plan,
        execution_state=execution_state,
        context=state.get("artifacts"),
    )

    def _has_in_flight_steps(current_state: ExecutionState) -> bool:
        return any(
            ns.status in {StepStatus.RUNNING, StepStatus.PENDING}
            for ns in current_state.node_states.values()
        )

    # 2. If already marked completed by executor, we just confirm
    if execution_state.completed:
        balance_message = _balance_completion_message(current_plan, execution_state)
        if balance_message:
            await upsert_task_from_state(
                cast(Dict[str, Any], state),
                title=str(getattr(current_plan, "goal", "") or "Task"),
                status="COMPLETED",
                latest_summary=balance_message,
                tool=(
                    next(iter(current_plan.nodes.values())).tool
                    if getattr(current_plan, "nodes", None)
                    else None
                ),
            )
            if _has_identical_ai_message(state.get("messages"), balance_message):
                return {
                    "route_decision": "FINISHED",
                    "confirmation_status": None,
                    "intents": [],
                    "reasoning_logs": ["[THOUGHT] Balance request completed."],
                }
            return {
                "route_decision": "FINISHED",
                "confirmation_status": None,
                "intents": [],
                "reasoning_logs": ["[THOUGHT] Balance request completed."],
                "messages": [AIMessage(content=balance_message)],
            }
        await upsert_task_from_state(
            cast(Dict[str, Any], state),
            title=str(getattr(current_plan, "goal", "") or "Task"),
            status="COMPLETED",
            latest_summary="Done. Your request is complete.",
            tool=(
                next(iter(current_plan.nodes.values())).tool
                if getattr(current_plan, "nodes", None)
                else None
            ),
        )
        return {
            "route_decision": "FINISHED",
            "confirmation_status": None,
            "intents": [],
            "reasoning_logs": ["[THOUGHT] Goal achieved."],
            "messages": [AIMessage(content="Done. Your request is complete.")],
        }

    # 2b. If nothing is ready and we're just waiting on running/pending steps,
    # skip the LLM call to avoid noisy loops.
    if state.get("waiting_for_funds"):
        return {
            "route_decision": "WAITING_FUNDS",
            "reasoning_logs": [
                "[THOUGHT] Waiting for reserved funds to become available."
            ],
        }

    ready_nodes = get_ready_nodes(current_plan, execution_state)
    if not ready_nodes:
        has_in_flight = _has_in_flight_steps(execution_state)
        if has_in_flight:
            return {
                "route_decision": "WAITING",
                "reasoning_logs": [
                    "[THOUGHT] Waiting for in-flight steps to complete."
                ],
                "messages": [
                    AIMessage(
                        content=(
                            "A step is still running. I'll continue automatically "
                            "when it completes."
                        )
                    )
                ],
            }

    # ── Deterministic short-circuit (happy path) ─────────────────────────────
    # If there are ready nodes and nothing has failed, the plan is on the happy
    # path — there is nothing to heal or re-plan.  Calling the LLM here would
    # just burn tokens to return "CONTINUE".  Skip it entirely.
    #
    # We only invoke the LLM when:
    #   a) At least one node has FAILED (self-healing / replanning needed), or
    #   b) There are no ready nodes (potential deadlock — LLM decides next step).
    has_any_failure = any(
        ns.status == StepStatus.FAILED for ns in execution_state.node_states.values()
    )

    if (
        not ready_nodes
        and not has_any_failure
        and not check_plan_complete(current_plan, execution_state)
    ):
        return {
            "route_decision": "WAITING",
            "reasoning_logs": [
                "[THOUGHT] Plan is not complete, but no additional steps are ready yet."
            ],
            "messages": [
                AIMessage(
                    content=(
                        "A step is still pending. I'll continue automatically "
                        "when it can proceed."
                    )
                )
            ],
        }

    if ready_nodes and not has_any_failure:
        return {
            "route_decision": "CONTINUE",
            "reasoning_logs": [
                f"[THOUGHT] {len(ready_nodes)} node(s) ready, no failures — "
                "continuing without LLM replanning."
            ],
        }

    # ── Prepare tool descriptions for the LLM prompt ─────────────────────────
    ledger = get_ledger()
    cb = CircuitBreaker(ledger)
    disabled_tools = _get_ttl_cached(_DISABLED_TOOLS_CACHE, cb.get_disabled_tools) or []
    tool_docs = _tool_docs_for_prompt(
        current_plan=current_plan,
        disabled_tools=disabled_tools,
    )
    goal_params = _compact_json_for_prompt(
        state.get("goal_parameters", {}), max_depth=2
    )
    raw_outputs = (execution_state.artifacts or {}).get("outputs", {})
    outputs = _compact_outputs_for_prompt(
        raw_outputs if isinstance(raw_outputs, dict) else {}
    )

    prompt = PLANNER_SYSTEM_PROMPT.format(
        goal=current_plan.goal,
        goal_params=json.dumps(goal_params, indent=2, default=str),
        outputs=json.dumps(outputs, indent=2, default=str),
        performance_ledger=_get_ttl_cached(_LEDGER_SUMMARY_CACHE, ledger.get_summary),
        disabled_tools=json.dumps(disabled_tools, indent=2)
        if disabled_tools
        else "None",
        history=json.dumps(history_data, indent=2, default=str),
        tools=json.dumps(tool_docs, indent=2, default=str),
    )

    # Call the LLM
    llm = _get_planning_llm()
    if llm is None:
        raise RuntimeError("planning_llm is not initialized")
    response = await llm.ainvoke(
        [SystemMessage(content=prompt), HumanMessage(content="What is the next step?")]
    )

    try:
        content = str(response.content)
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        data = json.loads(content)

        status = data.get("status", "FINISHED")
        reasoning = data.get("reasoning", "Thinking...")
        new_nodes_data = data.get("new_nodes", [])

        # Check if we actually need a NEW plan version
        if new_nodes_data:
            # CREATE NEW VERSION
            new_plan = current_plan.model_copy(deep=True)
            new_plan.version = current_plan.version + 1

            # Create a DELTA execution state for the new/mutated nodes
            updated_execution_state = ExecutionState(node_states={})

            needs_approval = False
            mutated_count = 0
            new_count = 0

            for node_data in new_nodes_data:
                node_id = node_data["id"]

                # Check if this is a "Mutation Fix" (reusing existing ID to update args)
                if node_id in new_plan.nodes:
                    existing_node = new_plan.nodes[node_id]
                    if existing_node.args != node_data["args"]:
                        # PERFORM MUTATION & RESET
                        existing_node.args = node_data["args"]
                        updated_execution_state = updated_execution_state.merge(
                            create_node_reset_state(node_id)
                        )
                        mutated_count += 1
                        if node_data.get("approval_required", True):
                            needs_approval = True
                    continue

                # HARD DEDUPLICATION (same tool/args elsewhere)
                already_exists = False
                for existing_node in new_plan.nodes.values():
                    if (
                        existing_node.tool == node_data["tool"]
                        and existing_node.args == node_data["args"]
                    ):
                        already_exists = True
                        break

                if not already_exists:
                    app_req = node_data.get("approval_required", True)
                    if app_req:
                        needs_approval = True

                    new_node = PlanNode(
                        id=node_id,
                        tool=node_data["tool"],
                        args=node_data["args"],
                        depends_on=node_data["depends_on"],
                        approval_required=app_req,
                    )
                    new_plan.nodes[node_id] = new_node
                    # Initialize state for the new node in our DELTA
                    updated_execution_state.node_states[node_id] = NodeState(
                        node_id=node_id
                    )
                    new_count += 1

            if mutated_count == 0 and new_count == 0:
                # No actual changes needed
                return {
                    "route_decision": "CONTINUE",
                    "reasoning_logs": ["[THOUGHT] No new steps required."],
                }

            decision = "CONTINUE"
            if needs_approval:
                decision = "REQUIRE_APPROVAL"

            log_msg = f"[THOUGHT] {reasoning} (Plan v{new_plan.version}"
            if mutated_count:
                log_msg += f", fixed {mutated_count} nodes"
            if new_count:
                log_msg += f", added {new_count} nodes"
            log_msg += ")"

            return {
                "plan_history": [new_plan],
                "execution_state": updated_execution_state,
                "route_decision": decision,
                "reasoning_logs": [log_msg],
                "messages": [AIMessage(content=reasoning)],
            }

        # If no new nodes, just update status
        if status == "FINISHED":
            if not check_plan_complete(
                current_plan, execution_state
            ) or _has_in_flight_steps(execution_state):
                return {
                    "route_decision": "WAITING",
                    "reasoning_logs": [
                        "[THOUGHT] Ignored premature FINISHED from planner while steps remain pending."
                    ],
                    "messages": [
                        AIMessage(
                            content=(
                                "A step is still pending. I'll continue automatically "
                                "when it can proceed."
                            )
                        )
                    ],
                }
            await upsert_task_from_state(
                cast(Dict[str, Any], state),
                title=str(getattr(current_plan, "goal", "") or "Task"),
                status="COMPLETED",
                latest_summary=reasoning,
                tool=(
                    next(iter(current_plan.nodes.values())).tool
                    if getattr(current_plan, "nodes", None)
                    else None
                ),
            )
            return {
                "execution_state": ExecutionState(node_states={}, completed=True),
                "route_decision": "FINISHED",
                "confirmation_status": None,
                "intents": [],
                "reasoning_logs": [f"[THOUGHT] {reasoning}"],
                "messages": [AIMessage(content=reasoning)],
            }

        return {
            "route_decision": "CONTINUE",
            "reasoning_logs": [f"[THOUGHT] {reasoning}"],
            "messages": [AIMessage(content=reasoning)],
        }

    except Exception as e:
        await upsert_task_from_state(
            cast(Dict[str, Any], state),
            title=str(getattr(current_plan, "goal", "") or "Task"),
            status="FAILED",
            latest_summary=f"Planner error: {str(e)}",
            tool=(
                next(iter(current_plan.nodes.values())).tool
                if getattr(current_plan, "nodes", None)
                else None
            ),
        )
        return {
            "route_decision": "FAILED",
            "messages": [AIMessage(content=f"Planner error: {str(e)}")],
        }
