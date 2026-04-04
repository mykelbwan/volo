"""
Volo Agent – compiled LangGraph application.

Node wiring
-----------

  START
    └─► onboarding
          ├─(end)──────────────────────────────────────────────────► END
          └─(router)────────────────────────────────────────────────►
                conversational_router
                  ├─(end)─────────────────────────────────────────► END
                  ├─(execute)─────────────────────────────────────►
                  │           execution_engine ──► planner_node
                  │                                    ├─(continue)─► execution_engine
                  │                                    ├─(approval)─► confirmation_node ──► END
                  │                                    └─(end)──────► END
                  └─(parse)────────────────────────────────────────►
                              intent_parser
                                ├─(end)───────────────────────────► END  [slot-fill]
                                ├─(wait_trigger)──────────────────►
                                │   wait_for_trigger_node
                                │     ├─(end)────────────────────► END  [cancelled/expired]
                                │     └─(resolve)────────────────►
                                │                  intent_resolver
                                │                    └──────────────►
                                │                                route_planner
                                │                                  └──────────────►
                                │                                      plan_optimizer
                                │                                        └────────────►
                                └─(resolve)───────────────────────►
                                            intent_resolver
                                              └──────────────────►
                                                          route_planner
                                                            └──────────────────►
                                                                  plan_optimizer
                                                                    └────────────►
                                                                        balance_check
                                                                          ├─(end)────────────────────────► END  [shortfall]
                                                                          ├─(confirm)────────────────────►
                                                                          │           confirmation_node ──► END
                                                                          └─(execute)────────────────────►
                                                                                      execution_engine ──► planner_node

Persistence
-----------
MongoDBSaver replaces MemorySaver so that:
  - Interrupted threads (wait_for_trigger_node) survive process restarts.
  - Multiple Volo instances can serve the same user sessions.
  - The ObserverWatcher service can resume sleeping threads from any process.
"""

import asyncio
import inspect
import os

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.graph import END, START, StateGraph

from core.database.mongodb_saver import MongoDBSaver
from core.database.mongodb_saver_async import AsyncMongoDBSaver
from core.planning.execution_plan import ExecutionPlan, ExecutionState, StepStatus
from core.utils.gc_runtime import configure_gc_runtime
from core.utils.telemetry import wrap_node
from core.utils.timeouts import resolve_tool_timeout
from graph.agent_state import AgentState
from graph.nodes.balance_check_node import balance_check_node
from graph.nodes.confirmation_node import confirmation_node
from graph.nodes.executor_node import execution_engine_node
from graph.nodes.onboarding_node import onboarding_node
from graph.nodes.parser_node import intent_parser_node
from graph.nodes.plan_optimizer_node import plan_optimizer_node
from graph.nodes.planner_node import planner_node
from graph.nodes.resolver_node import intent_resolver_node
from graph.nodes.route_planner_node import route_planner_node
from graph.nodes.router_node import conversational_router_node
from graph.nodes.routing import (
    route_after_plan_optimizer,
    route_after_resolver,
    route_after_trigger,
    route_after_wait_for_funds,
    route_balance_check,
    route_main,
    route_onboarding,
    route_planner,
    route_post_parse,
)
from graph.nodes.vws_preflight_node import vws_preflight_node
from graph.nodes.wait_for_funds_node import wait_for_funds_node
from graph.nodes.wait_for_trigger_node import wait_for_trigger_node
from intent_hub.ontology.intent import IntentStatus

configure_gc_runtime()

workflow = StateGraph(AgentState)


def _async_route(route_fn):
    async def _wrapped(state: AgentState, **_kwargs):
        result = route_fn(state)
        if inspect.isawaitable(result):
            return await result
        return result

    return _wrapped


workflow.add_node("onboarding", wrap_node("onboarding", onboarding_node))
workflow.add_node(
    "conversational_router",
    wrap_node("conversational_router", conversational_router_node),
)
workflow.add_node("intent_parser", wrap_node("intent_parser", intent_parser_node))
workflow.add_node(
    "wait_for_trigger",
    wrap_node("wait_for_trigger", wait_for_trigger_node),
)
workflow.add_node(
    "wait_for_funds",
    wrap_node("wait_for_funds", wait_for_funds_node),
)
workflow.add_node(
    "intent_resolver",
    wrap_node("intent_resolver", intent_resolver_node),
)
workflow.add_node(
    "route_planner",
    wrap_node("route_planner", route_planner_node),
)
workflow.add_node(
    "plan_optimizer",
    wrap_node("plan_optimizer", plan_optimizer_node),
)
workflow.add_node(
    "vws_preflight",
    wrap_node("vws_preflight", vws_preflight_node),
)
workflow.add_node("balance_check", wrap_node("balance_check", balance_check_node))
workflow.add_node(
    "confirmation_node",
    wrap_node("confirmation_node", confirmation_node),
)
_EXECUTION_ENGINE_NODE_TIMEOUT_SECONDS = max(
    360.0,
    (resolve_tool_timeout("swap", None) or 0.0) + 60.0,
    (resolve_tool_timeout("bridge", None) or 0.0) + 60.0,
)
workflow.add_node(
    "execution_engine",
    wrap_node(
        "execution_engine",
        execution_engine_node,
        timeout_seconds=_EXECUTION_ENGINE_NODE_TIMEOUT_SECONDS,
    ),
)
workflow.add_node("planner_node", wrap_node("planner_node", planner_node))

# Entry point
workflow.add_edge(START, "onboarding")

# Onboarding: identify / provision user
#   fail    → END
#   success → conversational_router
workflow.add_conditional_edges(
    "onboarding",
    _async_route(route_onboarding),
    {"end": END, "router": "conversational_router"},
)

# Conversational router: classify intent category
#   CONVERSATION / STATUS / CANCELLED → END
#   CONFIRMED (user approved receipt)  → execution_engine
#   ACTION                             → intent_parser
workflow.add_conditional_edges(
    "conversational_router",
    _async_route(route_main),
    {"end": END, "parse": "intent_parser", "execute": "execution_engine"},
)

# Parser: extract and validate intents from conversation history
#   incomplete intents   → END  (clarification prompt already emitted)
#   conditional intents  → wait_for_trigger  (event-driven path)
#   all complete         → intent_resolver   (immediate execution path)
workflow.add_conditional_edges(
    "intent_parser",
    _async_route(route_post_parse),
    {"end": END, "wait_trigger": "wait_for_trigger", "resolve": "intent_resolver"},
)

# Wait-for-trigger: register condition, interrupt graph, resume on event
#   First invocation  → graph pauses here (interrupt); no edge fires.
#   Resume invocation:
#     resolve → intent_resolver  (condition met, proceed to execution)
#     end     → END              (trigger cancelled / expired)
workflow.add_conditional_edges(
    "wait_for_trigger",
    _async_route(route_after_trigger),
    {"resolve": "intent_resolver", "end": END},
)

workflow.add_conditional_edges(
    "wait_for_funds",
    _async_route(route_after_wait_for_funds),
    {"resume": "vws_preflight", "end": END},
)

# Resolver: convert Intents → ExecutionPlan DAG
#   resolve → route_planner
#   end     → END
workflow.add_conditional_edges(
    "intent_resolver",
    _async_route(route_after_resolver),
    {"resolve": "route_planner", "end": END},
)

# Route planner: deterministic bounded candidate generation.
#   Produces routed candidate ExecutionPlans by combining topology variants
#   with alternative quotes while keeping the baseline routed plan available
#   for compatibility and fallback.
workflow.add_edge("route_planner", "plan_optimizer")

# Plan optimizer: simulate all candidates, score them, and pick the best plan.
#   On total simulation failure the graph hands off to planner_node for
#   mutation/retry; otherwise it proceeds to balance_check with the selected plan.
workflow.add_conditional_edges(
    "plan_optimizer",
    _async_route(route_after_plan_optimizer),
    {"balance_check": "balance_check", "planner": "planner_node"},
)

# VWS preflight: simulate the latest ExecutionPlan against the current wallet
# snapshot and attach projected deltas / reservation metadata. Always proceeds
# to balance_check, which converts the VWS output into final fee-aware checks.
workflow.add_edge("vws_preflight", "balance_check")

# Balance check: verify token balances and estimate gas + platform fees
#   confirm  → confirmation_node   (standard flow: show receipt to user)
#   execute  → execution_engine    (triggered flow: skip receipt, auto-execute)
#   end      → END                 (shortfall: error message already emitted)
workflow.add_conditional_edges(
    "balance_check",
    _async_route(route_balance_check),
    {"confirm": "confirmation_node", "execute": "execution_engine", "end": END},
)

# Confirmation: present receipt, set confirmation_status = WAITING → END
#   (User replies 'confirm' → conversational_router routes to execute on
#   the next invocation — no direct edge needed here.)
workflow.add_edge("confirmation_node", END)

# Executor → Planner (DAG execution loop)
workflow.add_edge("execution_engine", "planner_node")

# Planner: analyse execution state and decide next step
#   continue  → execution_engine    (more ready nodes in the DAG)
#   approval  → confirmation_node   (planner added nodes requiring sign-off)
#   end       → END                 (goal achieved or irrecoverably failed)
workflow.add_conditional_edges(
    "planner_node",
    _async_route(route_planner),
    {
        "continue": "execution_engine",
        "approval": "confirmation_node",
        "wait_funds": "wait_for_funds",
        "end": END,
    },
)

#
# MongoDBSaver persists LangGraph state across process restarts, enabling:
#   1. Event-driven execution: wait_for_trigger_node can survive restarts
#      because the checkpoint (including the interrupted node position) is
#      durably stored in MongoDB rather than process memory.
#   2. Multi-instance deployments: any Volo instance can resume a thread
#      started by a different instance, since all share the same MongoDB.
#   3. Auditability: the full execution history of every user thread is
#      queryable from the lg_checkpoints collection.
#
# Uses MemorySaver only when SKIP_MONGODB_HEALTHCHECK is explicitly enabled.

checkpoint_serde = JsonPlusSerializer(
    allowed_msgpack_modules=()
).with_msgpack_allowlist(
    [
        ExecutionPlan,
        StepStatus,
        ExecutionState,
        IntentStatus,
    ]
)

_skip_mongo = os.getenv("SKIP_MONGODB_HEALTHCHECK", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _build_memory_checkpointer():
    from langgraph.checkpoint.memory import MemorySaver

    return MemorySaver(serde=checkpoint_serde)


async def _assert_mongo_reachable_async() -> None:
    from core.database.mongodb_async import AsyncMongoDB

    await AsyncMongoDB.get_client().admin.command("ping")


def _assert_mongo_reachable_sync() -> None:
    from core.database.mongodb import MongoDB

    MongoDB.get_client().admin.command("ping")


def _in_running_event_loop() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _try_build_checkpointer():
    async_init_error: Exception | None = None
    # Try async first, then sync.
    try:
        cp = AsyncMongoDBSaver(serde=checkpoint_serde)
        # Module import can happen inside an already running loop (e.g. CLI).
        # In that case asyncio.run(...) is invalid, so use sync ping as probe.
        if _in_running_event_loop():
            _assert_mongo_reachable_sync()
        else:
            asyncio.run(_assert_mongo_reachable_async())
        return cp
    except Exception as exc:
        async_init_error = exc

    try:
        cp = MongoDBSaver(serde=checkpoint_serde)
        _assert_mongo_reachable_sync()
        return cp
    except Exception as exc:
        raise exc from async_init_error


checkpointer = (
    _build_memory_checkpointer() if _skip_mongo else _try_build_checkpointer()
)

app = workflow.compile(checkpointer=checkpointer)
