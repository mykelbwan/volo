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
workflow.add_edge(START, "onboarding")
workflow.add_conditional_edges(
    "onboarding",
    _async_route(route_onboarding),
    {"end": END, "router": "conversational_router"},
)
workflow.add_conditional_edges(
    "conversational_router",
    _async_route(route_main),
    {"end": END, "parse": "intent_parser", "execute": "execution_engine"},
)

workflow.add_conditional_edges(
    "intent_parser",
    _async_route(route_post_parse),
    {"end": END, "wait_trigger": "wait_for_trigger", "resolve": "intent_resolver"},
)

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

workflow.add_conditional_edges(
    "intent_resolver",
    _async_route(route_after_resolver),
    {"resolve": "route_planner", "end": END},
)

workflow.add_edge("route_planner", "plan_optimizer")

workflow.add_conditional_edges(
    "plan_optimizer",
    _async_route(route_after_plan_optimizer),
    {"balance_check": "balance_check", "planner": "planner_node"},
)

workflow.add_edge("vws_preflight", "balance_check")

workflow.add_conditional_edges(
    "balance_check",
    _async_route(route_balance_check),
    {"confirm": "confirmation_node", "execute": "execution_engine", "end": END},
)

workflow.add_edge("confirmation_node", END)
workflow.add_edge("execution_engine", "planner_node")
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
