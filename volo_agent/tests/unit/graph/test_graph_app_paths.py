import asyncio
import importlib
import sys
from typing import Callable, Type

import pytest
from langgraph.checkpoint.memory import MemorySaver


def _stub_node(name: str):
    async def _node(state, **_kwargs):
        return {
            "reasoning_logs": [name],
            "context": state.get("context", {}),
        }

    return _node


def _reload_graph(
    monkeypatch: pytest.MonkeyPatch, mongodb_saver: Type | Callable
):
    import core.database.mongodb_saver as mongo_saver
    import core.database.mongodb_saver_async as mongo_saver_async
    import graph.nodes.balance_check_node as balance_mod
    import graph.nodes.confirmation_node as confirm_mod
    import graph.nodes.executor_node as exec_mod
    import graph.nodes.onboarding_node as onboarding_mod
    import graph.nodes.parser_node as parser_mod
    import graph.nodes.plan_optimizer_node as optimizer_mod
    import graph.nodes.planner_node as planner_mod
    import graph.nodes.resolver_node as resolver_mod
    import graph.nodes.route_planner_node as route_planner_mod
    import graph.nodes.router_node as router_mod
    import graph.nodes.routing as routing_mod
    import graph.nodes.vws_preflight_node as vws_preflight_mod
    import graph.nodes.wait_for_funds_node as wait_funds_mod
    import graph.nodes.wait_for_trigger_node as wait_mod

    monkeypatch.setattr(mongo_saver, "MongoDBSaver", mongodb_saver)
    monkeypatch.setattr(mongo_saver_async, "AsyncMongoDBSaver", mongodb_saver)
    monkeypatch.setenv("SKIP_MONGODB_HEALTHCHECK", "1")

    monkeypatch.setattr(onboarding_mod, "onboarding_node", _stub_node("onboarding"))
    monkeypatch.setattr(
        router_mod, "conversational_router_node", _stub_node("conversational_router")
    )
    monkeypatch.setattr(parser_mod, "intent_parser_node", _stub_node("intent_parser"))
    monkeypatch.setattr(
        wait_funds_mod, "wait_for_funds_node", _stub_node("wait_for_funds")
    )
    monkeypatch.setattr(wait_mod, "wait_for_trigger_node", _stub_node("wait_for_trigger"))
    monkeypatch.setattr(
        resolver_mod, "intent_resolver_node", _stub_node("intent_resolver")
    )
    monkeypatch.setattr(
        route_planner_mod, "route_planner_node", _stub_node("route_planner")
    )
    monkeypatch.setattr(
        optimizer_mod, "plan_optimizer_node", _stub_node("plan_optimizer")
    )
    monkeypatch.setattr(balance_mod, "balance_check_node", _stub_node("balance_check"))
    monkeypatch.setattr(confirm_mod, "confirmation_node", _stub_node("confirmation_node"))
    monkeypatch.setattr(
        exec_mod, "execution_engine_node", _stub_node("execution_engine")
    )
    monkeypatch.setattr(
        vws_preflight_mod, "vws_preflight_node", _stub_node("vws_preflight")
    )
    monkeypatch.setattr(planner_mod, "planner_node", _stub_node("planner_node"))

    async def _route_onboarding(state, **_kwargs):
        return state.get("context", {}).get("route_onboarding", "router")

    async def _route_main(state, **_kwargs):
        return state.get("context", {}).get("route_main", "end")

    async def _route_post_parse(state, **_kwargs):
        return state.get("context", {}).get("route_post_parse", "end")

    async def _route_after_trigger(state, **_kwargs):
        return state.get("context", {}).get("route_after_trigger", "end")

    async def _route_after_plan_optimizer(state, **_kwargs):
        return state.get("context", {}).get("route_after_plan_optimizer", "balance_check")

    async def _route_after_wait_for_funds(state, **_kwargs):
        return state.get("context", {}).get("route_after_wait_for_funds", "end")

    async def _route_balance_check(state, **_kwargs):
        return state.get("context", {}).get("route_balance_check", "end")

    async def _route_planner(state, **_kwargs):
        return state.get("context", {}).get("route_planner", "end")

    monkeypatch.setattr(routing_mod, "route_onboarding", _route_onboarding)
    monkeypatch.setattr(routing_mod, "route_main", _route_main)
    monkeypatch.setattr(routing_mod, "route_post_parse", _route_post_parse)
    monkeypatch.setattr(routing_mod, "route_after_trigger", _route_after_trigger)
    monkeypatch.setattr(
        routing_mod, "route_after_plan_optimizer", _route_after_plan_optimizer
    )
    monkeypatch.setattr(
        routing_mod, "route_after_wait_for_funds", _route_after_wait_for_funds
    )
    monkeypatch.setattr(routing_mod, "route_balance_check", _route_balance_check)
    monkeypatch.setattr(routing_mod, "route_planner", _route_planner)

    sys.modules.pop("graph.graph", None)
    return importlib.import_module("graph.graph")


@pytest.fixture
def compiled_graph(monkeypatch: pytest.MonkeyPatch):
    graph = _reload_graph(monkeypatch, MemorySaver)
    yield graph
    sys.modules.pop("graph.graph", None)


def _base_state(**routes):
    state = {
        "user_id": "user-1",
        "provider": "discord",
        "username": "alice",
        "user_info": None,
        "intents": [],
        "plans": [],
        "goal_parameters": {},
        "plan_history": [],
        "execution_state": None,
        "artifacts": {},
        "context": dict(routes),
        "route_decision": None,
        "confirmation_status": None,
        "pending_transactions": [],
        "reasoning_logs": [],
        "messages": [],
        "fee_quotes": [],
        "trigger_id": None,
        "is_triggered_execution": None,
    }
    return state


@pytest.mark.parametrize(
    "routes, expected_logs",
    [
        ({"route_onboarding": "end"}, ["onboarding"]),
        (
            {"route_onboarding": "router", "route_main": "end"},
            ["onboarding", "conversational_router"],
        ),
        (
            {
                "route_onboarding": "router",
                "route_main": "parse",
                "route_post_parse": "end",
            },
            ["onboarding", "conversational_router", "intent_parser"],
        ),
        (
            {
                "route_onboarding": "router",
                "route_main": "parse",
                "route_post_parse": "wait_trigger",
                "route_after_trigger": "end",
            },
            [
                "onboarding",
                "conversational_router",
                "intent_parser",
                "wait_for_trigger",
            ],
        ),
        (
            {
                "route_onboarding": "router",
                "route_main": "parse",
                "route_post_parse": "resolve",
                "route_after_plan_optimizer": "balance_check",
                "route_balance_check": "confirm",
            },
            [
                "onboarding",
                "conversational_router",
                "intent_parser",
                "intent_resolver",
                "route_planner",
                "plan_optimizer",
                "balance_check",
                "confirmation_node",
            ],
        ),
        (
            {
                "route_onboarding": "router",
                "route_main": "parse",
                "route_post_parse": "resolve",
                "route_after_plan_optimizer": "balance_check",
                "route_balance_check": "execute",
                "route_planner": "end",
            },
            [
                "onboarding",
                "conversational_router",
                "intent_parser",
                "intent_resolver",
                "route_planner",
                "plan_optimizer",
                "balance_check",
                "execution_engine",
                "planner_node",
            ],
        ),
        (
            {
                "route_onboarding": "router",
                "route_main": "parse",
                "route_post_parse": "resolve",
                "route_after_plan_optimizer": "planner",
                "route_planner": "end",
            },
            [
                "onboarding",
                "conversational_router",
                "intent_parser",
                "intent_resolver",
                "route_planner",
                "plan_optimizer",
                "planner_node",
            ],
        ),
        (
            {
                "route_onboarding": "router",
                "route_main": "execute",
                "route_planner": "approval",
            },
            [
                "onboarding",
                "conversational_router",
                "execution_engine",
                "planner_node",
                "confirmation_node",
            ],
        ),
        (
            {
                "route_onboarding": "router",
                "route_main": "execute",
                "route_planner": "wait_funds",
                "route_after_wait_for_funds": "resume",
                "route_balance_check": "end",
            },
            [
                "onboarding",
                "conversational_router",
                "execution_engine",
                "planner_node",
                "wait_for_funds",
                "vws_preflight",
                "balance_check",
            ],
        ),
    ],
)
def test_app_execution_paths(compiled_graph, routes, expected_logs):
    result = asyncio.run(
        compiled_graph.app.ainvoke(
            _base_state(**routes),
            config={"configurable": {"thread_id": "test-thread"}},
        )
    )
    assert result["reasoning_logs"] == expected_logs
