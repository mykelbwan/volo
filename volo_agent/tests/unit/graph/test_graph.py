import importlib
import sys
from typing import Callable, Type

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START


def _reload_graph(
    monkeypatch: pytest.MonkeyPatch,
    mongodb_saver: Type | Callable,
    *,
    skip_mongo_healthcheck: bool = True,
):
    import core.database.mongodb_saver as mongo_saver
    import core.database.mongodb_saver_async as mongo_saver_async

    monkeypatch.setattr(mongo_saver, "MongoDBSaver", mongodb_saver)
    monkeypatch.setattr(mongo_saver_async, "AsyncMongoDBSaver", mongodb_saver)
    if skip_mongo_healthcheck:
        monkeypatch.setenv("SKIP_MONGODB_HEALTHCHECK", "1")
    else:
        monkeypatch.delenv("SKIP_MONGODB_HEALTHCHECK", raising=False)
    sys.modules.pop("graph.graph", None)
    return importlib.import_module("graph.graph")


def _get_single_branch(branches: dict):
    assert len(branches) == 1
    return next(iter(branches.values()))


def test_graph_wiring(monkeypatch: pytest.MonkeyPatch):
    graph = _reload_graph(monkeypatch, MemorySaver)

    workflow = graph.workflow
    expected_nodes = {
        "onboarding",
        "conversational_router",
        "intent_parser",
        "wait_for_funds",
        "wait_for_trigger",
        "intent_resolver",
        "plan_optimizer",
        "vws_preflight",
        "balance_check",
        "confirmation_node",
        "execution_engine",
        "planner_node",
        "route_planner",
    }
    assert set(workflow.nodes.keys()) == expected_nodes

    assert (START, "onboarding") in workflow.edges
    assert ("intent_resolver", "route_planner") in workflow.edges
    assert ("route_planner", "plan_optimizer") in workflow.edges
    assert ("vws_preflight", "balance_check") in workflow.edges
    assert ("execution_engine", "planner_node") in workflow.edges
    assert ("confirmation_node", END) in workflow.edges

    branches = workflow.branches
    assert set(branches.keys()) == {
        "onboarding",
        "conversational_router",
        "intent_parser",
        "wait_for_funds",
        "wait_for_trigger",
        "plan_optimizer",
        "balance_check",
        "planner_node",
    }

    onboarding_branch = _get_single_branch(branches["onboarding"])
    assert onboarding_branch.ends == {"end": END, "router": "conversational_router"}

    router_branch = _get_single_branch(branches["conversational_router"])
    assert router_branch.ends == {
        "end": END,
        "parse": "intent_parser",
        "execute": "execution_engine",
    }

    parser_branch = _get_single_branch(branches["intent_parser"])
    assert parser_branch.ends == {
        "end": END,
        "wait_trigger": "wait_for_trigger",
        "resolve": "intent_resolver",
    }

    trigger_branch = _get_single_branch(branches["wait_for_trigger"])
    assert trigger_branch.ends == {"resolve": "intent_resolver", "end": END}

    funds_branch = _get_single_branch(branches["wait_for_funds"])
    assert funds_branch.ends == {"resume": "vws_preflight", "end": END}

    optimizer_branch = _get_single_branch(branches["plan_optimizer"])
    assert optimizer_branch.ends == {
        "balance_check": "balance_check",
        "planner": "planner_node",
    }

    balance_branch = _get_single_branch(branches["balance_check"])
    assert balance_branch.ends == {
        "confirm": "confirmation_node",
        "execute": "execution_engine",
        "end": END,
    }

    planner_branch = _get_single_branch(branches["planner_node"])
    assert planner_branch.ends == {
        "continue": "execution_engine",
        "approval": "confirmation_node",
        "wait_funds": "wait_for_funds",
        "end": END,
    }


def test_graph_falls_back_to_memory_saver(monkeypatch: pytest.MonkeyPatch):
    def _raise(*_args, **_kwargs):
        raise RuntimeError("no mongo")

    with pytest.warns(RuntimeWarning):
        graph = _reload_graph(monkeypatch, _raise, skip_mongo_healthcheck=False)

    assert isinstance(graph.checkpointer, MemorySaver)
