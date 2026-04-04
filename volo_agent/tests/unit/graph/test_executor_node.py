import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import AIMessage

from core.execution import runtime as execution_runtime
from core.memory.ledger import ErrorCategory
from core.reservations.models import ReservationClaimResult
from core.reservations.service import WalletReservationService
from core.reservations.store import InMemoryReservationStore
from core.tools.base import Tool
from core.utils.timeouts import TOOL_DEFAULT_TIMEOUTS, resolve_tool_timeout

from core.planning.execution_plan import (
    ExecutionPlan,
    ExecutionState,
    NodeState,
    PlanNode,
    StepStatus,
)
from graph.agent_state import AgentState
from graph.nodes.executor_node import _normalize_output, execution_engine_node

# ── Shared helpers ─────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _stub_noncritical_side_effects(monkeypatch: pytest.MonkeyPatch):
    execution_runtime._side_effect_cooldowns.clear()

    class _NoopLedger:
        def record_execution(self, *args, **kwargs):
            return None

        def record_fee(self, *args, **kwargs):
            return None

    monkeypatch.setattr("core.execution.runtime.get_ledger", lambda: _NoopLedger())
    monkeypatch.setattr(
        "core.execution.runtime.track_execution_volume",
        lambda *args, **kwargs: None,
    )
    async def _noop_publish_async(*args, **kwargs):
        return False

    monkeypatch.setattr(
        "graph.nodes.executor_node.publish_event", lambda *args, **kwargs: False
    )
    monkeypatch.setattr(
        "graph.nodes.executor_node.publish_event_async", _noop_publish_async
    )
    yield
    execution_runtime._side_effect_cooldowns.clear()


@pytest.fixture(autouse=True)
def _stub_task_registry():
    with patch(
        "graph.nodes.executor_node.ConversationTaskRegistry", return_value=AsyncMock()
    ):
        yield


def _make_swap_node(node_id: str, approval_required: bool = False) -> PlanNode:
    return PlanNode(
        id=node_id,
        tool="swap",
        args={
            "token_in_symbol": "USDC",
            "token_out_symbol": "WETH",
            "amount_in": 100.0,
            "chain": "somnia testnet",
            "sub_org_id": "{{SUB_ORG_ID}}",
            "sender": "{{SENDER_ADDRESS}}",
        },
        depends_on=[],
        approval_required=approval_required,
    )


def _make_bridge_node(node_id: str, approval_required: bool = False) -> PlanNode:
    return PlanNode(
        id=node_id,
        tool="bridge",
        args={
            "token_symbol": "USDC",
            "source_chain": "ethereum",
            "target_chain": "base",
            "amount": 50.0,
            "sub_org_id": "{{SUB_ORG_ID}}",
            "sender": "{{SENDER_ADDRESS}}",
            "recipient": "{{SENDER_ADDRESS}}",
        },
        depends_on=[],
        approval_required=approval_required,
    )


def _make_transfer_node(node_id: str, approval_required: bool = False) -> PlanNode:
    return PlanNode(
        id=node_id,
        tool="transfer",
        args={
            "asset_symbol": "USDC",
            "asset_ref": "0xusdc",
            "amount": 5,
            "recipient": "0xrecipient",
            "network": "base",
            "sub_org_id": "{{SUB_ORG_ID}}",
            "sender": "{{SENDER_ADDRESS}}",
        },
        depends_on=[],
        approval_required=approval_required,
    )


def test_normalize_output_preserves_transfer_neutral_fields_and_legacy_aliases():
    node = _make_transfer_node("step_0")

    output = _normalize_output(
        node,
        {
            "status": "success",
            "tx_hash": "0xhash",
            "asset_symbol": "USDC",
            "network": "base",
            "token_symbol": "USDC",
        },
    )

    assert output == {
        "tool": "transfer",
        "network": "base",
        "chain": "base",
        "tx_hash": "0xhash",
        "amount": 5,
        "asset_symbol": "USDC",
        "token_symbol": "USDC",
        "recipient": "0xrecipient",
    }


def _make_swap_route_meta(
    *,
    chain_id: int = 1,
    expiry_timestamp: int | None = None,
    allow_fallback: bool = False,
    fallback_reason: str | None = None,
    amount_out: str = "95",
    amount_out_min: str = "90",
) -> dict[str, object]:
    route_meta: dict[str, object] = {
        "aggregator": "uniswap_v3",
        "provider": "uniswap_v3",
        "chain_id": chain_id,
        "token_in": "0xaaa",
        "token_out": "0xbbb",
        "amount_in": "100",
        "amount_out": amount_out,
        "amount_out_min": amount_out_min,
        "gas_estimate": 200000,
        "execution": {
            "protocol": "v3",
            "path": ["0xaaa", "0xbbb"],
            "fee_tiers": [3000],
        },
    }
    if expiry_timestamp is not None:
        route_meta["expiry_timestamp"] = expiry_timestamp
    if allow_fallback:
        route_meta["allow_fallback"] = True
    if fallback_reason is not None:
        route_meta["fallback_reason"] = fallback_reason
    return route_meta


def _make_agent_state(plan: ExecutionPlan, exec_state: ExecutionState) -> AgentState:
    """
    Build a minimal AgentState dict suitable for direct node invocation.
    The executor reads the plan from ``plan_history[-1]`` and execution
    state from ``execution_state``.
    """
    return {
        "user_id": "user-1",
        "provider": "discord",
        "username": "alice",
        "user_info": {},
        "intents": [],
        "plans": [],
        "goal_parameters": {},
        "plan_history": [plan],
        "execution_state": exec_state,
        "artifacts": {
            "sub_org_id": "mock-sub-org-id",
            "sender_address": "0xSenderAddress",
        },
        "context": {},
        "route_decision": "CONFIRMED",
        "confirmation_status": "WAITING",
        "pending_transactions": [],
        "reasoning_logs": [],
        "messages": [],
        "fee_quotes": [],
        "balance_snapshot": {},
        "resource_snapshots": {},
        "native_requirements": {},
        "reservation_requirements": {},
        "trigger_id": None,
        "is_triggered_execution": None,
        "execution_id": "exec-1",
    }


def _resource_snapshot(
    *,
    resource_key: str,
    wallet_scope: str,
    sender: str,
    chain: str,
    token_ref: str,
    symbol: str,
    decimals: int,
    available: str,
    available_base_units: str,
    chain_family: str = "evm",
) -> dict[str, object]:
    return {
        "resource_key": resource_key,
        "wallet_scope": wallet_scope,
        "sender": sender,
        "chain": chain,
        "token_ref": token_ref,
        "symbol": symbol,
        "decimals": decimals,
        "available": available,
        "available_base_units": available_base_units,
        "observed_at": "1",
        "chain_family": chain_family,
    }


def _reservation_requirement(
    *,
    resource_key: str,
    wallet_scope: str,
    sender: str,
    chain: str,
    token_ref: str,
    symbol: str,
    decimals: int,
    required: str,
    required_base_units: str,
    kind: str,
) -> dict[str, object]:
    return {
        "resource_key": resource_key,
        "wallet_scope": wallet_scope,
        "sender": sender,
        "chain": chain,
        "token_ref": token_ref,
        "symbol": symbol,
        "decimals": decimals,
        "required": required,
        "required_base_units": required_base_units,
        "kind": kind,
    }


# ── Tests ──────────────────────────────────────────────────────────────────────


def test_executor_node_success():
    """
    A node with approval_required=False should be executed.
    The executor delta must contain:
      - confirmation_status == "EXECUTED"
      - one pending_transaction of type "swap"
      - the node in SUCCESS state
      - execution_state.completed == True
      - an AIMessage summarising the run
    """
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    mock_tool_result = {
        "message": "Swap executed",
        "tx_hash": "0xabc123",
        "nonce": 7,
        "raw_tx": "0xdeadbeef",
        "tx_payload": {"nonce": 7, "chainId": 1},
    }

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(mock_tool_result, 0.05)),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ) as history_registry:
        result = asyncio.run(execution_engine_node(agent_state))

    history_registry.return_value.record_event.assert_called_once()
    assert result["confirmation_status"] == "EXECUTED"
    assert len(result["pending_transactions"]) == 1
    assert result["pending_transactions"][0]["type"] == "swap"
    assert result["pending_transactions"][0]["nonce"] == 7
    assert result["pending_transactions"][0]["sender"] == "0xSenderAddress"
    assert result["pending_transactions"][0].get("submitted_at")
    assert result["pending_transactions"][0]["raw_tx"] == "0xdeadbeef"
    assert result["pending_transactions"][0]["tx_payload"] == {"nonce": 7, "chainId": 1}
    assert result["pending_transactions"][0]["sub_org_id"] == "mock-sub-org-id"
    assert result["pending_transactions"][0]["execution_id"] == "exec-1"
    assert result["execution_state"].node_states[node_id].status == StepStatus.SUCCESS
    assert result["execution_state"].completed is True
    assert isinstance(result["messages"][0], AIMessage)


def test_executor_node_uses_route_meta_only_and_never_calls_router():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.args["chain"] = "ethereum"
    node.args["token_in_address"] = "0xaaa"
    node.args["token_out_address"] = "0xbbb"
    node.metadata = {"route": _make_swap_route_meta(expiry_timestamp=int(time.time()) + 3600)}
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
        metadata={"route_planner": {"applied": True}},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    sender = "0xsenderaddress"
    agent_state["balance_snapshot"] = {
        f"{sender}|ethereum|0xaaa": "1000",
        f"{sender}|ethereum|0x0000000000000000000000000000000000000000": "1",
    }

    captured_args: dict[str, object] = {}

    async def _capture_run_with_timing(_tool, args):
        captured_args.update(args)
        return (
            {
                "message": "Swap executed",
                "tx_hash": "0xabc123",
                "amount_out": "95",
                "amount_out_minimum": "95",
            },
            0.05,
        )

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(side_effect=_capture_run_with_timing),
    ), patch(
        "core.routing.router.RoutePlanner.get_best_swap_route",
        new=AsyncMock(side_effect=AssertionError("router should not be called")),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    merged = exec_state.merge(result["execution_state"])
    assert merged.node_states[node_id].status == StepStatus.SUCCESS
    assert captured_args["_route_meta"] == node.metadata["route"]
    assert captured_args["_fallback_policy"] == {
        "allow_fallback": False,
        "reason": None,
    }


def test_executor_node_fails_expired_route_before_dispatch():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.args["chain"] = "ethereum"
    node.args["token_in_address"] = "0xaaa"
    node.args["token_out_address"] = "0xbbb"
    node.metadata = {
        "route": {
            "aggregator": "uniswap_v3",
            "provider": "uniswap_v3",
            "chain_id": 1,
            "token_in": "0xaaa",
            "token_out": "0xbbb",
            "amount_in": "100",
            "amount_out": "95",
            "amount_out_min": "90",
            "gas_estimate": 200000,
            "expiry_timestamp": 1,
            "execution": {
                "protocol": "v3",
                "path": ["0xaaa", "0xbbb"],
                "fee_tiers": [3000],
            },
        }
    }
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
        metadata={"route_planner": {"applied": True}},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    sender = "0xsenderaddress"
    agent_state["balance_snapshot"] = {
        f"{sender}|ethereum|0xaaa": "1000",
        f"{sender}|ethereum|0x0000000000000000000000000000000000000000": "1",
    }

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(side_effect=AssertionError("tool should not execute")),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    merged = exec_state.merge(result["execution_state"])
    assert merged.node_states[node_id].status == StepStatus.FAILED
    assert "Route expired" in (merged.node_states[node_id].error or "")


def test_executor_node_route_validation_failure_emits_no_sending_progress_event():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.args["chain"] = "ethereum"
    node.args["token_in_address"] = "0xaaa"
    node.args["token_out_address"] = "0xbbb"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
        metadata={"route_planner": {"applied": True}},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    sender = "0xsenderaddress"
    agent_state["balance_snapshot"] = {
        f"{sender}|ethereum|0xaaa": "1000",
        f"{sender}|ethereum|0x0000000000000000000000000000000000000000": "1",
    }
    published: list[dict[str, object]] = []

    async def _capture_publish_async(payload):
        published.append(dict(payload))
        return True

    run_with_timing = AsyncMock(return_value=({"message": "should not run"}, 0.05))
    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=run_with_timing,
    ), patch(
        "graph.nodes.executor_node.publish_event_async",
        new=_capture_publish_async,
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    merged = exec_state.merge(result["execution_state"])
    assert merged.node_states[node_id].status == StepStatus.FAILED
    run_with_timing.assert_not_awaited()
    assert not any(
        item.get("event") == "node_progress" and item.get("stage") == "sending"
        for item in published
    )


def test_executor_node_executes_expired_route_with_explicit_fallback_reason():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.args["chain"] = "ethereum"
    node.args["token_in_address"] = "0xaaa"
    node.args["token_out_address"] = "0xbbb"
    node.metadata = {
        "route": _make_swap_route_meta(
            expiry_timestamp=1,
            allow_fallback=True,
            fallback_reason="ROUTE_EXPIRED",
        )
    }
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
        metadata={"route_planner": {"applied": True}},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    sender = "0xsenderaddress"
    agent_state["balance_snapshot"] = {
        f"{sender}|ethereum|0xaaa": "1000",
        f"{sender}|ethereum|0x0000000000000000000000000000000000000000": "1",
    }

    captured_args: dict[str, object] = {}

    async def _capture_run_with_timing(_tool, args):
        captured_args.update(args)
        return (
            {
                "message": "Swap executed",
                "tx_hash": "0xabc123",
                "amount_out": "95",
                "amount_out_minimum": "95",
            },
            0.05,
        )

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(side_effect=_capture_run_with_timing),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    merged = exec_state.merge(result["execution_state"])
    assert merged.node_states[node_id].status == StepStatus.SUCCESS
    assert "_route_meta" not in captured_args
    assert captured_args["_fallback_policy"] == {
        "allow_fallback": True,
        "reason": "ROUTE_EXPIRED",
    }


def test_executor_node_rejects_fallback_without_explicit_policy():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.args["chain"] = "ethereum"
    node.args["token_in_address"] = "0xaaa"
    node.args["token_out_address"] = "0xbbb"
    node.metadata = {"route": _make_swap_route_meta(expiry_timestamp=int(time.time()) + 3600)}
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
        metadata={"route_planner": {"applied": True}},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    sender = "0xsenderaddress"
    agent_state["balance_snapshot"] = {
        f"{sender}|ethereum|0xaaa": "1000",
        f"{sender}|ethereum|0x0000000000000000000000000000000000000000": "1",
    }

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(
            return_value=(
                {
                    "message": "Swap executed",
                    "tx_hash": "0xabc123",
                    "amount_out": "95",
                    "amount_out_minimum": "95",
                    "fallback_used": True,
                    "fallback_reason": "tool used dynamic routing",
                },
                0.05,
            )
        ),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    merged = exec_state.merge(result["execution_state"])
    assert merged.node_states[node_id].status == StepStatus.FAILED
    assert any(
        "violated fallback policy" in log for log in result["reasoning_logs"]
    )


def test_executor_node_rejects_enabled_fallback_without_reason_before_dispatch():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.args["chain"] = "ethereum"
    node.args["token_in_address"] = "0xaaa"
    node.args["token_out_address"] = "0xbbb"
    node.metadata = {
        "route": _make_swap_route_meta(
            expiry_timestamp=int(time.time()) + 3600,
            allow_fallback=True,
        )
    }
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
        metadata={"route_planner": {"applied": True}},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    sender = "0xsenderaddress"
    agent_state["balance_snapshot"] = {
        f"{sender}|ethereum|0xaaa": "1000",
        f"{sender}|ethereum|0x0000000000000000000000000000000000000000": "1",
    }

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(side_effect=AssertionError("tool should not execute")),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    merged = exec_state.merge(result["execution_state"])
    assert merged.node_states[node_id].status == StepStatus.FAILED
    assert "fallback reason is required" in (merged.node_states[node_id].error or "")


def test_executor_node_fails_when_actual_output_is_below_min_output():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.args["chain"] = "ethereum"
    node.args["token_in_address"] = "0xaaa"
    node.args["token_out_address"] = "0xbbb"
    node.metadata = {
        "route": {
            "aggregator": "uniswap_v3",
            "provider": "uniswap_v3",
            "chain_id": 1,
            "token_in": "0xaaa",
            "token_out": "0xbbb",
            "amount_in": "100",
            "amount_out": "95",
            "amount_out_min": "90",
            "gas_estimate": 200000,
            "expiry_timestamp": int(time.time()) + 3600,
            "execution": {
                "protocol": "v3",
                "path": ["0xaaa", "0xbbb"],
                "fee_tiers": [3000],
            },
        }
    }
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
        metadata={"route_planner": {"applied": True}},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    sender = "0xsenderaddress"
    agent_state["balance_snapshot"] = {
        f"{sender}|ethereum|0xaaa": "1000",
        f"{sender}|ethereum|0x0000000000000000000000000000000000000000": "1",
    }

    mock_tool_result = {
        "message": "Swap executed",
        "tx_hash": "0xabc123",
        "amount_out": "89",
        "amount_out_minimum": "89",
    }

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(mock_tool_result, 0.05)),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    merged = exec_state.merge(result["execution_state"])
    assert merged.node_states[node_id].status == StepStatus.FAILED
    assert "below minimum output" in (
        merged.node_states[node_id].error or ""
    )


def test_executor_node_approval_gating():
    """
    A node with approval_required=True must NOT be executed.
    The executor delta must reflect that no transactions were dispatched
    and the node remains in PENDING state (as seen via the merged state).
    """
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=True)},
    )
    initial_exec_state = ExecutionState(
        node_states={node_id: NodeState(node_id=node_id)}
    )
    agent_state = _make_agent_state(plan, initial_exec_state)

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    # The executor returns a *delta* — merge it with the original state to
    # simulate what LangGraph's reducer would produce.
    merged = initial_exec_state.merge(result["execution_state"])

    assert merged.node_states[node_id].status == StepStatus.PENDING
    assert result["execution_state"].completed is False
    assert len(result["pending_transactions"]) == 0


def test_executor_node_native_reservation_defers_parallel_swap():
    """
    When two steps compete for the same native resource reservation,
    the runtime schedules one and defers the other.
    """
    node_id_0 = "step_0"
    node_id_1 = "step_1"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH twice",
        nodes={
            node_id_0: _make_swap_node(node_id_0, approval_required=False),
            node_id_1: _make_swap_node(node_id_1, approval_required=False),
        },
    )
    exec_state = ExecutionState(
        node_states={
            node_id_0: NodeState(node_id=node_id_0),
            node_id_1: NodeState(node_id=node_id_1),
        }
    )
    agent_state = _make_agent_state(plan, exec_state)

    from graph.nodes.executor_node import _NATIVE

    sender = "0xSenderAddress".lower()
    chain = "somnia testnet"
    wallet_scope = f"sender:{sender}"
    native_key = f"{sender}|{chain}|{_NATIVE}"

    agent_state["resource_snapshots"] = {
        native_key: _resource_snapshot(
            resource_key=native_key,
            wallet_scope=wallet_scope,
            sender=sender,
            chain=chain,
            token_ref=_NATIVE,
            symbol="STT",
            decimals=18,
            available="0.03",
            available_base_units="30000000000000000",
        )
    }
    agent_state["reservation_requirements"] = {
        node_id_0: [
            _reservation_requirement(
                resource_key=native_key,
                wallet_scope=wallet_scope,
                sender=sender,
                chain=chain,
                token_ref=_NATIVE,
                symbol="STT",
                decimals=18,
                required="0.02",
                required_base_units="20000000000000000",
                kind="gas",
            )
        ],
        node_id_1: [
            _reservation_requirement(
                resource_key=native_key,
                wallet_scope=wallet_scope,
                sender=sender,
                chain=chain,
                token_ref=_NATIVE,
                symbol="STT",
                decimals=18,
                required="0.02",
                required_base_units="20000000000000000",
                kind="gas",
            )
        ],
    }
    shared_service = WalletReservationService(store=InMemoryReservationStore())

    async def _shared_getter():
        return shared_service

    mock_tool_result = {"message": "Swap executed", "tx_hash": "0xabc123"}

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(mock_tool_result, 0.05)),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ), patch(
        "graph.nodes.executor_node.get_reservation_service",
        new=_shared_getter,
    ), patch(
        "core.execution.runtime.get_native_balance_async",
        new=AsyncMock(return_value="0.03"),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    statuses = {
        result["execution_state"].node_states[node_id_0].status,
        result["execution_state"].node_states[node_id_1].status,
    }

    assert statuses == {StepStatus.SUCCESS, StepStatus.PENDING}
    assert result["waiting_for_funds"]["node_id"] in {node_id_0, node_id_1}
    assert result["waiting_for_funds"]["wait_id"]
    assert result["waiting_for_funds"]["wallet_scope"] == wallet_scope
    assert any("[RESERVE:GLOBAL]" in line for line in result["reasoning_logs"])
    assert any(
        "already reserved" in line.lower() and native_key in line.lower()
        for line in result["reasoning_logs"]
    )


def test_executor_skips_idempotency_hash_when_store_unavailable():
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    mock_tool_result = {"message": "Swap executed", "tx_hash": "0xabc123"}

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "core.execution.runtime.compute_args_hash",
        side_effect=AssertionError("compute_args_hash should not run without store"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(mock_tool_result, 0.05)),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    assert result["execution_state"].node_states[node_id].status == StepStatus.SUCCESS


def test_executor_node_idempotent_bridge_pending_resolves_success():
    """
    Pending idempotency record for a bridge should be resolved via Across
    status polling and marked success when the deposit is filled.
    """
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Bridge USDC to Base",
        nodes={node_id: _make_bridge_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    class _StubIdemStore:
        def __init__(self) -> None:
            self.mark_success_called = False
            self.mark_success_result = None

        def claim(self, *, key, metadata, ttl_seconds=None):
            record = type(
                "Record",
                (),
                {
                    "status": "pending",
                    "result": None,
                    "tx_hash": "0xbridgehash",
                    "metadata": {"chain": "ethereum"},
                },
            )()
            return record, False

        def mark_success(self, *, key, result, ttl_seconds=None):
            self.mark_success_called = True
            self.mark_success_result = result

        def mark_failed(self, *, key, error, ttl_seconds=None):
            raise AssertionError("mark_failed should not be called")

        def mark_inflight(self, *, key, tx_hash):
            pass

    stub_store = _StubIdemStore()

    from core.utils.bridge_status_registry import BridgeStatusResult

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        return_value=stub_store,
    ), patch(
        "graph.nodes.executor_node.run_blocking",
        new=AsyncMock(
            return_value=BridgeStatusResult(
                raw_status="filled", normalized_status="success"
            )
        ),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(side_effect=AssertionError("Tool execution should not run")),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    assert result["execution_state"].node_states[node_id].status == StepStatus.SUCCESS
    assert stub_store.mark_success_called is True
    assert stub_store.mark_success_result is not None


def test_executor_retries_pending_idempotent_record_without_tx_hash():
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    class _StubIdemStore:
        def __init__(self) -> None:
            self.claim_calls = 0
            self.mark_success_called = False
            self.mark_inflight_tx_hash = None

        def claim(self, *, key, metadata, ttl_seconds=None):
            self.claim_calls += 1
            record = type(
                "Record",
                (),
                {
                    "status": "pending",
                    "result": None,
                    "tx_hash": None,
                    "metadata": dict(metadata),
                    "created_at": datetime.now(timezone.utc) - timedelta(seconds=180),
                },
            )()
            return record, self.claim_calls == 1

        def mark_success(self, *, key, result, ttl_seconds=None):
            self.mark_success_called = True

        def mark_failed(self, *, key, error, ttl_seconds=None):
            raise AssertionError("mark_failed should not be called")

        def mark_inflight(self, *, key, tx_hash):
            self.mark_inflight_tx_hash = tx_hash

    stub_store = _StubIdemStore()
    mock_tool_result = {"message": "Swap executed", "tx_hash": "0xabc123"}

    run_with_timing = AsyncMock(
        side_effect=[
            (RuntimeError("transient network"), 0.05),
            (mock_tool_result, 0.05),
        ]
    )

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        return_value=stub_store,
    ), patch.dict(
        os.environ,
        {"SOMNIA_TESTNET_RPC_URL": "https://rpc.test"},
    ), patch(
        "core.execution.runtime.make_async_web3",
        return_value=type(
            "_FakeWeb3",
            (),
            {
                "eth": type(
                    "_FakeEth",
                    (),
                    {
                        "get_transaction_count": AsyncMock(side_effect=[7, 7]),
                    },
                )()
            },
        )(),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=run_with_timing,
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    node_state = result["execution_state"].node_states[node_id]
    assert node_state.status == StepStatus.SUCCESS
    assert run_with_timing.await_count == 2
    assert stub_store.mark_success_called is True
    assert stub_store.mark_inflight_tx_hash == "0xabc123"
    assert any(
        "Pending record for step_0 had no tx hash after cooldown; reclaiming current execution."
        in log
        for log in result["reasoning_logs"]
    )


def test_executor_node_failure_message_is_user_friendly():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.retry_policy = {"max_retries": 0, "backoff_factor": 1.0}
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(RuntimeError("boom: revert"), 0.05)),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ) as history_registry:
        result = asyncio.run(execution_engine_node(agent_state))

    history_registry.return_value.record_event.assert_not_called()
    message = result["messages"][0].content.lower()
    assert "didn't go through" in message
    assert "reply 'retry'" in message
    assert "boom" not in message
    assert "swap" in message
    node_state = result["execution_state"].node_states[node_id]
    assert node_state.user_message is not None
    assert "reply 'retry'" in node_state.user_message.lower()


def test_executor_node_failure_with_suggested_fix_prompts_go_ahead():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.retry_policy = {"max_retries": 0, "backoff_factor": 1.0}
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    tool = Tool(
        name="swap",
        description="test swap tool",
        func=AsyncMock(return_value={"message": "ok"}),
        on_suggest_fix=lambda *_args, **_kwargs: {"amount_in": 50.0},
    )

    class _StubRegistry:
        def get(self, _name):
            return tool

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(RuntimeError("boom: revert"), 0.05)),
    ), patch(
        "graph.nodes.executor_node.tools_registry",
        new=_StubRegistry(),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ) as history_registry:
        result = asyncio.run(execution_engine_node(agent_state))

    history_registry.return_value.record_event.assert_not_called()
    message = result["messages"][0].content.lower()
    assert "go ahead" in message
    assert "safer setting" in message
    node_state = result["execution_state"].node_states[node_id]
    assert node_state.mutated_args == {"amount_in": 50.0}
    assert "go ahead" in (node_state.user_message or "").lower()


def test_executor_node_logic_error_does_not_retry():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.retry_policy = {"max_retries": 3, "backoff_factor": 1.0}
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(ValueError("bad params"), 0.05)),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ) as history_registry:
        result = asyncio.run(execution_engine_node(agent_state))

    history_registry.return_value.record_event.assert_not_called()
    assert result["execution_state"].node_states[node_id].status == StepStatus.FAILED


def test_executor_node_invalid_guardrail_policy_fails_closed():
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    agent_state["guardrail_policy"] = {"max_parallel_nodes": "not-a-number"}

    run_with_timing = AsyncMock(return_value=({"message": "should not run"}, 0.05))

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=run_with_timing,
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    node_state = result["execution_state"].node_states[node_id]
    assert node_state.status == StepStatus.FAILED
    assert node_state.error_category == ErrorCategory.SECURITY.value
    assert "security policy" in (node_state.user_message or "").lower()
    assert "reply 'retry'" in result["messages"][0].content.lower()
    run_with_timing.assert_not_awaited()


def test_executor_node_unresolved_markers_fail_closed_before_tool_run():
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    agent_state["artifacts"] = {}

    run_with_timing = AsyncMock(return_value=({"message": "should not run"}, 0.05))

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=run_with_timing,
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    node_state = result["execution_state"].node_states[node_id]
    assert node_state.status == StepStatus.FAILED
    assert node_state.error_category == ErrorCategory.LOGIC.value
    assert "unresolved" in (node_state.error or "").lower()
    assert "required values" in (node_state.user_message or "").lower()
    run_with_timing.assert_not_awaited()


def test_executor_node_drains_async_event_publish_tasks_before_return():
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    published = []

    async def _slow_publish_async(payload):
        await asyncio.sleep(0.05)
        published.append(payload)
        return True

    started = time.perf_counter()
    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=({"message": "Swap executed", "tx_hash": "0xabc123"}, 0.05)),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ), patch(
        "graph.nodes.executor_node.publish_event_async",
        new=_slow_publish_async,
    ):
        result = asyncio.run(execution_engine_node(agent_state))
    elapsed = time.perf_counter() - started

    assert result["execution_state"].node_states[node_id].status == StepStatus.SUCCESS
    assert elapsed >= 0.05
    assert any(item.get("event") == "node_completed" for item in published)
    assert any(
        item.get("event") == "node_progress" and item.get("stage") == "sending"
        for item in published
    )
    assert any(
        item.get("event") == "node_progress" and item.get("stage") == "submitted"
        for item in published
    )


def test_executor_node_tool_timeout_marks_failure():
    node_id = "step_0"
    node = _make_swap_node(node_id, approval_required=False)
    node.retry_policy = {"max_retries": 0, "backoff_factor": 1.0}
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: node},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    async def _slow_tool(_args):
        await asyncio.sleep(0.05)
        return {"message": "Swap executed", "tx_hash": "0xabc123"}

    tool = Tool(
        name="swap",
        description="slow test tool",
        func=_slow_tool,
        timeout_seconds=0.01,
    )

    class _StubRegistry:
        def get(self, _name):
            return tool

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node.tools_registry",
        new=_StubRegistry(),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    node_state = result["execution_state"].node_states[node_id]
    assert node_state.status == StepStatus.FAILED
    assert node_state.error_category == ErrorCategory.NETWORK.value
    assert node_state.error is not None
    assert "timeout" in node_state.error.lower()


def test_executor_bridge_success_then_swap_failure_marks_partial_run_failed():
    bridge_node_id = "step_0"
    swap_node_id = "step_1"
    bridge_node = _make_bridge_node(bridge_node_id, approval_required=False)
    swap_node = _make_swap_node(swap_node_id, approval_required=False)
    swap_node.depends_on = [bridge_node_id]
    swap_node.retry_policy = {"max_retries": 0, "backoff_factor": 1.0}
    plan = ExecutionPlan(
        goal="Bridge then swap",
        nodes={
            bridge_node_id: bridge_node,
            swap_node_id: swap_node,
        },
    )
    exec_state = ExecutionState(
        node_states={
            bridge_node_id: NodeState(node_id=bridge_node_id),
            swap_node_id: NodeState(node_id=swap_node_id),
        }
    )
    agent_state = _make_agent_state(plan, exec_state)
    recorded_task_updates: list[dict[str, object]] = []

    class _TaskRegistry:
        async def upsert_execution_task(self, **kwargs):
            recorded_task_updates.append(kwargs)
            return kwargs

    run_with_timing = AsyncMock(
        side_effect=[
            (
                {
                    "message": "Bridge executed",
                    "tx_hash": "0xbridge",
                    "protocol": "across",
                    "source_chain": "ethereum",
                    "dest_chain": "base",
                },
                0.05,
            ),
            (RuntimeError("swap revert"), 0.05),
        ]
    )

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=run_with_timing,
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ) as history_registry, patch(
        "graph.nodes.executor_node.ConversationTaskRegistry",
        return_value=_TaskRegistry(),
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    final_state = exec_state.merge(result["execution_state"])
    assert final_state.node_states[bridge_node_id].status == StepStatus.SUCCESS
    assert final_state.node_states[swap_node_id].status == StepStatus.FAILED
    assert final_state.completed is False
    assert result["pending_transactions"][0]["type"] == "bridge"
    assert any(update["status"] == "FAILED" for update in recorded_task_updates)
    assert any(
        update.get("latest_summary")
        and "didn't go through" in str(update.get("latest_summary")).lower()
        for update in recorded_task_updates
    )
    history_registry.return_value.record_event.assert_called_once()


def test_executor_node_slow_task_history_write_is_timeboxed():
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    mock_tool_result = {
        "message": "Swap executed",
        "tx_hash": "0xabc123",
    }

    class _SlowHistoryRegistry:
        async def record_event(self, **_kwargs):
            await asyncio.sleep(1.0)

    started = time.perf_counter()
    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(mock_tool_result, 0.05)),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
        return_value=_SlowHistoryRegistry(),
    ):
        result = asyncio.run(execution_engine_node(agent_state))
    elapsed = time.perf_counter() - started

    assert result["execution_state"].node_states[node_id].status == StepStatus.SUCCESS
    assert elapsed < 0.6


def test_executor_node_slow_production_task_registry_write_is_detached():
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    mock_tool_result = {
        "message": "Swap executed",
        "tx_hash": "0xabc123",
    }

    class _SlowTaskRegistry:
        __module__ = "core.tasks.registry"

        async def upsert_execution_task(self, **_kwargs):
            await asyncio.sleep(1.0)
            return None

    started = time.perf_counter()
    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(mock_tool_result, 0.05)),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ), patch(
        "graph.nodes.executor_node.ConversationTaskRegistry",
        return_value=_SlowTaskRegistry(),
    ):
        result = asyncio.run(execution_engine_node(agent_state))
    elapsed = time.perf_counter() - started

    assert result["execution_state"].node_states[node_id].status == StepStatus.SUCCESS
    assert elapsed < 0.6


def test_executor_node_sync_task_registry_wrapper_is_detached_without_module_match():
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    mock_tool_result = {
        "message": "Swap executed",
        "tx_hash": "0xabc123",
    }

    class _SyncTaskRegistry:
        def __init__(self) -> None:
            self.calls = 0

        def upsert_execution_task(self, **_kwargs):
            self.calls += 1
            time.sleep(0.3)
            return None

    registry = _SyncTaskRegistry()
    from core.execution import runtime as execution_runtime

    execution_runtime._side_effect_cooldowns.clear()

    started = time.perf_counter()
    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(mock_tool_result, 0.05)),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ), patch(
        "graph.nodes.executor_node.ConversationTaskRegistry",
        return_value=registry,
    ):
        result = asyncio.run(execution_engine_node(agent_state))
    elapsed = time.perf_counter() - started
    time.sleep(0.05)

    assert result["execution_state"].node_states[node_id].status == StepStatus.SUCCESS
    assert elapsed < 0.45
    assert registry.calls >= 1
    execution_runtime._side_effect_cooldowns.clear()


def test_executor_node_task_registry_cooldown_skips_repeated_unhealthy_writes():
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)

    mock_tool_result = {
        "message": "Swap executed",
        "tx_hash": "0xabc123",
    }

    class _SlowTaskRegistry:
        __module__ = "core.tasks.registry"

        def __init__(self) -> None:
            self.calls = 0

        async def upsert_execution_task(self, **_kwargs):
            self.calls += 1
            await asyncio.sleep(1.0)
            return None

    registry = _SlowTaskRegistry()

    from core.execution import runtime as execution_runtime

    execution_runtime._side_effect_cooldowns.clear()

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(mock_tool_result, 0.05)),
    ), patch(
        "graph.nodes.executor_node._TASK_HISTORY_WRITE_TIMEOUT_SECONDS",
        new=0.01,
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ), patch(
        "graph.nodes.executor_node.ConversationTaskRegistry",
        return_value=registry,
    ):
        first = asyncio.run(execution_engine_node(agent_state))
        time.sleep(0.05)
        first_call_count = registry.calls
        second = asyncio.run(execution_engine_node(agent_state))

    assert first["execution_state"].node_states[node_id].status == StepStatus.SUCCESS
    assert second["execution_state"].node_states[node_id].status == StepStatus.SUCCESS
    assert first_call_count >= 1
    assert registry.calls == first_call_count
    execution_runtime._side_effect_cooldowns.clear()


@pytest.mark.parametrize("balance_snapshot", [{}, {"0xsenderaddress|somnia testnet|0x0000000000000000000000000000000000000000": "not-a-number"}])
def test_executor_node_store_unavailable_global_reservation_fails_closed(
    balance_snapshot,
):
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})
    agent_state = _make_agent_state(plan, exec_state)
    agent_state["balance_snapshot"] = balance_snapshot
    resource_key = "0xsenderaddress|somnia testnet|usdc"
    agent_state["resource_snapshots"] = {
        resource_key: _resource_snapshot(
            resource_key=resource_key,
            wallet_scope="sender:0xsenderaddress",
            sender="0xsenderaddress",
            chain="somnia testnet",
            token_ref="usdc",
            symbol="USDC",
            decimals=6,
            available="100",
            available_base_units="100000000",
        )
    }
    agent_state["reservation_requirements"] = {
        node_id: [
            _reservation_requirement(
                resource_key=resource_key,
                wallet_scope="sender:0xsenderaddress",
                sender="0xsenderaddress",
                chain="somnia testnet",
                token_ref="usdc",
                symbol="USDC",
                decimals=6,
                required="1",
                required_base_units="1000000",
                kind="token_spend",
            )
        ]
    }

    class _UnavailableReservationService:
        async def claim(self, **_kwargs):
            return ReservationClaimResult(
                acquired=False,
                store_unavailable=True,
                deferred_reason="Reservation store unavailable.",
            )

    async def _service_getter():
        return _UnavailableReservationService()

    run_with_timing = AsyncMock(return_value=({"message": "should not run"}, 0.05))

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=run_with_timing,
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ), patch(
        "graph.nodes.executor_node.get_reservation_service",
        new=_service_getter,
    ):
        result = asyncio.run(execution_engine_node(agent_state))

    node_state = result["execution_state"].node_states[node_id]
    assert node_state.status == StepStatus.FAILED
    assert node_state.error == "Reservation store unavailable."
    assert node_state.error_category == ErrorCategory.SECURITY.value
    assert result["waiting_for_funds"] is None
    assert "secure global fund reservation was unavailable" in result["messages"][0].content.lower()
    assert any(
        "[SECURITY] Reservation store unavailable." in line
        for line in result["reasoning_logs"]
    )
    run_with_timing.assert_not_awaited()


def test_executor_node_global_reservation_defers_conflicting_execution():
    node_id = "step_0"
    plan = ExecutionPlan(
        goal="Swap USDC for WETH",
        nodes={node_id: _make_swap_node(node_id, approval_required=False)},
    )
    exec_state = ExecutionState(node_states={node_id: NodeState(node_id=node_id)})

    shared_service = WalletReservationService(store=InMemoryReservationStore())

    async def _shared_getter():
        return shared_service

    resource_key = "0xsenderaddress|somnia testnet|usdc"
    snapshot = {
        resource_key: _resource_snapshot(
            resource_key=resource_key,
            wallet_scope="sender:0xsenderaddress",
            sender="0xsenderaddress",
            chain="somnia testnet",
            token_ref="usdc",
            symbol="USDC",
            decimals=6,
            available="100",
            available_base_units="100000000",
        )
    }
    requirement = {
        node_id: [
            _reservation_requirement(
                resource_key=resource_key,
                wallet_scope="sender:0xsenderaddress",
                sender="0xsenderaddress",
                chain="somnia testnet",
                token_ref="usdc",
                symbol="USDC",
                decimals=6,
                required="100",
                required_base_units="100000000",
                kind="token_spend",
            )
        ]
    }

    first_state = _make_agent_state(plan, exec_state)
    first_state["execution_id"] = "exec-1"
    first_state["context"] = {"thread_id": "thread-1"}
    first_state["resource_snapshots"] = snapshot
    first_state["reservation_requirements"] = requirement

    second_state = _make_agent_state(plan, exec_state)
    second_state["execution_id"] = "exec-2"
    second_state["context"] = {"thread_id": "thread-2"}
    second_state["selected_task_number"] = 2
    second_state["resource_snapshots"] = snapshot
    second_state["reservation_requirements"] = requirement

    mock_tool_result = {"message": "Swap executed", "tx_hash": "0xabc123"}

    with patch(
        "graph.nodes.executor_node.IdempotencyStore",
        side_effect=Exception("idempotency disabled for test"),
    ), patch(
        "graph.nodes.executor_node._run_with_timing",
        new=AsyncMock(return_value=(mock_tool_result, 0.05)),
    ), patch(
        "core.execution.runtime.get_token_balance_async",
        new=AsyncMock(return_value="100"),
    ), patch(
        "graph.nodes.executor_node.TaskHistoryRegistry",
    ), patch(
        "graph.nodes.executor_node.get_reservation_service",
        new=_shared_getter,
    ):
        first = asyncio.run(execution_engine_node(first_state))
        second = asyncio.run(execution_engine_node(second_state))

    assert first["execution_state"].node_states[node_id].status == StepStatus.SUCCESS
    assert second["execution_state"].node_states[node_id].status == StepStatus.PENDING
    assert second["waiting_for_funds"]["node_id"] == node_id
    assert second["waiting_for_funds"]["wait_id"]
    assert any("[RESERVE:GLOBAL]" in line for line in second["reasoning_logs"])
    assert any("already reserved" in line.lower() for line in second["reasoning_logs"])


def test_resolve_tool_timeout_uses_registry_default():
    from tools_registry.register import tools_registry
    import os
    from unittest.mock import patch

    tool = tools_registry.get("swap")
    assert tool is not None

    with patch.dict(os.environ, {"TOOL_TIMEOUT_SECONDS_SWAP": "0"}):
        timeout = resolve_tool_timeout(tool.name, tool.timeout_seconds)

    assert timeout == TOOL_DEFAULT_TIMEOUTS["swap"]


if __name__ == "__main__":
    test_executor_node_success()
    test_executor_node_approval_gating()
    print("All executor node tests passed!")
