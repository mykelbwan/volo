import asyncio
from unittest.mock import AsyncMock, patch

from graph.agent_state import AgentState
from graph.nodes.resolver_node import intent_resolver_node
from intent_hub.ontology.intent import ExecutionPlan, Intent, IntentStatus


def _make_state(intents) -> AgentState:
    return {
        "user_id": "user-1",
        "provider": "discord",
        "username": "alice",
        "user_info": {"volo_user_id": "user-1"},
        "intents": intents,
        "plans": [],
        "goal_parameters": {},
        "plan_history": [],
        "execution_state": None,
        "artifacts": {},
        "context": {},
        "route_decision": None,
        "confirmation_status": None,
        "pending_transactions": [],
        "reasoning_logs": [],
        "messages": [],
        "fee_quotes": [],
        "trigger_id": None,
        "is_triggered_execution": None,
    }


def test_resolver_node_builds_plan_with_slippage():
    intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "ETH"},
            "token_out": {"symbol": "USDC"},
            "amount": 1,
            "chain": "ethereum",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="swap",
    )
    plan = ExecutionPlan(
        intent_type="swap",
        chain="Ethereum",
        parameters={
            "amount_in": 1,
            "token_in_symbol": "ETH",
            "token_out_symbol": "USDC",
        },
        constraints="slippage 1%",
    )

    with patch(
        "graph.nodes.resolver_node.resolve_intent", new=AsyncMock(return_value=plan)
    ):
        state = _make_state([intent.model_dump()])
        result = asyncio.run(intent_resolver_node(state))

    plan_history = result["plan_history"]
    assert plan_history
    node = plan_history[0].nodes["step_0"]
    assert node.args["slippage"] == 1.0
    assert node.tool == "swap"


def test_resolver_node_stops_on_incomplete_intent():
    intent_complete = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "ETH"},
            "token_out": {"symbol": "USDC"},
            "amount": 1,
            "chain": "ethereum",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="swap",
    )
    intent_incomplete = Intent(
        intent_type="swap",
        slots={},
        missing_slots=["amount"],
        constraints=None,
        confidence=0.6,
        status=IntentStatus.INCOMPLETE,
        raw_input="swap",
    )
    plan = ExecutionPlan(
        intent_type="swap",
        chain="Ethereum",
        parameters={"amount_in": 1, "token_in_symbol": "ETH", "token_out_symbol": "USDC"},
        constraints=None,
    )

    with patch(
        "graph.nodes.resolver_node.resolve_intent", new=AsyncMock(return_value=plan)
    ):
        state = _make_state([intent_complete.model_dump(), intent_incomplete.model_dump()])
        result = asyncio.run(intent_resolver_node(state))

    assert len(result["plan_history"][0].nodes) == 1


def test_resolver_node_marks_unwrap_as_no_approval_required():
    intent = Intent(
        intent_type="unwrap",
        slots={
            "token": {"symbol": "ETH"},
            "chain": "base sepolia",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="unwrap eth on base sepolia",
    )
    plan = ExecutionPlan(
        intent_type="unwrap",
        chain="base sepolia",
        parameters={
            "token_symbol": "ETH",
            "token_address": "0x4200000000000000000000000000000000000006",
            "chain": "base sepolia",
        },
        constraints=None,
    )

    with patch(
        "graph.nodes.resolver_node.resolve_intent", new=AsyncMock(return_value=plan)
    ):
        state = _make_state([intent.model_dump()])
        result = asyncio.run(intent_resolver_node(state))

    node = result["plan_history"][0].nodes["step_0"]
    assert node.tool == "unwrap"
    assert node.approval_required is False


def test_resolver_node_surfaces_resolution_error():
    intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "1STT"},
            "token_out": {"symbol": "NIA"},
            "amount": 1,
            "chain": "somnia testnet",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="buy 1stt worth of nia on somnia testnet",
    )

    with patch(
        "graph.nodes.resolver_node.resolve_intent",
        new=AsyncMock(
            side_effect=ValueError(
                "Could not resolve addresses for 1STT/NIA on somnia testnet"
            )
        ),
    ):
        state = _make_state([intent.model_dump()])
        result = asyncio.run(intent_resolver_node(state))

    assert "messages" in result
    assert result["messages"]
    assert "couldn't find" in result["messages"][0].content.lower()


def test_resolver_node_preflights_invalid_symbol():
    intent = Intent(
        intent_type="swap",
        slots={
            "token_in": {"symbol": "US DC"},
            "token_out": {"symbol": "USDC"},
            "amount": 1,
            "chain": "base",
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input="swap 123 to usdc on base",
    )

    with patch(
        "graph.nodes.resolver_node.resolve_intent",
        new=AsyncMock(),
    ) as resolve_intent:
        state = _make_state([intent.model_dump()])
        result = asyncio.run(intent_resolver_node(state))

    resolve_intent.assert_not_called()
    assert "messages" in result
    assert result["messages"]
    assert "couldn't find" in result["messages"][0].content.lower()


def test_parallel_transfers_different_tokens():
    intents = [
        Intent(
            intent_type="transfer",
            slots={
                "token": {"symbol": "USDC"},
                "amount": 10,
                "recipient": "0x000000000000000000000000000000000000dead",
                "chain": "base",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="transfer usdc",
        ),
        Intent(
            intent_type="transfer",
            slots={
                "token": {"symbol": "DAI"},
                "amount": 5,
                "recipient": "0x000000000000000000000000000000000000beef",
                "chain": "base",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="transfer dai",
        ),
    ]
    plans = [
        ExecutionPlan(
            intent_type="transfer",
            chain="base",
            parameters={
                "asset_symbol": "USDC",
                "asset_ref": "0x1",
                "amount": 10,
                "recipient": "0x000000000000000000000000000000000000dead",
                "network": "base",
            },
            constraints=None,
        ),
        ExecutionPlan(
            intent_type="transfer",
            chain="base",
            parameters={
                "asset_symbol": "DAI",
                "asset_ref": "0x2",
                "amount": 5,
                "recipient": "0x000000000000000000000000000000000000beef",
                "network": "base",
            },
            constraints=None,
        ),
    ]

    with patch(
        "graph.nodes.resolver_node.resolve_intent",
        new=AsyncMock(side_effect=plans),
    ):
        state = _make_state([i.model_dump() for i in intents])
        result = asyncio.run(intent_resolver_node(state))

    nodes = result["plan_history"][0].nodes
    assert nodes["step_0"].depends_on == []
    assert nodes["step_1"].depends_on == []


def test_serial_transfers_same_token():
    intents = [
        Intent(
            intent_type="transfer",
            slots={
                "token": {"symbol": "USDC"},
                "amount": 10,
                "recipient": "0x000000000000000000000000000000000000dead",
                "chain": "base",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="transfer usdc",
        ),
        Intent(
            intent_type="transfer",
            slots={
                "token": {"symbol": "USDC"},
                "amount": 5,
                "recipient": "0x000000000000000000000000000000000000beef",
                "chain": "base",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="transfer usdc",
        ),
    ]
    plans = [
        ExecutionPlan(
            intent_type="transfer",
            chain="base",
            parameters={
                "asset_symbol": "USDC",
                "asset_ref": "0x1",
                "amount": 10,
                "recipient": "0x000000000000000000000000000000000000dead",
                "network": "base",
            },
            constraints=None,
        ),
        ExecutionPlan(
            intent_type="transfer",
            chain="base",
            parameters={
                "asset_symbol": "USDC",
                "asset_ref": "0x1",
                "amount": 5,
                "recipient": "0x000000000000000000000000000000000000beef",
                "network": "base",
            },
            constraints=None,
        ),
    ]

    with patch(
        "graph.nodes.resolver_node.resolve_intent",
        new=AsyncMock(side_effect=plans),
    ):
        state = _make_state([i.model_dump() for i in intents])
        result = asyncio.run(intent_resolver_node(state))

    nodes = result["plan_history"][0].nodes
    assert nodes["step_0"].depends_on == []
    assert nodes["step_1"].depends_on == ["step_0"]


def test_parallel_transfers_after_swap_barrier():
    intents = [
        Intent(
            intent_type="swap",
            slots={
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 1,
                "chain": "base",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="swap",
        ),
        Intent(
            intent_type="transfer",
            slots={
                "token": {"symbol": "USDC"},
                "amount": 10,
                "recipient": "0x000000000000000000000000000000000000dead",
                "chain": "base",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="transfer usdc",
        ),
        Intent(
            intent_type="transfer",
            slots={
                "token": {"symbol": "DAI"},
                "amount": 5,
                "recipient": "0x000000000000000000000000000000000000beef",
                "chain": "base",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="transfer dai",
        ),
    ]
    plans = [
        ExecutionPlan(
            intent_type="swap",
            chain="base",
            parameters={
                "amount_in": 1,
                "token_in_symbol": "ETH",
                "token_out_symbol": "USDC",
                "chain": "base",
            },
            constraints=None,
        ),
        ExecutionPlan(
            intent_type="transfer",
            chain="base",
            parameters={
                "asset_symbol": "USDC",
                "asset_ref": "0x1",
                "amount": 10,
                "recipient": "0x000000000000000000000000000000000000dead",
                "network": "base",
            },
            constraints=None,
        ),
        ExecutionPlan(
            intent_type="transfer",
            chain="base",
            parameters={
                "asset_symbol": "DAI",
                "asset_ref": "0x2",
                "amount": 5,
                "recipient": "0x000000000000000000000000000000000000beef",
                "network": "base",
            },
            constraints=None,
        ),
    ]

    with patch("graph.nodes.resolver_node.resolve_intent", side_effect=plans):
        state = _make_state([i.model_dump() for i in intents])
        result = asyncio.run(intent_resolver_node(state))

    nodes = result["plan_history"][0].nodes
    assert nodes["step_0"].depends_on == []
    assert nodes["step_1"].depends_on == ["step_0"]
    assert nodes["step_2"].depends_on == ["step_0"]


def test_parallel_swaps_different_chains():
    intents = [
        Intent(
            intent_type="swap",
            slots={
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 1,
                "chain": "base",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="swap base",
        ),
        Intent(
            intent_type="swap",
            slots={
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 1,
                "chain": "arbitrum one",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="swap arbitrum",
        ),
    ]
    plans = [
        ExecutionPlan(
            intent_type="swap",
            chain="base",
            parameters={
                "amount_in": 1,
                "token_in_symbol": "ETH",
                "token_out_symbol": "USDC",
                "chain": "base",
            },
            constraints=None,
        ),
        ExecutionPlan(
            intent_type="swap",
            chain="arbitrum one",
            parameters={
                "amount_in": 1,
                "token_in_symbol": "ETH",
                "token_out_symbol": "USDC",
                "chain": "arbitrum one",
            },
            constraints=None,
        ),
    ]

    with patch("graph.nodes.resolver_node.resolve_intent", side_effect=plans):
        state = _make_state([i.model_dump() for i in intents])
        result = asyncio.run(intent_resolver_node(state))

    nodes = result["plan_history"][0].nodes
    assert nodes["step_0"].depends_on == []
    assert nodes["step_1"].depends_on == []


def test_serial_swaps_same_chain(monkeypatch):
    monkeypatch.delenv("ENABLE_SAME_CHAIN_PARALLEL_SWAPS", raising=False)
    monkeypatch.delenv("PARALLEL_SWAP_MAX_PER_CHAIN", raising=False)
    monkeypatch.delenv("PARALLEL_SWAP_MAX_SLIPPAGE", raising=False)

    intents = [
        Intent(
            intent_type="swap",
            slots={
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 1,
                "chain": "base",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="swap base",
        ),
        Intent(
            intent_type="swap",
            slots={
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 2,
                "chain": "base",
            },
            missing_slots=[],
            constraints=None,
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="swap base again",
        ),
    ]
    plans = [
        ExecutionPlan(
            intent_type="swap",
            chain="base",
            parameters={
                "amount_in": 1,
                "token_in_symbol": "ETH",
                "token_out_symbol": "USDC",
                "chain": "base",
            },
            constraints=None,
        ),
        ExecutionPlan(
            intent_type="swap",
            chain="base",
            parameters={
                "amount_in": 2,
                "token_in_symbol": "ETH",
                "token_out_symbol": "USDC",
                "chain": "base",
            },
            constraints=None,
        ),
    ]

    with patch("graph.nodes.resolver_node.resolve_intent", side_effect=plans):
        state = _make_state([i.model_dump() for i in intents])
        result = asyncio.run(intent_resolver_node(state))

    nodes = result["plan_history"][0].nodes
    assert nodes["step_0"].depends_on == []
    assert nodes["step_1"].depends_on == ["step_0"]


def test_parallel_swaps_same_chain_with_flag(monkeypatch):
    monkeypatch.setenv("ENABLE_SAME_CHAIN_PARALLEL_SWAPS", "true")
    monkeypatch.setenv("PARALLEL_SWAP_MAX_PER_CHAIN", "2")
    monkeypatch.setenv("PARALLEL_SWAP_MAX_SLIPPAGE", "1.0")

    intents = [
        Intent(
            intent_type="swap",
            slots={
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 1,
                "chain": "base",
            },
            missing_slots=[],
            constraints={"slippage": 0.5},
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="swap base",
        ),
        Intent(
            intent_type="swap",
            slots={
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "DAI"},
                "amount": 1,
                "chain": "base",
            },
            missing_slots=[],
            constraints={"slippage": 0.5},
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="swap base",
        ),
    ]
    plans = [
        ExecutionPlan(
            intent_type="swap",
            chain="base",
            parameters={
                "amount_in": 1,
                "token_in_symbol": "ETH",
                "token_out_symbol": "USDC",
                "chain": "base",
            },
            constraints={"slippage": 0.5},
        ),
        ExecutionPlan(
            intent_type="swap",
            chain="base",
            parameters={
                "amount_in": 1,
                "token_in_symbol": "ETH",
                "token_out_symbol": "DAI",
                "chain": "base",
            },
            constraints={"slippage": 0.5},
        ),
    ]

    with patch("graph.nodes.resolver_node.resolve_intent", side_effect=plans):
        state = _make_state([i.model_dump() for i in intents])
        result = asyncio.run(intent_resolver_node(state))

    nodes = result["plan_history"][0].nodes
    assert nodes["step_0"].depends_on == []
    assert nodes["step_1"].depends_on == []


def test_serial_swaps_same_chain_high_slippage(monkeypatch):
    monkeypatch.setenv("ENABLE_SAME_CHAIN_PARALLEL_SWAPS", "true")
    monkeypatch.setenv("PARALLEL_SWAP_MAX_PER_CHAIN", "2")
    monkeypatch.setenv("PARALLEL_SWAP_MAX_SLIPPAGE", "0.5")

    intents = [
        Intent(
            intent_type="swap",
            slots={
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "USDC"},
                "amount": 1,
                "chain": "base",
            },
            missing_slots=[],
            constraints={"slippage": 1.0},
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="swap base",
        ),
        Intent(
            intent_type="swap",
            slots={
                "token_in": {"symbol": "ETH"},
                "token_out": {"symbol": "DAI"},
                "amount": 1,
                "chain": "base",
            },
            missing_slots=[],
            constraints={"slippage": 1.0},
            confidence=0.9,
            status=IntentStatus.COMPLETE,
            raw_input="swap base",
        ),
    ]
    plans = [
        ExecutionPlan(
            intent_type="swap",
            chain="base",
            parameters={
                "amount_in": 1,
                "token_in_symbol": "ETH",
                "token_out_symbol": "USDC",
                "chain": "base",
            },
            constraints={"slippage": 1.0},
        ),
        ExecutionPlan(
            intent_type="swap",
            chain="base",
            parameters={
                "amount_in": 1,
                "token_in_symbol": "ETH",
                "token_out_symbol": "DAI",
                "chain": "base",
            },
            constraints={"slippage": 1.0},
        ),
    ]

    with patch("graph.nodes.resolver_node.resolve_intent", side_effect=plans):
        state = _make_state([i.model_dump() for i in intents])
        result = asyncio.run(intent_resolver_node(state))

    nodes = result["plan_history"][0].nodes
    assert nodes["step_0"].depends_on == []
    assert nodes["step_1"].depends_on == ["step_0"]
