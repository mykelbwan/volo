import pytest

from core.security.guardrails import GuardrailPolicy, GuardrailService, RiskViolationError
from core.planning.execution_plan import ExecutionState, NodeState, PlanNode, StepStatus


def _state_with_running(count: int) -> ExecutionState:
    return ExecutionState(
        node_states={
            f"n{i}": NodeState(node_id=f"n{i}", status=StepStatus.RUNNING)
            for i in range(count)
        }
    )


def test_guardrails_rejects_high_slippage():
    service = GuardrailService(GuardrailPolicy(max_slippage_percent=1.0))
    node = PlanNode(
        id="n1",
        tool="swap",
        args={"slippage": 2.0},
        depends_on=[],
        approval_required=True,
    )

    with pytest.raises(RiskViolationError):
        service.validate_node(node, ExecutionState(node_states={}))


def test_guardrails_rejects_blocked_chain():
    policy = GuardrailPolicy(blocked_chains=["evil-chain"])
    service = GuardrailService(policy)
    node = PlanNode(
        id="n1",
        tool="swap",
        args={"chain": "evil-chain"},
        depends_on=[],
        approval_required=True,
    )

    with pytest.raises(RiskViolationError):
        service.validate_node(node, ExecutionState(node_states={}))


def test_guardrails_rejects_parallel_limit():
    policy = GuardrailPolicy(max_parallel_nodes=2)
    service = GuardrailService(policy)
    node = PlanNode(
        id="n3",
        tool="swap",
        args={},
        depends_on=[],
        approval_required=True,
    )

    with pytest.raises(RiskViolationError):
        service.validate_node(node, _state_with_running(2))


def test_guardrails_min_amount_disabled_when_zero():
    policy = GuardrailPolicy(min_amount_usd=0)
    service = GuardrailService(policy)
    node = PlanNode(
        id="n4",
        tool="swap",
        args={"amount_in": 0.001},
        depends_on=[],
        approval_required=True,
    )

    assert service.validate_node(node, ExecutionState(node_states={})) is True


def test_guardrails_min_amount_enforced_when_positive():
    policy = GuardrailPolicy(min_amount_usd=1.0)
    service = GuardrailService(policy)
    node = PlanNode(
        id="n5",
        tool="swap",
        args={"amount_in": 0.5},
        depends_on=[],
        approval_required=True,
    )

    with pytest.raises(RiskViolationError):
        service.validate_node(node, ExecutionState(node_states={}))
