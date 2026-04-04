from core.planning.execution_plan import ExecutionState, NodeState, StepStatus, resolve_dynamic_args


def test_resolve_output_of_marker():
    state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.SUCCESS,
                result={"output_amount": "123.45"},
            )
        }
    )
    args = {"amount": "{{OUTPUT_OF:step_0}}"}
    resolved = resolve_dynamic_args(args, state)
    assert resolved["amount"] == 123.45


def test_resolve_balance_of_marker():
    state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.SUCCESS,
                result={
                    "balances": [
                        {"symbol": "USDC", "balance_formatted": "42.5"},
                        {"symbol": "ETH", "balance_formatted": "1.2"},
                    ]
                },
            )
        }
    )
    args = {"amount": "{{BALANCE_OF:step_0:USDC}}"}
    resolved = resolve_dynamic_args(args, state)
    assert resolved["amount"] == 42.5


def test_resolve_output_of_marker_from_artifacts():
    state = ExecutionState(
        node_states={
            "step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS, result={})
        },
        artifacts={"outputs": {"step_0": {"output_amount": "7.25"}}},
    )
    args = {"amount": "{{OUTPUT_OF:step_0}}"}
    resolved = resolve_dynamic_args(args, state)
    assert resolved["amount"] == 7.25


def test_resolve_balance_of_marker_from_artifacts():
    state = ExecutionState(
        node_states={
            "step_0": NodeState(node_id="step_0", status=StepStatus.SUCCESS, result={})
        },
        artifacts={
            "outputs": {
                "step_0": {
                    "balances": [
                        {"symbol": "USDC", "balance_formatted": "100.0"},
                        {"symbol": "ETH", "balance_formatted": "0.2"},
                    ]
                }
            }
        },
    )
    args = {"amount": "{{BALANCE_OF:step_0:USDC}}"}
    resolved = resolve_dynamic_args(args, state)
    assert resolved["amount"] == 100.0
