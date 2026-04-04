from decimal import Decimal
from core.planning.execution_plan import ExecutionState, NodeState, StepStatus, resolve_dynamic_args

def test_resolve_sum_from_previous_marker():
    state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.SUCCESS,
                result={"output_amount": "100.0"},
            ),
            "step_1": NodeState(
                node_id="step_1",
                status=StepStatus.SUCCESS,
                result={"details": {"amount_out": "50.0"}},
            )
        }
    )
    args = {"input_amount": "{{SUM_FROM_PREVIOUS}}"}
    resolved = resolve_dynamic_args(args, state)
    assert resolved["input_amount"] == 150.0

def test_resolve_total_balance_marker_wei():
    state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.SUCCESS,
                result={"output_amount": "1.5"},
            )
        }
    )
    args = {"amount_wei": "{{TOTAL_BALANCE}}"}
    # 1.5 * 10^18 = 1500000000000000000
    resolved = resolve_dynamic_args(args, state)
    assert resolved["amount_wei"] == "1500000000000000000"

def test_resolve_session_context_markers():
    context = {"SUB_ORG_ID": "org_123", "USER_ID": "user_456"}
    state = ExecutionState(node_states={})
    args = {
        "org": "{{SUB_ORG_ID}}",
        "msg": "Hello {{USER_ID}}"
    }
    resolved = resolve_dynamic_args(args, state, context=context)
    assert resolved["org"] == "org_123"
    assert resolved["msg"] == "Hello user_456"

def test_resolve_family_specific_wallet_markers_from_artifacts():
    context = {
        "evm_address": "0xEvmSender",
        "evm_sub_org_id": "evm-sub-org",
        "solana_address": "So1anaSender111111111111111111111111111111111",
        "solana_sub_org_id": "solana-sub-org",
    }
    state = ExecutionState(node_states={})
    args = {
        "evm_sender": "{{EVM_ADDRESS}}",
        "evm_sub_org_id": "{{EVM_SUB_ORG_ID}}",
        "solana_sender": "{{SOLANA_ADDRESS}}",
        "solana_sub_org_id": "{{SOLANA_SUB_ORG_ID}}",
    }

    resolved = resolve_dynamic_args(args, state, context=context)

    assert resolved["evm_sender"] == "0xEvmSender"
    assert resolved["evm_sub_org_id"] == "evm-sub-org"
    assert resolved["solana_sender"] == "So1anaSender111111111111111111111111111111111"
    assert resolved["solana_sub_org_id"] == "solana-sub-org"

def test_resolve_legacy_wallet_markers_backfill_evm_aliases():
    context = {
        "sender_address": "0xLegacySender",
        "sub_org_id": "legacy-sub",
    }
    state = ExecutionState(node_states={})
    args = {
        "sender": "{{EVM_ADDRESS}}",
        "sub_org_id": "{{EVM_SUB_ORG_ID}}",
    }

    resolved = resolve_dynamic_args(args, state, context=context)

    assert resolved["sender"] == "0xLegacySender"
    assert resolved["sub_org_id"] == "legacy-sub"

def test_resolve_legacy_wallet_markers_do_not_infer_solana():
    context = {
        "sender_address": "0xLegacySender",
        "sub_org_id": "legacy-sub",
    }
    state = ExecutionState(node_states={})
    args = {"sender": "{{SOLANA_ADDRESS}}"}

    resolved = resolve_dynamic_args(args, state, context=context)

    assert resolved["sender"] == "{{SOLANA_ADDRESS}}"

def test_resolve_explicit_evm_wallet_markers_not_overwritten_by_legacy_values():
    context = {
        "sender_address": "0xLegacySender",
        "sub_org_id": "legacy-sub",
        "evm_address": "0xExplicitEvm",
        "evm_sub_org_id": "explicit-evm-sub",
    }
    state = ExecutionState(node_states={})
    args = {
        "sender": "{{EVM_ADDRESS}}",
        "sub_org_id": "{{EVM_SUB_ORG_ID}}",
    }

    resolved = resolve_dynamic_args(args, state, context=context)

    assert resolved["sender"] == "0xExplicitEvm"
    assert resolved["sub_org_id"] == "explicit-evm-sub"

def test_resolve_total_available_lazy_loading():
    # Verify that total_available is only calculated if markers are present
    # We can't easily check internal state, but we can verify it still works correctly
    state = ExecutionState(
        node_states={
            "step_0": NodeState(
                node_id="step_0",
                status=StepStatus.SUCCESS,
                result={"output_amount": "10.0"},
            )
        }
    )
    # No marker
    args_no_marker = {"amount": "100.0"}
    resolved_no_marker = resolve_dynamic_args(args_no_marker, state)
    assert resolved_no_marker["amount"] == "100.0"
    
    # With marker
    args_marker = {"amount": "{{TOTAL_BALANCE}}"}
    resolved_marker = resolve_dynamic_args(args_marker, state)
    assert resolved_marker["amount"] == 10.0
