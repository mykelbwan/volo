from __future__ import annotations

from core.utils.user_feedback import (
    FeedbackAction,
    bridge_not_supported,
    execution_failed,
    insufficient_balance,
    intent_missing_info,
)


def test_insufficient_balance_includes_recovery_path_for_native_fee_shortfall():
    feedback = insufficient_balance(
        [
            {
                "kind": "gas",
                "symbol": "SOL",
                "required": "0.005010",
                "available": "0.000001",
                "shortfall": "0.005009",
                "chain": "Solana",
                "sender": "SoLUser1111111111111111111111111111111111111",
                "label": "network fee + platform fee",
            }
        ]
    )

    rendered = feedback.render().lower()
    assert "insufficient balance to continue" in rendered
    assert "network fee + platform fee on solana" in rendered
    assert "add at least 0.005009 sol" in rendered
    assert "after funding, try again." in rendered


def test_execution_failed_retryable_actions_are_explicit():
    feedback = execution_failed("network", tool="swap", chain="Base")

    assert feedback.actions == (
        FeedbackAction.RETRY,
        FeedbackAction.EDIT,
        FeedbackAction.CANCEL,
    )
    rendered = feedback.render().lower()
    assert "swap failed on base" in rendered
    assert "reply with: retry, edit, cancel" in rendered


def test_execution_failed_non_retryable_omits_retry_action():
    feedback = execution_failed("non_retryable", tool="bridge", chain="Ethereum")

    assert feedback.actions == (FeedbackAction.EDIT, FeedbackAction.CANCEL)
    rendered = feedback.render().lower()
    assert "bridge failed on ethereum" in rendered
    assert "reply with: edit, cancel" in rendered
    assert "retry" not in rendered.split("reply with:", 1)[1]


def test_bridge_not_supported_includes_suggestions_and_recovery_actions():
    feedback = bridge_not_supported(
        "ETH",
        "Ethereum",
        "Base",
        chain_pairs=["Ethereum → Optimism", "Ethereum → Arbitrum"],
        tokens=["USDC", "WETH"],
    )

    rendered = feedback.render().lower()
    assert "bridge not supported for eth from ethereum to base" in rendered
    assert "supported chains for this token" in rendered
    assert "supported tokens for this chain pair" in rendered
    assert "reply with: edit, cancel" in rendered


def test_intent_missing_info_single_chain_is_short_and_specific():
    feedback = intent_missing_info(["chain"])

    assert feedback.render() == "Which chain?"


def test_intent_missing_info_multiple_fields_stays_specific():
    feedback = intent_missing_info(["amount", "chain"])

    assert feedback.render() == "Please share the amount and chain."
