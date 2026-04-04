from __future__ import annotations

from decimal import Decimal

import pytest

from core.routing.route_meta import (
    FallbackPolicy,
    FallbackReason,
    RouteMeta,
    RouteMetaValidationError,
    canonicalize_route_meta,
    enforce_fallback_policy,
    is_route_expired,
    log_fallback_event,
    log_route_expiry,
    log_route_validation,
    route_meta_strictly_enforced,
    route_meta_matches_node,
    validate_route_meta,
)
from core.utils.errors import DeterminismViolationError


def test_validate_route_meta_accepts_valid_evm_route() -> None:
    route_meta = RouteMeta(
        token_in="0x1111",
        token_out="0x2222",
        amount_in=1_000_000,
        expected_output=950_000,
        min_output=900_000,
        gas_estimate=210_000,
        expiry_timestamp=1_700_000_000,
        calldata=b"\xde\xad\xbe\xef",
        to="0x3333",
        provider="1inch",
        route_id="route-1",
        chain_type="evm",
    )

    assert validate_route_meta(route_meta) is None


def test_validate_route_meta_rejects_missing_execution_primitive() -> None:
    route_meta = RouteMeta(
        token_in="So111",
        token_out="EPjF",
        amount_in=1,
        expected_output=2,
        min_output=1,
        gas_estimate=0,
        provider="jupiter",
        chain_type="solana",
    )

    with pytest.raises(RouteMetaValidationError, match="exactly one execution form"):
        validate_route_meta(route_meta)


def test_validate_route_meta_rejects_invalid_min_output() -> None:
    route_meta = RouteMeta(
        token_in="0x1111",
        token_out="0x2222",
        amount_in=100,
        expected_output=90,
        min_output=91,
        gas_estimate=10,
        calldata=b"\x01",
        to="0x3333",
        provider="0x",
        chain_type="evm",
    )

    with pytest.raises(RouteMetaValidationError, match="min_output cannot exceed"):
        validate_route_meta(route_meta)


def test_validate_route_meta_rejects_missing_provider_in_planned_dict_payload() -> None:
    route_meta = {
        "chain_id": 1,
        "token_in": "0x1111",
        "token_out": "0x2222",
        "amount_in": "100",
        "amount_out": "95",
        "amount_out_min": "90",
        "gas_estimate": 210000,
        "execution": {
            "protocol": "v3",
            "path": ["0x1111", "0x2222"],
            "fee_tiers": [3000],
        },
    }

    with pytest.raises(RouteMetaValidationError, match="missing provider"):
        validate_route_meta(route_meta)


def test_validate_route_meta_requires_solana_instruction_set() -> None:
    route_meta = RouteMeta(
        token_in="So111",
        token_out="EPjF",
        amount_in=10,
        expected_output=9,
        min_output=8,
        gas_estimate=0,
        provider="jupiter",
        structured_route_steps=({"amm": "jupiter"},),
        chain_type="solana",
    )

    with pytest.raises(RouteMetaValidationError, match="instruction set"):
        validate_route_meta(route_meta)


def test_fallback_policy_requires_reason_when_enabled() -> None:
    with pytest.raises(ValueError, match="fallback reason is required"):
        FallbackPolicy(allow_fallback=True)


def test_enforce_fallback_policy_rejects_unplanned_fallback() -> None:
    with pytest.raises(
        DeterminismViolationError,
        match="tool attempted dynamic fallback",
    ):
        enforce_fallback_policy(
            policy=FallbackPolicy(),
            detail="tool attempted dynamic fallback",
        )


def test_route_expiry_helper_and_log_payloads() -> None:
    route_meta = RouteMeta(
        token_in="0x1111",
        token_out="0x2222",
        amount_in=100,
        expected_output=99,
        min_output=98,
        gas_estimate=123_456,
        expiry_timestamp=200,
        calldata=b"\x01\x02",
        to="0x3333",
        provider="paraswap",
        route_id="route-99",
        chain_type="evm",
    )

    assert is_route_expired(route_meta, now=201) is True
    assert log_route_validation(route_meta=route_meta, valid=True, tool="swap") == {
        "provider": "paraswap",
        "route_id": "route-99",
        "token_in": "0x1111",
        "token_out": "0x2222",
        "amount_in": "100",
        "expected_output": "99",
        "min_output": "98",
        "event": "route_validation",
        "tool": "swap",
        "valid": True,
        "error": None,
    }
    assert log_fallback_event(
        policy=FallbackPolicy(
            allow_fallback=True,
            reason=FallbackReason.ROUTE_EXPIRED,
        ),
        route_meta=route_meta,
        detail="quote aged out",
    ) == {
        "provider": "paraswap",
        "route_id": "route-99",
        "token_in": "0x1111",
        "token_out": "0x2222",
        "amount_in": "100",
        "expected_output": "99",
        "min_output": "98",
        "event": "route_fallback",
        "allow_fallback": True,
        "fallback_reason": "ROUTE_EXPIRED",
        "detail": "quote aged out",
    }
    assert log_route_expiry(route_meta=route_meta, now=201) == {
        "provider": "paraswap",
        "route_id": "route-99",
        "token_in": "0x1111",
        "token_out": "0x2222",
        "amount_in": "100",
        "expected_output": "99",
        "min_output": "98",
        "event": "route_expiry",
        "now": 201,
        "expiry_timestamp": 200,
        "expired": True,
    }


def test_legacy_validate_route_meta_marks_missing_route_meta_without_fallback() -> None:
    validation = validate_route_meta(
        tool="swap",
        resolved_args={
            "chain": "Ethereum",
            "token_in_address": "0x1111",
            "token_out_address": "0x2222",
        },
        route_meta=None,
        strict_missing=False,
    )

    assert validation.valid is True
    assert validation.should_use_route_meta is False
    assert validation.allow_dynamic_fallback is False
    assert validation.fallback_policy == FallbackPolicy()


def test_legacy_validate_route_meta_rejects_invalid_route() -> None:
    validation = validate_route_meta(
        tool="bridge",
        resolved_args={
            "source_chain": "Ethereum",
            "target_chain": "Base",
            "token_symbol": "USDC",
        },
        route_meta={
            "aggregator": "socket",
            "invalid": True,
            "invalid_reason": "route expired at source",
        },
        strict_missing=False,
    )

    assert validation.valid is False
    assert validation.should_use_route_meta is False
    assert validation.allow_dynamic_fallback is False
    assert validation.reason == "route expired at source"
    assert validation.fallback_policy == FallbackPolicy()


def test_validate_route_meta_accepts_planned_dict_payload() -> None:
    route_meta = {
        "aggregator": "uniswap_v3",
        "provider": "uniswap_v3",
        "chain_id": 1,
        "token_in": "0x1111",
        "token_out": "0x2222",
        "amount_in": "1.5",
        "amount_out": "1.2",
        "amount_out_min": "1.1",
        "gas_estimate": 210000,
        "fetched_at": 100.0,
        "execution": {
            "protocol": "v3",
            "path": ["0x1111", "0x2222"],
            "fee_tiers": [3000],
        },
    }

    assert validate_route_meta(route_meta) is None
    canonical = canonicalize_route_meta(route_meta, tool="swap")
    assert canonical.amount_in == Decimal("1.5")
    assert canonical.expected_output == Decimal("1.2")
    assert canonical.min_output == Decimal("1.1")


def test_legacy_validate_route_meta_accepts_matching_route_with_fallback_reason() -> None:
    validation = validate_route_meta(
        tool="swap",
        resolved_args={
            "chain": "Ethereum",
            "token_in_address": "0x1111",
            "token_out_address": "0x2222",
        },
        route_meta={
            "aggregator": "uniswap_v3",
            "provider": "uniswap_v3",
            "chain_id": 1,
            "token_in": "0x1111",
            "token_out": "0x2222",
            "amount_in": "100",
            "amount_out": "95",
            "amount_out_min": "90",
            "gas_estimate": 210000,
            "allow_fallback": True,
            "fallback_reason": "PLANNER_OVERRIDE",
            "execution": {
                "protocol": "v3",
                "path": ["0x1111", "0x2222"],
                "fee_tiers": [3000],
            },
        },
        strict_missing=True,
    )

    assert validation.valid is True
    assert validation.should_use_route_meta is True
    assert validation.allow_dynamic_fallback is True
    assert validation.fallback_policy == FallbackPolicy(
        allow_fallback=True,
        reason=FallbackReason.PLANNER_OVERRIDE,
    )


def test_legacy_validate_route_meta_rejects_enabled_fallback_without_reason() -> None:
    validation = validate_route_meta(
        tool="swap",
        resolved_args={
            "chain": "Ethereum",
            "token_in_address": "0x1111",
            "token_out_address": "0x2222",
        },
        route_meta={
            "aggregator": "uniswap_v3",
            "provider": "uniswap_v3",
            "chain_id": 1,
            "token_in": "0x1111",
            "token_out": "0x2222",
            "amount_in": "100",
            "amount_out": "95",
            "amount_out_min": "90",
            "gas_estimate": 210000,
            "allow_fallback": True,
            "execution": {
                "protocol": "v3",
                "path": ["0x1111", "0x2222"],
                "fee_tiers": [3000],
            },
        },
        strict_missing=True,
    )

    assert validation.valid is False
    assert validation.should_use_route_meta is True
    assert validation.reason == "fallback reason is required when fallback is enabled"


def test_legacy_validate_route_meta_rejects_chain_mismatched_route() -> None:
    validation = validate_route_meta(
        tool="swap",
        resolved_args={
            "chain": "Ethereum",
            "token_in_address": "0x1111",
            "token_out_address": "0x2222",
        },
        route_meta={
            "aggregator": "uniswap_v3",
            "provider": "uniswap_v3",
            "chain_id": 8453,
            "token_in": "0x1111",
            "token_out": "0x2222",
            "amount_in": "100",
            "amount_out": "95",
            "amount_out_min": "90",
            "gas_estimate": 210000,
            "execution": {
                "protocol": "v3",
                "path": ["0x1111", "0x2222"],
                "fee_tiers": [3000],
            },
        },
        strict_missing=True,
    )

    assert validation.valid is False
    assert validation.should_use_route_meta is True
    assert validation.reason == "route metadata does not match the planned node arguments"


def test_route_meta_matches_node_rejects_bridge_shadow_amount_mismatch() -> None:
    route_meta = {
        "aggregator": "across",
        "source_chain_id": 1,
        "dest_chain_id": 8453,
        "token_symbol": "USDC",
        "input_amount": "100",
        "tool_data": {
            "planned_quote": {
                "protocol": "across",
                "source_chain_id": 1,
                "dest_chain_id": 8453,
                "token_symbol": "USDC",
                "input_amount": "1000000",
            }
        },
    }

    assert (
        route_meta_matches_node(
            tool="bridge",
            route_meta=route_meta,
            resolved_args={
                "source_chain": "ethereum",
                "target_chain": "base",
                "token_symbol": "USDC",
                "amount": "100",
                "recipient": "0xrecipient",
            },
        )
        is False
    )


def test_route_meta_strictly_enforced_disabled_when_unrouted_nodes_present() -> None:
    assert (
        route_meta_strictly_enforced(
            {
                "route_planner": {
                    "applied": True,
                    "routable_nodes": 2,
                    "routed_nodes": 1,
                    "unrouted_nodes": 1,
                }
            }
        )
        is False
    )


def test_route_meta_strictly_enforced_true_when_planner_fully_routed() -> None:
    assert (
        route_meta_strictly_enforced(
            {
                "route_planner": {
                    "applied": True,
                    "routable_nodes": 2,
                    "routed_nodes": 2,
                    "unrouted_nodes": 0,
                }
            }
        )
        is True
    )
