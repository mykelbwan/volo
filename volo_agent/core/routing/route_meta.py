from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Dict, Mapping, Sequence, overload

from config.chains import get_chain_by_name
from config.solana_chains import get_solana_chain, is_solana_network

ROUTED_EXECUTION_TOOLS = frozenset({"swap", "bridge", "solana_swap"})
_LOGGER = logging.getLogger("volo.route_meta")
_ROUTE_TTL_SECONDS = {
    "swap": 45,
    "bridge": 45,
    "solana_swap": 30,
}
# Keep 1inch disabled until API/KYC is ready.
_TRUSTED_SWAP_CALLDATA_AGGREGATORS = frozenset({"0x", "paraswap"})


class RouteMetaValidationError(ValueError):
    """Raised when canonical route metadata fails strict validation."""


class FallbackReason(str, Enum):
    ROUTE_EXPIRED = "ROUTE_EXPIRED"
    ROUTE_INVALID = "ROUTE_INVALID"
    PLANNER_OVERRIDE = "PLANNER_OVERRIDE"


@dataclass(frozen=True, slots=True)
class FallbackPolicy:
    allow_fallback: bool = False
    reason: FallbackReason | None = None

    def __post_init__(self) -> None:
        if self.allow_fallback and self.reason is None:
            raise ValueError("fallback reason is required when fallback is enabled")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allow_fallback": self.allow_fallback,
            "reason": self.reason.value if self.reason is not None else None,
        }


@dataclass(frozen=True, slots=True)
class RouteMeta:
    token_in: str
    token_out: str
    amount_in: int
    expected_output: int
    min_output: int
    gas_estimate: int
    expiry_timestamp: int | None = None

    # execution-specific
    calldata: bytes | None = None
    to: str | None = None
    value: int | None = None

    # metadata
    provider: str = ""
    route_id: str | None = None

    # structured execution primitives
    structured_route_steps: tuple[Dict[str, Any], ...] = ()
    instruction_set: tuple[Dict[str, Any], ...] = ()
    route_path: tuple[Any, ...] = ()
    chain_type: str | None = None

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
        *,
        chain_type: str | None = None,
    ) -> "RouteMeta":
        execution = payload.get("execution")
        execution_map = execution if isinstance(execution, Mapping) else {}
        tool_data = payload.get("tool_data")
        tool_data_map = tool_data if isinstance(tool_data, Mapping) else {}
        inferred_chain_type = chain_type or _infer_chain_type(payload)

        expected_output = _coerce_int(
            payload.get("expected_output"),
            fallback=payload.get("amount_out"),
            second_fallback=payload.get("output_amount"),
            third_fallback=payload.get("amount_out_lamports"),
        )
        min_output = _coerce_int(
            payload.get("min_output"),
            fallback=payload.get("amount_out_min"),
            second_fallback=payload.get("amount_out_minimum"),
            third_fallback=expected_output,
        )

        return cls(
            token_in=str(
                payload.get("token_in")
                or payload.get("input_mint")
                or payload.get("source_address")
                or ""
            ),
            token_out=str(
                payload.get("token_out")
                or payload.get("output_mint")
                or payload.get("target_address")
                or ""
            ),
            amount_in=_coerce_int(
                payload.get("amount_in"),
                fallback=payload.get("amount_in_lamports"),
            ),
            expected_output=expected_output,
            min_output=min_output,
            gas_estimate=_coerce_int(payload.get("gas_estimate")),
            expiry_timestamp=_coerce_optional_int(payload.get("expiry_timestamp")),
            calldata=_coerce_calldata(payload.get("calldata")),
            to=_coerce_optional_str(payload.get("to")),
            value=_coerce_optional_int(payload.get("value")),
            provider=str(payload.get("provider") or payload.get("aggregator") or ""),
            route_id=_coerce_optional_str(payload.get("route_id")),
            structured_route_steps=_coerce_step_tuple(
                payload.get("structured_route_steps"),
                fallback=execution_map.get("path"),
            ),
            instruction_set=_coerce_step_tuple(
                payload.get("instruction_set"),
                fallback=payload.get("instructions"),
            ),
            route_path=_coerce_route_path_tuple(
                payload.get("route_path"),
                fallback=tool_data_map.get("route"),
                second_fallback=tool_data_map.get("planned_quote"),
            ),
            chain_type=inferred_chain_type,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": self.amount_in,
            "expected_output": self.expected_output,
            "min_output": self.min_output,
            "gas_estimate": self.gas_estimate,
            "expiry_timestamp": self.expiry_timestamp,
            "calldata": self.calldata.hex() if self.calldata is not None else None,
            "to": self.to,
            "value": self.value,
            "provider": self.provider,
            "route_id": self.route_id,
            "structured_route_steps": [
                dict(step) for step in self.structured_route_steps
            ],
            "instruction_set": [dict(step) for step in self.instruction_set],
            "route_path": list(self.route_path),
            "chain_type": self.chain_type,
        }


@dataclass(frozen=True, slots=True)
class CanonicalRouteMeta:
    provider: str
    token_in: str
    token_out: str
    amount_in: Decimal
    expected_output: Decimal
    min_output: Decimal
    gas_estimate: int
    expiry_timestamp: int | None = None
    route_id: str | None = None
    fallback_policy: FallbackPolicy = field(default_factory=FallbackPolicy)
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RouteMetaValidationResult:
    tool: str
    valid: bool
    required: bool
    should_use_route_meta: bool
    allow_dynamic_fallback: bool
    reason: str | None = None
    fallback_policy: FallbackPolicy = field(default_factory=FallbackPolicy)


def route_meta_required(tool: str) -> bool:
    return str(tool or "").strip().lower() in ROUTED_EXECUTION_TOOLS


def route_meta_strictly_enforced(plan_metadata: Mapping[str, Any] | None) -> bool:
    """
    Decide whether missing route metadata should fail closed.

    We enforce strict route metadata only when route-planner routing was
    successfully applied for the whole routed subset. If planner metadata
    reports unrouted nodes or timeout, execution should remain non-blocking
    and fall back to dynamic routing.
    """
    if not isinstance(plan_metadata, Mapping):
        return False
    planner_meta = plan_metadata.get("route_planner")
    if not isinstance(planner_meta, Mapping):
        return False

    explicit = planner_meta.get("enforce_route_meta")
    if isinstance(explicit, bool):
        return explicit

    if not bool(planner_meta.get("applied")):
        return False
    if bool(planner_meta.get("timed_out")):
        return False

    routed_nodes = planner_meta.get("routed_nodes")
    routable_nodes = planner_meta.get("routable_nodes")
    if isinstance(routed_nodes, int) and isinstance(routable_nodes, int):
        if routed_nodes < routable_nodes:
            return False

    return True


def resolve_route_chain_id(chain_name: str) -> int | None:
    name = str(chain_name or "").strip()
    if not name:
        return None
    try:
        return int(get_chain_by_name(name).chain_id)
    except KeyError:
        if not is_solana_network(name):
            return None
    try:
        return int(get_solana_chain(name).chain_id)
    except KeyError:
        return None


def route_meta_matches_node(
    *,
    tool: str,
    route_meta: Dict[str, Any],
    resolved_args: Dict[str, Any],
) -> bool:
    if tool == "swap":
        expected_chain_id = resolve_route_chain_id(
            str(resolved_args.get("chain") or "")
        )
        actual_chain_id = route_meta.get("chain_id")
        if expected_chain_id is not None and actual_chain_id is not None:
            if int(actual_chain_id) != expected_chain_id:
                return False
        for meta_key, arg_key in (
            ("token_in", "token_in_address"),
            ("token_out", "token_out_address"),
        ):
            expected = str(resolved_args.get(arg_key) or "").strip().lower()
            actual = str(route_meta.get(meta_key) or "").strip().lower()
            if expected and actual and expected != actual:
                return False
        if not _route_amount_matches(
            expected_amount=resolved_args.get("amount_in"),
            candidates=(route_meta.get("amount_in"),),
        ):
            return False
        return True

    if tool == "bridge":
        expected_source_chain_id = resolve_route_chain_id(
            str(resolved_args.get("source_chain") or "")
        )
        expected_dest_chain_id = resolve_route_chain_id(
            str(resolved_args.get("target_chain") or "")
        )
        actual_source_chain_id = route_meta.get("source_chain_id")
        actual_dest_chain_id = route_meta.get("dest_chain_id")
        if expected_source_chain_id is not None and actual_source_chain_id is not None:
            if int(actual_source_chain_id) != expected_source_chain_id:
                return False
        if expected_dest_chain_id is not None and actual_dest_chain_id is not None:
            if int(actual_dest_chain_id) != expected_dest_chain_id:
                return False
        expected_symbol = str(resolved_args.get("token_symbol") or "").strip().upper()
        actual_symbol = str(route_meta.get("token_symbol") or "").strip().upper()
        if expected_symbol and actual_symbol and expected_symbol != actual_symbol:
            return False
        tool_data = route_meta.get("tool_data")
        planned_quote = (
            tool_data.get("planned_quote") if isinstance(tool_data, Mapping) else None
        )
        if not _route_amount_matches(
            expected_amount=resolved_args.get("amount"),
            candidates=(
                route_meta.get("input_amount"),
                planned_quote.get("input_amount")
                if isinstance(planned_quote, Mapping)
                else None,
            ),
        ):
            return False
        return True

    if tool == "solana_swap":
        expected_network = str(resolved_args.get("network") or "solana").strip().lower()
        actual_network = str(route_meta.get("network") or "").strip().lower()
        if expected_network and actual_network and expected_network != actual_network:
            return False
        for meta_key, arg_key in (
            ("input_mint", "token_in_mint"),
            ("output_mint", "token_out_mint"),
        ):
            expected = str(resolved_args.get(arg_key) or "").strip()
            actual = str(route_meta.get(meta_key) or "").strip()
            if expected and actual and expected != actual:
                return False
        if not _route_amount_matches(
            expected_amount=resolved_args.get("amount_in"),
            candidates=(
                route_meta.get("amount_in"),
            ),
        ):
            return False
        return True

    return True


def is_route_expired(route_meta: RouteMeta | CanonicalRouteMeta, now: int) -> bool:
    expiry_timestamp = route_meta.expiry_timestamp
    return expiry_timestamp is not None and now > expiry_timestamp


def log_route_validation(
    *,
    route_meta: RouteMeta | CanonicalRouteMeta | None,
    valid: bool,
    error: str | None = None,
    tool: str | None = None,
) -> Dict[str, Any]:
    payload = _base_log_payload(route_meta)
    payload.update(
        {
            "event": "route_validation",
            "tool": tool,
            "valid": valid,
            "error": error,
        }
    )
    return payload


def log_fallback_event(
    *,
    policy: FallbackPolicy,
    route_meta: RouteMeta | CanonicalRouteMeta | None = None,
    detail: str | None = None,
) -> Dict[str, Any]:
    payload = _base_log_payload(route_meta)
    payload.update(
        {
            "event": "route_fallback",
            "allow_fallback": policy.allow_fallback,
            "fallback_reason": policy.reason.value if policy.reason else None,
            "detail": detail,
        }
    )
    return payload


def log_route_expiry(
    *,
    route_meta: RouteMeta | CanonicalRouteMeta,
    now: int,
) -> Dict[str, Any]:
    payload = _base_log_payload(route_meta)
    payload.update(
        {
            "event": "route_expiry",
            "now": now,
            "expiry_timestamp": route_meta.expiry_timestamp,
            "expired": is_route_expired(route_meta, now),
        }
    )
    return payload


def _validate_route_meta_contract(route_meta: RouteMeta) -> None:
    if not route_meta.token_in.strip():
        raise RouteMetaValidationError("route metadata is missing token_in")
    if not route_meta.token_out.strip():
        raise RouteMetaValidationError("route metadata is missing token_out")
    if not route_meta.provider.strip():
        raise RouteMetaValidationError("route metadata is missing provider")
    if route_meta.amount_in <= 0:
        raise RouteMetaValidationError(
            "route metadata amount_in must be greater than zero"
        )
    if route_meta.expected_output <= 0:
        raise RouteMetaValidationError(
            "route metadata expected_output must be greater than zero"
        )
    if route_meta.min_output <= 0:
        raise RouteMetaValidationError(
            "route metadata min_output must be greater than zero"
        )
    if route_meta.min_output > route_meta.expected_output:
        raise RouteMetaValidationError(
            "route metadata min_output cannot exceed expected_output"
        )
    if route_meta.gas_estimate < 0:
        raise RouteMetaValidationError("route metadata gas_estimate cannot be negative")
    if route_meta.value is not None and route_meta.value < 0:
        raise RouteMetaValidationError("route metadata value cannot be negative")

    has_calldata = bool(route_meta.calldata)
    has_structured_steps = bool(
        route_meta.structured_route_steps
        or route_meta.instruction_set
        or route_meta.route_path
    )
    if has_calldata == has_structured_steps:
        raise RouteMetaValidationError(
            "route metadata must include exactly one execution form: calldata or "
            "structured route steps"
        )

    chain_type = _normalized_chain_type(route_meta)
    if chain_type == "evm":
        if not route_meta.calldata:
            raise RouteMetaValidationError("EVM route metadata requires calldata")
        if not route_meta.to or not route_meta.to.strip():
            raise RouteMetaValidationError(
                "EVM route metadata requires a target address"
            )
    elif chain_type == "solana":
        if not route_meta.instruction_set:
            raise RouteMetaValidationError(
                "Solana route metadata requires an instruction set"
            )
    elif chain_type == "bridge":
        if not route_meta.route_path:
            raise RouteMetaValidationError(
                "Bridge route metadata requires a route path"
            )
    elif not route_meta.structured_route_steps:
        raise RouteMetaValidationError(
            "route metadata structured routes must include explicit route steps"
        )


def _validate_canonical_route_meta_contract(route_meta: CanonicalRouteMeta) -> None:
    if not route_meta.token_in.strip():
        raise RouteMetaValidationError("route metadata is missing token_in")
    if not route_meta.token_out.strip():
        raise RouteMetaValidationError("route metadata is missing token_out")
    if not route_meta.provider.strip():
        raise RouteMetaValidationError("route metadata is missing provider")
    if route_meta.amount_in <= 0:
        raise RouteMetaValidationError(
            "route metadata amount_in must be greater than zero"
        )
    if route_meta.expected_output <= 0:
        raise RouteMetaValidationError(
            "route metadata expected_output must be greater than zero"
        )
    if route_meta.min_output <= 0:
        raise RouteMetaValidationError(
            "route metadata min_output must be greater than zero"
        )
    if route_meta.min_output > route_meta.expected_output:
        raise RouteMetaValidationError(
            "route metadata min_output cannot exceed expected_output"
        )
    if route_meta.gas_estimate < 0:
        raise RouteMetaValidationError("route metadata gas_estimate cannot be negative")


def _legacy_validation_result(
    *,
    tool: str,
    valid: bool,
    required: bool,
    should_use_route_meta: bool,
    fallback_policy: FallbackPolicy | None = None,
    reason: str | None = None,
) -> RouteMetaValidationResult:
    policy = fallback_policy or FallbackPolicy()
    return RouteMetaValidationResult(
        tool=tool,
        valid=valid,
        required=required,
        should_use_route_meta=should_use_route_meta,
        allow_dynamic_fallback=policy.allow_fallback,
        reason=reason,
        fallback_policy=policy,
    )


def _legacy_swap_route_supported(route_meta: Dict[str, Any]) -> bool:
    if route_meta.get("calldata") and route_meta.get("to"):
        aggregator = str(route_meta.get("aggregator") or "").strip().lower()
        return aggregator in _TRUSTED_SWAP_CALLDATA_AGGREGATORS
    execution = route_meta.get("execution") or {}
    if not isinstance(execution, Mapping):
        return False
    protocol = str(execution.get("protocol") or "").strip().lower()
    if protocol == "v3":
        return bool(execution.get("path") and execution.get("fee_tiers"))
    if protocol == "v2":
        return bool(execution.get("path"))
    return False


def _legacy_solana_route_supported(route_meta: Dict[str, Any]) -> bool:
    return bool(route_meta.get("swap_transaction") or route_meta.get("calldata"))


def _legacy_bridge_route_supported(route_meta: Dict[str, Any]) -> bool:
    aggregator = str(route_meta.get("aggregator") or "").strip().lower()
    tool_data = route_meta.get("tool_data") or {}
    if not isinstance(tool_data, Mapping):
        tool_data = {}

    if aggregator == "lifi":
        tx_request = tool_data.get("transactionRequest")
        return isinstance(tx_request, Mapping) and bool(tx_request.get("data"))
    if aggregator == "mayan":
        return bool(tool_data.get("quoteId"))
    if aggregator in {"across", "relay"}:
        return isinstance(tool_data.get("planned_quote"), Mapping)
    return False


def _validate_route_meta_legacy(
    *,
    tool: str,
    resolved_args: Dict[str, Any],
    route_meta: Dict[str, Any] | None,
    strict_missing: bool,
) -> RouteMetaValidationResult:
    if not route_meta_required(tool):
        return _legacy_validation_result(
            tool=tool,
            valid=True,
            required=False,
            should_use_route_meta=False,
        )

    if not isinstance(route_meta, dict) or not route_meta:
        if strict_missing:
            return _legacy_validation_result(
                tool=tool,
                valid=False,
                required=True,
                should_use_route_meta=False,
                reason="missing route metadata from route_planner_node",
            )
        return _legacy_validation_result(
            tool=tool,
            valid=True,
            required=True,
            should_use_route_meta=False,
            reason="route metadata missing",
        )

    if route_meta.get("invalid") is True:
        return _legacy_validation_result(
            tool=tool,
            valid=False,
            required=True,
            should_use_route_meta=False,
            reason=str(
                route_meta.get("invalid_reason") or "route metadata marked invalid"
            ),
        )

    if not route_meta_matches_node(
        tool=tool,
        route_meta=route_meta,
        resolved_args=resolved_args,
    ):
        return _legacy_validation_result(
            tool=tool,
            valid=False,
            required=True,
            should_use_route_meta=True,
            reason="route metadata does not match the planned node arguments",
        )
    try:
        canonicalize_route_meta(route_meta, tool=tool)
    except RouteMetaValidationError as exc:
        return _legacy_validation_result(
            tool=tool,
            valid=False,
            required=True,
            should_use_route_meta=True,
            reason=str(exc),
        )

    return _legacy_validation_result(
        tool=tool,
        valid=True,
        required=True,
        should_use_route_meta=True,
        fallback_policy=coerce_fallback_policy(route_meta),
    )


@overload
def validate_route_meta(route_meta: RouteMeta) -> None: ...


@overload
def validate_route_meta(route_meta: Mapping[str, Any]) -> None: ...


@overload
def validate_route_meta(
    *,
    tool: str,
    resolved_args: Dict[str, Any],
    route_meta: Dict[str, Any] | None,
    strict_missing: bool,
) -> RouteMetaValidationResult: ...


def validate_route_meta(*args: Any, **kwargs: Any) -> RouteMetaValidationResult | None:
    if len(args) == 1 and not kwargs:
        if isinstance(args[0], RouteMeta):
            _validate_route_meta_contract(args[0])
            return None
        if isinstance(args[0], Mapping):
            canonicalize_route_meta(args[0])
            return None

    if args:
        raise TypeError(
            "validate_route_meta accepts either a RouteMeta or keyword args"
        )

    tool = kwargs.get("tool")
    resolved_args = kwargs.get("resolved_args")
    route_meta = kwargs.get("route_meta")
    strict_missing = kwargs.get("strict_missing")

    if not isinstance(tool, str):
        raise TypeError("validate_route_meta legacy mode requires 'tool'")
    if not isinstance(resolved_args, dict):
        raise TypeError("validate_route_meta legacy mode requires 'resolved_args'")
    if not isinstance(strict_missing, bool):
        raise TypeError("validate_route_meta legacy mode requires 'strict_missing'")
    if route_meta is not None and not isinstance(route_meta, dict):
        raise TypeError("validate_route_meta legacy mode requires dict route metadata")

    return _validate_route_meta_legacy(
        tool=tool,
        resolved_args=resolved_args,
        route_meta=route_meta,
        strict_missing=strict_missing,
    )


def infer_route_tool(route_meta: Mapping[str, Any]) -> str:
    if any(
        key in route_meta
        for key in (
            "source_chain_id",
            "dest_chain_id",
            "source_chain",
            "target_chain",
            "output_amount",
            "token_symbol",
        )
    ):
        return "bridge"
    if any(
        key in route_meta
        for key in (
            "input_mint",
            "output_mint",
            "swap_transaction",
            "network",
            "amount_out_lamports",
        )
    ):
        return "solana_swap"
    return "swap"


def coerce_fallback_policy(payload: Any) -> FallbackPolicy:
    if isinstance(payload, FallbackPolicy):
        return payload
    if not isinstance(payload, Mapping):
        return FallbackPolicy()

    nested = payload.get("fallback_policy")
    if isinstance(nested, Mapping):
        payload = nested

    allow = bool(payload.get("allow_fallback", False))
    raw_reason = payload.get("reason")
    if raw_reason is None:
        raw_reason = payload.get("fallback_reason")

    reason: FallbackReason | None = None
    if raw_reason is not None and raw_reason != "":
        try:
            reason = FallbackReason(str(raw_reason).strip().upper())
        except ValueError as exc:
            raise RouteMetaValidationError(
                f"unsupported fallback reason: {raw_reason}"
            ) from exc

    try:
        return FallbackPolicy(allow_fallback=allow, reason=reason)
    except ValueError as exc:
        raise RouteMetaValidationError(str(exc)) from exc


def canonicalize_route_meta(
    route_meta: Mapping[str, Any],
    *,
    tool: str | None = None,
) -> CanonicalRouteMeta:
    if not isinstance(route_meta, Mapping) or not route_meta:
        raise RouteMetaValidationError("missing route metadata from route_planner_node")
    if route_meta.get("invalid") is True:
        raise RouteMetaValidationError(
            str(route_meta.get("invalid_reason") or "route metadata marked invalid")
        )

    resolved_tool = str(tool or infer_route_tool(route_meta)).strip().lower()
    provider = str(
        route_meta.get("provider") or route_meta.get("aggregator") or ""
    ).strip()
    token_in = str(
        route_meta.get("token_in")
        or route_meta.get("input_mint")
        or route_meta.get("source_address")
        or route_meta.get("input_token")
        or route_meta.get("token_symbol")
        or ""
    ).strip()
    token_out = str(
        route_meta.get("token_out")
        or route_meta.get("output_mint")
        or route_meta.get("target_address")
        or route_meta.get("dest_token_address")
        or route_meta.get("output_token")
        or route_meta.get("token_symbol")
        or ""
    ).strip()
    amount_in = _coerce_decimal(
        route_meta.get("amount_in"),
        fallback=route_meta.get("input_amount"),
        second_fallback=route_meta.get("amount_in_lamports"),
    )
    expected_output = _coerce_decimal(
        route_meta.get("expected_output"),
        fallback=route_meta.get("amount_out"),
        second_fallback=route_meta.get("output_amount"),
        third_fallback=route_meta.get("amount_out_lamports"),
    )
    min_output = _coerce_decimal(
        route_meta.get("min_output"),
        fallback=route_meta.get("amount_out_min"),
        second_fallback=route_meta.get("amount_out_minimum"),
        third_fallback=expected_output,
    )
    gas_estimate = _coerce_int(route_meta.get("gas_estimate"))
    expiry_timestamp = _coerce_optional_int(route_meta.get("expiry_timestamp"))
    if expiry_timestamp is None:
        fetched_at = route_meta.get("fetched_at")
        if fetched_at not in (None, ""):
            try:
                expiry_timestamp = int(
                    float(str(fetched_at)) + _ROUTE_TTL_SECONDS.get(resolved_tool, 45)
                )
            except Exception:
                expiry_timestamp = None

    if resolved_tool == "swap" and not _legacy_swap_route_supported(dict(route_meta)):
        raise RouteMetaValidationError(
            "swap route metadata is missing executable calldata or route details"
        )
    if resolved_tool == "solana_swap" and not _legacy_solana_route_supported(
        dict(route_meta)
    ):
        raise RouteMetaValidationError(
            "Solana route metadata is missing the pre-built transaction"
        )
    if resolved_tool == "bridge" and not _legacy_bridge_route_supported(
        dict(route_meta)
    ):
        raise RouteMetaValidationError(
            "bridge route metadata is missing executable route details"
        )

    canonical = CanonicalRouteMeta(
        provider=provider,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        expected_output=expected_output,
        min_output=min_output,
        gas_estimate=gas_estimate,
        expiry_timestamp=expiry_timestamp,
        route_id=_coerce_optional_str(route_meta.get("route_id")),
        fallback_policy=coerce_fallback_policy(route_meta),
        raw=route_meta,
    )
    _validate_canonical_route_meta_contract(canonical)
    return canonical


def enforce_fallback_policy(
    *,
    policy: FallbackPolicy,
    route_meta: RouteMeta | CanonicalRouteMeta | None = None,
    detail: str | None = None,
) -> Dict[str, Any]:
    from core.utils.errors import DeterminismViolationError

    if policy.allow_fallback:
        assert policy.reason is not None
        payload = log_fallback_event(
            policy=policy, route_meta=route_meta, detail=detail
        )
        _LOGGER.warning("route_fallback %s", payload)
        return payload
    raise DeterminismViolationError(
        detail or "fallback attempted without an explicit fallback policy"
    )


def preflight_from_route_meta(
    tool: str, route_meta: Dict[str, Any] | None
) -> Dict[str, Any]:
    route_meta = route_meta if isinstance(route_meta, dict) else {}
    if not route_meta:
        return {}
    if tool == "swap":
        return {
            "protocol": route_meta.get("aggregator"),
            "amount_out": route_meta.get("amount_out"),
            "amount_out_min": route_meta.get("amount_out_min"),
            "gas_estimate": route_meta.get("gas_estimate"),
            "price_impact_pct": route_meta.get("price_impact_pct"),
            "fetched_at": route_meta.get("fetched_at"),
            "routed_by": "route_planner",
        }
    if tool == "solana_swap":
        return {
            "protocol": route_meta.get("aggregator"),
            "amount_out": route_meta.get("amount_out"),
            "amount_out_min": route_meta.get("amount_out_min"),
            "price_impact_pct": route_meta.get("price_impact_pct"),
            "gas_estimate": 0,
            "fetched_at": route_meta.get("fetched_at"),
            "network": route_meta.get("network"),
            "routed_by": "route_planner",
        }
    if tool == "bridge":
        return {
            "protocol": route_meta.get("aggregator"),
            "output_amount": route_meta.get("output_amount"),
            "total_fee_pct": route_meta.get("total_fee_pct"),
            "estimated_fill_time_seconds": route_meta.get("fill_time_seconds"),
            "source_chain": route_meta.get("source_chain"),
            "target_chain": route_meta.get("target_chain"),
            "fetched_at": route_meta.get("fetched_at"),
            "routed_by": "route_planner",
        }
    return {}


def log_execution_comparison(
    *,
    route_meta: RouteMeta | CanonicalRouteMeta,
    node_id: str,
    tool: str,
    actual_output: Decimal | None,
) -> Dict[str, Any]:
    payload = _base_log_payload(route_meta)
    payload.update(
        {
            "event": "route_execution_output",
            "node_id": node_id,
            "tool": tool,
            "expected_output": str(route_meta.expected_output),
            "min_output": str(route_meta.min_output),
            "actual_output": str(actual_output) if actual_output is not None else None,
        }
    )
    return payload


def _coerce_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(str(value))


def _coerce_int(
    value: Any,
    *,
    fallback: Any = None,
    second_fallback: Any = None,
    third_fallback: Any = None,
) -> int:
    for candidate in (value, fallback, second_fallback, third_fallback):
        if candidate is None or candidate == "":
            continue
        return int(str(candidate))
    return 0


def _coerce_decimal(
    value: Any,
    *,
    fallback: Any = None,
    second_fallback: Any = None,
    third_fallback: Any = None,
) -> Decimal:
    for candidate in (value, fallback, second_fallback, third_fallback):
        if candidate is None or candidate == "":
            continue
        try:
            return Decimal(str(candidate))
        except (InvalidOperation, ValueError) as exc:
            raise RouteMetaValidationError(
                f"route metadata numeric value is invalid: {candidate}"
            ) from exc
    return Decimal("0")


def _route_amount_matches(
    *,
    expected_amount: Any,
    candidates: tuple[Any, ...],
) -> bool:
    if expected_amount in (None, ""):
        return True
    try:
        expected_decimal = Decimal(str(expected_amount))
    except (InvalidOperation, ValueError):
        return False

    for candidate in candidates:
        if candidate in (None, ""):
            continue
        try:
            if Decimal(str(candidate)) != expected_decimal:
                return False
        except (InvalidOperation, ValueError):
            return False
    return True


def _coerce_calldata(value: Any) -> bytes | None:
    if value is None or value == "":
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        encoded = value.strip()
        if not encoded:
            return None
        if encoded.startswith("0x"):
            raw_hex = encoded[2:]
            if len(raw_hex) % 2:
                raise RouteMetaValidationError(
                    "route metadata calldata must be even-length hex"
                )
            try:
                return bytes.fromhex(raw_hex)
            except ValueError as exc:
                raise RouteMetaValidationError(
                    "route metadata calldata must be valid hex"
                ) from exc
    raise RouteMetaValidationError(
        "route metadata calldata must be bytes or 0x-prefixed hex"
    )


def _coerce_step_tuple(
    value: Any, *, fallback: Any = None
) -> tuple[Dict[str, Any], ...]:
    candidate = value if value not in (None, "") else fallback
    if candidate is None:
        return ()
    if isinstance(candidate, Sequence) and not isinstance(
        candidate, (str, bytes, bytearray)
    ):
        steps: list[Dict[str, Any]] = []
        for index, item in enumerate(candidate):
            if isinstance(item, Mapping):
                steps.append(dict(item))
            else:
                steps.append({"index": index, "value": item})
        return tuple(steps)
    if isinstance(candidate, Mapping):
        return (dict(candidate),)
    return ()


def _coerce_route_path_tuple(
    value: Any,
    *,
    fallback: Any = None,
    second_fallback: Any = None,
) -> tuple[Any, ...]:
    for candidate in (value, fallback, second_fallback):
        if candidate is None or candidate == "":
            continue
        if isinstance(candidate, Sequence) and not isinstance(
            candidate,
            (str, bytes, bytearray),
        ):
            return tuple(candidate)
        return (candidate,)
    return ()


def _infer_chain_type(payload: Mapping[str, Any]) -> str | None:
    explicit = payload.get("chain_type")
    if explicit:
        return str(explicit).strip().lower()

    network = str(payload.get("network") or "").strip().lower()
    if network and is_solana_network(network):
        return "solana"
    if any(
        key in payload
        for key in (
            "instruction_set",
            "instructions",
            "swap_transaction",
            "input_mint",
            "output_mint",
        )
    ):
        return "solana"
    if any(
        key in payload
        for key in ("source_chain_id", "dest_chain_id", "source_chain", "target_chain")
    ):
        return "bridge"
    if any(key in payload for key in ("chain_id", "execution", "calldata", "to")):
        return "evm"
    return None


def _normalized_chain_type(route_meta: RouteMeta) -> str:
    if route_meta.chain_type:
        return route_meta.chain_type.strip().lower()
    if route_meta.instruction_set:
        return "solana"
    if route_meta.route_path:
        return "bridge"
    if route_meta.calldata or route_meta.to:
        return "evm"
    return "generic"


def _base_log_payload(
    route_meta: RouteMeta | CanonicalRouteMeta | None,
) -> Dict[str, Any]:
    if route_meta is None:
        return {
            "provider": None,
            "route_id": None,
            "token_in": None,
            "token_out": None,
            "amount_in": None,
            "expected_output": None,
            "min_output": None,
        }
    return {
        "provider": route_meta.provider,
        "route_id": route_meta.route_id,
        "token_in": route_meta.token_in,
        "token_out": route_meta.token_out,
        "amount_in": str(route_meta.amount_in),
        "expected_output": str(route_meta.expected_output),
        "min_output": str(route_meta.min_output),
    }
