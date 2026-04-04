from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict

from langchain_core.messages import AIMessage

from core.utils.user_feedback import insufficient_balance


@dataclass(frozen=True)
class BridgePreflightDeps:
    cached_bridge_quote: Callable[[dict[str, Any]], dict[str, Any] | None]
    simulate_bridge_preview: Callable[
        ..., Awaitable[tuple[dict[str, Any] | None, str | None]]
    ]
    suggest_bridge_options: Callable[..., Awaitable[tuple[list[str], list[str]]]]
    bridge_not_supported: Callable[..., Any]
    is_bridge_unsupported_error: Callable[[str], bool]
    get_chain_by_name: Callable[[str], Any]
    is_native: Callable[[str, Any], bool]
    normalize_token_key: Callable[[str, Any], str]
    balance_key: Callable[[str, str, str], str]


@dataclass(frozen=True)
class NativeReserveResult:
    gas_cost: Decimal
    fee_amount: Decimal
    total_required: Decimal
    reserve_amount: Decimal
    gas_estimate_units: str
    gas_label: str


@dataclass(frozen=True)
class BalanceFailureResult:
    route_decision: str
    reasoning_logs: list[str]
    balance_snapshot: dict[str, str]
    native_requirements: dict[str, str]
    messages: list[AIMessage]


async def handle_swap_like_preflight(
    *,
    node_id: str,
    node_tool: str,
    sender: str,
    args: Dict[str, Any],
    chain_ctx: Any,
    cached_preflight: dict[str, Any],
    cached_age_seconds: float,
    use_cached_route: bool,
    preflight: dict[str, Any],
    projected_deltas: dict[str, Decimal],
    preflight_estimate_service: Any,
    simulate_swap_preview: Callable[
        ..., Awaitable[tuple[dict[str, Any] | None, str | None]]
    ],
    normalize_token_key: Callable[[str, Any], str],
    balance_key: Callable[[str, str, str], str],
    get_chain_by_name: Callable[[str], Any],
    logs: list[str],
) -> None:
    cached_swap_quote = preflight_estimate_service.cached_swap_quote(cached_preflight)
    if node_tool == "solana_swap":
        if cached_swap_quote:
            freshness = "fresh" if use_cached_route else "stale"
            logs.append(
                f"[BALANCE_CHECK] {node_id}: consuming {freshness} Solana route "
                f"quote (aggregator={cached_preflight.get('protocol')}, "
                f"out={cached_preflight.get('amount_out')}, age={cached_age_seconds:.0f}s)"
            )
            preflight["swap_quote"] = {
                **cached_swap_quote,
                "gas_estimate": 0,
            }
            _project_swap_output(
                sender=sender,
                chain_ctx=chain_ctx,
                amount_out=_to_decimal(cached_preflight.get("amount_out")),
                token_out_ref=args.get("token_out_mint"),
                projected_deltas=projected_deltas,
                normalize_token_key=normalize_token_key,
                balance_key=balance_key,
            )
        else:
            logs.append(
                f"[BALANCE_CHECK] {node_id}: no Solana route quote cached; "
                "skipping output projection."
            )
        return

    if use_cached_route and cached_swap_quote:
        logs.append(
            f"[BALANCE_CHECK] {node_id}: consuming route_planner cached swap "
            f"quote (aggregator={cached_preflight.get('protocol')}, "
            f"out={cached_preflight.get('amount_out')}, age={cached_age_seconds:.0f}s)"
        )
        preflight["swap_quote"] = cached_swap_quote
        _project_swap_output(
            sender=sender,
            chain_ctx=chain_ctx,
            amount_out=_to_decimal(cached_preflight.get("amount_out")),
            token_out_ref=args.get("token_out_address"),
            projected_deltas=projected_deltas,
            normalize_token_key=normalize_token_key,
            balance_key=balance_key,
        )
        return

    if (
        use_cached_route is False
        and cached_preflight.get("routed_by") == "route_planner"
    ):
        logs.append(
            f"[BALANCE_CHECK] {node_id}: route_planner cache stale "
            f"({cached_age_seconds:.0f}s) — re-simulating"
        )

    chain = get_chain_by_name(str(args.get("chain") or args.get("network") or ""))
    quote, err = await simulate_swap_preview(
        args=args,
        chain_name=str(args.get("chain") or args.get("network") or ""),
        chain_has_v3=bool(
            getattr(chain, "v3_router", None) and getattr(chain, "v3_quoter", None)
        ),
        chain_has_v2=bool(
            getattr(chain, "v2_router", None) and getattr(chain, "v2_factory", None)
        ),
    )
    if quote:
        preflight["swap_quote"] = quote
        _project_swap_output(
            sender=sender,
            chain_ctx=chain_ctx,
            amount_out=_to_decimal(quote.get("amount_out")),
            token_out_ref=args.get("token_out_address"),
            projected_deltas=projected_deltas,
            normalize_token_key=normalize_token_key,
            balance_key=balance_key,
        )
    elif err:
        preflight["swap_quote_error"] = err


async def handle_bridge_preflight(
    *,
    node_id: str,
    sender: str,
    args: Dict[str, Any],
    cached_preflight: dict[str, Any],
    cached_age_seconds: float,
    use_cached_route: bool,
    preflight: dict[str, Any],
    projected_deltas: dict[str, Decimal],
    logs: list[str],
    deps: BridgePreflightDeps,
) -> BalanceFailureResult | None:
    cached_bridge_quote = deps.cached_bridge_quote(cached_preflight)
    if use_cached_route and cached_bridge_quote:
        logs.append(
            f"[BALANCE_CHECK] {node_id}: consuming route_planner cached bridge "
            f"quote (aggregator={cached_preflight.get('protocol')}, "
            f"out={cached_preflight.get('output_amount')}, "
            f"fee={cached_preflight.get('total_fee_pct')}%, age={cached_age_seconds:.0f}s)"
        )
        preflight["bridge_quote"] = cached_bridge_quote
        _project_bridge_output(
            sender=sender,
            target_chain_name=str(
                cached_preflight.get("target_chain") or args.get("target_chain") or ""
            ),
            target_address=args.get("target_address"),
            recipient=args.get("recipient") or sender,
            output_amount=_to_decimal(cached_preflight.get("output_amount")),
            projected_deltas=projected_deltas,
            deps=deps,
        )
        return None

    if (
        use_cached_route is False
        and cached_preflight.get("routed_by") == "route_planner"
    ):
        logs.append(
            f"[BALANCE_CHECK] {node_id}: route_planner bridge cache stale "
            f"({cached_age_seconds:.0f}s) — re-simulating"
        )

    source_cfg = None
    target_cfg = None
    try:
        source_cfg = deps.get_chain_by_name(
            str(args.get("source_chain") or args.get("chain") or "")
        )
        target_cfg = deps.get_chain_by_name(str(args.get("target_chain") or ""))
    except Exception:
        source_cfg = None
        target_cfg = None

    quote, err = (None, None)
    if not use_cached_route:
        quote, err = await deps.simulate_bridge_preview(args=args)

    if quote:
        preflight["bridge_quote"] = quote
        _project_bridge_output(
            sender=sender,
            target_chain_name=str(
                quote.get("target_chain") or args.get("target_chain") or ""
            ),
            target_address=args.get("target_address"),
            recipient=args.get("recipient") or sender,
            output_amount=_to_decimal(quote.get("output_amount")),
            projected_deltas=projected_deltas,
            deps=deps,
        )
        return None

    if err:
        preflight["bridge_quote_error"] = err
        if deps.is_bridge_unsupported_error(err):
            chain_pairs: list[str] = []
            token_suggestions: list[str] = []
            if source_cfg and target_cfg:
                chain_pairs, token_suggestions = await deps.suggest_bridge_options(
                    token_symbol=str(args.get("token_symbol") or ""),
                    source_chain=source_cfg,
                    target_chain=target_cfg,
                )
            feedback = deps.bridge_not_supported(
                str(args.get("token_symbol") or ""),
                str(args.get("source_chain") or ""),
                str(args.get("target_chain") or ""),
                chain_pairs=chain_pairs,
                tokens=token_suggestions,
            )
            return BalanceFailureResult(
                route_decision="end",
                reasoning_logs=list(logs),
                balance_snapshot={},
                native_requirements={},
                messages=[AIMessage(content=feedback.render())],
            )
    return None


def evaluate_native_reserve(
    *,
    node_tool: str,
    gas_cost: Decimal,
    fee_amount: Decimal,
    native_balance: Decimal,
    native_symbol: str,
    chain_label: str,
    sender: str,
    token_address: str | None,
    raw_amount_dec: Decimal | None,
    native_amount_matches_token: bool,
    transfer_without_token_address: bool,
    failure_label: str,
) -> tuple[NativeReserveResult, dict[str, Any] | None]:
    include_fee = node_tool not in {"transfer", "unwrap"}
    gas_label = failure_label
    native_reserved = Decimal("0")
    if raw_amount_dec is not None and raw_amount_dec > 0:
        if native_amount_matches_token or transfer_without_token_address:
            native_reserved = raw_amount_dec

    total_required = gas_cost + fee_amount + native_reserved
    failure = None
    if native_balance < total_required:
        failure = {
            "kind": "gas",
            "symbol": native_symbol,
            "required": total_required,
            "available": native_balance,
            "shortfall": total_required - native_balance,
            "chain": chain_label,
            "sender": sender,
            "label": failure_label if include_fee else "network fee",
        }

    reserve_amount = gas_cost + fee_amount
    token_reservation_expected = (
        token_address is not None and raw_amount_dec is not None and raw_amount_dec > 0
    )
    if native_reserved > 0 and not token_reservation_expected:
        reserve_amount += native_reserved

    return (
        NativeReserveResult(
            gas_cost=gas_cost,
            fee_amount=fee_amount,
            total_required=total_required,
            reserve_amount=reserve_amount,
            gas_estimate_units="",
            gas_label=gas_label,
        ),
        failure,
    )


def build_balance_failure_result(
    failures: list[dict[str, Any]],
    *,
    logs: list[str],
    balance_snapshot: dict[str, str],
    native_requirements: dict[str, str],
) -> BalanceFailureResult:
    first_sender = None
    for entry in failures:
        if entry.get("sender"):
            first_sender = entry["sender"]
            break
    feedback = insufficient_balance(failures, sender_address=first_sender)
    return BalanceFailureResult(
        route_decision="end",
        reasoning_logs=list(logs),
        balance_snapshot=dict(balance_snapshot),
        native_requirements=dict(native_requirements),
        messages=[AIMessage(content=feedback.render())],
    )


def _project_swap_output(
    *,
    sender: str,
    chain_ctx: Any,
    amount_out: Decimal | None,
    token_out_ref: Any,
    projected_deltas: dict[str, Decimal],
    normalize_token_key: Callable[[str, Any], str],
    balance_key: Callable[[str, str, str], str],
) -> None:
    if amount_out is None or not token_out_ref:
        return
    key = balance_key(
        sender,
        chain_ctx.cache_name,
        normalize_token_key(str(token_out_ref), chain_ctx),
    )
    projected_deltas[key] = projected_deltas.get(key, Decimal("0")) + amount_out


def _project_bridge_output(
    *,
    sender: str,
    target_chain_name: str,
    target_address: Any,
    recipient: Any,
    output_amount: Decimal | None,
    projected_deltas: dict[str, Decimal],
    deps: BridgePreflightDeps,
) -> None:
    if output_amount is None or not target_chain_name or not target_address:
        return
    try:
        target_cfg = deps.get_chain_by_name(target_chain_name)
    except Exception:
        return
    if deps.is_native(str(target_address), target_cfg):
        token_key = "0x0000000000000000000000000000000000000000"
    else:
        token_key = str(target_address).strip().lower()
    key = deps.balance_key(
        str(recipient or sender).strip().lower(),
        target_chain_name.strip().lower(),
        token_key,
    )
    projected_deltas[key] = projected_deltas.get(key, Decimal("0")) + output_amount


def _to_decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except Exception:
        return None
