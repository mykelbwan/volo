from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Dict

from config.chains import get_chain_by_name
from core.utils.errors import NonRetryableError, categorize_error
from tool_nodes.common.input_utils import (
    format_with_recovery,
    parse_decimal_field,
    parse_float_field,
    require_fields,
)
from wallet_service.common.transfer_idempotency import (
    canonicalize_decimal_idempotency_value,
    claim_transfer_idempotency,
    load_transfer_idempotency_claim,
    mark_transfer_failed,
    mark_transfer_inflight,
    mark_transfer_success,
    resolve_transfer_idempotency,
    resume_transfer_idempotency_claim,
)
from wallet_service.evm.gas_price import gas_price_cache

_EXECUTE_SWAP_FN: Any | None = None
_EXECUTE_PRECOMPUTED_SWAP_ROUTE_FN: Any | None = None
_SIMULATE_SWAP_V3_FN: Any | None = None
_SIMULATE_SWAP_V2_FN: Any | None = None
_SIMULATION_ERROR_V3_CLS: Any | None = None
_SIMULATION_ERROR_V2_CLS: Any | None = None
# Keep 1inch disabled until API/KYC is ready.
_TRUSTED_PRECOMPUTED_SWAP_AGGREGATORS = frozenset({"0x", "paraswap"})


async def execute_swap(*args: Any, **kwargs: Any) -> Any:
    global _EXECUTE_SWAP_FN
    if _EXECUTE_SWAP_FN is None:
        from tool_nodes.dex.swap_executor import execute_swap as _execute_swap_impl

        _EXECUTE_SWAP_FN = _execute_swap_impl
    return await _EXECUTE_SWAP_FN(*args, **kwargs)


async def execute_precomputed_swap_route(*args: Any, **kwargs: Any) -> Any:
    global _EXECUTE_PRECOMPUTED_SWAP_ROUTE_FN
    if _EXECUTE_PRECOMPUTED_SWAP_ROUTE_FN is None:
        from tool_nodes.dex.swap_executor import (
            execute_precomputed_swap_route as _execute_precomputed_swap_route_impl,
        )

        _EXECUTE_PRECOMPUTED_SWAP_ROUTE_FN = _execute_precomputed_swap_route_impl
    return await _EXECUTE_PRECOMPUTED_SWAP_ROUTE_FN(*args, **kwargs)


async def simulate_swap(*args: Any, **kwargs: Any) -> Any:
    global _SIMULATE_SWAP_V3_FN
    if _SIMULATE_SWAP_V3_FN is None:
        from tool_nodes.dex.swap_simulator_v3 import (
            simulate_swap as _simulate_swap_v3_impl,
        )

        _SIMULATE_SWAP_V3_FN = _simulate_swap_v3_impl
    return await _SIMULATE_SWAP_V3_FN(*args, **kwargs)


async def simulate_swap_v2(*args: Any, **kwargs: Any) -> Any:
    global _SIMULATE_SWAP_V2_FN
    if _SIMULATE_SWAP_V2_FN is None:
        from tool_nodes.dex.swap_simulator_v2 import (
            simulate_swap_v2 as _simulate_swap_v2_impl,
        )

        _SIMULATE_SWAP_V2_FN = _simulate_swap_v2_impl
    return await _SIMULATE_SWAP_V2_FN(*args, **kwargs)


def _is_v3_simulation_error(value: Any) -> bool:
    global _SIMULATION_ERROR_V3_CLS
    if _SIMULATION_ERROR_V3_CLS is None:
        from tool_nodes.dex.swap_simulator_v3 import SimulationError as _SimulationError

        _SIMULATION_ERROR_V3_CLS = _SimulationError
    return isinstance(value, _SIMULATION_ERROR_V3_CLS)


def _is_v2_simulation_error(value: Any) -> bool:
    global _SIMULATION_ERROR_V2_CLS
    if _SIMULATION_ERROR_V2_CLS is None:
        from tool_nodes.dex.swap_simulator_v2 import (
            SimulationErrorV2 as _SimulationErrorV2,
        )

        _SIMULATION_ERROR_V2_CLS = _SimulationErrorV2
    return isinstance(value, _SIMULATION_ERROR_V2_CLS)


def _route_meta_aggregator(route_meta: Any) -> str:
    if not isinstance(route_meta, dict):
        return ""
    return str(route_meta.get("aggregator") or "").strip().lower()


def _is_trusted_precomputed_swap_route(route_meta: Any) -> bool:
    if not isinstance(route_meta, dict):
        return False
    aggregator = _route_meta_aggregator(route_meta)
    if aggregator not in _TRUSTED_PRECOMPUTED_SWAP_AGGREGATORS:
        return False
    calldata = str(route_meta.get("calldata") or "").strip()
    to_address = str(route_meta.get("to") or "").strip()
    return bool(calldata and calldata.startswith("0x") and to_address)


def _route_meta_amount_out(route_meta: Dict[str, Any]) -> Decimal:
    for key in ("amount_out", "expected_output", "min_output", "amount_out_min"):
        raw = route_meta.get(key)
        if raw is None:
            continue
        try:
            return Decimal(str(raw))
        except Exception:
            continue
    return Decimal("0")


def _route_meta_amount_out_min(route_meta: Dict[str, Any]) -> Decimal:
    for key in ("amount_out_min", "min_output", "expected_output", "amount_out"):
        raw = route_meta.get(key)
        if raw is None:
            continue
        try:
            return Decimal(str(raw))
        except Exception:
            continue
    return Decimal("0")


def _route_meta_path(route_meta: Dict[str, Any], token_in: str, token_out: str) -> list[str]:
    raw_path = route_meta.get("path")
    if isinstance(raw_path, list):
        path = [str(item).strip() for item in raw_path if str(item).strip()]
        if path:
            return path
    return [token_in, token_out]


def _route_meta_route_label(route_meta: Dict[str, Any]) -> str:
    route = str(route_meta.get("route") or "").strip()
    return route or "aggregated"


def _quote_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _internal_quote_sort_key(candidate: tuple[str, Any]) -> tuple[Decimal, Decimal, str]:
    protocol, quote = candidate
    amount_out = _quote_decimal(getattr(quote, "amount_out", Decimal("0")))
    amount_out_min = _quote_decimal(
        getattr(quote, "amount_out_minimum", amount_out), amount_out
    )
    # Best output first, then best minimum output, then deterministic protocol key.
    return (-amount_out, -amount_out_min, protocol)


def _route_meta_contains_untrusted_swap_data(route_meta: Any) -> bool:
    if not isinstance(route_meta, dict):
        return False
    if route_meta.get("swap_transaction"):
        return True
    if route_meta.get("calldata") and not _is_trusted_precomputed_swap_route(route_meta):
        return True
    return False


def _validate_non_executable_route_meta(
    *,
    route_meta: Any,
    token_in: str,
    token_out: str,
    chain_name: str,
    amount_in: Decimal,
) -> None:
    if not isinstance(route_meta, dict) or not route_meta:
        return
    if _route_meta_contains_untrusted_swap_data(route_meta):
        raise NonRetryableError("Untrusted precomputed transaction data is not allowed")
    if route_meta.get("calldata"):
        aggregator = _route_meta_aggregator(route_meta)
        if aggregator not in _TRUSTED_PRECOMPUTED_SWAP_AGGREGATORS:
            raise NonRetryableError(
                format_with_recovery(
                    f"Planned {aggregator or 'unknown'} calldata routes are not allowed",
                    "request a fresh route and retry",
                )
            )
        if not str(route_meta.get("to") or "").strip():
            raise NonRetryableError(
                format_with_recovery(
                    "The planned swap route is missing a destination contract",
                    "request a fresh route and retry",
                )
            )
    if str(route_meta.get("token_in") or "").strip().lower() not in {
        "",
        token_in.lower(),
    }:
        raise NonRetryableError(
            format_with_recovery(
                "The planned swap metadata does not match the requested input token",
                "request a fresh route and retry",
            )
        )
    if str(route_meta.get("token_out") or "").strip().lower() not in {
        "",
        token_out.lower(),
    }:
        raise NonRetryableError(
            format_with_recovery(
                "The planned swap metadata does not match the requested output token",
                "request a fresh route and retry",
            )
        )
    route_chain_id = route_meta.get("chain_id")
    if route_chain_id is not None:
        expected_chain = get_chain_by_name(chain_name)
        if int(route_chain_id) != int(expected_chain.chain_id):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned swap metadata does not match the requested chain",
                    "request a fresh route and retry",
                )
            )
    route_amount = route_meta.get("amount_in")
    if route_amount is not None and Decimal(str(route_amount)) != amount_in:
        raise NonRetryableError(
            format_with_recovery(
                "The planned swap metadata does not match the requested amount",
                "request a fresh route and retry",
            )
        )


def _extract_execution_state(result: Any) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    execution_state = result.get("execution_state")
    return execution_state if isinstance(execution_state, dict) else {}


def _has_incomplete_execution_state(result: Any) -> bool:
    execution_state = _extract_execution_state(result)
    if not execution_state:
        return False
    return (
        str(execution_state.get("completion_status") or "").strip().lower()
        != "completed"
    )


def _latest_step_tx_hash(execution_state: Dict[str, Any]) -> str | None:
    steps = execution_state.get("steps")
    if not isinstance(steps, dict):
        return None
    for step_name in ("unwrap", "swap", "approve", "wrap"):
        step = steps.get(step_name)
        if isinstance(step, dict):
            tx_hash = str(step.get("tx_hash") or "").strip()
            if tx_hash:
                return tx_hash
    return None


async def swap_token(parameters: Dict[str, Any]) -> Dict[str, Any]:
    token_in = parameters.get("token_in_address")
    token_out = parameters.get("token_out_address")
    amount_in_raw = parameters.get("amount_in")
    chain_name = parameters.get("chain")
    sub_org_id = parameters.get("sub_org_id")
    sender = parameters.get("sender")

    require_fields(
        parameters,
        [
            "token_in_address",
            "token_out_address",
            "amount_in",
            "chain",
            "sub_org_id",
            "sender",
        ],
        context="swap",
    )

    # After the missing-params guard above, all of these are guaranteed non-None.
    # Cast explicitly so the type checker can track them as str/float from here on.
    token_in = str(token_in).strip()
    token_out = str(token_out).strip()
    amount_in_decimal = parse_decimal_field(
        amount_in_raw,
        field="amount_in",
        positive=True,
        invalid_recovery=(
            "use a positive numeric amount (for example, 1.25) and retry"
        ),
    )
    chain_name = str(chain_name).strip()
    sub_org_id = str(sub_org_id).strip()
    sender = str(sender).strip()
    slippage = parse_float_field(
        parameters.get("slippage", 0.5),
        field="slippage",
        min_value=0.01,
        max_value=50.0,
        invalid_recovery="use a slippage value between 0.01 and 50",
    )

    # ── 2. Resolve chain and gas price ────────────────────────────────────────
    chain = get_chain_by_name(chain_name)
    idempotency_fields = {
        "sender": sender.strip().lower(),
        "chain_id": int(chain.chain_id),
        "tool_name": "dex_swap",
        "token_in": token_in.strip().lower(),
        "token_out": token_out.strip().lower(),
        "amount_in": canonicalize_decimal_idempotency_value(amount_in_decimal),
    }
    idempotency_key, idempotency_fields, request_id = resolve_transfer_idempotency(
        tool_name="dex_swap",
        request_fields=idempotency_fields,
        external_key=parameters.get("idempotency_key"),
        request_id=parameters.get("request_id"),
    )
    claim = await claim_transfer_idempotency(
        operation="dex_swap",
        idempotency_key=idempotency_key,
        request_fields=idempotency_fields,
    )
    current_claim = await load_transfer_idempotency_claim(claim)
    current_result = dict((current_claim.result if current_claim else None) or {})
    should_resume = _has_incomplete_execution_state(current_result)
    active_claim = (
        resume_transfer_idempotency_claim(current_claim or claim)
        if should_resume
        else claim
    )
    if claim is not None and claim.reused:
        if claim.result and not should_resume:
            return dict(claim.result)
        if claim.tx_hash:
            if should_resume:
                pass
            else:
                return {
                    "status": "success" if claim.status == "success" else "pending",
                    "tx_hash": claim.tx_hash,
                    "message": f"Swap already submitted. Transaction ID: {claim.tx_hash}.",
                }
    if (
        current_claim is not None
        and current_claim.tx_hash
        and current_claim.result
        and not should_resume
    ):
        return dict(current_claim.result)

    route_meta = parameters.get("_route_meta") or {}
    _validate_non_executable_route_meta(
        route_meta=route_meta,
        token_in=token_in,
        token_out=token_out,
        chain_name=chain_name,
        amount_in=amount_in_decimal,
    )
    route_meta_used = bool(isinstance(route_meta, dict) and route_meta)
    route_meta_aggregator = _route_meta_aggregator(route_meta)
    has_trusted_precomputed_route = _is_trusted_precomputed_swap_route(route_meta)

    try:
        # ── 3. Execute trusted precomputed route first (if available) ────────────
        quote = None
        remaining_candidates: list[tuple[str, Any]] = []
        simulation_errors: list[str] = []
        used_protocol = ""
        fallback_reason: str | None = None

        if has_trusted_precomputed_route:
            precomputed_path = _route_meta_path(route_meta, token_in, token_out)
            precomputed_route = _route_meta_route_label(route_meta)
            precomputed_amount_out = _route_meta_amount_out(route_meta)
            precomputed_amount_out_min = _route_meta_amount_out_min(route_meta)

            async def _persist_precomputed_broadcast(result: Any) -> None:
                await mark_transfer_inflight(
                    active_claim,
                    tx_hash=result.tx_hash,
                    result={
                        "status": "pending",
                        "tx_hash": result.tx_hash,
                        "approve_hash": result.approve_hash,
                        "protocol": result.protocol,
                        "route": precomputed_route,
                        "path": precomputed_path,
                        "amount_in": str(result.amount_in),
                        "amount_out": str(precomputed_amount_out),
                        "amount_out_minimum": str(precomputed_amount_out_min),
                        "chain": result.chain_name,
                        "fallback_used": False,
                        "fallback_reason": None,
                        "route_meta_used": route_meta_used,
                        "route_meta_source": route_meta_aggregator,
                        "request_id": request_id,
                        "execution_state": {"completion_status": "pending"},
                        "message": f"Swap submitted. Transaction ID: {result.tx_hash}.",
                    },
                )

            gas_price_gwei = float(await gas_price_cache.get_gwei(chain_id=chain.chain_id))
            try:
                precomputed_result = await execute_precomputed_swap_route(
                    route_meta=route_meta,
                    amount_in=amount_in_decimal,
                    chain_name=chain_name,
                    sub_org_id=sub_org_id,
                    sender=sender,
                    gas_price_gwei=gas_price_gwei,
                    persist_broadcast=_persist_precomputed_broadcast,
                )
                token_in_label = parameters.get("token_in_symbol") or "token"
                token_out_label = parameters.get("token_out_symbol") or "token"
                message = (
                    f"Swap submitted: {precomputed_result.amount_in} "
                    f"{token_in_label} to {token_out_label}."
                )
                if precomputed_result.tx_hash:
                    message += f" Transaction ID: {precomputed_result.tx_hash}."

                response = {
                    "status": "success",
                    "tx_hash": precomputed_result.tx_hash,
                    "approve_hash": precomputed_result.approve_hash,
                    "protocol": precomputed_result.protocol,
                    "route": precomputed_route,
                    "path": precomputed_path,
                    "amount_in": str(precomputed_result.amount_in),
                    "amount_out": str(precomputed_amount_out),
                    "amount_out_minimum": str(
                        precomputed_amount_out_min
                        if precomputed_amount_out_min > 0
                        else precomputed_result.amount_out_minimum
                    ),
                    "chain": precomputed_result.chain_name,
                    "fallback_used": False,
                    "fallback_reason": None,
                    "route_meta_used": route_meta_used,
                    "route_meta_source": route_meta_aggregator,
                    "request_id": request_id,
                    "execution_state": {"completion_status": "completed"},
                    "message": message,
                }
                await mark_transfer_success(
                    active_claim,
                    tx_hash=precomputed_result.tx_hash,
                    result=response,
                )
                return response
            except Exception as exc:
                fallback_reason = (
                    f"{route_meta_aggregator or 'precomputed'} execution failed "
                    f"({categorize_error(exc).value}): {exc}"
                )

        # ── 4. Simulate internal routes — V3 first, then V2 fallback ────────────

        internal_simulation_tasks: list[tuple[str, asyncio.Task[Any]]] = []
        if chain.v3_quoter and chain.v3_router:
            internal_simulation_tasks.append(
                (
                    "v3",
                    asyncio.create_task(
                        simulate_swap(
                            token_in=token_in,
                            token_out=token_out,
                            amount_in=amount_in_decimal,
                            sender=sender,
                            slippage_pct=slippage,
                            chain_name=chain_name,
                        )
                    ),
                )
            )
        if chain.v2_router and chain.v2_factory:
            internal_simulation_tasks.append(
                (
                    "v2",
                    asyncio.create_task(
                        simulate_swap_v2(
                            token_in=token_in,
                            token_out=token_out,
                            amount_in=amount_in_decimal,
                            sender=sender,
                            slippage_pct=slippage,
                            chain_name=chain_name,
                        )
                    ),
                )
            )

        if not internal_simulation_tasks:
            raise RuntimeError(
                format_with_recovery(
                    f"No internal swap simulator is configured for {chain.name}.",
                    "use a chain with swap support and retry",
                )
            )

        internal_results = await asyncio.gather(
            *[task for _, task in internal_simulation_tasks], return_exceptions=True
        )
        candidate_quotes: list[tuple[str, Any]] = []
        for (protocol, _task), sim_result in zip(internal_simulation_tasks, internal_results):
            if isinstance(sim_result, BaseException):
                simulation_errors.append(
                    f"{protocol.upper()}: [UNEXPECTED_ERROR] {sim_result}"
                )
                continue
            if protocol == "v3" and _is_v3_simulation_error(sim_result):
                simulation_errors.append(
                    f"V3: [{sim_result.reason}] {sim_result.message}"
                )
                continue
            if protocol == "v2" and _is_v2_simulation_error(sim_result):
                simulation_errors.append(
                    f"V2: [{sim_result.reason}] {sim_result.message}"
                )
                continue
            candidate_quotes.append((protocol, sim_result))

        if candidate_quotes:
            candidate_quotes.sort(key=_internal_quote_sort_key)
            used_protocol, quote = candidate_quotes[0]
            remaining_candidates = candidate_quotes[1:]

        if quote is None:
            error_summary = (
                "; ".join(simulation_errors[:6])
                if simulation_errors
                else "no quotes returned"
            )
            raise RuntimeError(
                format_with_recovery(
                    (
                        f"Swap simulation failed on all protocols for "
                        f"{token_in} -> {token_out} on {chain.name}. "
                        f"Errors: {error_summary}"
                    ),
                    "retry in a moment, then try a smaller amount or higher slippage",
                )
            )

        gas_price_gwei = float(await gas_price_cache.get_gwei(chain_id=chain.chain_id))

        if isinstance(quote, BaseException):
            raise RuntimeError(
                format_with_recovery(
                    "Swap simulation produced a non-quote error object",
                    "retry in a moment",
                )
            )

        quote_used: Any = quote
        recovered_execution_state = _extract_execution_state(current_result)
        if recovered_execution_state and current_claim is not None:
            legacy_claim_tx_hash = str(current_claim.tx_hash or "").strip() or None
            if legacy_claim_tx_hash:
                metadata = recovered_execution_state.setdefault("metadata", {})
                if isinstance(metadata, dict):
                    metadata.setdefault("legacy_claim_tx_hash", legacy_claim_tx_hash)

        async def _persist_quote_broadcast(result: Any) -> None:
            route_name = getattr(quote_used, "route", "direct")
            path_value = getattr(quote_used, "path", [token_in, token_out])
            amount_out = getattr(quote_used, "amount_out", result.amount_out_minimum)
            await mark_transfer_inflight(
                active_claim,
                tx_hash=result.tx_hash,
                result={
                    "status": "pending",
                    "tx_hash": result.tx_hash,
                    "approve_hash": result.approve_hash,
                    "protocol": result.protocol,
                    "route": route_name,
                    "path": path_value,
                    "amount_in": str(result.amount_in),
                    "amount_out": str(amount_out),
                    "amount_out_minimum": str(result.amount_out_minimum),
                    "chain": result.chain_name,
                    "fallback_used": bool(fallback_reason),
                    "fallback_reason": fallback_reason,
                    "route_meta_used": route_meta_used,
                    "route_meta_source": route_meta_aggregator,
                    "request_id": request_id,
                    "execution_state": recovered_execution_state,
                    "message": f"Swap submitted. Transaction ID: {result.tx_hash}.",
                },
            )

        async def _persist_execution_state(execution_state: Dict[str, Any]) -> None:
            recovered_execution_state.clear()
            recovered_execution_state.update(execution_state)
            latest_tx_hash = _latest_step_tx_hash(recovered_execution_state)
            if not latest_tx_hash:
                return
            route_name = getattr(quote_used, "route", "direct")
            path_value = getattr(quote_used, "path", [token_in, token_out])
            amount_out = getattr(
                quote_used, "amount_out", quote_used.amount_out_minimum
            )
            await mark_transfer_inflight(
                active_claim,
                tx_hash=latest_tx_hash,
                result={
                    "status": "pending",
                    "tx_hash": latest_tx_hash,
                    "approve_hash": str(
                        (
                            (recovered_execution_state.get("steps") or {}).get(
                                "approve"
                            )
                            or {}
                        ).get("tx_hash")
                        or ""
                    )
                    or None,
                    "protocol": getattr(quote_used, "protocol", used_protocol),
                    "route": route_name,
                    "path": path_value,
                    "amount_in": str(quote_used.amount_in),
                    "amount_out": str(amount_out),
                    "amount_out_minimum": str(quote_used.amount_out_minimum),
                    "chain": quote_used.chain_name,
                    "fallback_used": bool(fallback_reason),
                    "fallback_reason": fallback_reason,
                    "route_meta_used": route_meta_used,
                    "route_meta_source": route_meta_aggregator,
                    "request_id": request_id,
                    "execution_state": recovered_execution_state,
                    "message": "Swap execution resumed from the last confirmed step.",
                },
            )

        async def _execute_with_quote(q):
            return await execute_swap(
                quote=q,
                sub_org_id=sub_org_id,
                sender=sender,
                gas_price_gwei=gas_price_gwei,
                persist_broadcast=_persist_quote_broadcast,
                execution_state=recovered_execution_state,
                persist_execution_state=_persist_execution_state,
            )

        try:
            result = await _execute_with_quote(quote_used)
        except Exception as exc:
            exc_msg = str(exc).lower()
            is_pending_timeout = "pending beyond" in exc_msg or (
                "pending" in exc_msg and "transaction" in exc_msg
            )
            if is_pending_timeout or not remaining_candidates:
                raise

            primary_protocol = used_protocol

            primary_failure = (
                f"{primary_protocol.upper()} execution failed "
                f"({categorize_error(exc).value}): {exc}"
            )
            fallback_errors: list[str] = []
            try:
                for fallback_protocol, fallback_quote in remaining_candidates:
                    try:
                        quote_used = fallback_quote
                        result = await _execute_with_quote(quote_used)
                        used_protocol = fallback_protocol
                        fallback_reason = (
                            f"{fallback_reason}; {primary_failure}"
                            if fallback_reason
                            else primary_failure
                        )
                        break
                    except Exception as fallback_exc:
                        fallback_errors.append(
                            f"{fallback_protocol.upper()}: {fallback_exc}"
                        )
                else:
                    raise RuntimeError(
                        format_with_recovery(
                            (
                                f"{primary_protocol.upper()} execution failed and all "
                                f"fallback executions failed: {'; '.join(fallback_errors[:4])}"
                            ),
                            "retry in a moment; if the issue persists, reduce amount",
                        )
                    )
            except Exception:
                raise RuntimeError(
                    format_with_recovery(
                        "Swap execution failed on all candidate routes",
                        "retry in a moment; if the issue persists, reduce amount",
                    )
                ) from exc

        protocol = result.protocol
        route = getattr(quote_used, "route", "direct")
        path = getattr(quote_used, "path", [token_in, token_out])

        token_in_label = parameters.get("token_in_symbol") or "token"
        token_out_label = parameters.get("token_out_symbol") or "token"
        amount_in_text = str(result.amount_in)
        message = (
            f"Swap submitted: {amount_in_text} {token_in_label} to {token_out_label}."
        )
        if result.tx_hash:
            message += f" Transaction ID: {result.tx_hash}."

        response = {
            "status": "success",
            "tx_hash": result.tx_hash,
            "approve_hash": result.approve_hash,
            "protocol": protocol,
            "route": route,
            "path": path,
            "amount_in": str(result.amount_in),
            "amount_out": str(
                getattr(quote_used, "amount_out", result.amount_out_minimum)
            ),
            "amount_out_minimum": str(result.amount_out_minimum),
            "chain": result.chain_name,
            "fallback_used": bool(fallback_reason),
            "fallback_reason": fallback_reason,
            "route_meta_used": route_meta_used,
            "route_meta_source": route_meta_aggregator,
            "request_id": request_id,
            "execution_state": {
                **recovered_execution_state,
                "completion_status": "completed",
            },
            "message": message,
        }
        await mark_transfer_success(
            active_claim,
            tx_hash=result.tx_hash,
            result=response,
        )
        return response
    except Exception as exc:
        latest_claim = await load_transfer_idempotency_claim(active_claim)
        if latest_claim is None or not latest_claim.tx_hash:
            await mark_transfer_failed(active_claim, error=str(exc))
        raise
