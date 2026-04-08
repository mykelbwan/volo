from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional, cast
from weakref import WeakKeyDictionary

from config.chains import get_chain_by_name
from config.solana_chains import (
    get_solana_chain,
    is_solana_network,
)
from core.routing.models import BridgeRouteQuote
from core.routing.route_meta import coerce_fallback_policy, enforce_fallback_policy
from core.utils.errors import NonRetryableError
from tool_nodes.bridge.providers import (
    BridgeCandidate,
    BridgeCandidateOrigin,
    BridgeRequest,
    get_bridge_provider,
)
from tool_nodes.bridge.providers import (
    collect_candidates as collect_provider_candidates_shadow,
)
from tool_nodes.bridge.simulators.across_simulator import (
    AcrossBridgeQuote,
    AcrossSimulationError,
)
from tool_nodes.bridge.simulators.relay_simulator import (
    RelayBridgeQuote,
    RelaySimulationError,
)
from tool_nodes.bridge.utils import format_fill_time
from tool_nodes.common.input_utils import (
    format_with_recovery,
    parse_decimal_field,
    require_fields,
)
from wallet_service.common.transfer_idempotency import (
    canonicalize_decimal_idempotency_value,
    claim_transfer_idempotency,
    load_transfer_idempotency_claim,
    mark_transfer_failed,
    mark_transfer_success,
    resolve_transfer_idempotency,
)
from wallet_service.common.wallet_lock import wallet_lock
from wallet_service.evm.gas_price import gas_price_cache

_LOGGER = logging.getLogger("volo.bridge")

if TYPE_CHECKING:
    from tool_nodes.bridge.executors.across_executor import AcrossBridgeResult
    from tool_nodes.bridge.executors.lifi_executor import LiFiBridgeResult
    from tool_nodes.bridge.executors.mayan_executor import MayanBridgeResult
    from tool_nodes.bridge.executors.relay_executor import RelayBridgeResult

SimulationResult = (
    AcrossBridgeQuote | RelayBridgeQuote | AcrossSimulationError | RelaySimulationError
)
ExecutionQuote = AcrossBridgeQuote | RelayBridgeQuote | BridgeRouteQuote
ExecutionMetadata = tuple[
    "AcrossBridgeResult | RelayBridgeResult | LiFiBridgeResult | MayanBridgeResult",
    str,
    str | None,
    str | None,
    Optional[dict],
    Optional[str],
    Optional[list[str]],
    Optional[str],
    Optional[str],
    Optional[int],
    Optional[int],
]


def _get_env_float(key: str, default: float) -> float:
    raw = str(os.getenv(key, "")).strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if value <= 0:
        return default
    return value


_SIMULATION_TIMEOUT_SECONDS = _get_env_float("BRIDGE_SIMULATION_TIMEOUT_SECONDS", 6.0)
_BRIDGE_SIMULATION_MAX_CONCURRENCY = int(
    str(os.getenv("BRIDGE_SIMULATION_MAX_CONCURRENCY", "64")).strip() or "64"
)
_BRIDGE_SIMULATION_SEMAPHORES: WeakKeyDictionary[
    asyncio.AbstractEventLoop, asyncio.Semaphore
] = WeakKeyDictionary()


def _validate_planned_route_meta_with_provider(
    *,
    request: BridgeRequest,
    route_meta: Mapping[str, Any] | None,
) -> None:
    if not isinstance(route_meta, Mapping) or not route_meta:
        return None

    aggregator = str(route_meta.get("aggregator") or "").strip().lower()
    if not aggregator:
        return None

    provider = get_bridge_provider(aggregator)
    if provider is None:
        return None

    planned_quote = provider.quote_from_route_meta(
        request=request, route_meta=route_meta
    )
    if planned_quote is None:
        return None

    provider.validate_route_meta(request=request, route_meta=route_meta)
    return None


def _quote_provider(quote: ExecutionQuote) -> str:
    if isinstance(quote, BridgeRouteQuote):
        return str(quote.aggregator or "").strip().lower()
    return str(quote.protocol or "").strip().lower()


def _as_execution_quote(value: Any) -> ExecutionQuote | None:
    if isinstance(value, AcrossBridgeQuote):
        return value
    if isinstance(value, RelayBridgeQuote):
        return value
    if isinstance(value, BridgeRouteQuote):
        return value
    return None


def _quote_fill_time_seconds(quote: ExecutionQuote) -> int:
    if isinstance(quote, AcrossBridgeQuote):
        return max(0, int(quote.avg_fill_time_seconds))
    if isinstance(quote, RelayBridgeQuote):
        return max(0, int(quote.avg_fill_time_seconds))
    return max(0, int(quote.estimated_fill_time_seconds))


def _sort_execution_candidates(
    candidates: list[BridgeCandidate],
) -> list[BridgeCandidate]:
    # Mirror quote ordering while preserving provider ownership on each candidate.
    return sorted(
        candidates,
        key=lambda candidate: (
            -cast(ExecutionQuote, candidate.quote).output_amount,
            _quote_fill_time_seconds(cast(ExecutionQuote, candidate.quote)),
            _quote_provider(cast(ExecutionQuote, candidate.quote)),
        ),
    )


async def execute_candidate(
    *,
    candidate: BridgeCandidate,
    request: BridgeRequest,
    quote: ExecutionQuote,
    gas_price_gwei: float | None = None,
) -> ExecutionMetadata:
    provider_name = str(candidate.provider_name or "").strip().lower()
    if provider_name == "across":
        if not isinstance(quote, AcrossBridgeQuote):
            raise RuntimeError("Across candidate quote type is invalid.")

        route_meta: dict[str, Any] | None = None
        if gas_price_gwei is not None:
            route_meta = {"gas_price_gwei": gas_price_gwei}

        async with wallet_lock(request.sender, request.source_chain_id):
            result = await candidate.provider.execute(
                request=request,
                quote=quote,
                route_meta=route_meta,
            )
        across_result = cast("AcrossBridgeResult", result)
        return (
            across_result,
            across_result.estimated_fill_time,
            across_result.approve_hash,
            across_result.status,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )

    if provider_name == "lifi":
        is_lifi_quote = (
            isinstance(quote, BridgeRouteQuote)
            and str(quote.aggregator or "").strip().lower() == "lifi"
        )
        if not is_lifi_quote:
            raise RuntimeError("LiFi candidate quote type is invalid.")

        if request.source_is_solana:
            result = await candidate.provider.execute(
                request=request,
                quote=quote,
                route_meta=None,
            )
        else:
            async with wallet_lock(request.sender, request.source_chain_id):
                result = await candidate.provider.execute(
                    request=request,
                    quote=quote,
                    route_meta=None,
                )
        lifi_result = cast("LiFiBridgeResult", result)
        lifi_quote = cast(BridgeRouteQuote, quote)
        return (
            lifi_result,
            format_fill_time(lifi_quote.estimated_fill_time_seconds),
            lifi_result.approve_hash,
            lifi_result.status,
            None,
            None,
            None,
            None,
            lifi_result.bridge,
            lifi_result.from_chain_id,
            lifi_result.to_chain_id,
        )

    if provider_name == "relay":
        if not isinstance(quote, RelayBridgeQuote):
            raise RuntimeError("Relay candidate quote type is invalid.")

        async with wallet_lock(request.sender, request.source_chain_id):
            result = await candidate.provider.execute(
                request=request,
                quote=quote,
                route_meta=None,
            )
        relay_result = cast("RelayBridgeResult", result)
        relay_quote = cast(RelayBridgeQuote, quote)
        return (
            relay_result,
            format_fill_time(relay_quote.avg_fill_time_seconds),
            None,
            relay_result.status,
            relay_quote.fees,
            relay_result.request_id,
            relay_result.tx_hashes,
            relay_result.relay_status,
            None,
            None,
            None,
        )

    if provider_name == "mayan":
        is_mayan_quote = (
            isinstance(quote, BridgeRouteQuote)
            and str(quote.aggregator or "").strip().lower() == "mayan"
        )
        if not is_mayan_quote:
            raise RuntimeError("Mayan candidate quote type is invalid.")

        if request.source_is_solana:
            result = await candidate.provider.execute(
                request=request,
                quote=quote,
                route_meta=None,
            )
        else:
            async with wallet_lock(request.sender, request.source_chain_id):
                result = await candidate.provider.execute(
                    request=request,
                    quote=quote,
                    route_meta=None,
                )
        mayan_result = cast("MayanBridgeResult", result)
        mayan_quote = cast(BridgeRouteQuote, quote)
        return (
            mayan_result,
            format_fill_time(mayan_quote.estimated_fill_time_seconds),
            mayan_result.approve_hash,
            mayan_result.status,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )

    raise RuntimeError(
        f"execute_candidate is not yet wired for provider {provider_name!r}."
    )


async def bridge_token(parameters: Dict[str, Any]) -> Dict[str, Any]:
    token_symbol = parameters.get("token_symbol")
    source_chain_name = parameters.get("source_chain")
    target_chain_name = parameters.get("target_chain")
    amount_raw = parameters.get("amount")
    sub_org_id = parameters.get("sub_org_id")
    sender = parameters.get("sender")
    recipient = parameters.get("recipient") or sender

    require_fields(
        parameters,
        [
            "token_symbol",
            "source_chain",
            "target_chain",
            "amount",
            "sub_org_id",
            "sender",
        ],
        context="bridge",
    )

    # Cast to concrete types after validation
    token_symbol = str(token_symbol).strip().upper()
    source_chain_name = str(source_chain_name).strip()
    target_chain_name = str(target_chain_name).strip()
    amount = parse_decimal_field(
        amount_raw,
        field="amount",
        positive=True,
        invalid_recovery=(
            "use a positive numeric amount (for example, 10.5) and retry"
        ),
    )
    sub_org_id = str(sub_org_id).strip()
    sender = str(sender).strip()
    recipient = str(recipient).strip()

    # Solana is not in the EVM chain registry — handle it separately.
    source_chain = None
    dest_chain = None
    source_chain_id: int
    dest_chain_id: int
    source_chain_display: str
    dest_chain_display: str
    source_is_solana = False
    dest_is_solana = False

    try:
        source_chain = get_chain_by_name(source_chain_name)
        source_chain_id = source_chain.chain_id
        source_chain_display = source_chain.name
    except KeyError:
        if is_solana_network(source_chain_name):
            solana_chain = get_solana_chain(source_chain_name)
            source_chain_id = solana_chain.chain_id
            source_chain_display = solana_chain.name
            source_is_solana = True
        else:
            raise ValueError(f"'{source_chain_name}' is not a supported source chain.")

    try:
        dest_chain = get_chain_by_name(target_chain_name)
        dest_chain_id = dest_chain.chain_id
        dest_chain_display = dest_chain.name
    except KeyError:
        if is_solana_network(target_chain_name):
            solana_chain = get_solana_chain(target_chain_name)
            dest_chain_id = solana_chain.chain_id
            dest_chain_display = solana_chain.name
            dest_is_solana = True
        else:
            raise ValueError(
                f"'{target_chain_name}' is not a supported destination chain."
            )

    if source_chain_id == dest_chain_id:
        raise ValueError(
            format_with_recovery(
                "Source and destination chains are the same",
                "choose a different destination chain and retry",
            )
        )

    idempotency_fields = {
        "sender": sender.strip().lower(),
        "source_chain_id": int(source_chain_id),
        "dest_chain_id": int(dest_chain_id),
        "tool_name": "bridge_transfer",
        "token_symbol": token_symbol,
        "amount": canonicalize_decimal_idempotency_value(amount),
        "recipient": recipient.strip().lower(),
    }
    idempotency_key, idempotency_fields, request_id = resolve_transfer_idempotency(
        tool_name="bridge_transfer",
        request_fields=idempotency_fields,
        external_key=parameters.get("idempotency_key"),
        request_id=parameters.get("request_id"),
    )
    claim = await claim_transfer_idempotency(
        operation="bridge_transfer",
        idempotency_key=idempotency_key,
        request_fields=idempotency_fields,
    )
    if claim is not None and claim.reused:
        if claim.result:
            return dict(claim.result)
        if claim.tx_hash:
            return {
                "status": "pending" if claim.status == "pending" else "success",
                "tx_hash": claim.tx_hash,
                "message": f"Bridge already started. Tracking ID: {claim.tx_hash}.",
            }

    current_claim = await load_transfer_idempotency_claim(claim)
    if current_claim is not None and current_claim.tx_hash and current_claim.result:
        return dict(current_claim.result)

    async def _store_bridge_response(
        response: Dict[str, Any],
    ) -> Dict[str, Any]:
        tx_hash = str(response.get("tx_hash") or "")
        if tx_hash:
            await mark_transfer_success(claim, tx_hash=tx_hash, result=response)
        return response

    route_meta = parameters.get("_route_meta") or {}
    fallback_policy = coerce_fallback_policy(parameters.get("_fallback_policy"))

    shadow_candidates: list[BridgeCandidate] = []
    shadow_diagnostics: list[str] = []

    # Provider candidate collection (primary ranking source).
    shadow_request = BridgeRequest(
        token_symbol=token_symbol,
        source_chain_id=source_chain_id,
        dest_chain_id=dest_chain_id,
        source_chain_name=source_chain_display,
        dest_chain_name=dest_chain_display,
        amount=amount,
        sub_org_id=sub_org_id,
        sender=sender,
        recipient=recipient,
        source_is_solana=source_is_solana,
        dest_is_solana=dest_is_solana,
    )
    _validate_planned_route_meta_with_provider(
        request=shadow_request,
        route_meta=route_meta if isinstance(route_meta, dict) else None,
    )
    try:
        (
            shadow_candidates,
            shadow_diagnostics,
        ) = await collect_provider_candidates_shadow(
            request=shadow_request,
            route_meta=route_meta if isinstance(route_meta, dict) else None,
        )
        if shadow_candidates or shadow_diagnostics:
            _LOGGER.info(
                "shadow_candidates count=%d providers=%s diagnostics=%s",
                len(shadow_candidates),
                ",".join(
                    f"{candidate.provider_name}:{candidate.origin.value}"
                    for candidate in shadow_candidates
                ),
                "; ".join(shadow_diagnostics[:4]),
            )
    except Exception as exc:
        _LOGGER.warning("shadow_collect_candidates_failed: %s", exc)

    executable_candidates: list[BridgeCandidate] = []
    simulation_errors: list[str] = list(shadow_diagnostics)
    gas_price_task: asyncio.Task[Decimal] | None = None

    for candidate in shadow_candidates:
        execution_quote = _as_execution_quote(getattr(candidate, "quote", None))
        if execution_quote is None:
            simulation_errors.append(
                f"[shadow:{getattr(candidate, 'provider_name', 'unknown')}] unsupported quote type"
            )
            continue
        executable_candidates.append(candidate)

    if not executable_candidates:
        if gas_price_task is not None and not gas_price_task.done():
            gas_price_task.cancel()
            with suppress(asyncio.CancelledError):
                await gas_price_task
        error_summary = (
            "; ".join(simulation_errors[:6])
            if simulation_errors
            else "no quotes returned"
        )
        await mark_transfer_failed(claim, error=error_summary)
        raise RuntimeError(
            format_with_recovery(
                (
                    f"All bridge simulations failed for {token_symbol} "
                    f"from {source_chain_display} to {dest_chain_display}. "
                    f"Errors: {error_summary}"
                ),
                "retry in a few seconds, or reduce the amount and try again",
            )
        )

    candidates_sorted = _sort_execution_candidates(executable_candidates)
    quotes_sorted = [
        cast(ExecutionQuote, candidate.quote) for candidate in candidates_sorted
    ]
    fallback_reason: str | None = None
    used_quote: ExecutionQuote | None = None
    used_candidate: BridgeCandidate | None = None
    exec_result: (
        AcrossBridgeResult
        | RelayBridgeResult
        | LiFiBridgeResult
        | MayanBridgeResult
        | None
    ) = None
    execution_errors: list[str] = []
    estimated_fill_time: str | None = None
    approve_hash: str | None = None
    bridge_status: str | None = None
    relay_fees: dict | None = None
    relay_request_id: str | None = None
    tx_hashes: list[str] | None = None
    relay_status: str | None = None
    lifi_bridge_name: str | None = None
    lifi_from_chain_id: int | None = None
    lifi_to_chain_id: int | None = None

    gas_price_gwei: float | None = None
    if any(_quote_provider(q) == "across" for q in quotes_sorted):
        if gas_price_task is None:
            gas_price_task = asyncio.create_task(
                gas_price_cache.get_gwei(chain_id=source_chain_id)
            )
        gas_price_gwei = float(await gas_price_task)
    elif gas_price_task is not None and not gas_price_task.done():
        gas_price_task.cancel()
        with suppress(asyncio.CancelledError):
            await gas_price_task

    async def _execute_quote(
        quote: ExecutionQuote,
        *,
        candidate: BridgeCandidate,
    ) -> ExecutionMetadata:
        return await execute_candidate(
            candidate=candidate,
            request=shadow_request,
            quote=quote,
            gas_price_gwei=gas_price_gwei,
        )

    primary_error: str | None = None
    for idx, candidate in enumerate(candidates_sorted):
        quote = cast(ExecutionQuote, candidate.quote)
        provider = _quote_provider(quote)
        try:
            if idx > 0:
                enforce_fallback_policy(
                    policy=fallback_policy,
                    detail=f"primary bridge execution failed; attempting {provider}",
                )
            (
                exec_result,
                estimated_fill_time,
                approve_hash,
                bridge_status,
                relay_fees,
                relay_request_id,
                tx_hashes,
                relay_status,
                lifi_bridge_name,
                lifi_from_chain_id,
                lifi_to_chain_id,
            ) = await _execute_quote(quote, candidate=candidate)
            used_quote = quote
            used_candidate = candidate
            if idx > 0 and primary_error:
                fallback_reason = f"Primary quote failed: {primary_error}"
            break
        except NonRetryableError:
            raise
        except Exception as exc:
            err_str = f"{provider}: {exc}"
            execution_errors.append(err_str)
            if idx == 0:
                primary_error = err_str
            continue

    if exec_result is None or used_quote is None:
        error_summary = (
            "; ".join(execution_errors[:6])
            if execution_errors
            else "no executor returned a result"
        )
        await mark_transfer_failed(claim, error=error_summary)
        raise RuntimeError(
            format_with_recovery(
                (
                    f"All bridge executions failed for {token_symbol} "
                    f"from {source_chain_display} to {dest_chain_display}. "
                    f"Errors: {error_summary}"
                ),
                "retry in a moment or request a new route quote",
            )
        )
    if estimated_fill_time is None:
        await mark_transfer_failed(
            claim, error="Bridge execution did not return a fill time."
        )
        raise RuntimeError("Bridge execution did not return a fill time.")

    normalized_status = (
        "pending"
        if not bridge_status or str(bridge_status).lower() == "pending"
        else "success"
    )

    message_parts = [
        f"Bridge started: {exec_result.input_amount} {exec_result.token_symbol}."
    ]
    message_parts.append(
        f"From {exec_result.source_chain_name} to {exec_result.dest_chain_name}."
    )
    if estimated_fill_time:
        message_parts.append(f"Estimated time: {estimated_fill_time}.")
    if exec_result.tx_hash:
        message_parts.append(f"Tracking ID: {exec_result.tx_hash}.")

    response = {
        "status": normalized_status,
        "tx_hash": exec_result.tx_hash,
        "approve_hash": approve_hash,
        "protocol": exec_result.protocol,
        "token_symbol": exec_result.token_symbol,
        "input_amount": str(exec_result.input_amount),
        "output_amount": str(exec_result.output_amount),
        "total_fee": str(used_quote.total_fee),
        "total_fee_pct": str(used_quote.total_fee_pct),
        "source_chain": exec_result.source_chain_name,
        "dest_chain": exec_result.dest_chain_name,
        "recipient": exec_result.recipient,
        "estimated_fill_time": estimated_fill_time,
        "bridge_status": bridge_status,
        "fallback_used": bool(fallback_reason),
        "fallback_reason": fallback_reason,
        "route_meta_used": bool(
            used_candidate and used_candidate.origin == BridgeCandidateOrigin.PLANNED
        ),
        "message": " ".join(message_parts),
    }

    nonce = getattr(exec_result, "nonce", None)
    if nonce is not None:
        response["nonce"] = nonce
    raw_tx = getattr(exec_result, "raw_tx", None)
    if raw_tx:
        response["raw_tx"] = raw_tx
    tx_payload = getattr(exec_result, "tx_payload", None)
    if tx_payload:
        response["tx_payload"] = tx_payload

    if relay_status is not None:
        response["relay_status"] = relay_status
    if relay_fees is not None:
        response["relay_fees"] = relay_fees
    if relay_request_id is not None:
        response["relay_request_id"] = relay_request_id
    if tx_hashes is not None:
        response["tx_hashes"] = tx_hashes
    if lifi_bridge_name is not None:
        response["bridge"] = lifi_bridge_name
    if lifi_from_chain_id is not None:
        response["from_chain_id"] = lifi_from_chain_id
    if lifi_to_chain_id is not None:
        response["to_chain_id"] = lifi_to_chain_id

    return await _store_bridge_response(response)
