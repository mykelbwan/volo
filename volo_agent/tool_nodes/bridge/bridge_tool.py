from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Awaitable, Dict, Optional, cast
from weakref import WeakKeyDictionary

from config.bridge_registry import (
    ACROSS,
    BridgeProtocolConfig,
    BridgeRoute,
    get_dynamic_protocols,
    get_routes,
    relay_api_base_url,
)
from config.chains import get_chain_by_name
from config.solana_chains import (
    get_solana_chain,
    is_solana_chain_id,
    is_solana_network,
)
from core.routing.models import BridgeRouteQuote
from core.routing.route_meta import coerce_fallback_policy, enforce_fallback_policy
from core.token_security.registry_lookup import (
    get_native_decimals,
    get_registry_decimals_by_address_async as get_registry_decimals_by_address,
)
from core.token_security.registry_lookup import (
    get_registry_decimals_by_symbol_async as get_registry_decimals_by_symbol,
)
from core.utils.errors import NonRetryableError
from tool_nodes.bridge.simulators.across_simulator import (
    AcrossBridgeQuote,
    AcrossSimulationError,
    fetch_across_available_routes_async,
    simulate_across_bridge_async,
)
from tool_nodes.bridge.simulators.relay_simulator import (
    RelayBridgeQuote,
    RelaySimulationError,
    simulate_relay_bridge_async,
)
from tool_nodes.bridge.utils import format_fill_time
from tool_nodes.common.input_utils import (
    format_with_recovery,
    parse_decimal_field,
    require_fields,
    safe_decimal,
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
    from tool_nodes.bridge.executors.relay_executor import RelayBridgeResult

SimulationResult = (
    AcrossBridgeQuote | RelayBridgeQuote | AcrossSimulationError | RelaySimulationError
)

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

_EXECUTE_ACROSS_BRIDGE_FN: Any | None = None
_EXECUTE_MAYAN_BRIDGE_FN: Any | None = None
_EXECUTE_LIFI_BRIDGE_FN: Any | None = None
_EXECUTE_RELAY_BRIDGE_FN: Any | None = None
_MAYAN_AGGREGATOR: Any | None = None


async def execute_across_bridge(*args: Any, **kwargs: Any) -> Any:
    global _EXECUTE_ACROSS_BRIDGE_FN
    if _EXECUTE_ACROSS_BRIDGE_FN is None:
        from tool_nodes.bridge.executors.across_executor import (
            execute_across_bridge as _execute_across_bridge_impl,
        )

        _EXECUTE_ACROSS_BRIDGE_FN = _execute_across_bridge_impl
    return await _EXECUTE_ACROSS_BRIDGE_FN(*args, **kwargs)


async def execute_mayan_bridge(*args: Any, **kwargs: Any) -> Any:
    global _EXECUTE_MAYAN_BRIDGE_FN
    if _EXECUTE_MAYAN_BRIDGE_FN is None:
        from tool_nodes.bridge.executors.mayan_executor import (
            execute_mayan_bridge as _execute_mayan_bridge_impl,
        )

        _EXECUTE_MAYAN_BRIDGE_FN = _execute_mayan_bridge_impl
    return await _EXECUTE_MAYAN_BRIDGE_FN(*args, **kwargs)


async def execute_lifi_bridge(*args: Any, **kwargs: Any) -> Any:
    global _EXECUTE_LIFI_BRIDGE_FN
    if _EXECUTE_LIFI_BRIDGE_FN is None:
        from tool_nodes.bridge.executors.lifi_executor import (
            execute_lifi_bridge as _execute_lifi_bridge_impl,
        )

        _EXECUTE_LIFI_BRIDGE_FN = _execute_lifi_bridge_impl
    return await _EXECUTE_LIFI_BRIDGE_FN(*args, **kwargs)


async def execute_relay_bridge(*args: Any, **kwargs: Any) -> Any:
    global _EXECUTE_RELAY_BRIDGE_FN
    if _EXECUTE_RELAY_BRIDGE_FN is None:
        from tool_nodes.bridge.executors.relay_executor import (
            execute_relay_bridge as _execute_relay_bridge_impl,
        )

        _EXECUTE_RELAY_BRIDGE_FN = _execute_relay_bridge_impl
    return await _EXECUTE_RELAY_BRIDGE_FN(*args, **kwargs)


def _get_mayan_aggregator() -> Any:
    global _MAYAN_AGGREGATOR
    if _MAYAN_AGGREGATOR is None:
        from core.routing.bridge.mayan import MayanAggregator

        _MAYAN_AGGREGATOR = MayanAggregator()
    return _MAYAN_AGGREGATOR


def _bridge_quote_from_route_meta(route_meta: Dict[str, Any]) -> Any | None:
    aggregator = str(route_meta.get("aggregator") or "").strip().lower()
    tool_data = route_meta.get("tool_data") or {}
    if aggregator == "across":
        payload = (
            tool_data.get("planned_quote") if isinstance(tool_data, dict) else None
        )
        if not isinstance(payload, dict):
            return None
        return AcrossBridgeQuote(
            protocol=str(payload.get("protocol") or "across"),
            token_symbol=str(
                payload.get("token_symbol") or route_meta.get("token_symbol") or ""
            ),
            input_token=str(payload.get("input_token") or ""),
            output_token=str(payload.get("output_token") or ""),
            source_chain_id=int(str(payload.get("source_chain_id"))),
            dest_chain_id=int(str(payload.get("dest_chain_id"))),
            source_chain_name=str(
                payload.get("source_chain_name") or route_meta.get("source_chain") or ""
            ),
            dest_chain_name=str(
                payload.get("dest_chain_name") or route_meta.get("target_chain") or ""
            ),
            input_amount=safe_decimal(payload.get("input_amount")) or Decimal("0"),
            output_amount=safe_decimal(payload.get("output_amount")) or Decimal("0"),
            total_fee=safe_decimal(payload.get("total_fee")) or Decimal("0"),
            total_fee_pct=safe_decimal(payload.get("total_fee_pct")) or Decimal("0"),
            lp_fee=safe_decimal(payload.get("lp_fee")) or Decimal("0"),
            relayer_fee=safe_decimal(payload.get("relayer_fee")) or Decimal("0"),
            gas_fee=safe_decimal(payload.get("gas_fee")) or Decimal("0"),
            input_decimals=int(payload.get("input_decimals") or 18),
            output_decimals=int(payload.get("output_decimals") or 18),
            quote_timestamp=int(payload.get("quote_timestamp") or 0),
            fill_deadline=int(payload.get("fill_deadline") or 0),
            exclusivity_deadline=int(payload.get("exclusivity_deadline") or 0),
            exclusive_relayer=str(payload.get("exclusive_relayer") or ""),
            spoke_pool=str(payload.get("spoke_pool") or ""),
            is_native_input=bool(payload.get("is_native_input")),
            avg_fill_time_seconds=int(payload.get("avg_fill_time_seconds") or 120),
        )
    if aggregator == "relay":
        payload = (
            tool_data.get("planned_quote") if isinstance(tool_data, dict) else None
        )
        if not isinstance(payload, dict):
            return None
        relay_source_chain_id = int(str(payload.get("source_chain_id")))
        relay_dest_chain_id = int(str(payload.get("dest_chain_id")))
        return RelayBridgeQuote(
            protocol=str(payload.get("protocol") or "relay"),
            token_symbol=str(
                payload.get("token_symbol") or route_meta.get("token_symbol") or ""
            ),
            source_chain_id=relay_source_chain_id,
            dest_chain_id=relay_dest_chain_id,
            source_chain_name=str(
                payload.get("source_chain_name") or route_meta.get("source_chain") or ""
            ),
            dest_chain_name=str(
                payload.get("dest_chain_name") or route_meta.get("target_chain") or ""
            ),
            input_amount=safe_decimal(payload.get("input_amount")) or Decimal("0"),
            output_amount=safe_decimal(payload.get("output_amount")) or Decimal("0"),
            total_fee=safe_decimal(payload.get("total_fee")) or Decimal("0"),
            total_fee_pct=safe_decimal(payload.get("total_fee_pct")) or Decimal("0"),
            fees=payload.get("fees") or {},
            steps=list(payload.get("steps") or []),
            request_id=payload.get("request_id"),
            avg_fill_time_seconds=int(payload.get("avg_fill_time_seconds") or 180),
            api_base_url=str(
                payload.get("api_base_url")
                or relay_api_base_url(relay_source_chain_id, relay_dest_chain_id)
            ),
        )
    if aggregator == "mayan" and isinstance(tool_data, dict):
        return BridgeRouteQuote(
            aggregator="mayan",
            token_symbol=str(route_meta.get("token_symbol") or ""),
            source_chain_id=int(str(route_meta.get("source_chain_id"))),
            dest_chain_id=int(str(route_meta.get("dest_chain_id"))),
            source_chain_name=str(route_meta.get("source_chain") or ""),
            dest_chain_name=str(route_meta.get("target_chain") or ""),
            input_amount=Decimal("0"),
            output_amount=safe_decimal(route_meta.get("output_amount")) or Decimal("0"),
            total_fee=Decimal("0"),
            total_fee_pct=safe_decimal(route_meta.get("total_fee_pct")) or Decimal("0"),
            estimated_fill_time_seconds=int(route_meta.get("fill_time_seconds") or 0),
            gas_cost_source=None,
            calldata=route_meta.get("calldata"),
            to=route_meta.get("to"),
            tool_data=tool_data,
        )
    return None


def _route_meta_contains_untrusted_bridge_tx(route_meta: Any) -> bool:
    if not isinstance(route_meta, dict):
        return False
    tool_data = route_meta.get("tool_data") or {}
    if route_meta.get("calldata") or route_meta.get("swap_transaction"):
        return True
    if not isinstance(tool_data, dict):
        return False
    tx_request = tool_data.get("transactionRequest")
    if isinstance(tx_request, dict) and tx_request.get("data"):
        return True
    build_tx = tool_data.get("buildTxResult")
    if isinstance(build_tx, dict) and build_tx.get("txData"):
        return True
    planned_quote = tool_data.get("planned_quote")
    if isinstance(planned_quote, dict):
        steps = planned_quote.get("steps")
        if isinstance(steps, list) and steps:
            return True
    return False


def _validate_non_executable_bridge_route_meta(
    *,
    route_meta: Any,
    token_symbol: str,
    source_chain_id: int,
    dest_chain_id: int,
    amount: Decimal,
    recipient: str,
) -> None:
    if not isinstance(route_meta, dict) or not route_meta:
        return
    aggregator = str(route_meta.get("aggregator") or "").strip().lower()
    tool_data = route_meta.get("tool_data")
    planned_quote = (
        tool_data.get("planned_quote") if isinstance(tool_data, dict) else None
    )
    if aggregator != "lifi" and _route_meta_contains_untrusted_bridge_tx(route_meta):
        _LOGGER.warning(
            "route_meta_rejected reason=untrusted_precomputed_tx aggregator=%s",
            aggregator or "unknown",
        )
        raise NonRetryableError("Untrusted precomputed transaction data is not allowed")
    if aggregator == "lifi":
        tx_request = tool_data.get("transactionRequest") if isinstance(tool_data, dict) else None
        if not isinstance(tx_request, dict) or not tx_request.get("data"):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned LiFi route is missing transaction data",
                    "request a fresh route and retry",
                )
            )
        chain_id_raw = tx_request.get("chainId")
        if chain_id_raw not in (None, ""):
            tx_chain_id: int | None = None
            raw_text = str(chain_id_raw).strip()
            try:
                tx_chain_id = int(raw_text, 0)
            except ValueError:
                try:
                    tx_chain_id = int(raw_text)
                except ValueError:
                    tx_chain_id = None
            if tx_chain_id is None or tx_chain_id != int(source_chain_id):
                raise NonRetryableError(
                    format_with_recovery(
                        "The planned LiFi route targets a different source chain",
                        "request a fresh route and retry",
                    )
                )
        is_solana_route = is_solana_chain_id(source_chain_id) or is_solana_chain_id(
            dest_chain_id
        )
        if not is_solana_route and not tx_request.get("to"):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned LiFi route is missing a destination contract",
                    "request a fresh route and retry",
                )
            )
    if str(route_meta.get("token_symbol") or "").strip().upper() not in {
        "",
        token_symbol,
    }:
        raise NonRetryableError(
            format_with_recovery(
                "The planned bridge metadata does not match the requested token",
                "request a fresh route and retry",
            )
        )
    if route_meta.get("source_chain_id") is not None and int(
        str(route_meta.get("source_chain_id"))
    ) != int(source_chain_id):
        raise NonRetryableError(
            format_with_recovery(
                "The planned bridge metadata does not match the requested source chain",
                "request a fresh route and retry",
            )
        )
    if route_meta.get("dest_chain_id") is not None and int(
        str(route_meta.get("dest_chain_id"))
    ) != int(dest_chain_id):
        raise NonRetryableError(
            format_with_recovery(
                "The planned bridge metadata does not match the requested destination chain",
                "request a fresh route and retry",
            )
        )
    if (
        route_meta.get("input_amount") is not None
        and Decimal(str(route_meta.get("input_amount"))) != amount
    ):
        raise NonRetryableError(
            format_with_recovery(
                "The planned bridge metadata does not match the requested amount",
                "request a fresh route and retry",
            )
        )
    if isinstance(planned_quote, dict):
        if planned_quote.get("source_chain_id") is not None and int(
            str(planned_quote.get("source_chain_id"))
        ) != int(source_chain_id):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned bridge metadata does not match the requested source chain",
                    "request a fresh route and retry",
                )
            )
        if planned_quote.get("dest_chain_id") is not None and int(
            str(planned_quote.get("dest_chain_id"))
        ) != int(dest_chain_id):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned bridge metadata does not match the requested destination chain",
                    "request a fresh route and retry",
                )
            )
        if (
            planned_quote.get("token_symbol") is not None
            and str(planned_quote.get("token_symbol") or "").strip().upper()
            != token_symbol
        ):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned bridge metadata does not match the requested token",
                    "request a fresh route and retry",
                )
            )
        if (
            planned_quote.get("input_amount") is not None
            and Decimal(str(planned_quote.get("input_amount"))) != amount
        ):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned bridge metadata does not match the requested amount",
                    "request a fresh route and retry",
                )
            )
    planned_recipient = str(route_meta.get("recipient") or "").strip().lower()
    if planned_recipient and planned_recipient != recipient.strip().lower():
        raise NonRetryableError(
            format_with_recovery(
                "The planned bridge metadata does not match the requested recipient",
                "request a fresh route and retry",
            )
        )
    if aggregator == "mayan":
        _LOGGER.warning(
            "route_meta_rejected reason=unverifiable_mayan_hints source_chain_id=%s dest_chain_id=%s",
            source_chain_id,
            dest_chain_id,
        )
        raise NonRetryableError(
            format_with_recovery(
                "The planned Mayan route metadata cannot be executed safely",
                "request a fresh route and retry",
            )
        )


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


async def _simulate_with_timeout(
    simulation: Awaitable[SimulationResult], *, label: str
) -> SimulationResult:
    try:
        return await asyncio.wait_for(simulation, timeout=_SIMULATION_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        return AcrossSimulationError(
            reason="SIM_TIMEOUT",
            message=(
                f"{label} simulation timed out after "
                f"{_SIMULATION_TIMEOUT_SECONDS:.1f}s."
            ),
        )
    except Exception as exc:
        return AcrossSimulationError(
            reason="SIM_ERROR",
            message=f"{label} simulation failed: {exc}",
        )


def _get_bridge_simulation_semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    semaphore = _BRIDGE_SIMULATION_SEMAPHORES.get(loop)
    if semaphore is None:
        semaphore = asyncio.Semaphore(_BRIDGE_SIMULATION_MAX_CONCURRENCY)
        _BRIDGE_SIMULATION_SEMAPHORES[loop] = semaphore
    return semaphore


async def _run_limited_bridge_simulation(
    simulation: Awaitable[SimulationResult], *, label: str
) -> SimulationResult:
    async with _get_bridge_simulation_semaphore():
        return await _simulate_with_timeout(simulation, label=label)


async def _get_route_decimals(route: BridgeRoute, token_symbol: str) -> tuple[int, int]:
    symbol = token_symbol.upper()

    if route.is_native_input:
        input_decimals = get_native_decimals(route.source_chain_id)
    else:
        input_decimals = await get_registry_decimals_by_address(
            route.input_token, route.source_chain_id
        )
        if input_decimals is None:
            input_decimals = await get_registry_decimals_by_symbol(
                symbol, route.source_chain_id
            )

    if route.is_native_output:
        output_decimals = get_native_decimals(route.dest_chain_id)
    else:
        output_decimals = await get_registry_decimals_by_address(
            route.output_token, route.dest_chain_id
        )
        if output_decimals is None:
            output_decimals = await get_registry_decimals_by_symbol(
                symbol, route.dest_chain_id
            )

    if input_decimals is None or output_decimals is None:
        _LOGGER.error(
            "bridge_tool: could not resolve decimals for %s. input=%s, output=%s",
            symbol,
            input_decimals,
            output_decimals,
        )
        raise NonRetryableError(
            f"Could not resolve decimals for {symbol} on the requested chains."
        )

    return int(input_decimals), int(output_decimals)


async def _simulate_route_async(
    route_tuple: tuple,
    amount: Decimal,
    sender: str,
    recipient: str,
    input_decimals: int,
    output_decimals: int,
) -> AcrossBridgeQuote | AcrossSimulationError:
    """
    Run a single simulator asynchronously.
    """
    protocol_config, route = route_tuple

    # Only Across is supported for now — dispatch by protocol name.
    if route.protocol == "across":
        return await simulate_across_bridge_async(
            route,
            amount,
            sender,
            recipient,
            input_decimals,
            output_decimals,
            protocol_config,
        )

    return AcrossSimulationError(
        reason="UNSUPPORTED_PROTOCOL",
        message=f"Protocol {route.protocol!r} is not yet implemented.",
    )


async def _simulate_dynamic_protocol_async(
    protocol: BridgeProtocolConfig,
    token_symbol: str,
    source_chain_id: int,
    dest_chain_id: int,
    amount: Decimal,
    sender: str,
    recipient: str,
) -> RelayBridgeQuote | RelaySimulationError:
    if protocol.name == "relay":
        base_url = relay_api_base_url(source_chain_id, dest_chain_id)
        base_url = (base_url or "").strip()
        if not base_url.startswith("http"):
            return RelaySimulationError(
                reason="DISABLED",
                message="Relay API base URL is not configured.",
            )
        return await simulate_relay_bridge_async(
            token_symbol,
            source_chain_id,
            dest_chain_id,
            amount,
            sender,
            recipient,
            protocol,
        )

    return RelaySimulationError(
        reason="UNSUPPORTED_PROTOCOL",
        message=f"Protocol {protocol.name!r} is not yet implemented.",
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
    _validate_non_executable_bridge_route_meta(
        route_meta=route_meta,
        token_symbol=token_symbol,
        source_chain_id=source_chain_id,
        dest_chain_id=dest_chain_id,
        amount=amount,
        recipient=recipient,
    )
    fallback_policy = coerce_fallback_policy(parameters.get("_fallback_policy"))
    if isinstance(route_meta, dict) and route_meta:
        planned_quote = _bridge_quote_from_route_meta(route_meta)
        aggregator = str(route_meta.get("aggregator") or "").strip().lower()
        if aggregator == "lifi":
            output_amount = safe_decimal(route_meta.get("output_amount")) or Decimal("0")
            if output_amount <= 0:
                output_amount = amount
            try:
                if source_is_solana:
                    lifi_result = await execute_lifi_bridge(
                        route_meta=route_meta,
                        token_symbol=token_symbol,
                        source_chain_id=source_chain_id,
                        dest_chain_id=dest_chain_id,
                        source_chain_name=source_chain_display,
                        dest_chain_name=dest_chain_display,
                        input_amount=amount,
                        output_amount=output_amount,
                        sub_org_id=sub_org_id,
                        sender=sender,
                        recipient=recipient,
                        solana_network=str(source_chain_name).strip().lower(),
                    )
                else:
                    async with wallet_lock(sender, source_chain_id):
                        lifi_result = await execute_lifi_bridge(
                            route_meta=route_meta,
                            token_symbol=token_symbol,
                            source_chain_id=source_chain_id,
                            dest_chain_id=dest_chain_id,
                            source_chain_name=source_chain_display,
                            dest_chain_name=dest_chain_display,
                            input_amount=amount,
                            output_amount=output_amount,
                            sub_org_id=sub_org_id,
                            sender=sender,
                            recipient=recipient,
                        )
            except NonRetryableError:
                raise
            except Exception as exc:
                await mark_transfer_failed(claim, error=str(exc))
                raise RuntimeError(
                    format_with_recovery(
                        "The selected LiFi route could not be executed",
                        "retry in a moment or request a fresh quote",
                    )
                ) from exc

            fill_time = format_fill_time(_safe_int(route_meta.get("fill_time_seconds"), 0))
            msg = (
                f"Bridge started: {lifi_result.input_amount} {lifi_result.token_symbol}. "
                f"From {lifi_result.source_chain_name} to {lifi_result.dest_chain_name}. "
                f"Estimated time: {fill_time}. "
                f"Tracking ID: {lifi_result.tx_hash}."
            )
            response = {
                "status": "pending",
                "tx_hash": lifi_result.tx_hash,
                "approve_hash": lifi_result.approve_hash,
                "protocol": "lifi",
                "token_symbol": lifi_result.token_symbol,
                "input_amount": str(lifi_result.input_amount),
                "output_amount": str(lifi_result.output_amount),
                "total_fee": str(safe_decimal(route_meta.get("total_fee")) or Decimal("0")),
                "total_fee_pct": str(
                    safe_decimal(route_meta.get("total_fee_pct")) or Decimal("0")
                ),
                "source_chain": lifi_result.source_chain_name,
                "dest_chain": lifi_result.dest_chain_name,
                "recipient": lifi_result.recipient,
                "estimated_fill_time": fill_time,
                "bridge_status": lifi_result.status,
                "fallback_used": False,
                "fallback_reason": None,
                "route_meta_used": True,
                "request_id": request_id,
                "message": msg,
            }
            if lifi_result.nonce is not None:
                response["nonce"] = lifi_result.nonce
            if lifi_result.raw_tx:
                response["raw_tx"] = lifi_result.raw_tx
            if lifi_result.tx_payload:
                response["tx_payload"] = lifi_result.tx_payload
            if lifi_result.bridge:
                response["bridge"] = lifi_result.bridge
            if lifi_result.from_chain_id is not None:
                response["from_chain_id"] = lifi_result.from_chain_id
            if lifi_result.to_chain_id is not None:
                response["to_chain_id"] = lifi_result.to_chain_id
            return await _store_bridge_response(response)

        if aggregator == "mayan" and planned_quote is not None:
            try:
                if source_is_solana:
                    mayan_result = await execute_mayan_bridge(
                        quote=planned_quote,
                        sub_org_id=sub_org_id,
                        sender=sender,
                        recipient=recipient,
                    )
                else:
                    async with wallet_lock(sender, source_chain_id):
                        mayan_result = await execute_mayan_bridge(
                            quote=planned_quote,
                            sub_org_id=sub_org_id,
                            sender=sender,
                            recipient=recipient,
                        )
            except NonRetryableError:
                raise
            except Exception as exc:
                await mark_transfer_failed(claim, error=str(exc))
                raise RuntimeError(
                    format_with_recovery(
                        "The selected Mayan route could not be executed",
                        "retry in a moment or request a fresh quote",
                    )
                ) from exc
            fill_time = format_fill_time(
                _safe_int(route_meta.get("fill_time_seconds"), 0)
            )
            msg = (
                f"Bridge started! Sending {mayan_result.input_amount} "
                f"{mayan_result.token_symbol} from {mayan_result.source_chain_name} "
                f"to {mayan_result.dest_chain_name} via Mayan ({mayan_result.route_type}). "
                f"Estimated time: {fill_time}."
            )
            return await _store_bridge_response(
                {
                    "status": "pending",
                    "tx_hash": mayan_result.tx_hash,
                    "approve_hash": mayan_result.approve_hash,
                    "protocol": "mayan",
                    "token_symbol": mayan_result.token_symbol,
                    "input_amount": str(mayan_result.input_amount),
                    "output_amount": str(mayan_result.output_amount),
                    "total_fee": str(
                        safe_decimal(route_meta.get("total_fee")) or Decimal("0")
                    ),
                    "total_fee_pct": str(
                        safe_decimal(route_meta.get("total_fee_pct")) or Decimal("0")
                    ),
                    "source_chain": mayan_result.source_chain_name,
                    "dest_chain": mayan_result.dest_chain_name,
                    "recipient": mayan_result.recipient,
                    "estimated_fill_time": fill_time,
                    "bridge_status": "pending",
                    "fallback_used": False,
                    "fallback_reason": None,
                    "route_meta_used": True,
                    "request_id": request_id,
                    "message": msg,
                }
            )

        if aggregator == "across" and planned_quote is not None:
            gas_price_gwei = float(
                await gas_price_cache.get_gwei(chain_id=source_chain_id)
            )
            async with wallet_lock(sender, source_chain_id):
                across_result = await execute_across_bridge(
                    planned_quote,
                    sub_org_id,
                    sender,
                    recipient,
                    gas_price_gwei,
                )
            fill_time = across_result.estimated_fill_time
            msg = (
                f"Bridge started: {across_result.input_amount} {across_result.token_symbol}. "
                f"From {across_result.source_chain_name} to {across_result.dest_chain_name}. "
                f"Estimated time: {fill_time}. "
                f"Tracking ID: {across_result.tx_hash}."
            )
            return await _store_bridge_response(
                {
                    "status": "pending",
                    "tx_hash": across_result.tx_hash,
                    "approve_hash": across_result.approve_hash,
                    "protocol": across_result.protocol,
                    "token_symbol": across_result.token_symbol,
                    "input_amount": str(across_result.input_amount),
                    "output_amount": str(across_result.output_amount),
                    "total_fee": str(planned_quote.total_fee),
                    "total_fee_pct": str(planned_quote.total_fee_pct),
                    "source_chain": across_result.source_chain_name,
                    "dest_chain": across_result.dest_chain_name,
                    "recipient": across_result.recipient,
                    "estimated_fill_time": fill_time,
                    "bridge_status": across_result.status,
                    "fallback_used": False,
                    "fallback_reason": None,
                    "route_meta_used": True,
                    "request_id": request_id,
                    "message": msg,
                }
            )

        if aggregator == "relay" and planned_quote is not None:
            async with wallet_lock(sender, source_chain_id):
                relay_result = await execute_relay_bridge(
                    planned_quote,
                    sub_org_id,
                    sender,
                    recipient,
                    timeout=300.0,
                )
            fill_time = format_fill_time(planned_quote.avg_fill_time_seconds)
            msg = (
                f"Bridge started: {relay_result.input_amount} {relay_result.token_symbol}. "
                f"From {relay_result.source_chain_name} to {relay_result.dest_chain_name}. "
                f"Estimated time: {fill_time}. "
                f"Tracking ID: {relay_result.tx_hash}."
            )
            return await _store_bridge_response(
                {
                    "status": "pending",
                    "tx_hash": relay_result.tx_hash,
                    "approve_hash": None,
                    "protocol": relay_result.protocol,
                    "token_symbol": relay_result.token_symbol,
                    "input_amount": str(relay_result.input_amount),
                    "output_amount": str(relay_result.output_amount),
                    "total_fee": str(planned_quote.total_fee),
                    "total_fee_pct": str(planned_quote.total_fee_pct),
                    "source_chain": relay_result.source_chain_name,
                    "dest_chain": relay_result.dest_chain_name,
                    "recipient": relay_result.recipient,
                    "estimated_fill_time": fill_time,
                    "bridge_status": relay_result.status,
                    "fallback_used": False,
                    "fallback_reason": None,
                    "route_meta_used": True,
                    "request_id": request_id,
                    "message": msg,
                    "relay_status": relay_result.relay_status,
                    "relay_request_id": relay_result.request_id,
                    "tx_hashes": relay_result.tx_hashes,
                }
            )

        if aggregator:
            await mark_transfer_failed(
                claim,
                error=f"The planned {aggregator} bridge route is incomplete",
            )
            raise NonRetryableError(
                format_with_recovery(
                    f"The planned {aggregator} bridge route is incomplete",
                    "request a fresh route and retry",
                )
            )

    # Across / Relay are EVM-only — skip them for Solana routes.
    routes = []
    dynamic_route_tuples: list[tuple[BridgeProtocolConfig, BridgeRoute]] = []
    dynamic_routes_task: asyncio.Task[list[BridgeRoute]] | None = None
    gas_price_task: asyncio.Task[Decimal] | None = None
    if not source_is_solana and not dest_is_solana:
        routes = get_routes(
            source_chain_id=source_chain_id,
            dest_chain_id=dest_chain_id,
            token_symbol=token_symbol,
        )
        dynamic_routes_task = asyncio.create_task(
            fetch_across_available_routes_async(
                source_chain_id,
                dest_chain_id,
                token_symbol,
                ACROSS,
            )
        )
        if any(route_tuple[1].protocol == "across" for route_tuple in routes):
            gas_price_task = asyncio.create_task(
                gas_price_cache.get_gwei(chain_id=source_chain_id)
            )

    # For Solana routes, use Mayan directly (no EVM simulators).
    # For EVM routes, use existing dynamic protocols (Relay) as usual.
    dynamic_protocols = (
        [] if source_is_solana or dest_is_solana else get_dynamic_protocols()
    )

    simulation_tasks: list[asyncio.Task[Any]] = []
    route_decimals = await asyncio.gather(
        *[_get_route_decimals(route_tuple[1], token_symbol) for route_tuple in routes]
    )
    for route_tuple, (input_decimals, output_decimals) in zip(routes, route_decimals):
        route = route_tuple[1]
        simulation_tasks.append(
            asyncio.create_task(
                _run_limited_bridge_simulation(
                    _simulate_route_async(
                        route_tuple,
                        amount,
                        sender,
                        recipient,
                        input_decimals,
                        output_decimals,
                    ),
                    label=f"route:{route.protocol}",
                )
            )
        )
    simulation_tasks.extend(
        asyncio.create_task(
            _run_limited_bridge_simulation(
                _simulate_dynamic_protocol_async(
                    protocol,
                    token_symbol,
                    source_chain_id,
                    dest_chain_id,
                    amount,
                    sender,
                    recipient,
                ),
                label=f"dynamic:{protocol.name}",
            )
        )
        for protocol in dynamic_protocols
    )

    if dynamic_routes_task is not None:
        try:
            dynamic_routes = await dynamic_routes_task
            dynamic_route_tuples = [(ACROSS, route) for route in dynamic_routes]
            routes.extend(dynamic_route_tuples)
        except Exception as exc:
            _LOGGER.warning("across available-routes lookup failed: %s", exc)

    # Check whether we have any source of quotes at all.
    has_mayan_route = source_is_solana or dest_is_solana
    if not routes and not dynamic_protocols and not has_mayan_route:
        await mark_transfer_failed(
            claim,
            error=(
                f"No bridge route found for {token_symbol} "
                f"from {source_chain_display} to {dest_chain_display}."
            ),
        )
        raise RuntimeError(
            f"No bridge route found for {token_symbol} "
            f"from {source_chain_display} to {dest_chain_display}. "
            "This token/chain pair is not currently supported."
        )

    dynamic_route_decimals = await asyncio.gather(
        *[
            _get_route_decimals(route_tuple[1], token_symbol)
            for route_tuple in dynamic_route_tuples
        ]
    )
    for route_tuple, (input_decimals, output_decimals) in zip(
        dynamic_route_tuples, dynamic_route_decimals
    ):
        route = route_tuple[1]
        simulation_tasks.append(
            asyncio.create_task(
                _run_limited_bridge_simulation(
                    _simulate_route_async(
                        route_tuple,
                        amount,
                        sender,
                        recipient,
                        input_decimals,
                        output_decimals,
                    ),
                    label=f"route:{route.protocol}",
                )
            )
        )
    simulation_results = await asyncio.gather(*simulation_tasks, return_exceptions=True)

    quotes: list[AcrossBridgeQuote | RelayBridgeQuote] = []
    simulation_errors: list[str] = []

    for sim_result in simulation_results:
        if isinstance(sim_result, Exception):
            simulation_errors.append(f"[UNEXPECTED_ERROR] {sim_result}")
            continue
        if isinstance(sim_result, AcrossSimulationError):
            simulation_errors.append(f"[{sim_result.reason}] {sim_result.message}")
            continue
        if isinstance(sim_result, RelaySimulationError):
            simulation_errors.append(f"[{sim_result.reason}] {sim_result.message}")
            continue
        if isinstance(sim_result, AcrossBridgeQuote):
            quotes.append(sim_result)
            continue
        if isinstance(sim_result, RelayBridgeQuote):
            _LOGGER.info(
                "relay_quote request_id=%s fees=%s",
                sim_result.request_id,
                sim_result.fees,
            )
            quotes.append(sim_result)

    mayan_quote: Any | None = None
    if source_is_solana or dest_is_solana:
        try:
            _mayan_agg = _get_mayan_aggregator()
            mayan_quote = await _mayan_agg.get_quote(
                token_symbol=token_symbol,
                source_chain_id=source_chain_id,
                dest_chain_id=dest_chain_id,
                source_chain_name=source_chain_display,
                dest_chain_name=dest_chain_display,
                amount=amount,
                sender=sender,
                recipient=recipient,
            )
            if mayan_quote:
                _LOGGER.info(
                    "mayan_quote out=%s fee_pct=%s eta=%ds",
                    mayan_quote.output_amount,
                    mayan_quote.total_fee_pct,
                    mayan_quote.estimated_fill_time_seconds,
                )
            else:
                mayan_reason = str(getattr(_mayan_agg, "last_error", "") or "").strip()
                if mayan_reason:
                    simulation_errors.append(f"[mayan] {mayan_reason}")
        except Exception as exc:
            simulation_errors.append(f"[mayan] {exc}")
            _LOGGER.warning("mayan quote failed: %s", exc)

    if not quotes and not mayan_quote:
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

    # For Solana routes: Mayan is the only executor.
    # For EVM routes: prefer Across, fall back to Relay.
    if mayan_quote:
        # Mayan wins for Solana routes — use it directly.
        quotes_sorted: list = []
        mayan_only = True
    else:
        mayan_only = False
        across_quotes = [q for q in quotes if q.protocol == "across"]
        relay_quotes = [q for q in quotes if q.protocol == "relay"]
        primary_quotes: list[AcrossBridgeQuote | RelayBridgeQuote]
        fallback_quotes: list[AcrossBridgeQuote | RelayBridgeQuote]

        if across_quotes:
            primary_quotes = sorted(
                across_quotes, key=lambda q: q.output_amount, reverse=True
            )
            fallback_quotes = sorted(
                relay_quotes, key=lambda q: q.output_amount, reverse=True
            )
        else:
            primary_quotes = sorted(
                relay_quotes, key=lambda q: q.output_amount, reverse=True
            )
            fallback_quotes = []

        quotes_sorted = primary_quotes + fallback_quotes
    fallback_reason: str | None = None
    used_quote: AcrossBridgeQuote | RelayBridgeQuote | None = None
    exec_result: AcrossBridgeResult | RelayBridgeResult | None = None
    execution_errors: list[str] = []
    estimated_fill_time: str | None = None
    approve_hash: str | None = None
    bridge_status: str | None = None
    relay_fees: dict | None = None
    request_id: str | None = None
    tx_hashes: list[str] | None = None
    relay_status: str | None = None

    gas_price_gwei: float | None = None
    if not mayan_only and any(q.protocol == "across" for q in quotes_sorted):
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
        quote: AcrossBridgeQuote | RelayBridgeQuote,
    ) -> tuple[
        AcrossBridgeResult | RelayBridgeResult,
        str,
        str | None,
        str | None,
        Optional[dict],
        Optional[str],
        Optional[list[str]],
        Optional[str],
    ]:
        if quote.protocol == "across":
            async with wallet_lock(sender, source_chain_id):
                across_result = await execute_across_bridge(
                    quote,
                    sub_org_id,
                    sender,
                    recipient,
                    gas_price_gwei,
                )
            return (
                across_result,
                across_result.estimated_fill_time,
                across_result.approve_hash,
                across_result.status,
                None,
                None,
                None,
                None,
            )
        if quote.protocol == "relay":
            relay_quote = cast(RelayBridgeQuote, quote)
            async with wallet_lock(sender, source_chain_id):
                relay_result = await execute_relay_bridge(
                    relay_quote,
                    sub_org_id,
                    sender,
                    recipient,
                    timeout=300.0,
                )
            return (
                relay_result,
                format_fill_time(relay_quote.avg_fill_time_seconds),
                None,
                relay_result.status,
                relay_quote.fees,
                relay_result.request_id,
                relay_result.tx_hashes,
                relay_result.relay_status,
            )
        await mark_transfer_failed(
            claim,
            error=f"Executor for protocol {quote.protocol!r} is not yet implemented.",
        )
        raise RuntimeError(
            f"Executor for protocol {quote.protocol!r} is not yet implemented."
        )

    # ── 6b. Execute Mayan for Solana routes ──────────────────────────────────
    if mayan_only and mayan_quote:
        try:
            mayan_result = await execute_mayan_bridge(
                quote=mayan_quote,
                sub_org_id=sub_org_id,
                sender=sender,
                recipient=recipient,
            )
        except NonRetryableError:
            raise
        except Exception as exc:
            await mark_transfer_failed(claim, error=str(exc))
            raise RuntimeError(
                format_with_recovery(
                    f"The Solana bridge didn't go through ({exc})",
                    "retry in a moment; if it keeps failing, request a fresh quote",
                )
            ) from exc

        fill_time = format_fill_time(mayan_quote.estimated_fill_time_seconds)
        msg = (
            f"Bridge started! Sending {mayan_result.input_amount} "
            f"{mayan_result.token_symbol} from {mayan_result.source_chain_name} "
            f"to {mayan_result.dest_chain_name} via Mayan ({mayan_result.route_type}). "
            f"Estimated time: {fill_time}."
        )
        return await _store_bridge_response(
            {
                "status": "pending",
                "tx_hash": mayan_result.tx_hash,
                "approve_hash": mayan_result.approve_hash,
                "protocol": "mayan",
                "token_symbol": mayan_result.token_symbol,
                "input_amount": str(mayan_result.input_amount),
                "output_amount": str(mayan_result.output_amount),
                "total_fee": str(mayan_quote.total_fee),
                "total_fee_pct": str(mayan_quote.total_fee_pct),
                "source_chain": mayan_result.source_chain_name,
                "dest_chain": mayan_result.dest_chain_name,
                "recipient": mayan_result.recipient,
                "estimated_fill_time": fill_time,
                "bridge_status": "pending",
                "fallback_used": False,
                "fallback_reason": None,
                "route_meta_used": False,
                "message": msg,
            }
        )

    primary_error: str | None = None
    for idx, quote in enumerate(quotes_sorted):
        try:
            if idx > 0:
                enforce_fallback_policy(
                    policy=fallback_policy,
                    detail=f"primary bridge execution failed; attempting {quote.protocol}",
                )
            (
                exec_result,
                estimated_fill_time,
                approve_hash,
                bridge_status,
                relay_fees,
                request_id,
                tx_hashes,
                relay_status,
            ) = await _execute_quote(quote)
            used_quote = quote
            if idx > 0 and primary_error:
                fallback_reason = f"Primary quote failed: {primary_error}"
            break
        except NonRetryableError:
            raise
        except Exception as exc:
            err_str = f"{quote.protocol}: {exc}"
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

    # ── 8. Build response ─────────────────────────────────────────────────────
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
        "route_meta_used": False,
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
    if request_id is not None:
        response["relay_request_id"] = request_id
    if tx_hashes is not None:
        response["tx_hashes"] = tx_hashes

    return await _store_bridge_response(response)
