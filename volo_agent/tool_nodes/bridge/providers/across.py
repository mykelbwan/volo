from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from config.bridge_registry import ACROSS, BridgeRoute
from core.token_security.registry_lookup import (
    get_native_decimals,
)
from core.token_security.registry_lookup import (
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
from tool_nodes.common.input_utils import format_with_recovery, safe_decimal

from .base import BridgeProvider, BridgeRequest
from .capabilities import provider_supports_request


def _route_meta_contains_untrusted_bridge_tx(route_meta: Mapping[str, Any]) -> bool:
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


async def _get_route_decimals(
    route: BridgeRoute,
    *,
    token_symbol: str,
) -> tuple[int, int] | None:
    symbol = str(token_symbol).strip().upper()

    if route.is_native_input:
        input_decimals = get_native_decimals(route.source_chain_id)
    else:
        input_decimals = await get_registry_decimals_by_address(
            route.input_token,
            route.source_chain_id,
        )
        if input_decimals is None:
            input_decimals = await get_registry_decimals_by_symbol(
                symbol,
                route.source_chain_id,
            )

    if route.is_native_output:
        output_decimals = get_native_decimals(route.dest_chain_id)
    else:
        output_decimals = await get_registry_decimals_by_address(
            route.output_token,
            route.dest_chain_id,
        )
        if output_decimals is None:
            output_decimals = await get_registry_decimals_by_symbol(
                symbol,
                route.dest_chain_id,
            )

    if input_decimals is None or output_decimals is None:
        return None
    return int(input_decimals), int(output_decimals)


class AcrossProvider(BridgeProvider):
    name = "across"

    def supports(self, request: BridgeRequest) -> bool:
        return provider_supports_request(self.name, request)

    async def quote_dynamic(self, request: BridgeRequest) -> Any | None:
        if request.source_is_solana or request.dest_is_solana:
            return None

        try:
            routes = await fetch_across_available_routes_async(
                request.source_chain_id,
                request.dest_chain_id,
                request.token_symbol,
                ACROSS,
            )
        except Exception:
            return None

        if not routes:
            return None

        quotes: list[AcrossBridgeQuote] = []
        for route in routes:
            try:
                decimals = await _get_route_decimals(
                    route,
                    token_symbol=request.token_symbol,
                )
            except Exception:
                continue
            if decimals is None:
                continue

            input_decimals, output_decimals = decimals
            try:
                simulation = await simulate_across_bridge_async(
                    route,
                    request.amount,
                    request.sender,
                    request.recipient,
                    input_decimals,
                    output_decimals,
                    ACROSS,
                )
            except Exception:
                continue
            if isinstance(simulation, AcrossBridgeQuote):
                quotes.append(simulation)
            elif isinstance(simulation, AcrossSimulationError):
                continue

        if not quotes:
            return None

        return sorted(
            quotes,
            key=lambda quote: (-quote.output_amount, int(quote.avg_fill_time_seconds)),
        )[0]

    def quote_from_route_meta(
        self,
        *,
        request: BridgeRequest,
        route_meta: Mapping[str, Any],
    ) -> AcrossBridgeQuote | None:
        aggregator = str(route_meta.get("aggregator") or "").strip().lower()
        if aggregator != "across":
            return None

        tool_data = route_meta.get("tool_data") or {}
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

    def validate_route_meta(
        self,
        *,
        request: BridgeRequest,
        route_meta: Mapping[str, Any],
    ) -> None:
        aggregator = str(route_meta.get("aggregator") or "").strip().lower()
        if aggregator not in {"", self.name}:
            return None

        if _route_meta_contains_untrusted_bridge_tx(route_meta):
            raise NonRetryableError("Untrusted precomputed transaction data is not allowed")

        token_symbol = str(request.token_symbol).strip().upper()
        source_chain_id = int(request.source_chain_id)
        dest_chain_id = int(request.dest_chain_id)
        amount = request.amount
        recipient = str(request.recipient).strip().lower()
        tool_data = route_meta.get("tool_data")
        planned_quote = (
            tool_data.get("planned_quote") if isinstance(tool_data, dict) else None
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
        ) != source_chain_id:
            raise NonRetryableError(
                format_with_recovery(
                    "The planned bridge metadata does not match the requested source chain",
                    "request a fresh route and retry",
                )
            )
        if route_meta.get("dest_chain_id") is not None and int(
            str(route_meta.get("dest_chain_id"))
        ) != dest_chain_id:
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
            ) != source_chain_id:
                raise NonRetryableError(
                    format_with_recovery(
                        "The planned bridge metadata does not match the requested source chain",
                        "request a fresh route and retry",
                    )
                )
            if planned_quote.get("dest_chain_id") is not None and int(
                str(planned_quote.get("dest_chain_id"))
            ) != dest_chain_id:
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
        if planned_recipient and planned_recipient != recipient:
            raise NonRetryableError(
                format_with_recovery(
                    "The planned bridge metadata does not match the requested recipient",
                    "request a fresh route and retry",
                )
            )
        return None

    async def execute(
        self,
        *,
        request: BridgeRequest,
        quote: Any,
        route_meta: Mapping[str, Any] | None = None,
    ) -> Any:
        if not isinstance(quote, AcrossBridgeQuote):
            raise NonRetryableError(
                format_with_recovery(
                    "Across execution requires a valid Across quote",
                    "request a fresh Across route and retry",
                )
            )

        from tool_nodes.bridge.executors.across_executor import execute_across_bridge

        gas_price_gwei: float | None = None
        if isinstance(route_meta, Mapping):
            raw_gas_price = route_meta.get("gas_price_gwei")
            if raw_gas_price is not None:
                try:
                    gas_price_gwei = float(raw_gas_price)
                except (TypeError, ValueError):
                    gas_price_gwei = None

        return await execute_across_bridge(
            quote,
            request.sub_org_id,
            request.sender,
            request.recipient,
            gas_price_gwei=gas_price_gwei,
        )
