from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from config.bridge_registry import RELAY, relay_api_base_url
from core.utils.errors import NonRetryableError
from tool_nodes.bridge.simulators.relay_simulator import (
    RelayBridgeQuote,
    RelaySimulationError,
    simulate_relay_bridge_async,
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


class RelayProvider(BridgeProvider):
    name = "relay"

    def supports(self, request: BridgeRequest) -> bool:
        return provider_supports_request(self.name, request)

    async def quote_dynamic(self, request: BridgeRequest) -> Any | None:
        if request.source_is_solana or request.dest_is_solana:
            return None
        try:
            simulation = await simulate_relay_bridge_async(
                request.token_symbol,
                request.source_chain_id,
                request.dest_chain_id,
                request.amount,
                request.sender,
                request.recipient,
                RELAY,
            )
        except Exception:
            return None
        if isinstance(simulation, RelayBridgeQuote):
            return simulation
        if isinstance(simulation, RelaySimulationError):
            return None
        return None

    def quote_from_route_meta(
        self,
        *,
        request: BridgeRequest,
        route_meta: Mapping[str, Any],
    ) -> RelayBridgeQuote | None:
        aggregator = str(route_meta.get("aggregator") or "").strip().lower()
        if aggregator != "relay":
            return None

        tool_data = route_meta.get("tool_data") or {}
        payload = (
            tool_data.get("planned_quote") if isinstance(tool_data, dict) else None
        )
        if not isinstance(payload, dict):
            return None

        source_chain_id = int(str(payload.get("source_chain_id")))
        dest_chain_id = int(str(payload.get("dest_chain_id")))
        return RelayBridgeQuote(
            protocol=str(payload.get("protocol") or "relay"),
            token_symbol=str(
                payload.get("token_symbol") or route_meta.get("token_symbol") or ""
            ),
            source_chain_id=source_chain_id,
            dest_chain_id=dest_chain_id,
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
                or relay_api_base_url(source_chain_id, dest_chain_id)
            ),
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
            raise NonRetryableError(
                "Untrusted precomputed transaction data is not allowed"
            )

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
        if (
            route_meta.get("source_chain_id") is not None
            and int(str(route_meta.get("source_chain_id"))) != source_chain_id
        ):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned bridge metadata does not match the requested source chain",
                    "request a fresh route and retry",
                )
            )
        if (
            route_meta.get("dest_chain_id") is not None
            and int(str(route_meta.get("dest_chain_id"))) != dest_chain_id
        ):
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
            if (
                planned_quote.get("source_chain_id") is not None
                and int(str(planned_quote.get("source_chain_id"))) != source_chain_id
            ):
                raise NonRetryableError(
                    format_with_recovery(
                        "The planned bridge metadata does not match the requested source chain",
                        "request a fresh route and retry",
                    )
                )
            if (
                planned_quote.get("dest_chain_id") is not None
                and int(str(planned_quote.get("dest_chain_id"))) != dest_chain_id
            ):
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
        if not isinstance(quote, RelayBridgeQuote):
            raise NonRetryableError(
                format_with_recovery(
                    "Relay execution requires a valid Relay quote",
                    "request a fresh Relay route and retry",
                )
            )

        from tool_nodes.bridge.executors.relay_executor import execute_relay_bridge

        return await execute_relay_bridge(
            quote,
            request.sub_org_id,
            request.sender,
            request.recipient,
            timeout=300.0,
        )
