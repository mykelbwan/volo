from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from config.solana_chains import is_solana_chain_id
from core.utils.errors import NonRetryableError
from core.routing.models import BridgeRouteQuote
from tool_nodes.common.input_utils import format_with_recovery, safe_decimal

from .base import BridgeProvider, BridgeRequest
from .capabilities import provider_supports_request

_LIFI_AGGREGATOR: Any | None = None


def _get_lifi_aggregator() -> Any:
    global _LIFI_AGGREGATOR
    if _LIFI_AGGREGATOR is None:
        from core.routing.bridge.lifi import LiFiAggregator

        _LIFI_AGGREGATOR = LiFiAggregator()
    return _LIFI_AGGREGATOR


def _route_meta_from_lifi_quote(quote: BridgeRouteQuote) -> dict[str, Any]:
    return {
        "aggregator": "lifi",
        "token_symbol": quote.token_symbol,
        "source_chain_id": quote.source_chain_id,
        "dest_chain_id": quote.dest_chain_id,
        "source_chain": quote.source_chain_name,
        "target_chain": quote.dest_chain_name,
        "input_amount": str(quote.input_amount),
        "output_amount": str(quote.output_amount),
        "total_fee": str(quote.total_fee),
        "total_fee_pct": str(quote.total_fee_pct),
        "fill_time_seconds": int(quote.estimated_fill_time_seconds),
        "calldata": quote.calldata,
        "to": quote.to,
        "tool_data": quote.tool_data or {},
    }


class LiFiProvider(BridgeProvider):
    name = "lifi"

    def supports(self, request: BridgeRequest) -> bool:
        return provider_supports_request(self.name, request)

    async def quote_dynamic(self, request: BridgeRequest) -> Any | None:
        if not self.supports(request):
            return None
        try:
            lifi_agg = _get_lifi_aggregator()
        except Exception:
            return None
        if lifi_agg is None:
            return None
        try:
            quote = await lifi_agg.get_quote(
                token_symbol=request.token_symbol,
                source_chain_id=request.source_chain_id,
                dest_chain_id=request.dest_chain_id,
                source_chain_name=request.source_chain_name,
                dest_chain_name=request.dest_chain_name,
                amount=request.amount,
                sender=request.sender,
                recipient=request.recipient,
            )
        except Exception:
            return None
        if isinstance(quote, BridgeRouteQuote):
            return quote
        return None

    def quote_from_route_meta(
        self,
        *,
        request: BridgeRequest,
        route_meta: Mapping[str, Any],
    ) -> BridgeRouteQuote | None:
        aggregator = str(route_meta.get("aggregator") or "").strip().lower()
        tool_data = route_meta.get("tool_data")
        if aggregator != "lifi" or not isinstance(tool_data, dict):
            return None

        return BridgeRouteQuote(
            aggregator="lifi",
            token_symbol=str(route_meta.get("token_symbol") or ""),
            source_chain_id=int(str(route_meta.get("source_chain_id"))),
            dest_chain_id=int(str(route_meta.get("dest_chain_id"))),
            source_chain_name=str(route_meta.get("source_chain") or ""),
            dest_chain_name=str(route_meta.get("target_chain") or ""),
            input_amount=safe_decimal(route_meta.get("input_amount")) or Decimal("0"),
            output_amount=safe_decimal(route_meta.get("output_amount")) or Decimal("0"),
            total_fee=safe_decimal(route_meta.get("total_fee")) or Decimal("0"),
            total_fee_pct=safe_decimal(route_meta.get("total_fee_pct")) or Decimal("0"),
            estimated_fill_time_seconds=int(route_meta.get("fill_time_seconds") or 0),
            gas_cost_source=None,
            calldata=route_meta.get("calldata"),
            to=route_meta.get("to"),
            tool_data=tool_data,
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

        token_symbol = str(request.token_symbol).strip().upper()
        source_chain_id = int(request.source_chain_id)
        dest_chain_id = int(request.dest_chain_id)
        amount = request.amount
        recipient = str(request.recipient).strip().lower()
        tool_data = route_meta.get("tool_data")
        planned_quote = (
            tool_data.get("planned_quote") if isinstance(tool_data, dict) else None
        )

        tx_request = (
            tool_data.get("transactionRequest") if isinstance(tool_data, dict) else None
        )
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
            if tx_chain_id is None or tx_chain_id != source_chain_id:
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
        is_lifi_quote = isinstance(quote, BridgeRouteQuote) and str(
            quote.aggregator or ""
        ).strip().lower() == self.name
        if not is_lifi_quote:
            raise NonRetryableError(
                format_with_recovery(
                    "LiFi execution requires a valid LiFi quote",
                    "request a fresh LiFi route and retry",
                )
            )

        from tool_nodes.bridge.executors.lifi_executor import execute_lifi_bridge

        output_amount = quote.output_amount if quote.output_amount > 0 else request.amount
        exec_kwargs: dict[str, Any] = {
            "route_meta": _route_meta_from_lifi_quote(quote),
            "token_symbol": request.token_symbol,
            "source_chain_id": request.source_chain_id,
            "dest_chain_id": request.dest_chain_id,
            "source_chain_name": request.source_chain_name,
            "dest_chain_name": request.dest_chain_name,
            "input_amount": request.amount,
            "output_amount": output_amount,
            "sub_org_id": request.sub_org_id,
            "sender": request.sender,
            "recipient": request.recipient,
        }
        if request.source_is_solana:
            exec_kwargs["solana_network"] = str(request.source_chain_name).strip().lower()

        return await execute_lifi_bridge(**exec_kwargs)
