from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from config.chains import find_chain_by_id
from config.solana_chains import get_solana_chain_by_id, is_solana_chain_id
from core.utils.errors import NonRetryableError
from core.routing.models import BridgeRouteQuote
from tool_nodes.common.input_utils import format_with_recovery, safe_decimal

from .base import BridgeProvider, BridgeRequest
from .capabilities import provider_supports_request

_MAYAN_AGGREGATOR: Any | None = None


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


def _is_hex_address(value: str | None) -> bool:
    if not value:
        return False
    addr = str(value).strip().lower()
    if not addr.startswith("0x") or len(addr) != 42:
        return False
    try:
        int(addr[2:], 16)
        return True
    except Exception:
        return False


def _expected_mayan_chain_id(chain_id: int) -> str | None:
    try:
        if is_solana_chain_id(chain_id):
            return str(
                get_solana_chain_by_id(chain_id).dexscreener_slug or "solana"
            ).lower()
        chain_cfg = find_chain_by_id(chain_id)
    except KeyError:
        return None
    slug = str(chain_cfg.dexscreener_slug or "").strip().lower()
    if slug:
        return slug
    name = str(chain_cfg.name or "").strip().lower()
    return name or None


def _get_mayan_aggregator() -> Any:
    global _MAYAN_AGGREGATOR
    if _MAYAN_AGGREGATOR is None:
        from core.routing.bridge.mayan import MayanAggregator

        _MAYAN_AGGREGATOR = MayanAggregator()
    return _MAYAN_AGGREGATOR


class MayanProvider(BridgeProvider):
    name = "mayan"

    def supports(self, request: BridgeRequest) -> bool:
        return provider_supports_request(self.name, request)

    async def quote_dynamic(self, request: BridgeRequest) -> Any | None:
        if not self.supports(request):
            return None
        try:
            mayan_agg = _get_mayan_aggregator()
        except Exception:
            return None
        if mayan_agg is None:
            return None
        try:
            quote = await mayan_agg.get_quote(
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
        if aggregator != "mayan" or not isinstance(tool_data, dict):
            return None

        return BridgeRouteQuote(
            aggregator="mayan",
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

        if not isinstance(tool_data, dict):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned Mayan route metadata is missing quote details",
                    "request a fresh route and retry",
                )
            )
        quote_id = str(tool_data.get("quoteId") or "").strip()
        if not quote_id:
            raise NonRetryableError(
                format_with_recovery(
                    "The planned Mayan route metadata is missing quote ID",
                    "request a fresh route and retry",
                )
            )

        expected_src_is_solana = is_solana_chain_id(source_chain_id)
        expected_dst_is_solana = is_solana_chain_id(dest_chain_id)
        if "srcIsSolana" in tool_data and bool(tool_data.get("srcIsSolana")) != bool(
            expected_src_is_solana
        ):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned Mayan route metadata does not match the requested source chain",
                    "request a fresh route and retry",
                )
            )
        if "dstIsSolana" in tool_data and bool(tool_data.get("dstIsSolana")) != bool(
            expected_dst_is_solana
        ):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned Mayan route metadata does not match the requested destination chain",
                    "request a fresh route and retry",
                )
            )

        expected_from_chain = _expected_mayan_chain_id(source_chain_id)
        expected_to_chain = _expected_mayan_chain_id(dest_chain_id)
        planned_from_chain = str(tool_data.get("fromChain") or "").strip().lower()
        planned_to_chain = str(tool_data.get("toChain") or "").strip().lower()
        if (
            planned_from_chain
            and expected_from_chain
            and planned_from_chain != expected_from_chain
        ):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned Mayan route metadata does not match the requested source chain",
                    "request a fresh route and retry",
                )
            )
        if (
            planned_to_chain
            and expected_to_chain
            and planned_to_chain != expected_to_chain
        ):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned Mayan route metadata does not match the requested destination chain",
                    "request a fresh route and retry",
                )
            )

        from_token = str(tool_data.get("fromToken") or "").strip()
        to_token = str(tool_data.get("toToken") or "").strip()
        if not expected_src_is_solana:
            if not from_token:
                raise NonRetryableError(
                    format_with_recovery(
                        "The planned Mayan route metadata is missing the source token",
                        "request a fresh route and retry",
                    )
                )
            if not _is_hex_address(from_token):
                raise NonRetryableError(
                    format_with_recovery(
                        "The planned Mayan route metadata has an invalid source token address",
                        "request a fresh route and retry",
                    )
                )
        if not expected_dst_is_solana and to_token and not _is_hex_address(to_token):
            raise NonRetryableError(
                format_with_recovery(
                    "The planned Mayan route metadata has an invalid destination token address",
                    "request a fresh route and retry",
                )
            )

        destination_address = (
            str(tool_data.get("destinationAddress") or "").strip().lower()
        )
        if destination_address and destination_address != recipient:
            raise NonRetryableError(
                format_with_recovery(
                    "The planned Mayan route metadata does not match the requested recipient",
                    "request a fresh route and retry",
                )
            )

        raw_route = tool_data.get("rawRoute")
        if isinstance(raw_route, dict):
            raw_quote_id = str(raw_route.get("quoteId") or "").strip()
            if raw_quote_id and raw_quote_id != quote_id:
                raise NonRetryableError(
                    format_with_recovery(
                        "The planned Mayan route metadata is internally inconsistent",
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
        is_mayan_quote = isinstance(quote, BridgeRouteQuote) and str(
            quote.aggregator or ""
        ).strip().lower() == self.name
        if not is_mayan_quote:
            raise NonRetryableError(
                format_with_recovery(
                    "Mayan execution requires a valid Mayan quote",
                    "request a fresh Mayan route and retry",
                )
            )

        from tool_nodes.bridge.executors.mayan_executor import execute_mayan_bridge

        return await execute_mayan_bridge(
            quote=quote,
            sub_org_id=request.sub_org_id,
            sender=request.sender,
            recipient=request.recipient,
        )
