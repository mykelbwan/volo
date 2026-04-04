from __future__ import annotations

import asyncio
import logging
import os
from decimal import Decimal
from functools import partial
from typing import Any, Dict, List, Optional

from config.chains import CHAINS
from core.routing.bridge.base import BridgeAggregator
from core.routing.bridge.token_resolver import resolve_bridge_token
from core.routing.models import BridgeRouteQuote
from core.utils.async_tools import run_blocking
from core.utils.http import ExternalServiceError, raise_for_status, request_json
from core.utils.timeouts import EXTERNAL_HTTP_TIMEOUT_SECONDS

_LOGGER = logging.getLogger("volo.routing.bridge.socket")
_API_BASE_URL = "https://api.socket.tech"
_QUOTE_ENDPOINT = "/v2/quote"
_BUILD_TX_ENDPOINT = "/v2/build-tx"

_NATIVE_TOKEN_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeeeE"

# Maximum number of routes to request from Socket.
# We only need the best one, but requesting a few gives us fallback options
# if the top route's calldata build fails.
_MAX_ROUTES = 3

# Chain IDs supported by Socket v2 are now derived from the global registry.
SUPPORTED_CHAIN_IDS: frozenset[int] = frozenset(CHAINS.keys())


def _api_key() -> Optional[str]:
    return os.getenv("SOCKET_API_KEY", "").strip() or None


def _auth_headers() -> Dict[str, str]:
    key = _api_key()
    if not key:
        return {}
    return {
        "API-KEY": key,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _from_smallest_unit(amount_raw: Any, decimals: int) -> Decimal:
    """
    Convert a raw amount string/int (smallest unit) to human-readable units.
    Returns Decimal("0") on any parse failure.
    """
    if amount_raw is None:
        return Decimal("0")
    try:
        # If it's already a float or Decimal, handle it directly.
        if isinstance(amount_raw, (float, Decimal)):
            return Decimal(str(amount_raw)) / Decimal(10**decimals)
        # Otherwise, assume it's a string/int representing the smallest unit.
        return Decimal(str(int(float(str(amount_raw))))) / Decimal(10**decimals)
    except (ValueError, TypeError):
        return Decimal("0")


def _extract_fill_time_seconds(route: Dict[str, Any]) -> int:
    fill_time_seconds = 0
    raw_service_time = route.get("serviceTime")
    if raw_service_time is not None:
        try:
            fill_time_seconds = int(float(str(raw_service_time)))
        except (ValueError, TypeError):
            pass

    if fill_time_seconds == 0:
        raw_max_service = route.get("maxServiceTime")
        if raw_max_service is not None:
            try:
                fill_time_seconds = int(float(str(raw_max_service)))
            except (ValueError, TypeError):
                pass

    return fill_time_seconds


def _parse_route_data(
    route: Dict[str, Any], input_amount: Decimal, dest_decimals: int
) -> Dict[str, Any]:
    """
    Extract common fields from a Socket route object.
    Returns a dict with: output_amount, total_fee, total_fee_pct, fill_time, gas_cost_usd, bridge_name.
    """
    raw_to_amount = route.get("toAmount")
    # Security/Logic: Avoid falling back to USD values for token amounts.
    # Only use toAmount or toAmountMinimum if they represent the actual token.
    if raw_to_amount is None:
        raw_to_amount = route.get("toAmountMinimum")

    output_amount = _from_smallest_unit(raw_to_amount, dest_decimals)
    fill_time = _extract_fill_time_seconds(route)

    # Fee computation
    total_fee = input_amount - output_amount if output_amount > 0 else Decimal("0")
    if total_fee < 0:
        total_fee = Decimal("0")

    total_fee_pct = (
        (total_fee / input_amount * Decimal("100"))
        if input_amount > 0
        else Decimal("0")
    )
    total_fee_pct = max(Decimal("0"), min(Decimal("100"), total_fee_pct))

    # Gas cost extraction (optional/non-blocking)
    gas_cost_usd: Optional[Decimal] = None
    total_gas_usd = route.get("totalGasFeesInUsd")
    if total_gas_usd is not None:
        try:
            gas_cost_usd = Decimal(str(total_gas_usd))
        except (ValueError, TypeError):
            pass

    used_bridge_names = route.get("usedBridgeNames") or []
    bridge_name = ", ".join(used_bridge_names) if used_bridge_names else "unknown"

    return {
        "output_amount": output_amount,
        "total_fee": total_fee,
        "total_fee_pct": total_fee_pct,
        "fill_time": fill_time,
        "gas_cost_usd": gas_cost_usd,
        "bridge_name": bridge_name,
        "used_bridge_names": used_bridge_names,
    }


# ---------------------------------------------------------------------------
# Synchronous HTTP helpers (called via run_blocking)
# ---------------------------------------------------------------------------


def _fetch_quote(
    from_chain_id: int,
    to_chain_id: int,
    from_token_address: str,
    to_token_address: str,
    from_amount_wei: int,
    user_address: str,
    recipient_address: str,
    timeout: float,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "fromChainId": from_chain_id,
        "toChainId": to_chain_id,
        "fromTokenAddress": from_token_address,
        "toTokenAddress": to_token_address,
        "fromAmount": str(from_amount_wei),
        "userAddress": user_address,
        "recipient": recipient_address,
        # Sort by maximum output to the destination wallet.
        "sort": "output",
        # Enable bridges and DEXes.  Empty value = allow all.
        "bridgeWithGas": "false",
        "maxUserTxs": str(_MAX_ROUTES),
        "singleTxOnly": "false",  # allow multi-tx routes for better pricing
        "isContractCall": "false",
        "defaultSwapSlippage": "0.5",
    }

    resp = request_json(
        "GET",
        f"{_API_BASE_URL}{_QUOTE_ENDPOINT}",
        params=params,
        headers=_auth_headers(),
        timeout=timeout,
        service="socket-quote",
    )
    raise_for_status(resp, "socket-quote")
    return resp.json()


def _fetch_build_tx(
    route: Dict[str, Any],
    timeout: float,
) -> Dict[str, Any]:
    payload = {"route": route}
    resp = request_json(
        "POST",
        f"{_API_BASE_URL}{_BUILD_TX_ENDPOINT}",
        json=payload,
        headers=_auth_headers(),
        timeout=timeout,
        service="socket-build-tx",
    )
    raise_for_status(resp, "socket-build-tx")
    return resp.json()


class SocketAggregator(BridgeAggregator):
    name: str = "socket"
    TIMEOUT_SECONDS: float = 8.0

    async def get_quote(
        self,
        *,
        token_symbol: str,
        source_chain_id: int,
        dest_chain_id: int,
        source_chain_name: str,
        dest_chain_name: str,
        amount: Decimal,
        sender: str,
        recipient: str,
    ) -> Optional[BridgeRouteQuote]:
        if not _api_key():
            self._log_debug("SOCKET_API_KEY not set — skipping")
            return None

        # ── Guard: same-chain bridge is not meaningful ────────────────────
        if source_chain_id == dest_chain_id:
            self._log_debug(
                f"source_chain_id == dest_chain_id ({source_chain_id}) — skipping"
            )
            return None

        # ── Guard: unsupported chains ─────────────────────────────────────
        if source_chain_id not in SUPPORTED_CHAIN_IDS:
            self._log_debug(
                f"source_chain_id {source_chain_id} not in Socket supported chains"
            )
            return None
        if dest_chain_id not in SUPPORTED_CHAIN_IDS:
            self._log_debug(
                f"dest_chain_id {dest_chain_id} not in Socket supported chains"
            )
            return None

        symbol = token_symbol.strip().upper()

        # ── Resolve token addresses + decimals via registry/Dexscreener ───
        source_token, dest_token = await asyncio.gather(
            resolve_bridge_token(
                symbol,
                chain_id=source_chain_id,
                chain_name=source_chain_name,
            ),
            resolve_bridge_token(
                symbol,
                chain_id=dest_chain_id,
                chain_name=dest_chain_name,
            ),
        )
        if source_token is None or dest_token is None:
            self._log_failure("token resolution failed for Socket quote")
            return None

        from_token_address = (
            _NATIVE_TOKEN_ADDRESS if source_token.is_native else source_token.address
        )
        to_token_address = (
            _NATIVE_TOKEN_ADDRESS if dest_token.is_native else dest_token.address
        )

        src_decimals = int(source_token.decimals)
        dest_decimals = int(dest_token.decimals)

        # Convert amount to smallest unit.
        from_amount_wei = int(amount * Decimal(10**src_decimals))
        if from_amount_wei <= 0:
            self._log_failure("from_amount_wei is zero or negative after conversion")
            return None

        timeout = min(self.TIMEOUT_SECONDS, EXTERNAL_HTTP_TIMEOUT_SECONDS)

        # ── Phase 1: fetch routes ─────────────────────────────────────────
        try:
            quote_data = await run_blocking(
                partial(
                    _fetch_quote,
                    source_chain_id,
                    dest_chain_id,
                    from_token_address,
                    to_token_address,
                    from_amount_wei,
                    sender,
                    recipient,
                    timeout,
                )
            )
        except ExternalServiceError as exc:
            self._log_failure("quote API error", exc)
            return None
        except Exception as exc:
            self._log_failure("quote unexpected error", exc)
            return None

        # ── Parse response ────────────────────────────────────────────────
        if not quote_data.get("success"):
            err_msg = quote_data.get("message", "unknown error")
            self._log_failure(f"quote API failed: {err_msg}")
            return None

        result = quote_data.get("result") or {}
        routes: List[Dict[str, Any]] = result.get("routes") or []

        if not routes:
            return None

        # ── Phase 2: Select best route and build calldata ─────────────────
        calldata: Optional[str] = None
        to_contract: Optional[str] = None
        build_tx_result: Dict[str, Any] = {}
        selected_route: Dict[str, Any] = routes[0]

        # Try up to _MAX_ROUTES times.
        for candidate_route in routes[:_MAX_ROUTES]:
            try:
                build_response = await run_blocking(
                    partial(_fetch_build_tx, candidate_route, timeout)
                )
            except Exception:
                continue

            if not build_response.get("success"):
                continue

            raw_build_result = build_response.get("result")
            if not isinstance(raw_build_result, dict):
                continue

            calldata = raw_build_result.get("txData")
            to_contract = raw_build_result.get("txTarget")

            if calldata and to_contract:
                build_tx_result = raw_build_result
                selected_route = candidate_route
                break
        else:
            self._log_debug(
                "build-tx failed for all routes — returning without calldata"
            )

        # ── Finalize Quote Data ───────────────────────────────────────────
        parsed = _parse_route_data(selected_route, amount, dest_decimals)

        # Extract native 'value' from build result if present (security/correctness)
        native_value_raw = build_tx_result.get("value")
        native_value: Optional[Decimal] = None
        if native_value_raw is not None:
            try:
                native_value = Decimal(str(int(native_value_raw)))
            except (ValueError, TypeError):
                pass

        self._log_debug(
            f"quote ok via {parsed['bridge_name']}: "
            f"{source_chain_name}→{dest_chain_name} "
            f"out={parsed['output_amount']:.6f} fee={parsed['total_fee_pct']:.2f}% "
            f"calldata={'yes' if calldata else 'no'}"
        )

        return BridgeRouteQuote(
            aggregator=self.name,
            token_symbol=symbol,
            source_chain_id=source_chain_id,
            dest_chain_id=dest_chain_id,
            source_chain_name=source_chain_name,
            dest_chain_name=dest_chain_name,
            input_amount=amount,
            output_amount=parsed["output_amount"],
            total_fee=parsed["total_fee"],
            total_fee_pct=parsed["total_fee_pct"],
            estimated_fill_time_seconds=parsed["fill_time"],
            calldata=calldata,
            to=to_contract,
            tool_data={
                "route": selected_route,
                "buildTxResult": build_tx_result,
                "usedBridgeNames": parsed["used_bridge_names"],
                "allRouteCount": len(routes),
                "nativeValue": str(native_value) if native_value else None,
                "gasCostUsd": str(parsed["gas_cost_usd"])
                if parsed["gas_cost_usd"]
                else None,
            },
            raw=quote_data,
        )
