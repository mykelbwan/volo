from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import requests

from config.bridge_registry import ACROSS, BridgeProtocolConfig, BridgeRoute
from config.chains import get_chain_by_id
from core.utils.http import (
    ExternalServiceError,
    async_raise_for_status,
    async_request_json,
    raise_for_status,
    request_json,
)
from core.utils.timeouts import EXTERNAL_HTTP_TIMEOUT_SECONDS
from tool_nodes.common.input_utils import safe_decimal
from tool_nodes.bridge.executors.evm_utils import to_raw
from tool_nodes.bridge.simulators.utils import now

_QUOTE_ENDPOINT = "/suggested-fees"
_AVAILABLE_ROUTES_ENDPOINT = "/available-routes"
_TESTNET_API_BASE_URL = "https://testnet.across.to/api"
_REQUEST_TIMEOUT = EXTERNAL_HTTP_TIMEOUT_SECONDS
_AVAILABLE_ROUTES_CACHE_TTL_SECONDS = 300
_available_routes_cache: dict[
    tuple[str, int, int, str], tuple[float, list[BridgeRoute]]
] = {}

_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _across_api_base_url(is_testnet: bool, protocol: BridgeProtocolConfig) -> str:
    if is_testnet:
        return _TESTNET_API_BASE_URL
    return protocol.api_base_url


def _is_address(value: str | None) -> bool:
    if not value:
        return False
    v = value.strip().lower()
    return v.startswith("0x") and len(v) == 42


def _safe_int(value: object, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _parse_available_routes(
    raw_routes: object,
    *,
    source_chain,
    dest_chain_config,
    symbol: str,
    source_chain_id: int,
    dest_chain_id: int,
) -> list[BridgeRoute]:
    if not isinstance(raw_routes, list):
        raw_routes = []

    routes: list[BridgeRoute] = []
    for entry in raw_routes:
        if not isinstance(entry, dict):
            continue
        origin_chain = _safe_int(entry.get("originChainId"))
        dest_chain_id_entry = _safe_int(entry.get("destinationChainId"))
        if origin_chain is None or dest_chain_id_entry is None:
            continue
        if origin_chain != source_chain_id or dest_chain_id_entry != dest_chain_id:
            continue

        is_native = bool(entry.get("isNative", False))
        origin_symbol = str(entry.get("originTokenSymbol", "")).upper()
        dest_symbol = str(entry.get("destinationTokenSymbol", "")).upper()
        if symbol and symbol != "*":
            if origin_symbol == symbol and dest_symbol == symbol:
                pass
            elif is_native and (
                symbol == source_chain.native_symbol.upper()
                or symbol == dest_chain_config.native_symbol.upper()
            ):
                pass
            else:
                continue
        origin_token = str(entry.get("originToken", "")).strip()
        dest_token = str(entry.get("destinationToken", "")).strip()

        if is_native:
            if source_chain.wrapped_native:
                origin_token = source_chain.wrapped_native
            else:
                origin_token = _ZERO_ADDRESS
            if dest_chain_config.wrapped_native:
                dest_token = dest_chain_config.wrapped_native
            else:
                dest_token = _ZERO_ADDRESS
        elif not _is_address(origin_token) or not _is_address(dest_token):
            continue

        routes.append(
            BridgeRoute(
                protocol="across",
                source_chain_id=origin_chain,
                dest_chain_id=dest_chain_id_entry,
                token_symbol=origin_symbol or symbol,
                source_contract=_ZERO_ADDRESS,
                dest_contract=_ZERO_ADDRESS,
                input_token=origin_token,
                output_token=dest_token,
                is_native_input=is_native,
                is_native_output=is_native,
            )
        )

    return routes


@dataclass
class AcrossBridgeQuote:
    protocol: str
    token_symbol: str
    input_token: str
    output_token: str
    source_chain_id: int
    dest_chain_id: int
    source_chain_name: str
    dest_chain_name: str
    input_amount: Decimal
    output_amount: Decimal
    total_fee: Decimal
    total_fee_pct: Decimal
    lp_fee: Decimal
    relayer_fee: Decimal
    gas_fee: Decimal
    input_decimals: int
    output_decimals: int
    quote_timestamp: int
    fill_deadline: int
    exclusivity_deadline: int
    exclusive_relayer: str
    spoke_pool: str
    is_native_input: bool
    avg_fill_time_seconds: int


@dataclass
class AcrossSimulationError:
    reason: str
    message: str


def _from_raw(raw: int, decimals: int) -> Decimal:
    return Decimal(raw) / Decimal(10**decimals)


def _build_quote_from_response(
    data: dict,
    *,
    route: BridgeRoute,
    amount_decimal: Decimal,
    token_decimals: int,
    output_token_decimals: int,
    source_chain,
    dest_chain,
    protocol: BridgeProtocolConfig,
) -> AcrossBridgeQuote | AcrossSimulationError:
    # Across returns all fee components and the final output amount.
    # All amounts from the API are in raw token units (wei-scale).
    try:
        output_amount_raw = int(data["outputAmount"])
        total_fee_raw = int(
            (data.get("totalRelayFee") or {}).get("total", data.get("relayFeeTotal", 0))
        )
        lp_fee_raw = int(
            (data.get("lpFee") or {}).get("total", data.get("lpFeeTotal", 0))
        )
        relayer_fee_raw = int(
            (data.get("relayerCapitalFee") or data.get("relayerFee") or {}).get(
                "total", data.get("capitalFeeTotal", 0)
            )
        )
        gas_fee_raw = int(
            (data.get("relayerGasFee") or data.get("destinationGasFee") or {}).get(
                "total", data.get("relayGasFeeTotal", 0)
            )
        )
        quote_timestamp = int(data["timestamp"])
        fill_deadline = int(
            data.get("fillDeadline", int(time.time()) + 18000)  # 5h default
        )
        exclusivity_deadline = int(data.get("exclusivityDeadline", 0))
        exclusive_relayer = data.get(
            "exclusiveRelayer", "0x0000000000000000000000000000000000000000"
        )
        expected_fill_time = _safe_int(data.get("expectedFillTimeSec"))

        input_token_info = data.get("inputToken", {}) if isinstance(data, dict) else {}
        output_token_info = (
            data.get("outputToken", {}) if isinstance(data, dict) else {}
        )
        input_decimals = _safe_int(input_token_info.get("decimals"), token_decimals)
        output_decimals = _safe_int(
            output_token_info.get("decimals"), output_token_decimals
        )
        input_decimals = input_decimals or token_decimals
        output_decimals = output_decimals or output_token_decimals

    except (KeyError, ValueError, TypeError, AttributeError) as e:
        return AcrossSimulationError(
            reason="RESPONSE_PARSE_ERROR",
            message=(f"Could not parse Across API response: {e}. Raw response: {data}"),
        )

    if output_amount_raw <= 0:
        return AcrossSimulationError(
            reason="NO_LIQUIDITY",
            message=(
                f"Across has no liquidity for {route.token_symbol} "
                f"from chain {source_chain.name} to {dest_chain.name} "
                f"for amount {amount_decimal}. "
                "Try a smaller amount or a different route."
            ),
        )

    output_amount_preview = _from_raw(output_amount_raw, output_decimals)
    if output_amount_preview >= amount_decimal * 2:
        return AcrossSimulationError(
            reason="SUSPICIOUS_QUOTE",
            message=(
                "Across returned a suspiciously large output amount. "
                "This may indicate a stale or malformed quote. Please retry."
            ),
        )

    output_amount = output_amount_preview
    total_fee = _from_raw(total_fee_raw, input_decimals)
    lp_fee = _from_raw(lp_fee_raw, input_decimals)
    relayer_fee = _from_raw(relayer_fee_raw, input_decimals)
    gas_fee = _from_raw(gas_fee_raw, input_decimals)

    total_fee_pct = (
        (total_fee / amount_decimal * Decimal(100)).quantize(Decimal("0.0001"))
        if amount_decimal > 0
        else Decimal(0)
    )

    return AcrossBridgeQuote(
        protocol="across",
        token_symbol=route.token_symbol,
        input_token=route.input_token,
        output_token=route.output_token,
        source_chain_id=route.source_chain_id,
        dest_chain_id=route.dest_chain_id,
        source_chain_name=source_chain.name,
        dest_chain_name=dest_chain.name,
        input_amount=amount_decimal,
        output_amount=output_amount,
        total_fee=total_fee,
        total_fee_pct=total_fee_pct,
        lp_fee=lp_fee,
        relayer_fee=relayer_fee,
        gas_fee=gas_fee,
        input_decimals=input_decimals,
        output_decimals=output_decimals,
        quote_timestamp=quote_timestamp,
        fill_deadline=fill_deadline,
        exclusivity_deadline=exclusivity_deadline,
        exclusive_relayer=exclusive_relayer,
        spoke_pool=str(data.get("spokePoolAddress") or route.source_contract),
        is_native_input=route.is_native_input,
        avg_fill_time_seconds=expected_fill_time or protocol.avg_fill_time_seconds,
    )


def fetch_across_available_routes(
    source_chain_id: int,
    dest_chain_id: int,
    token_symbol: str,
    protocol: BridgeProtocolConfig = ACROSS,
) -> list[BridgeRoute]:
    symbol = token_symbol.upper().strip()
    try:
        source_chain = get_chain_by_id(source_chain_id)
        dest_chain_config = get_chain_by_id(dest_chain_id)
    except KeyError:
        return []

    base_url = _across_api_base_url(
        source_chain.is_testnet or dest_chain_config.is_testnet, protocol
    )
    cache_key = (base_url, source_chain_id, dest_chain_id, symbol)
    cached = _available_routes_cache.get(cache_key)
    if cached:
        ts, routes = cached
        if now() - ts < _AVAILABLE_ROUTES_CACHE_TTL_SECONDS:
            return routes

    response = request_json(
        "GET",
        f"{base_url}{_AVAILABLE_ROUTES_ENDPOINT}",
        params={
            "originChainId": source_chain_id,
            "destinationChainId": dest_chain_id,
        },
        timeout=_REQUEST_TIMEOUT,
        service="across",
    )
    raise_for_status(response, "across")
    data = response.json()
    raw_routes = data.get("availableRoutes") if isinstance(data, dict) else data
    routes = _parse_available_routes(
        raw_routes,
        source_chain=source_chain,
        dest_chain_config=dest_chain_config,
        symbol=symbol,
        source_chain_id=source_chain_id,
        dest_chain_id=dest_chain_id,
    )

    _available_routes_cache[cache_key] = (now(), routes)
    return routes


async def fetch_across_available_routes_async(
    source_chain_id: int,
    dest_chain_id: int,
    token_symbol: str,
    protocol: BridgeProtocolConfig = ACROSS,
) -> list[BridgeRoute]:
    symbol = token_symbol.upper().strip()
    try:
        source_chain = get_chain_by_id(source_chain_id)
        dest_chain_config = get_chain_by_id(dest_chain_id)
    except KeyError:
        return []

    base_url = _across_api_base_url(
        source_chain.is_testnet or dest_chain_config.is_testnet, protocol
    )
    cache_key = (base_url, source_chain_id, dest_chain_id, symbol)
    cached = _available_routes_cache.get(cache_key)
    if cached:
        ts, routes = cached
        if now() - ts < _AVAILABLE_ROUTES_CACHE_TTL_SECONDS:
            return routes

    response = await async_request_json(
        "GET",
        f"{base_url}{_AVAILABLE_ROUTES_ENDPOINT}",
        params={
            "originChainId": source_chain_id,
            "destinationChainId": dest_chain_id,
        },
        timeout=_REQUEST_TIMEOUT,
        service="across",
    )
    await async_raise_for_status(response, "across")
    data = response.json()
    raw_routes = data.get("availableRoutes") if isinstance(data, dict) else data
    routes = _parse_available_routes(
        raw_routes,
        source_chain=source_chain,
        dest_chain_config=dest_chain_config,
        symbol=symbol,
        source_chain_id=source_chain_id,
        dest_chain_id=dest_chain_id,
    )

    _available_routes_cache[cache_key] = (now(), routes)
    return routes


def simulate_across_bridge(
    route: BridgeRoute,
    amount: float | Decimal,
    sender: str,
    recipient: Optional[str] = None,
    token_decimals: int = 18,
    output_token_decimals: int | None = None,
    protocol: BridgeProtocolConfig = ACROSS,
) -> AcrossBridgeQuote | AcrossSimulationError:
    if not sender or not str(sender).strip():
        raise ValueError("sender address is required.")

    recipient = str(recipient or sender).strip()
    amount_decimal = safe_decimal(amount)
    if amount_decimal is None:
        return AcrossSimulationError(
            reason="INVALID_AMOUNT",
            message="Bridge amount must be a valid number.",
        )

    if amount_decimal <= 0:
        return AcrossSimulationError(
            reason="ZERO_AMOUNT",
            message="Bridge amount must be greater than zero.",
        )

    output_token_decimals = output_token_decimals or token_decimals
    input_amount_raw = to_raw(amount_decimal, token_decimals)

    try:
        source_chain = get_chain_by_id(route.source_chain_id)
        dest_chain = get_chain_by_id(route.dest_chain_id)
    except KeyError as e:
        return AcrossSimulationError(
            reason="UNKNOWN_CHAIN",
            message=str(e),
        )

    input_token_for_quote = route.input_token
    output_token_for_quote = route.output_token
    if route.is_native_input and source_chain.wrapped_native:
        input_token_for_quote = source_chain.wrapped_native
    if route.is_native_output and dest_chain.wrapped_native:
        output_token_for_quote = dest_chain.wrapped_native

    params = {
        "inputToken": input_token_for_quote,
        "outputToken": output_token_for_quote,
        "originChainId": route.source_chain_id,
        "destinationChainId": route.dest_chain_id,
        "amount": str(input_amount_raw),
        "depositor": sender,
        "recipient": recipient,
        "skipAmountLimit": "false",
    }

    response = None
    try:
        api_base_url = _across_api_base_url(
            source_chain.is_testnet or dest_chain.is_testnet, protocol
        )
        response = request_json(
            "GET",
            f"{api_base_url}{_QUOTE_ENDPOINT}",
            params=params,
            timeout=_REQUEST_TIMEOUT,
            service="across",
        )
        raise_for_status(response, "across")
    except requests.Timeout:
        return AcrossSimulationError(
            reason="API_TIMEOUT",
            message=(
                f"Across API did not respond within {_REQUEST_TIMEOUT}s. "
                "Try again in a moment."
            ),
        )
    except ExternalServiceError as exc:
        status = response.status_code if response is not None else "unknown"
        body = response.text if response is not None else ""
        return AcrossSimulationError(
            reason="API_ERROR",
            message=f"Across API returned an error [{status}]: {body or exc}",
        )

    try:
        data = response.json()
    except Exception:
        return AcrossSimulationError(
            reason="INVALID_RESPONSE",
            message="Across API returned a non-JSON response.",
        )

    return _build_quote_from_response(
        data,
        route=route,
        amount_decimal=amount_decimal,
        token_decimals=token_decimals,
        output_token_decimals=output_token_decimals,
        source_chain=source_chain,
        dest_chain=dest_chain,
        protocol=protocol,
    )


async def simulate_across_bridge_async(
    route: BridgeRoute,
    amount: float | Decimal,
    sender: str,
    recipient: Optional[str] = None,
    token_decimals: int = 18,
    output_token_decimals: int | None = None,
    protocol: BridgeProtocolConfig = ACROSS,
) -> AcrossBridgeQuote | AcrossSimulationError:
    if not sender or not str(sender).strip():
        raise ValueError("sender address is required.")

    recipient = str(recipient or sender).strip()
    amount_decimal = safe_decimal(amount)
    if amount_decimal is None:
        return AcrossSimulationError(
            reason="INVALID_AMOUNT",
            message="Bridge amount must be a valid number.",
        )

    if amount_decimal <= 0:
        return AcrossSimulationError(
            reason="ZERO_AMOUNT",
            message="Bridge amount must be greater than zero.",
        )

    output_token_decimals = output_token_decimals or token_decimals
    input_amount_raw = to_raw(amount_decimal, token_decimals)

    try:
        source_chain = get_chain_by_id(route.source_chain_id)
        dest_chain = get_chain_by_id(route.dest_chain_id)
    except KeyError as e:
        return AcrossSimulationError(
            reason="UNKNOWN_CHAIN",
            message=str(e),
        )

    input_token_for_quote = route.input_token
    output_token_for_quote = route.output_token
    if route.is_native_input and source_chain.wrapped_native:
        input_token_for_quote = source_chain.wrapped_native
    if route.is_native_output and dest_chain.wrapped_native:
        output_token_for_quote = dest_chain.wrapped_native

    params = {
        "inputToken": input_token_for_quote,
        "outputToken": output_token_for_quote,
        "originChainId": route.source_chain_id,
        "destinationChainId": route.dest_chain_id,
        "amount": str(input_amount_raw),
        "depositor": sender,
        "recipient": recipient,
        "skipAmountLimit": "false",
    }

    response = None
    try:
        api_base_url = _across_api_base_url(
            source_chain.is_testnet or dest_chain.is_testnet, protocol
        )
        response = await async_request_json(
            "GET",
            f"{api_base_url}{_QUOTE_ENDPOINT}",
            params=params,
            timeout=_REQUEST_TIMEOUT,
            service="across",
        )
        await async_raise_for_status(response, "across")
    except ExternalServiceError as exc:
        status = response.status_code if response is not None else "unknown"
        body = response.text if response is not None else ""
        return AcrossSimulationError(
            reason="API_ERROR",
            message=f"Across API returned an error [{status}]: {body or exc}",
        )
    except Exception:
        # Timeout or transport errors
        return AcrossSimulationError(
            reason="API_TIMEOUT",
            message=(
                f"Across API did not respond within {_REQUEST_TIMEOUT}s. "
                "Try again in a moment."
            ),
        )

    try:
        data = response.json()
    except Exception:
        return AcrossSimulationError(
            reason="INVALID_RESPONSE",
            message="Across API returned a non-JSON response.",
        )

    return _build_quote_from_response(
        data,
        route=route,
        amount_decimal=amount_decimal,
        token_decimals=token_decimals,
        output_token_decimals=output_token_decimals,
        source_chain=source_chain,
        dest_chain=dest_chain,
        protocol=protocol,
    )
