from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

import requests

from config.bridge_registry import RELAY, BridgeProtocolConfig, relay_api_base_url
from core.utils.http import (
    ExternalServiceError,
    async_raise_for_status,
    async_request_json,
    raise_for_status,
    request_json,
)
from core.utils.timeouts import EXTERNAL_HTTP_TIMEOUT_SECONDS
from tool_nodes.bridge.executors.evm_utils import to_raw
from tool_nodes.bridge.simulators.utils import now
from tool_nodes.common.input_utils import safe_decimal

_QUOTE_ENDPOINT = "/quote"
_CHAINS_ENDPOINT = "/chains"
_CURRENCIES_ENDPOINT = "/currencies/v2"

_REQUEST_TIMEOUT = EXTERNAL_HTTP_TIMEOUT_SECONDS

_CHAIN_CACHE_TTL_SECONDS = 600
_CURRENCY_CACHE_TTL_SECONDS = 600

_chain_cache: Dict[str, tuple[float, list[dict[str, Any]]]] = {}
_currency_cache: Dict[
    tuple[str, tuple[int, ...], str], tuple[float, list[dict[str, Any]]]
] = {}


@dataclass
class RelayBridgeQuote:
    """
    Relay quote wrapper for bridge execution.
    """

    protocol: str
    token_symbol: str
    source_chain_id: int
    dest_chain_id: int
    source_chain_name: str
    dest_chain_name: str
    input_amount: Decimal
    output_amount: Decimal
    total_fee: Decimal
    total_fee_pct: Decimal
    fees: Dict[str, Any]
    steps: list[dict[str, Any]]
    request_id: Optional[str]
    avg_fill_time_seconds: int
    api_base_url: str


@dataclass
class RelaySimulationError:
    reason: str
    message: str


def _header(protocol: BridgeProtocolConfig) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if protocol.api_key:
        headers["x-api-key"] = protocol.api_key
    return headers


def _is_address(value: str) -> bool:
    v = value.strip().lower()
    return v.startswith("0x") and len(v) == 42


def _get_cached_chains(
    protocol: BridgeProtocolConfig,
    base_url: str,
) -> list[dict[str, Any]]:
    cache_key = base_url
    cached = _chain_cache.get(cache_key)
    if cached:
        ts, chains = cached
        if now() - ts < _CHAIN_CACHE_TTL_SECONDS:
            return chains

    response = request_json(
        "GET",
        f"{base_url}{_CHAINS_ENDPOINT}",
        timeout=_REQUEST_TIMEOUT,
        headers=_header(protocol),
        service="relay",
    )
    raise_for_status(response, "relay")
    data = response.json()
    if isinstance(data, dict) and "chains" in data:
        chains = data.get("chains")
    else:
        chains = data.get("data") if isinstance(data, dict) else data
        if isinstance(chains, dict):
            chains = chains.get("chains") or chains.get("data") or []
    if not isinstance(chains, list):
        chains = []

    _chain_cache[cache_key] = (now(), chains)
    return chains


async def _get_cached_chains_async(
    protocol: BridgeProtocolConfig,
    base_url: str,
) -> list[dict[str, Any]]:
    cache_key = base_url
    cached = _chain_cache.get(cache_key)
    if cached:
        ts, chains = cached
        if now() - ts < _CHAIN_CACHE_TTL_SECONDS:
            return chains

    response = await async_request_json(
        "GET",
        f"{base_url}{_CHAINS_ENDPOINT}",
        timeout=_REQUEST_TIMEOUT,
        headers=_header(protocol),
        service="relay",
    )
    await async_raise_for_status(response, "relay")
    data = response.json()
    if isinstance(data, dict) and "chains" in data:
        chains = data.get("chains")
    else:
        chains = data.get("data") if isinstance(data, dict) else data
        if isinstance(chains, dict):
            chains = chains.get("chains") or chains.get("data") or []
    if not isinstance(chains, list):
        chains = []

    _chain_cache[cache_key] = (now(), chains)
    return chains


def _get_cached_currencies(
    protocol: BridgeProtocolConfig,
    base_url: str,
    chain_ids: list[int],
    term: str,
) -> list[dict[str, Any]]:
    cache_key = (base_url, tuple(sorted(chain_ids)), term.lower())
    cached = _currency_cache.get(cache_key)
    if cached:
        ts, currencies = cached
        if now() - ts < _CURRENCY_CACHE_TTL_SECONDS:
            return currencies

    payload: Dict[str, Any] = {
        "chainIds": chain_ids,
        "term": term,
        "limit": 50,
        "depositAddressOnly": True,
        "useExternalSearch": True,
        "defaultList": True,
    }
    if _is_address(term):
        payload["address"] = term

    response = request_json(
        "POST",
        f"{base_url}{_CURRENCIES_ENDPOINT}",
        timeout=_REQUEST_TIMEOUT,
        headers=_header(protocol),
        json=payload,
        service="relay",
    )
    raise_for_status(response, "relay")
    data = response.json()
    currencies = data.get("data") if isinstance(data, dict) else data
    if isinstance(currencies, dict):
        currencies = currencies.get("currencies") or currencies.get("data") or []
    if not isinstance(currencies, list):
        currencies = []

    _currency_cache[cache_key] = (now(), currencies)
    return currencies


async def _get_cached_currencies_async(
    protocol: BridgeProtocolConfig,
    base_url: str,
    chain_ids: list[int],
    term: str,
) -> list[dict[str, Any]]:
    cache_key = (base_url, tuple(sorted(chain_ids)), term.lower())
    cached = _currency_cache.get(cache_key)
    if cached:
        ts, currencies = cached
        if now() - ts < _CURRENCY_CACHE_TTL_SECONDS:
            return currencies

    payload: Dict[str, Any] = {
        "chainIds": chain_ids,
        "term": term,
        "limit": 50,
        "depositAddressOnly": True,
        "useExternalSearch": True,
        "defaultList": True,
    }
    if _is_address(term):
        payload["address"] = term

    response = await async_request_json(
        "POST",
        f"{base_url}{_CURRENCIES_ENDPOINT}",
        timeout=_REQUEST_TIMEOUT,
        headers=_header(protocol),
        json=payload,
        service="relay",
    )
    await async_raise_for_status(response, "relay")
    data = response.json()
    currencies = data.get("data") if isinstance(data, dict) else data
    if isinstance(currencies, dict):
        currencies = currencies.get("currencies") or currencies.get("data") or []
    if not isinstance(currencies, list):
        currencies = []

    _currency_cache[cache_key] = (now(), currencies)
    return currencies


def _find_chain(
    chains: list[dict[str, Any]], chain_id: int
) -> Optional[dict[str, Any]]:
    for chain in chains:
        if int(chain.get("id", -1)) == int(chain_id):
            return chain
    return None


def _chain_supported(chain: Optional[dict[str, Any]], *, require_deposit: bool) -> bool:
    if not chain:
        return False
    if chain.get("disabled") is True:
        return False
    if require_deposit and chain.get("depositEnabled") is False:
        return False
    if not require_deposit:
        if (
            chain.get("withdrawEnabled") is False
            or chain.get("withdrawalEnabled") is False
        ):
            return False
    return True


def _match_currency(
    currencies: list[dict[str, Any]],
    chain_id: int,
    symbol_or_address: str,
) -> Optional[dict[str, Any]]:
    target = symbol_or_address.strip().upper()
    addr_match = (
        symbol_or_address.strip().lower() if _is_address(symbol_or_address) else None
    )

    for cur in currencies:
        if int(cur.get("chainId", -1)) != int(chain_id):
            continue
        if addr_match and str(cur.get("address", "")).lower() == addr_match:
            return cur
        if str(cur.get("symbol", "")).upper() == target:
            return cur
    return None


def _currency_address(currency: dict[str, Any]) -> Optional[str]:
    for key in ("address", "currencyAddress", "tokenAddress", "contractAddress"):
        value = currency.get(key)
        if value:
            return str(value)
    return None


def _native_currency_for_chain(
    chain: Optional[dict[str, Any]], symbol: str
) -> Optional[dict[str, Any]]:
    if not chain:
        return None
    target = symbol.strip().upper()
    currency = chain.get("currency")
    if isinstance(currency, dict):
        cur_symbol = str(currency.get("symbol", "")).upper()
        if cur_symbol == target and currency.get("supportsBridging") is not False:
            return currency
    for key in ("featuredTokens", "erc20Currencies", "solverCurrencies"):
        entries = chain.get(key) or []
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_symbol = str(entry.get("symbol", "")).upper()
            if entry_symbol != target:
                continue
            if entry.get("supportsBridging") is False:
                continue
            return entry
    return None


def fetch_relay_supported_tokens(
    source_chain_id: int,
    dest_chain_id: int,
    protocol: BridgeProtocolConfig = RELAY,
) -> list[str]:
    """
    Return token symbols supported by Relay on the given chain pair.
    """
    base_url = relay_api_base_url(source_chain_id, dest_chain_id)
    if not base_url or not base_url.startswith("http"):
        return []

    chains = _get_cached_chains(protocol, base_url)
    source_chain = _find_chain(chains, source_chain_id)
    dest_chain = _find_chain(chains, dest_chain_id)
    if not _chain_supported(source_chain, require_deposit=True):
        return []
    if not _chain_supported(dest_chain, require_deposit=False):
        return []

    currencies = _get_cached_currencies(
        protocol, base_url, [source_chain_id, dest_chain_id], ""
    )
    if not currencies:
        return []

    source_symbols = {
        str(cur.get("symbol", "")).upper()
        for cur in currencies
        if int(cur.get("chainId", -1)) == int(source_chain_id)
    }
    dest_symbols = {
        str(cur.get("symbol", "")).upper()
        for cur in currencies
        if int(cur.get("chainId", -1)) == int(dest_chain_id)
    }
    supported = [s for s in sorted(source_symbols & dest_symbols) if s]
    return supported


def _parse_amount(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def simulate_relay_bridge(
    token_symbol: str,
    source_chain_id: int,
    dest_chain_id: int,
    amount: float | Decimal,
    sender: str,
    recipient: Optional[str] = None,
    protocol: BridgeProtocolConfig = RELAY,
) -> RelayBridgeQuote | RelaySimulationError:
    """
    Simulate a Relay bridge by calling the Relay /quote endpoint.
    """
    if not sender or not str(sender).strip():
        raise ValueError("sender address is required.")

    recipient = str(recipient or sender).strip()
    amount_decimal = safe_decimal(amount)
    if amount_decimal is None:
        return RelaySimulationError(
            reason="INVALID_AMOUNT",
            message="Bridge amount must be a valid number.",
        )
    if amount_decimal <= 0:
        return RelaySimulationError(
            reason="ZERO_AMOUNT",
            message="Bridge amount must be greater than zero.",
        )

    base_url = relay_api_base_url(source_chain_id, dest_chain_id)
    if not base_url or not base_url.startswith("http"):
        return RelaySimulationError(
            reason="DISABLED",
            message="Relay API base URL is not configured.",
        )

    response = None
    try:
        chains = _get_cached_chains(protocol, base_url)
        source_chain = _find_chain(chains, source_chain_id)
        dest_chain = _find_chain(chains, dest_chain_id)
        if not _chain_supported(source_chain, require_deposit=True):
            return RelaySimulationError(
                reason="UNSUPPORTED_CHAIN",
                message=f"Relay does not support deposits on chain {source_chain_id}.",
            )
        if not _chain_supported(dest_chain, require_deposit=False):
            return RelaySimulationError(
                reason="UNSUPPORTED_CHAIN",
                message=f"Relay does not support withdrawals on chain {dest_chain_id}.",
            )

        currencies = _get_cached_currencies(
            protocol,
            base_url,
            [source_chain_id, dest_chain_id],
            token_symbol,
        )
        origin_currency = _match_currency(currencies, source_chain_id, token_symbol)
        dest_currency = _match_currency(currencies, dest_chain_id, token_symbol)
        if not origin_currency:
            origin_currency = _native_currency_for_chain(
                source_chain, str(token_symbol)
            )
        if not dest_currency:
            dest_currency = _native_currency_for_chain(dest_chain, str(token_symbol))
        if not origin_currency or not dest_currency:
            return RelaySimulationError(
                reason="TOKEN_UNSUPPORTED",
                message=(
                    f"Relay does not support {token_symbol} on one of the chains."
                ),
            )

        origin_address = _currency_address(origin_currency)
        dest_address = _currency_address(dest_currency)
        if not origin_address or not dest_address:
            return RelaySimulationError(
                reason="TOKEN_UNSUPPORTED",
                message=(
                    f"Relay did not return a currency address for {token_symbol}."
                ),
            )

        decimals = int(origin_currency.get("decimals", 18))
        amount_raw = str(to_raw(amount_decimal, decimals))

        payload = {
            "user": sender,
            "originChainId": int(source_chain_id),
            "destinationChainId": int(dest_chain_id),
            "originCurrency": origin_address,
            "destinationCurrency": dest_address,
            "amount": amount_raw,
            "tradeType": "EXACT_INPUT",
            "recipient": recipient,
        }

        response = request_json(
            "POST",
            f"{base_url}{_QUOTE_ENDPOINT}",
            json=payload,
            timeout=_REQUEST_TIMEOUT,
            headers=_header(protocol),
            service="relay",
        )
        raise_for_status(response, "relay")
    except requests.Timeout:
        return RelaySimulationError(
            reason="API_TIMEOUT",
            message=(
                f"Relay API did not respond within {_REQUEST_TIMEOUT}s. "
                "Try again in a moment."
            ),
        )
    except ExternalServiceError as exc:
        status = response.status_code if response is not None else "unknown"
        body = response.text if response is not None else ""
        return RelaySimulationError(
            reason="API_ERROR",
            message=f"Relay API returned an error [{status}]: {body or exc}",
        )

    try:
        data = response.json()
    except Exception:
        return RelaySimulationError(
            reason="INVALID_RESPONSE",
            message="Relay API returned a non-JSON response.",
        )

    steps = data.get("steps") or []
    details = data.get("details") or {}
    fees = data.get("fees") or {}
    time_estimate = data.get("timeEstimate") or {}

    currency_in = details.get("currencyIn") or {}
    currency_out = details.get("currencyOut") or {}

    input_amount = _parse_amount(currency_in.get("amountFormatted") or amount_decimal)
    output_amount = _parse_amount(currency_out.get("amountFormatted"))

    total_fee = Decimal("0")
    total_fee_pct = Decimal("0")
    if input_amount > 0 and output_amount > 0:
        total_fee = input_amount - output_amount
        if total_fee < 0:
            total_fee = Decimal("0")
        total_fee_pct = (total_fee / input_amount * Decimal("100")).quantize(
            Decimal("0.0001")
        )

    request_id = None
    if steps:
        request_id = steps[0].get("requestId")

    source_name = (source_chain or {}).get("name") or str(source_chain_id)
    dest_name = (dest_chain or {}).get("name") or str(dest_chain_id)

    avg_fill_time = int(time_estimate.get("duration", protocol.avg_fill_time_seconds))

    return RelayBridgeQuote(
        protocol="relay",
        token_symbol=str(token_symbol).upper(),
        source_chain_id=int(source_chain_id),
        dest_chain_id=int(dest_chain_id),
        source_chain_name=str(source_name),
        dest_chain_name=str(dest_name),
        input_amount=input_amount,
        output_amount=output_amount,
        total_fee=total_fee,
        total_fee_pct=total_fee_pct,
        fees=fees if isinstance(fees, dict) else {},
        steps=steps if isinstance(steps, list) else [],
        request_id=request_id,
        avg_fill_time_seconds=avg_fill_time,
        api_base_url=base_url,
    )


async def simulate_relay_bridge_async(
    token_symbol: str,
    source_chain_id: int,
    dest_chain_id: int,
    amount: float | Decimal,
    sender: str,
    recipient: Optional[str] = None,
    protocol: BridgeProtocolConfig = RELAY,
) -> RelayBridgeQuote | RelaySimulationError:
    """
    Async version of simulate_relay_bridge.
    """
    if not sender or not str(sender).strip():
        raise ValueError("sender address is required.")

    recipient = str(recipient or sender).strip()
    amount_decimal = safe_decimal(amount)
    if amount_decimal is None:
        return RelaySimulationError(
            reason="INVALID_AMOUNT",
            message="Bridge amount must be a valid number.",
        )
    if amount_decimal <= 0:
        return RelaySimulationError(
            reason="ZERO_AMOUNT",
            message="Bridge amount must be greater than zero.",
        )

    base_url = relay_api_base_url(source_chain_id, dest_chain_id)
    if not base_url or not base_url.startswith("http"):
        return RelaySimulationError(
            reason="DISABLED",
            message="Relay API base URL is not configured.",
        )

    response = None
    try:
        chains = await _get_cached_chains_async(protocol, base_url)
        source_chain = _find_chain(chains, source_chain_id)
        dest_chain = _find_chain(chains, dest_chain_id)
        if not _chain_supported(source_chain, require_deposit=True):
            return RelaySimulationError(
                reason="UNSUPPORTED_CHAIN",
                message=f"Relay does not support deposits on chain {source_chain_id}.",
            )
        if not _chain_supported(dest_chain, require_deposit=False):
            return RelaySimulationError(
                reason="UNSUPPORTED_CHAIN",
                message=f"Relay does not support withdrawals on chain {dest_chain_id}.",
            )

        currencies = await _get_cached_currencies_async(
            protocol,
            base_url,
            [source_chain_id, dest_chain_id],
            token_symbol,
        )
        origin_currency = _match_currency(currencies, source_chain_id, token_symbol)
        dest_currency = _match_currency(currencies, dest_chain_id, token_symbol)
        if not origin_currency:
            origin_currency = _native_currency_for_chain(
                source_chain, str(token_symbol)
            )
        if not dest_currency:
            dest_currency = _native_currency_for_chain(dest_chain, str(token_symbol))
        if not origin_currency or not dest_currency:
            return RelaySimulationError(
                reason="TOKEN_UNSUPPORTED",
                message=(
                    f"Relay does not support {token_symbol} on one of the chains."
                ),
            )

        origin_address = _currency_address(origin_currency)
        dest_address = _currency_address(dest_currency)
        if not origin_address or not dest_address:
            return RelaySimulationError(
                reason="TOKEN_UNSUPPORTED",
                message=(
                    f"Relay did not return a currency address for {token_symbol}."
                ),
            )

        decimals = int(origin_currency.get("decimals", 18))
        amount_raw = str(to_raw(amount_decimal, decimals))

        payload = {
            "user": sender,
            "originChainId": int(source_chain_id),
            "destinationChainId": int(dest_chain_id),
            "originCurrency": origin_address,
            "destinationCurrency": dest_address,
            "amount": amount_raw,
            "tradeType": "EXACT_INPUT",
            "recipient": recipient,
        }

        response = await async_request_json(
            "POST",
            f"{base_url}{_QUOTE_ENDPOINT}",
            json=payload,
            timeout=_REQUEST_TIMEOUT,
            headers=_header(protocol),
            service="relay",
        )
        await async_raise_for_status(response, "relay")
    except ExternalServiceError as exc:
        status = response.status_code if response is not None else "unknown"
        body = response.text if response is not None else ""
        return RelaySimulationError(
            reason="API_ERROR",
            message=f"Relay API returned an error [{status}]: {body or exc}",
        )
    except Exception:
        return RelaySimulationError(
            reason="API_TIMEOUT",
            message=(
                f"Relay API did not respond within {_REQUEST_TIMEOUT}s. "
                "Try again in a moment."
            ),
        )

    try:
        data = response.json()
    except Exception:
        return RelaySimulationError(
            reason="INVALID_RESPONSE",
            message="Relay API returned a non-JSON response.",
        )

    steps = data.get("steps") or []
    details = data.get("details") or {}
    fees = data.get("fees") or {}
    time_estimate = data.get("timeEstimate") or {}

    currency_in = details.get("currencyIn") or {}
    currency_out = details.get("currencyOut") or {}

    input_amount = _parse_amount(currency_in.get("amountFormatted") or amount_decimal)
    output_amount = _parse_amount(currency_out.get("amountFormatted"))

    total_fee = Decimal("0")
    total_fee_pct = Decimal("0")
    if input_amount > 0 and output_amount > 0:
        total_fee = input_amount - output_amount
        if total_fee < 0:
            total_fee = Decimal("0")
        total_fee_pct = (total_fee / input_amount * Decimal("100")).quantize(
            Decimal("0.0001")
        )

    request_id = None
    if steps:
        request_id = steps[0].get("requestId")

    source_name = (source_chain or {}).get("name") or str(source_chain_id)
    dest_name = (dest_chain or {}).get("name") or str(dest_chain_id)

    avg_fill_time = int(time_estimate.get("duration", protocol.avg_fill_time_seconds))

    return RelayBridgeQuote(
        protocol="relay",
        token_symbol=str(token_symbol).upper(),
        source_chain_id=int(source_chain_id),
        dest_chain_id=int(dest_chain_id),
        source_chain_name=str(source_name),
        dest_chain_name=str(dest_name),
        input_amount=input_amount,
        output_amount=output_amount,
        total_fee=total_fee,
        total_fee_pct=total_fee_pct,
        fees=fees if isinstance(fees, dict) else {},
        steps=steps if isinstance(steps, list) else [],
        request_id=request_id,
        avg_fill_time_seconds=avg_fill_time,
        api_base_url=base_url,
    )
