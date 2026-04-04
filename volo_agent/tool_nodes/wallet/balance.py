from __future__ import annotations

import asyncio
import os
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, cast

import httpx

from config.chains import get_chain_by_name
from core.observer.price_observer import (
    fetch_prices_batch_coingecko,
    fetch_prices_dexscreener,
)
from core.utils.balance_chains import (
    ALL_SUPPORTED_CHAIN_KEY,
    BalanceChainSpec,
    canonicalize_balance_chain,
    is_all_supported_chain_request,
    list_supported_balance_chain_specs,
    resolve_balance_chain_spec,
)
from tool_nodes.common.input_utils import format_with_recovery

_EVM_WALLET_BALANCES_FN: Callable[..., Any] | None = None
_EVM_NATIVE_BALANCE_FN: Callable[..., Any] | None = None
_SOLANA_WALLET_BALANCES_FN: Callable[..., Any] | None = None
_SOLANA_NATIVE_BALANCE_FN: Callable[..., Any] | None = None


def _load_evm_wallet_balances_fn() -> Callable[..., Any]:
    global _EVM_WALLET_BALANCES_FN
    if _EVM_WALLET_BALANCES_FN is None:
        from wallet_service.evm.get_all_bal import (
            get_wallet_balances as _get_wallet_balances,
        )

        _EVM_WALLET_BALANCES_FN = _get_wallet_balances
    return _EVM_WALLET_BALANCES_FN


def _load_evm_native_balance_fn() -> Callable[..., Any]:
    global _EVM_NATIVE_BALANCE_FN
    if _EVM_NATIVE_BALANCE_FN is None:
        from wallet_service.evm.get_native_bal import (
            get_native_balance_async as _get_native_balance,
        )

        _EVM_NATIVE_BALANCE_FN = _get_native_balance
    return _EVM_NATIVE_BALANCE_FN


def _load_solana_wallet_balances_fn() -> Callable[..., Any]:
    global _SOLANA_WALLET_BALANCES_FN
    if _SOLANA_WALLET_BALANCES_FN is None:
        from wallet_service.solana.get_all_bal import (
            get_wallet_balances_async as _get_wallet_balances,
        )

        _SOLANA_WALLET_BALANCES_FN = _get_wallet_balances
    return _SOLANA_WALLET_BALANCES_FN


def _load_solana_native_balance_fn() -> Callable[..., Any]:
    global _SOLANA_NATIVE_BALANCE_FN
    if _SOLANA_NATIVE_BALANCE_FN is None:
        from wallet_service.solana.get_native_bal import (
            get_native_balance_async as _get_native_balance,
        )

        _SOLANA_NATIVE_BALANCE_FN = _get_native_balance
    return _SOLANA_NATIVE_BALANCE_FN


def get_wallet_balances(wallet_address: str, chain_name: str) -> Any:
    return _load_evm_wallet_balances_fn()(wallet_address, chain_name)


def get_native_balance(wallet_address: str, http_provider: str) -> Any:
    return _load_evm_native_balance_fn()(wallet_address, http_provider)


def get_solana_wallet_balances(wallet_address: str, network: str) -> Any:
    return _load_solana_wallet_balances_fn()(wallet_address, network)


def get_solana_native_balance(wallet_address: str, network: str | None = None) -> Any:
    return _load_solana_native_balance_fn()(wallet_address, network=network)


def _get_env_float(key: str, default: float) -> float:
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if value <= 0:
        return default
    return value


def _get_env_int(key: str, default: int) -> int:
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if value <= 0:
        return default
    return value


_PER_CHAIN_TIMEOUT_SECONDS = _get_env_float("BALANCE_CHAIN_TIMEOUT_SECONDS", 5.0)
_FULL_WALLET_TIMEOUT_SECONDS = _get_env_float(
    "BALANCE_FULL_WALLET_TIMEOUT_SECONDS",
    max(_PER_CHAIN_TIMEOUT_SECONDS, 12.0),
)
_ALL_CHAIN_CONCURRENCY = _get_env_int("BALANCE_CHAIN_CONCURRENCY", 8)
_PRICE_LOOKUP_TIMEOUT_SECONDS = _get_env_float(
    "BALANCE_PRICE_LOOKUP_TIMEOUT_SECONDS", 2.0
)
_PRICE_CACHE_TTL_SECONDS = _get_env_float("BALANCE_PRICE_CACHE_TTL_SECONDS", 20.0)
_PRICE_NEGATIVE_CACHE_TTL_SECONDS = _get_env_float(
    "BALANCE_PRICE_NEGATIVE_CACHE_TTL_SECONDS", 6.0
)
_INCLUDE_SOLANA_DEVNET = str(
    os.getenv("BALANCE_INCLUDE_SOLANA_DEVNET", "1")
).strip().lower() not in {"0", "false", "no", "off"}

# In-process symbol price cache to avoid repeated cold network lookups in
# tight loops and consecutive requests.
_PRICE_CACHE: Dict[str, tuple[float, float | None]] = {}
_PRICE_CACHE_LOCK = asyncio.Lock()
_PRICE_HTTP_CLIENT: httpx.AsyncClient | None = None
_PRICE_HTTP_CLIENT_LOCK = asyncio.Lock()


async def _get_price_http_client() -> httpx.AsyncClient:
    global _PRICE_HTTP_CLIENT
    async with _PRICE_HTTP_CLIENT_LOCK:
        if _PRICE_HTTP_CLIENT is None or _PRICE_HTTP_CLIENT.is_closed:
            _PRICE_HTTP_CLIENT = httpx.AsyncClient(
                limits=httpx.Limits(max_connections=64, max_keepalive_connections=16),
                http2=True,
            )
        return _PRICE_HTTP_CLIENT


def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"none", "null", "n/a", "na"}:
        return None
    return text


def _short_address(address: str | None) -> str:
    if not address:
        return "unknown address"
    if len(address) <= 14:
        return address
    return f"{address[:6]}...{address[-4:]}"


def _decimal_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _fmt_usd(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"${value:,.2f}"


def _fmt_balance(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _is_mock_callable(func: Any) -> bool:
    module_name = getattr(type(func), "__module__", "")
    return "unittest.mock" in str(module_name)


def _wallet_balance_timeout_seconds(family: str) -> float:
    # Full wallet balance lookups can legitimately take longer than direct
    # single-asset RPC reads, especially when a provider aggregates native +
    # token balances in one response. Keep those budgets separate so fast
    # direct checks still fail fast.
    if family in {"evm", "solana"}:
        return _FULL_WALLET_TIMEOUT_SECONDS
    return _PER_CHAIN_TIMEOUT_SECONDS


def _normalize_token_entries(balances: Any, *, chain_key: str) -> List[Dict[str, Any]]:
    if not isinstance(balances, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for entry in balances:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol") or "").strip().upper()
        amount = _decimal_value(entry.get("balance_formatted"))
        if not symbol or amount is None:
            continue
        if amount <= 0:
            continue
        normalized.append(
            {
                "name": entry.get("name"),
                "symbol": symbol,
                "decimals": entry.get("decimals"),
                "balance": entry.get("balance"),
                "balance_formatted": _fmt_balance(amount),
                "token_address": entry.get("token_address"),
                "chain": chain_key,
            }
        )
    return normalized


def _build_evm_native_entry(
    chain_name: str,
    chain_display_name: str,
    native_symbol: str,
    native_balance: Any,
) -> Dict[str, Any]:
    amount = _decimal_value(native_balance) or Decimal("0")
    return {
        "name": f"{chain_display_name} Native",
        "symbol": native_symbol,
        "decimals": 18,
        "balance": None,
        "balance_formatted": _fmt_balance(amount),
        "token_address": None,
        "chain": chain_name,
    }


async def _fetch_evm_native_entry(
    chain_name: str,
    wallet_address: str,
) -> Dict[str, Any]:
    chain = get_chain_by_name(chain_name)
    native_balance = await asyncio.wait_for(
        get_native_balance(
            wallet_address,
            chain.rpc_url,
        ),
        timeout=_PER_CHAIN_TIMEOUT_SECONDS,
    )
    return _build_evm_native_entry(
        chain_name=chain_name,
        chain_display_name=chain.name,
        native_symbol=chain.native_symbol,
        native_balance=native_balance,
    )


def _build_solana_native_entry(
    network: str,
    native_balance: Any,
) -> Dict[str, Any]:
    amount = _decimal_value(native_balance) or Decimal("0")
    return {
        "name": "Solana Native",
        "symbol": "SOL",
        "decimals": 9,
        "balance": None,
        "balance_formatted": _fmt_balance(amount),
        "token_address": None,
        "chain": network,
    }


async def _fetch_solana_native_entry(
    network: str,
    wallet_address: str,
) -> Dict[str, Any]:
    native_balance = await asyncio.wait_for(
        get_solana_native_balance(
            wallet_address,
            network,
        ),
        timeout=_PER_CHAIN_TIMEOUT_SECONDS,
    )
    return _build_solana_native_entry(
        network=network,
        native_balance=native_balance,
    )


async def _fetch_evm_chain_balances(
    *,
    chain_name: str,
    wallet_address: str,
) -> List[Dict[str, Any]]:
    first_error: Exception | None = None
    try:
        balances = await asyncio.wait_for(
            get_wallet_balances(wallet_address, chain_name),
            timeout=_wallet_balance_timeout_seconds("evm"),
        )
    except TimeoutError:
        raise
    except Exception as exc:
        balances = []
        first_error = exc

    normalized = _normalize_token_entries(balances, chain_key=chain_name)
    if normalized:
        return normalized

    try:
        native_entry = await asyncio.wait_for(
            _fetch_evm_native_entry(
                chain_name,
                wallet_address,
            ),
            timeout=_wallet_balance_timeout_seconds("evm"),
        )
        return _normalize_token_entries([native_entry], chain_key=chain_name)
    except Exception:
        if first_error is not None:
            raise first_error
        raise


async def _fetch_solana_chain_balances(
    *,
    network: str,
    wallet_address: str,
) -> List[Dict[str, Any]]:
    first_error: Exception | None = None
    try:
        balances = await asyncio.wait_for(
            get_solana_wallet_balances(wallet_address, network),
            timeout=_wallet_balance_timeout_seconds("solana"),
        )
    except TimeoutError:
        raise
    except Exception as exc:
        balances = []
        first_error = exc

    normalized = _normalize_token_entries(balances, chain_key=network)
    if normalized:
        return normalized

    try:
        native_entry = await asyncio.wait_for(
            _fetch_solana_native_entry(network, wallet_address),
            timeout=_PER_CHAIN_TIMEOUT_SECONDS,
        )
        return _normalize_token_entries([native_entry], chain_key=network)
    except Exception:
        if first_error is not None:
            raise first_error
        raise


def _build_failure_result(spec: BalanceChainSpec, reason: str) -> Dict[str, Any]:
    return {
        "status": "error",
        "chain": spec.key,
        "chain_display": spec.display_name,
        "family": spec.family,
        "is_testnet": spec.is_testnet,
        "error": reason,
        "balances": [],
    }


async def _fetch_chain_result(
    *,
    spec: BalanceChainSpec,
    evm_sender: str | None,
    solana_sender: str | None,
) -> Dict[str, Any]:
    if spec.family == "evm":
        if not evm_sender:
            return _build_failure_result(
                spec,
                "EVM wallet address is unavailable. Run onboarding and retry.",
            )
        address = evm_sender
    else:
        if not solana_sender:
            return _build_failure_result(
                spec,
                "Solana wallet address is unavailable. Create/retry Solana wallet setup.",
            )
        address = solana_sender

    try:
        if spec.family == "evm":
            balances = await _fetch_evm_chain_balances(
                chain_name=spec.key,
                wallet_address=address,
            )
        else:
            balances = await _fetch_solana_chain_balances(
                network=spec.key,
                wallet_address=address,
            )
    except TimeoutError:
        timeout_seconds = _wallet_balance_timeout_seconds(spec.family)
        return _build_failure_result(
            spec,
            (
                f"Timed out while fetching balances (>{timeout_seconds:.0f}s). "
                f"Retry with 'balance on {spec.key}'."
            ),
        )
    except Exception as exc:
        return _build_failure_result(
            spec,
            (
                f"Failed to fetch balances: {str(exc)}. "
                f"Retry with 'balance on {spec.key}' or check RPC/network connectivity."
            ),
        )

    return {
        "status": "success",
        "chain": spec.key,
        "chain_display": spec.display_name,
        "family": spec.family,
        "is_testnet": spec.is_testnet,
        "address": address,
        "balances": balances,
    }


async def _resolve_symbol_prices(symbols: set[str]) -> Dict[str, float]:
    if not symbols:
        return {}

    normalized_symbols = {str(s or "").upper() for s in symbols if str(s or "").strip()}
    if not normalized_symbols:
        return {}

    cached: Dict[str, float] = {}
    to_fetch: set[str] = set()
    now = time.monotonic()

    async with _PRICE_CACHE_LOCK:
        for symbol in normalized_symbols:
            cached_entry = _PRICE_CACHE.get(symbol)
            if cached_entry is None:
                to_fetch.add(symbol)
                continue
            expiry_ts, cached_price = cached_entry
            if expiry_ts <= now:
                _PRICE_CACHE.pop(symbol, None)
                to_fetch.add(symbol)
                continue
            if cached_price is not None:
                cached[symbol] = cached_price

    if not to_fetch:
        return cached

    try:
        client = await _get_price_http_client()
        prices = await asyncio.wait_for(
            fetch_prices_batch_coingecko(sorted(to_fetch), client),
            timeout=_PRICE_LOOKUP_TIMEOUT_SECONDS,
        )
        missing = sorted(to_fetch - set(prices.keys()))
        if missing:
            fallback = await asyncio.wait_for(
                fetch_prices_dexscreener(missing, client),
                timeout=_PRICE_LOOKUP_TIMEOUT_SECONDS,
            )
            prices.update(fallback)
    except Exception:
        prices = {}

    post_fetch = time.monotonic()
    unresolved = to_fetch - set(prices.keys())
    async with _PRICE_CACHE_LOCK:
        for symbol, price in prices.items():
            _PRICE_CACHE[symbol] = (post_fetch + _PRICE_CACHE_TTL_SECONDS, float(price))
        for symbol in unresolved:
            _PRICE_CACHE[symbol] = (
                post_fetch + _PRICE_NEGATIVE_CACHE_TTL_SECONDS,
                None,
            )

    merged = dict(cached)
    for symbol, price in prices.items():
        merged[symbol] = float(price)
    return merged


async def _attach_usd_valuations(chain_results: List[Dict[str, Any]]) -> None:
    symbols: set[str] = set()
    for result in chain_results:
        if result.get("status") != "success":
            continue
        if result.get("is_testnet"):
            continue
        for entry in result.get("balances", []):
            symbol = str(entry.get("symbol") or "").upper()
            amount = _decimal_value(entry.get("balance_formatted"))
            if symbol and amount and amount > 0:
                symbols.add(symbol)

    prices = await _resolve_symbol_prices(symbols)
    for result in chain_results:
        if result.get("status") != "success":
            continue
        if result.get("is_testnet"):
            result["usd_skipped"] = True
            continue

        total_usd = 0.0
        missing_any_price = False
        for entry in result.get("balances", []):
            symbol = str(entry.get("symbol") or "").upper()
            amount = _decimal_value(entry.get("balance_formatted"))
            price = prices.get(symbol)
            if amount is None or amount <= 0:
                continue
            if price is None:
                missing_any_price = True
                continue
            balance_usd = float(amount) * float(price)
            entry["balance_usd"] = balance_usd
            total_usd += balance_usd

        result["total_usd"] = total_usd
        result["usd_pricing_incomplete"] = missing_any_price


def _render_balance_lines(result: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    balances = result.get("balances", [])
    if not balances:
        lines.append("  - No assets found.")
    else:
        for entry in balances:
            symbol = str(entry.get("symbol") or "").upper()
            amount = str(entry.get("balance_formatted") or "0")
            name = str(entry.get("name") or symbol)
            line = f"  - {amount} {symbol} ({name})"
            if not result.get("is_testnet"):
                balance_usd = entry.get("balance_usd")
                if balance_usd is not None:
                    line = f"{line} ~{_fmt_usd(float(balance_usd))}"
            lines.append(line)

    if result.get("is_testnet"):
        lines.append("  - USD valuation skipped on testnet.")
    else:
        total_usd = result.get("total_usd")
        if total_usd is not None:
            lines.append(f"  Subtotal: ~{_fmt_usd(float(total_usd))}")
        if result.get("usd_pricing_incomplete"):
            lines.append("  Some token USD prices are unavailable right now.")
    return lines


def _has_positive_balances(result: Dict[str, Any]) -> bool:
    if result.get("status") != "success":
        return False
    for entry in result.get("balances", []):
        amount = _decimal_value(entry.get("balance_formatted"))
        if amount is not None and amount > 0:
            return True
    return False


def _render_single_chain_message(result: Dict[str, Any]) -> str:
    if result.get("status") != "success":
        reason = str(result.get("error") or "Unknown balance fetch error.")
        return (
            f"Couldn't fetch balances on {result.get('chain_display')}: {reason}\n"
            f"Recovery: retry 'balance on {result.get('chain')}'."
        )

    chain_display = result.get("chain_display")
    address = _short_address(result.get("address"))
    lines = [f"Balances on {chain_display} for {address}:"]
    lines.extend(_render_balance_lines(result))
    return "\n".join(lines)


def _render_all_chain_message(chain_results: List[Dict[str, Any]]) -> str:
    successes = [r for r in chain_results if r.get("status") == "success"]
    failures = [r for r in chain_results if r.get("status") != "success"]
    visible_successes = [r for r in successes if _has_positive_balances(r)]

    if not successes:
        failure_lines = ["I couldn't fetch balances on any configured chain."]
        for failure in failures:
            failure_lines.append(
                f"- {failure.get('chain_display')}: {failure.get('error')}"
            )
        failure_lines.append(
            "Recovery: check network/RPC access and retry with 'balance on <chain>'."
        )
        return "\n".join(failure_lines)

    if not visible_successes:
        lines = ["No assets found across supported chains."]
        if failures:
            lines.append("")
            lines.append("Some chains could not be fetched:")
            for failure in failures:
                lines.append(
                    f"- {failure.get('chain_display')}: {failure.get('error')}"
                )
            lines.append("Recovery: retry with 'balance on <chain>' for failed chains.")
        return "\n".join(lines)

    lines = ["Balances across supported chains:"]
    mainnet_total = 0.0
    for result in visible_successes:
        lines.append("")
        lines.append(f"{result.get('chain_display')}:")
        lines.extend(_render_balance_lines(result))
        if not result.get("is_testnet"):
            total_usd = result.get("total_usd")
            if total_usd is not None:
                mainnet_total += float(total_usd)

    lines.append("")
    lines.append(f"Estimated mainnet portfolio value: ~{_fmt_usd(mainnet_total)}")

    if failures:
        lines.append("")
        lines.append("Some chains could not be fetched:")
        for failure in failures:
            lines.append(f"- {failure.get('chain_display')}: {failure.get('error')}")
        lines.append("Recovery: retry with 'balance on <chain>' for failed chains.")

    return "\n".join(lines)


def _flatten_balances(chain_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flattened: List[Dict[str, Any]] = []
    for result in chain_results:
        if result.get("status") != "success":
            continue
        for entry in result.get("balances", []):
            item = dict(entry)
            item["chain"] = result.get("chain")
            flattened.append(item)
    return flattened


async def _check_all_supported(
    *,
    evm_sender: str | None,
    solana_sender: str | None,
) -> Dict[str, Any]:
    chain_specs = list_supported_balance_chain_specs(
        include_testnets=True,
        include_solana_devnet=_INCLUDE_SOLANA_DEVNET,
    )
    if not chain_specs:
        return {
            "status": "error",
            "balances": [],
            "message": (
                "No supported chains are configured for balance checks. "
                "Configure RPC endpoints and try again."
            ),
        }

    sem = asyncio.Semaphore(_ALL_CHAIN_CONCURRENCY)

    async def _run(spec: BalanceChainSpec) -> Dict[str, Any]:
        async with sem:
            return await _fetch_chain_result(
                spec=spec,
                evm_sender=evm_sender,
                solana_sender=solana_sender,
            )

    raw_results = await asyncio.gather(
        *(_run(spec) for spec in chain_specs), return_exceptions=True
    )
    chain_results: List[Dict[str, Any]] = []
    for spec, result in zip(chain_specs, raw_results):
        if isinstance(result, Exception):
            chain_results.append(
                _build_failure_result(
                    spec,
                    (
                        f"Unexpected balance fetch failure: {result}. "
                        "Retry with 'balance on <chain>' for this chain."
                    ),
                )
            )
            continue
        chain_results.append(cast(Dict[str, Any], result))

    await _attach_usd_valuations(chain_results)

    message = _render_all_chain_message(chain_results)
    successful = [r for r in chain_results if r.get("status") == "success"]
    status = "success" if successful else "error"
    return {
        "status": status,
        "scope": ALL_SUPPORTED_CHAIN_KEY,
        "chains": chain_results,
        "balances": _flatten_balances(chain_results),
        "message": message,
    }


def _supported_chain_examples() -> str:
    specs = list_supported_balance_chain_specs(
        include_testnets=True,
        include_solana_devnet=_INCLUDE_SOLANA_DEVNET,
    )
    if not specs:
        return ""
    names = [spec.key for spec in specs[:6]]
    if len(specs) > 6:
        names.append("...")
    return ", ".join(names)


async def check_balance(parameters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch and format wallet balances.

    Supports two modes:
      1. Single-chain:  ``chain=<network>``
      2. Multi-chain:   ``chain=all_supported``
    """
    chain_raw = _clean_optional(parameters.get("chain"))
    if not chain_raw:
        raise ValueError(
            format_with_recovery(
                "Missing chain for balance check",
                "set 'chain' to a supported network like 'ethereum' or 'all_supported'",
            )
        )

    evm_sender = _clean_optional(parameters.get("sender"))
    solana_sender = _clean_optional(
        parameters.get("solana_sender") or parameters.get("solana_address")
    )
    scope = _clean_optional(parameters.get("scope"))

    chain_canonical = canonicalize_balance_chain(chain_raw) or chain_raw.lower()
    all_supported = (
        chain_canonical == ALL_SUPPORTED_CHAIN_KEY
        or is_all_supported_chain_request(chain_raw)
        or is_all_supported_chain_request(scope)
    )

    if all_supported:
        return await _check_all_supported(
            evm_sender=evm_sender,
            solana_sender=solana_sender,
        )

    spec = resolve_balance_chain_spec(chain_canonical)
    if spec is None:
        examples = _supported_chain_examples()
        recovery = (
            f" Supported chains: {examples}."
            if examples
            else " Configure chain RPC endpoints first."
        )
        return {
            "status": "error",
            "balances": [],
            "message": (
                f"Unsupported chain '{chain_raw}'."
                f"{recovery} Retry with 'balance on <chain>'."
            ),
        }

    chain_result = await _fetch_chain_result(
        spec=spec,
        evm_sender=evm_sender,
        solana_sender=solana_sender,
    )
    await _attach_usd_valuations([chain_result])

    return {
        "status": chain_result.get("status", "success"),
        "chain": spec.key,
        "balances": chain_result.get("balances", []),
        "message": _render_single_chain_message(chain_result),
    }
