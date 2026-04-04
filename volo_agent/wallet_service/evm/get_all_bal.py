import asyncio
import re
from decimal import Decimal
from typing import Any, Dict, List

from config.chains import ChainConfig, get_chain_by_name
from core.utils.http import (
    ExternalServiceError,
    async_raise_for_status,
    async_request_json,
)
from wallet_service.common.balance_utils import (
    format_decimal as _format_decimal,
)
from wallet_service.common.balance_utils import (
    parse_decimal as _parse_decimal,
)
from wallet_service.common.messages import format_with_recovery, require_non_empty_str
from wallet_service.evm.cdp_utils import (
    list_token_balances_async as _cdp_list_token_balances_async,
)
from wallet_service.evm.cdp_utils import (
    normalize_evm_network,
)
from wallet_service.evm.get_native_bal import get_native_balance_async

_ALCHEMY_METHOD_BALANCES = "alchemy_getTokenBalances"
_ALCHEMY_METHOD_METADATA = "alchemy_getTokenMetadata"
_ALCHEMY_METADATA_SCAN_LIMIT = 60
_ALCHEMY_BATCH_SIZE = 50
_MAX_REASONABLE_TOKEN_DECIMALS = 36

_SUSPICIOUS_SYMBOL_RE = re.compile(r"^(?:DYN|MC|TEST|TOKEN)\d{2,}$", re.IGNORECASE)
_SUSPICIOUS_NAME_RE = re.compile(
    r"(?:\btest\b|\bdemo\b|\bfaucet\b|\bairdrop\b|\breward\b|\bclaim\b|http|www\.|website|verify)",
    re.IGNORECASE,
)
_SPAM_MARKERS = ("http", "www.", "claim", "airdrop", "reward", "verify", "website")


def _is_spam_token(name: str | None, symbol: str | None, is_testnet: bool) -> bool:
    name_val = str(name or "").strip().lower()
    symbol_val = str(symbol or "").strip().lower()

    if not name_val or not symbol_val:
        return True

    if len(name_val) > 48 or len(symbol_val) > 24:
        return True

    if any(marker in name_val for marker in _SPAM_MARKERS):
        return True
    if any(marker in symbol_val for marker in _SPAM_MARKERS):
        return True

    if is_testnet:
        if _SUSPICIOUS_SYMBOL_RE.match(symbol_val):
            return True
        if _SUSPICIOUS_NAME_RE.search(name_val):
            return True
        if len(symbol_val) >= 5 and any(ch.isdigit() for ch in symbol_val):
            if symbol_val.startswith(("dyn", "mc", "test", "token")):
                return True

    return False


def _is_native_contract(address: str | None, chain: ChainConfig) -> bool:
    if address is None:
        return True
    addr = str(address).strip().lower()
    if not addr:
        return True
    return addr in {a.lower() for a in chain.native_token_aliases}


def _should_hide_token(token: dict[str, Any], chain: ChainConfig) -> bool:
    address = token.get("token_address")
    if _is_native_contract(address, chain):
        return False
    if not address:
        return False
    return _is_spam_token(token.get("name"), token.get("symbol"), chain.is_testnet)


async def _build_native_entry(
    chain: ChainConfig, wallet_address: str
) -> Dict[str, Any] | None:
    try:
        native_balance = await get_native_balance_async(wallet_address, chain.rpc_url)
    except Exception:
        return None
    return {
        "name": f"{chain.name} Native",
        "symbol": chain.native_symbol,
        "decimals": 18,
        "balance": None,
        "balance_formatted": _format_decimal(native_balance),
        "token_address": None,
    }


def _token_entry_from_cdp(item: Any, chain: ChainConfig) -> Dict[str, Any] | None:
    token = getattr(item, "token", None)
    amount = getattr(item, "amount", None)
    raw_amount_val = getattr(amount, "amount", None) if amount else None
    decimals_val = getattr(amount, "decimals", None) if amount else None

    raw_amount = _parse_decimal(raw_amount_val)
    decimals = _parse_decimal(decimals_val)

    if raw_amount is None or decimals is None:
        return None

    try:
        decimals_int = int(decimals)
    except Exception:
        decimals_int = 18

    if decimals_int < 0 or decimals_int > _MAX_REASONABLE_TOKEN_DECIMALS:
        return None

    try:
        balance = raw_amount / (Decimal(10) ** decimals_int)
    except Exception:
        return None

    token_address = getattr(token, "contract_address", None) if token else None
    symbol = getattr(token, "symbol", None) if token else None
    name = getattr(token, "name", None) if token else None

    if _is_native_contract(token_address, chain):
        token_address = None
        symbol = symbol or chain.native_symbol
        name = name or f"{chain.name} Native"

    return {
        "name": name,
        "symbol": symbol,
        "decimals": decimals_int,
        "balance": str(raw_amount),
        "balance_formatted": _format_decimal(balance),
        "token_address": token_address,
    }


async def _fetch_balances_cdp(
    wallet_address: str, chain: ChainConfig
) -> list[dict[str, Any]]:
    result = await _cdp_list_token_balances_async(wallet_address, chain.name)
    balances = getattr(result, "balances", None)
    if not isinstance(balances, list):
        return []
    tokens: list[dict[str, Any]] = []
    for item in balances:
        entry = _token_entry_from_cdp(item, chain)
        if entry:
            tokens.append(entry)
    return tokens


async def _post_jsonrpc_async(
    rpc_url: str, payload: dict[str, Any], service: str
) -> dict[str, Any]:
    response = await async_request_json(
        "POST",
        rpc_url,
        service=service,
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    await async_raise_for_status(response, service)
    try:
        data = response.json()
    except (ValueError, TypeError) as e:
        raise ExternalServiceError(service, response.status_code, response.text) from e
    if isinstance(data, dict) and data.get("error"):
        raise ExternalServiceError(
            service, response.status_code, str(data.get("error"))
        )
    return data if isinstance(data, dict) else {}


async def _post_jsonrpc_batch_async(
    rpc_url: str, payloads: list[dict[str, Any]], service: str
) -> list[dict[str, Any]]:
    if not payloads:
        return []
    response = await async_request_json(
        "POST",
        rpc_url,
        service=service,
        json=payloads,
        headers={"Content-Type": "application/json"},
    )
    await async_raise_for_status(response, service)
    try:
        data = response.json()
    except (ValueError, TypeError) as e:
        raise ExternalServiceError(service, response.status_code, response.text) from e
    return data if isinstance(data, list) else [data] if isinstance(data, dict) else []


async def _fetch_balances_alchemy(
    wallet_address: str, chain: ChainConfig
) -> list[dict[str, Any]]:
    rpc_url = chain.rpc_url
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": _ALCHEMY_METHOD_BALANCES,
        "params": [wallet_address, "erc20"],
    }
    data = await _post_jsonrpc_async(rpc_url, payload, service="alchemy")
    result = data.get("result")
    balances = result.get("tokenBalances") if isinstance(result, dict) else None
    if not isinstance(balances, list):
        return []

    candidates = []
    for item in balances:
        if not isinstance(item, dict):
            continue
        contract = str(item.get("contractAddress") or "").strip()
        raw_hex = item.get("tokenBalance")
        if not contract or not isinstance(raw_hex, str):
            continue
        try:
            raw_int = int(raw_hex, 16)
        except Exception:
            continue
        if raw_int <= 0:
            continue
        candidates.append((contract, raw_int))

    candidates.sort(key=lambda x: x[1], reverse=True)
    top = candidates[:_ALCHEMY_METADATA_SCAN_LIMIT]

    all_tokens = []
    for i in range(0, len(top), _ALCHEMY_BATCH_SIZE):
        chunk = top[i : i + _ALCHEMY_BATCH_SIZE]
        batch_payloads = [
            {
                "jsonrpc": "2.0",
                "id": j,
                "method": _ALCHEMY_METHOD_METADATA,
                "params": [c],
            }
            for j, (c, _) in enumerate(chunk)
        ]
        batch_results = await _post_jsonrpc_batch_async(
            rpc_url, batch_payloads, service="alchemy"
        )

        metadata_map = {
            r.get("id"): r.get("result") for r in batch_results if isinstance(r, dict)
        }

        for j, (contract, raw_int) in enumerate(chunk):
            metadata = metadata_map.get(j)
            if not isinstance(metadata, dict):
                continue

            try:
                decimals_int = int(metadata.get("decimals") or 18)
            except Exception:
                decimals_int = 18

            if decimals_int < 0 or decimals_int > _MAX_REASONABLE_TOKEN_DECIMALS:
                decimals_int = 18

            balance = Decimal(raw_int) / (Decimal(10) ** decimals_int)
            all_tokens.append(
                {
                    "name": metadata.get("name"),
                    "symbol": metadata.get("symbol"),
                    "decimals": decimals_int,
                    "balance": str(raw_int),
                    "balance_formatted": _format_decimal(balance),
                    "token_address": contract,
                }
            )

    return all_tokens


def _filter_and_rank_tokens(
    tokens: list[dict[str, Any]],
    top_n: int,
    native_entry: dict[str, Any] | None,
    chain: ChainConfig,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []

    for token in tokens:
        # Avoid duplicating native entry if it was fetched separately
        if native_entry and _is_native_contract(token.get("token_address"), chain):
            continue

        if _should_hide_token(token, chain):
            continue

        balance_val = _parse_decimal(token.get("balance_formatted"))
        if balance_val is None or balance_val <= 0:
            continue

        token["_sort_balance"] = balance_val
        filtered.append(token)

    filtered.sort(key=lambda t: t["_sort_balance"], reverse=True)
    for t in filtered:
        t.pop("_sort_balance", None)

    num_results = max(0, top_n - (1 if native_entry else 0))
    results = filtered[:num_results]

    if native_entry:
        return [native_entry] + results
    return results


async def get_wallet_balances(
    wallet_address: str, chain_name: str
) -> List[Dict[str, Any]]:
    wallet = require_non_empty_str(wallet_address, field="wallet_address")
    chain_name = require_non_empty_str(chain_name, field="chain_name")

    clean = wallet.strip().lower().removeprefix("0x")
    if len(clean) != 40 or any(c not in "0123456789abcdef" for c in clean):
        raise ValueError(
            format_with_recovery(
                f"Invalid Ethereum address: {wallet_address!r}",
                "provide a valid 20-byte hex wallet address and retry",
            )
        )

    chain_config = get_chain_by_name(chain_name)

    # Fetch native balance in parallel with token discovery
    native_task = _build_native_entry(chain_config, wallet)

    tokens_task = None
    try:
        # Check if CDP supports this network
        normalize_evm_network(chain_name)
        tokens_task = _fetch_balances_cdp(wallet, chain_config)
    except Exception:
        # Fallback to Alchemy if RPC URL suggests it
        if "alchemy" in chain_config.rpc_url.lower():
            tokens_task = _fetch_balances_alchemy(wallet, chain_config)

    if tokens_task:
        native_entry, tokens = await asyncio.gather(native_task, tokens_task)
    else:
        native_entry = await native_task
        tokens = []

    return _filter_and_rank_tokens(tokens, 10, native_entry, chain_config)
