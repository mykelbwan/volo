from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any, Dict, Tuple, TypedDict

from wallet_service.common.cdp_helpers import (
    await_cdp_call,
    managed_cdp_client,
)
from wallet_service.common.cdp_helpers import (
    run_async as _run_async,
)
from wallet_service.common.messages import (
    format_with_recovery,
    require_mapping,
    require_non_empty_str,
)


class EvmTransaction(TypedDict, total=False):
    to: str
    value: int | str
    data: str
    nonce: int
    gasLimit: int | str
    maxFeePerGas: int | str
    maxPriorityFeePerGas: int | str
    chainId: int


_EVM_CDP_NETWORKS = {
    "ethereum": "ethereum",
    "eth": "ethereum",
    "base": "base",
    "base sepolia": "base-sepolia",
    "base-sepolia": "base-sepolia",
}

# Simple in-memory cache for EVM accounts to reduce CDP API round-trips.
# Key: account_name, Value: (account_object, expiry_timestamp)
_ACCOUNT_CACHE: Dict[str, Tuple[Any, float]] = {}
_ACCOUNT_CACHE_TTL_SECONDS = 600  # 10 minutes


def clear_evm_account_cache():
    """Clear the in-memory account cache. Useful for testing."""
    _ACCOUNT_CACHE.clear()


def normalize_evm_network(network: str | None) -> str:
    key = require_non_empty_str(network, field="network").strip().lower()
    if key in _EVM_CDP_NETWORKS:
        return _EVM_CDP_NETWORKS[key]
    raise ValueError(
        format_with_recovery(
            "Unsupported EVM network for CDP balance lookup",
            "use Ethereum, Base, or Base Sepolia, or fall back to direct RPC balance checks",
        )
    )


async def get_evm_account_async(account_name: str):
    account_name = require_non_empty_str(account_name, field="account_name")

    # Check cache first
    now = time.time()
    if account_name in _ACCOUNT_CACHE:
        account, expiry = _ACCOUNT_CACHE[account_name]
        if now < expiry:
            return account
        # Evict expired entry
        _ACCOUNT_CACHE.pop(account_name, None)

    async with managed_cdp_client() as cdp:
        account = await await_cdp_call(
            cdp.evm.get_account(name=account_name),
            operation="loading EVM account",
        )
        # Update cache
        _ACCOUNT_CACHE[account_name] = (account, now + _ACCOUNT_CACHE_TTL_SECONDS)
        return account


def get_evm_account(account_name: str):
    return _run_async(get_evm_account_async(account_name))


async def create_evm_account_async(account_name: str):
    account_name = require_non_empty_str(account_name, field="account_name")
    async with managed_cdp_client() as cdp:
        account = await await_cdp_call(
            cdp.evm.create_account(name=account_name),
            operation="creating EVM account",
        )
        # Update cache
        _ACCOUNT_CACHE[account_name] = (
            account,
            time.time() + _ACCOUNT_CACHE_TTL_SECONDS,
        )
        return account


def create_evm_account(account_name: str):
    return _run_async(create_evm_account_async(account_name))


async def list_token_balances_async(
    address: str,
    network: str,
    page_size: int | None = None,
    page_token: str | None = None,
):
    wallet_address = require_non_empty_str(address, field="address")
    async with managed_cdp_client() as cdp:
        return await await_cdp_call(
            cdp.evm.list_token_balances(
                address=wallet_address,
                network=normalize_evm_network(network),
                page_size=page_size,
                page_token=page_token,
            ),
            operation="loading EVM token balances",
        )


def _format_signed_raw(signed) -> str:
    raw = signed.raw_transaction
    if isinstance(raw, bytes):
        return "0x" + raw.hex()
    if isinstance(raw, str):
        return raw if raw.startswith("0x") else "0x" + raw
    raise ValueError(
        format_with_recovery(
            "CDP signing returned unsupported raw transaction type",
            "retry signing; if it persists, inspect CDP SDK response schema for this account",
        )
    )


async def sign_evm_transaction_by_name_async(
    account_name: str,
    unsigned_tx: EvmTransaction | Dict[str, Any],
    sign_with: str | None = None,
    account: Any | None = None,
) -> str:
    account_name = require_non_empty_str(account_name, field="account_name")
    tx_fields: Mapping[str, Any] = require_mapping(unsigned_tx, field="unsigned_tx")

    # Basic validation of required EVM fields for signing
    if not tx_fields.get("to") and not tx_fields.get("data"):
        raise ValueError(
            format_with_recovery(
                "EVM transaction must have either 'to' or 'data' field",
                "provide a recipient address or contract deployment data",
            )
        )

    sign_with_value = sign_with.strip() if isinstance(sign_with, str) else ""

    async with managed_cdp_client() as cdp:
        if account is None:
            # Try cache first
            now = time.time()
            if account_name in _ACCOUNT_CACHE:
                cached_account, expiry = _ACCOUNT_CACHE[account_name]
                if now < expiry:
                    account = cached_account

            if account is None:
                account = await await_cdp_call(
                    cdp.evm.get_account(name=account_name),
                    operation="loading EVM account",
                )
                # Update cache
                _ACCOUNT_CACHE[account_name] = (
                    account,
                    now + _ACCOUNT_CACHE_TTL_SECONDS,
                )

        if account is None:
            raise RuntimeError(
                format_with_recovery(
                    "CDP returned no EVM account object",
                    "retry account lookup; if it persists, verify account exists in CDP",
                )
            )

        account_address = getattr(account, "address", None)
        if not account_address:
            raise KeyError(
                format_with_recovery(
                    "CDP account response missing address",
                    "verify account provisioning in CDP and retry",
                )
            )
        if sign_with_value and account_address.lower() != sign_with_value.lower():
            raise ValueError(
                format_with_recovery(
                    f"CDP account address mismatch: account={account_address}, sign_with={sign_with_value}",
                    "retry with the account's exact wallet address as 'sign_with'",
                )
            )

        signed = await await_cdp_call(
            account.sign_transaction(dict(tx_fields)),
            operation="signing EVM transaction",
        )
        raw = _format_signed_raw(signed)
        if not isinstance(raw, str):
            raise RuntimeError(
                format_with_recovery(
                    "CDP signing did not return a raw transaction string",
                    "retry signing; if it persists, check CDP response format",
                )
            )
        return raw
