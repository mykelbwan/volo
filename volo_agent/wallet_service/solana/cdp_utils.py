from __future__ import annotations

import base64
from typing import Any, Iterable, cast

from solders.transaction import Transaction, VersionedTransaction

from config.solana_chains import get_solana_chain
from wallet_service.common.cdp_helpers import (
    await_cdp_call,
    managed_cdp_client,
)
from wallet_service.common.cdp_helpers import run_async as _run_async
from wallet_service.common.messages import (
    format_with_recovery,
    require_non_empty_str,
)


def normalize_solana_network(network: str | None) -> str:
    if not network:
        return "solana"
    try:
        return get_solana_chain(network).network
    except (KeyError, ValueError):
        raise ValueError(
            format_with_recovery(
                f"Unsupported Solana network: {network!r}",
                "use 'solana' or 'solana-devnet' and retry",
            )
        )


async def get_solana_account_async(account_name: str):
    account_name = require_non_empty_str(account_name, field="account_name")
    async with managed_cdp_client() as cdp:
        return await await_cdp_call(
            cast(Any, cdp).solana.get_account(name=account_name),
            operation="loading Solana account",
        )


def get_solana_account(account_name: str):
    return _run_async(get_solana_account_async(account_name))


async def create_solana_account_async(account_name: str):
    account_name = require_non_empty_str(account_name, field="account_name")
    async with managed_cdp_client() as cdp:
        return await await_cdp_call(
            cast(Any, cdp).solana.create_account(name=account_name),
            operation="creating Solana account",
        )


def create_solana_account(account_name: str):
    return _run_async(create_solana_account_async(account_name))


async def _get_solana_account_by_ref(cdp: Any, account_ref: str) -> Any:
    try:
        return await await_cdp_call(
            cdp.solana.get_account(name=account_ref),
            operation="loading Solana account",
        )
    except Exception as name_exc:
        try:
            return await await_cdp_call(
                cast(Any, cdp).solana.get_account(address=account_ref),
                operation="loading Solana account",
            )
        except Exception:
            raise name_exc


def _require_signed_solana_transaction(transaction_b64: str) -> None:
    try:
        tx_bytes = base64.b64decode(transaction_b64, validate=True)
    except Exception as exc:
        raise ValueError(
            format_with_recovery(
                "Invalid Solana transaction encoding",
                "provide a valid base64-encoded signed transaction and retry",
            )
        ) from exc

    try:
        tx = Transaction.from_bytes(tx_bytes)
        if not tx.is_signed():
            raise ValueError("legacy transaction is missing signatures")
        tx.verify()
        return
    except Exception:
        pass

    try:
        tx = VersionedTransaction.from_bytes(tx_bytes)
        results = tx.verify_with_results()
        if not results or not all(results):
            raise ValueError("versioned transaction is missing a valid signature")
        return
    except Exception as exc:
        raise ValueError(
            format_with_recovery(
                "Solana transaction must be signed before broadcast",
                "sign the transaction with the sender account and retry",
            )
        ) from exc


async def sign_solana_transaction_by_name_async(
    account_name: str, transaction_b64: str, sign_with: str | None = None
) -> str:
    account_ref = require_non_empty_str(account_name, field="account_name")
    tx_b64 = require_non_empty_str(transaction_b64, field="transaction_b64")
    sign_with_value = sign_with.strip() if isinstance(sign_with, str) else ""
    async with managed_cdp_client() as cdp:
        # Some call sites only have the wallet address. Resolve by name first
        # and fall back to address so signing stays backward compatible.
        account = await _get_solana_account_by_ref(cdp, account_ref)
        account_address = getattr(account, "address", None)
        if not account_address:
            raise KeyError(
                format_with_recovery(
                    "CDP Solana account response missing address",
                    "verify account provisioning in CDP and retry",
                )
            )
        # Solana public keys are base58 strings and should be matched as-is.
        if sign_with_value and account_address.strip() != sign_with_value:
            raise ValueError(
                format_with_recovery(
                    f"CDP account address mismatch: account={account_address}, sign_with={sign_with_value}",
                    "retry with the account's exact base58 address as 'sign_with'",
                )
            )
        signed = await await_cdp_call(
            account.sign_transaction(tx_b64),
            operation="signing Solana transaction",
        )
        signed_tx = getattr(signed, "signed_transaction", None)
        if not signed_tx:
            raise RuntimeError(
                format_with_recovery(
                    "CDP Solana signing did not return signed_transaction",
                    "retry signing; if it persists, inspect CDP SDK response format",
                )
            )
        if not isinstance(signed_tx, str):
            raise RuntimeError(
                format_with_recovery(
                    "CDP Solana signing did not return a transaction string",
                    "retry signing; if it persists, inspect CDP SDK response format",
                )
            )
        return signed_tx


async def send_solana_transaction_async(
    transaction_b64: str, network: str | None = None
) -> str:
    tx_b64 = require_non_empty_str(transaction_b64, field="transaction_b64")
    _require_signed_solana_transaction(tx_b64)
    async with managed_cdp_client() as cdp:
        signature = await await_cdp_call(
            cast(Any, cdp).solana.send_transaction(
                network=normalize_solana_network(network),
                transaction=tx_b64,
            ),
            operation="broadcasting Solana transaction",
        )
    value = getattr(signature, "transaction_signature", None)
    if isinstance(value, str) and value.strip():
        return value

    raise RuntimeError(
        format_with_recovery(
            "CDP Solana send did not return a transaction signature",
            "retry broadcast; if it persists, verify CDP Solana API response",
        )
    )


async def list_token_balances_async(
    address: str, network: str | None = None
) -> Iterable[Any]:
    wallet_address = require_non_empty_str(address, field="address")
    async with managed_cdp_client() as cdp:
        result = await await_cdp_call(
           cast(Any, cdp).solana.list_token_balances(
                address=wallet_address,
                network=normalize_solana_network(network),
            ),
            operation="loading Solana token balances",
        )
        return getattr(result, "balances", []) or []
