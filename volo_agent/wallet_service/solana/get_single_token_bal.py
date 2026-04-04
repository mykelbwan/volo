from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from solana.rpc.types import TokenAccountOpts
from solders.pubkey import Pubkey

from config.solana_chains import get_solana_chain
from wallet_service.common.messages import format_with_recovery, require_non_empty_str
from wallet_service.solana.cdp_utils import normalize_solana_network
from wallet_service.solana.rpc_client import get_shared_solana_client


def _invalid_pubkey(field: str, value: str) -> ValueError:
    return ValueError(
        format_with_recovery(
            f"Invalid {field}: {value!r}",
            f"provide a valid {field.replace('_', ' ')} base58 address and retry",
        )
    )


def _malformed_parsed_account(reason: str) -> RuntimeError:
    return RuntimeError(
        format_with_recovery(
            f"Unexpected Solana parsed token account shape: {reason}",
            "retry once; if it persists, inspect the upstream RPC parsed-account response",
        )
    )


def _validate_pubkey(value: str, *, field: str) -> Pubkey:
    text = require_non_empty_str(value, field=field)
    try:
        return Pubkey.from_string(text)
    except Exception as exc:
        raise _invalid_pubkey(field, text) from exc


def _require_attr(value: Any, attribute: str, *, context: str) -> Any:
    result = getattr(value, attribute, None)
    if result is None:
        raise _malformed_parsed_account(f"{context} missing {attribute!r}")
    return result


def _require_mapping(value: Any, *, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _malformed_parsed_account(f"{context} is not a mapping")
    return value


def _parse_raw_amount(value: Any) -> Decimal:
    if not isinstance(value, str) or not value.isdigit():
        raise _malformed_parsed_account(
            "tokenAmount.amount must be a base-10 digit string"
        )
    return Decimal(value)


def _parse_decimals(value: Any) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        or value > 255
    ):
        raise _malformed_parsed_account(
            "tokenAmount.decimals must be a non-negative integer within SPL token bounds"
        )
    return value


def _extract_account_balance(account_entry: Any, *, expected_mint: str) -> Decimal:
    account = _require_attr(account_entry, "account", context="token account entry")
    data = _require_attr(account, "data", context="token account")
    parsed = _require_attr(data, "parsed", context="token account data")
    parsed_mapping = _require_mapping(parsed, context="parsed token account")
    info = _require_mapping(
        parsed_mapping.get("info"), context="parsed token account info"
    )

    mint = info.get("mint")
    if not isinstance(mint, str) or not mint:
        raise _malformed_parsed_account("parsed token account info missing mint")
    if mint != expected_mint:
        raise _malformed_parsed_account(
            "parsed token account mint does not match the requested mint"
        )

    token_amount = _require_mapping(
        info.get("tokenAmount"),
        context="parsed token account tokenAmount",
    )
    raw_amount = _parse_raw_amount(token_amount.get("amount"))
    decimals = _parse_decimals(token_amount.get("decimals"))
    return raw_amount / (Decimal(10) ** decimals)


def _extract_token_balance_from_rpc_response(
    response: Any, *, expected_mint: str
) -> Decimal:
    accounts = getattr(response, "value", None)
    if not isinstance(accounts, list):
        raise _malformed_parsed_account("response.value is not a list")

    total = Decimal(0)
    for account_entry in accounts:
        total += _extract_account_balance(account_entry, expected_mint=expected_mint)
    return total


async def get_token_balance_async(
    wallet_address: str, token_mint: str, network: str | None = None
) -> Decimal:
    wallet = require_non_empty_str(wallet_address, field="wallet_address")
    mint = require_non_empty_str(token_mint, field="token_mint")
    owner_pubkey = _validate_pubkey(wallet, field="wallet_address")
    mint_pubkey = _validate_pubkey(mint, field="token_mint")
    normalized_network = normalize_solana_network(network)
    rpc_url = get_solana_chain(normalized_network).rpc_url
    client = await get_shared_solana_client(rpc_url)
    response = await client.get_token_accounts_by_owner_json_parsed(
        owner_pubkey,
        TokenAccountOpts(mint=mint_pubkey),
        commitment="confirmed",
    )
    return _extract_token_balance_from_rpc_response(
        response,
        expected_mint=str(mint_pubkey),
    )
