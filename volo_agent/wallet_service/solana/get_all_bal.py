from decimal import Decimal
from typing import Any, Dict, List

from wallet_service.common.balance_utils import (
    format_decimal as _format_decimal,
)
from wallet_service.common.balance_utils import (
    parse_decimal as _parse_decimal,
)
from wallet_service.common.messages import format_with_recovery, require_non_empty_str
from wallet_service.solana.cdp_utils import (
    list_token_balances_async,
)

_MAX_REASONABLE_TOKEN_DECIMALS = 36


def _token_entry(
    token: Any,
    amount: Any,
) -> Dict[str, Any] | None:
    token_symbol = getattr(token, "symbol", None) if token else None
    token_name = getattr(token, "name", None) if token else None
    mint_address = getattr(token, "mint_address", None) if token else None

    raw_amount = _parse_decimal(getattr(amount, "amount", None) if amount else None)
    decimals = _parse_decimal(getattr(amount, "decimals", None) if amount else None)
    if raw_amount is None or decimals is None:
        return None

    try:
        decimals_int = int(decimals)
    except Exception:
        decimals_int = 0
    if decimals_int < 0 or decimals_int > _MAX_REASONABLE_TOKEN_DECIMALS:
        return None

    try:
        balance = raw_amount / (Decimal(10) ** decimals_int)
    except Exception:
        return None

    return {
        "name": token_name,
        "symbol": token_symbol,
        "decimals": decimals_int,
        "balance": str(raw_amount),
        "balance_formatted": _format_decimal(balance),
        "token_address": mint_address,
    }


async def get_wallet_balances_async(
    wallet_address: str, network: str | None = None, top_n: int = 10
) -> List[Dict[str, Any]]:
    wallet = require_non_empty_str(wallet_address, field="wallet_address")
    if not isinstance(top_n, int) or top_n < 0:
        raise ValueError(
            format_with_recovery(
                "Invalid top_n value",
                "provide top_n as an integer greater than or equal to zero",
            )
        )

    balances = await list_token_balances_async(wallet, network=network)

    tokens: list[dict[str, Any]] = []
    for item in balances:
        token = getattr(item, "token", None)
        amount = getattr(item, "amount", None)
        entry = _token_entry(token, amount)
        if entry is None:
            continue
        balance_value = _parse_decimal(entry.get("balance_formatted"))
        if balance_value is None or balance_value <= 0:
            continue
        entry["_sort_balance"] = balance_value
        tokens.append(entry)

    tokens.sort(key=lambda t: t["_sort_balance"], reverse=True)
    for token in tokens:
        token.pop("_sort_balance", None)

    return tokens[: max(0, top_n)]
