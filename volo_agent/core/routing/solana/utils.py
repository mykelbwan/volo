from __future__ import annotations

from decimal import Decimal
from typing import Any

def lamports_from_decimal(amount: Decimal, decimals: int) -> int:
    return int(amount * Decimal(10**decimals))


def decimal_from_lamports(raw: int, decimals: int) -> Decimal:
    return Decimal(raw) / Decimal(10**decimals)


def safe_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback
