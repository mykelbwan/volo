from __future__ import annotations

from decimal import Decimal
from typing import Any


def safe_int(value: Any, fallback: int = 0) -> int:
    if value is None:
        return fallback
    if isinstance(value, int):
        return value
    s = str(value).strip()
    try:
        if s.startswith("0x"):
            return int(s, 16)
        return int(s)
    except (ValueError, TypeError):
        return fallback


def to_raw(amount: Decimal, decimals: int) -> int:
    return int(amount * Decimal(10**decimals))


__all__ = [
    "safe_int",
    "to_raw",
]
