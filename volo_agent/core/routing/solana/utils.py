from __future__ import annotations

from decimal import Decimal
from typing import Any

def lamports_from_decimal(amount: Decimal, decimals: int) -> int:
    """Convert a human-readable Decimal to the token's raw smallest unit."""
    return int(amount * Decimal(10**decimals))


def decimal_from_lamports(raw: int, decimals: int) -> Decimal:
    """Convert a raw integer token amount back to human-readable Decimal."""
    return Decimal(raw) / Decimal(10**decimals)


def safe_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback
