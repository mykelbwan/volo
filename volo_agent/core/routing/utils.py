from __future__ import annotations

from decimal import Decimal
from typing import Any


def safe_decimal(value: Any, fallback: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(fallback)
