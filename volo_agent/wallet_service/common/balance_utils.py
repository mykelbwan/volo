from __future__ import annotations

from decimal import Decimal, InvalidOperation


def format_decimal(value: Decimal) -> str:
    as_str = format(value, "f")
    if "." not in as_str:
        return as_str
    trimmed = as_str.rstrip("0").rstrip(".")
    return trimmed if trimmed else "0"


def parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None
