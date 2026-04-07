from __future__ import annotations

from decimal import Decimal, InvalidOperation

from intent_hub.utils.messages import format_with_recovery


def to_wei(amount: Decimal | str | float | int, decimals: int) -> int:
    if not isinstance(decimals, int) or decimals < 0:
        raise ValueError(
            format_with_recovery(
                "Invalid decimals value",
                "provide token decimals as a non-negative integer and retry",
            )
        )
    try:
        value = Decimal(str(amount))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(
            format_with_recovery(
                "Invalid token amount",
                "provide amount as a numeric value greater than zero and retry",
            )
        ) from exc
    if not value.is_finite() or value <= 0:
        raise ValueError(
            format_with_recovery(
                "Invalid token amount",
                "provide amount as a numeric value greater than zero and retry",
            )
        )
    base_units = int(value * (Decimal(10) ** decimals))
    if base_units <= 0:
        raise ValueError(
            format_with_recovery(
                "Amount is too small after conversion to base units",
                "increase amount or verify token decimals and retry",
            )
        )
    return base_units
