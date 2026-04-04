from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


def format_with_recovery(reason: str, recovery: str) -> str:
    reason_text = str(reason).strip().rstrip(".")
    recovery_text = str(recovery).strip().rstrip(".")
    return f"{reason_text}. Recovery: {recovery_text}."


def require_fields(
    parameters: Mapping[str, Any],
    required: list[str],
    *,
    context: str,
    exception_cls: type[Exception] = ValueError,
) -> None:
    missing = []
    for field in required:
        value = parameters.get(field)
        if value is None:
            missing.append(field)
            continue
        if isinstance(value, str) and not value.strip():
            missing.append(field)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise exception_cls(
            format_with_recovery(
                f"Missing required {context} parameters: {missing_list}",
                "provide the missing fields and retry",
            )
        )


def parse_decimal_field(
    value: Any,
    *,
    field: str,
    positive: bool = False,
    min_value: Decimal | None = None,
    max_value: Decimal | None = None,
    exception_cls: type[Exception] = ValueError,
    invalid_recovery: str | None = None,
) -> Decimal:
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError, AttributeError):
        raise exception_cls(
            format_with_recovery(
                f"Invalid value for '{field}': {value!r}",
                invalid_recovery or f"pass '{field}' as a numeric value",
            )
        ) from None

    if not parsed.is_finite():
        raise exception_cls(
            format_with_recovery(
                f"Invalid value for '{field}': {value!r}",
                invalid_recovery or f"pass '{field}' as a finite numeric value",
            )
        )

    if positive and parsed <= 0:
        raise exception_cls(
            format_with_recovery(
                f"'{field}' must be greater than zero",
                invalid_recovery or f"use a positive '{field}'",
            )
        )
    if min_value is not None and parsed < min_value:
        raise exception_cls(
            format_with_recovery(
                f"'{field}' must be at least {min_value}",
                invalid_recovery or f"use a '{field}' value >= {min_value}",
            )
        )
    if max_value is not None and parsed > max_value:
        raise exception_cls(
            format_with_recovery(
                f"'{field}' must be at most {max_value}",
                invalid_recovery or f"use a '{field}' value <= {max_value}",
            )
        )
    return parsed


def parse_float_field(
    value: Any,
    *,
    field: str,
    default: float | None = None,
    min_value: float | None = None,
    max_value: float | None = None,
    exception_cls: type[Exception] = ValueError,
    invalid_recovery: str | None = None,
) -> float:
    if value is None and default is not None:
        return float(default)
    decimal_value = parse_decimal_field(
        value,
        field=field,
        exception_cls=exception_cls,
        invalid_recovery=invalid_recovery,
    )
    parsed = float(decimal_value)
    if min_value is not None and parsed < min_value:
        raise exception_cls(
            format_with_recovery(
                f"'{field}' must be at least {min_value}",
                invalid_recovery or f"use a '{field}' value >= {min_value}",
            )
        )
    if max_value is not None and parsed > max_value:
        raise exception_cls(
            format_with_recovery(
                f"'{field}' must be at most {max_value}",
                invalid_recovery or f"use a '{field}' value <= {max_value}",
            )
        )
    return parsed


def safe_decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError, AttributeError):
        return None
    if not parsed.is_finite():
        return None
    return parsed

