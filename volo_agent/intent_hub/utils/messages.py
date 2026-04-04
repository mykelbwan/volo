from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def format_with_recovery(reason: str, recovery: str) -> str:
    reason_text = str(reason).strip().rstrip(".")
    recovery_text = str(recovery).strip().rstrip(".")
    if not reason_text:
        reason_text = "Operation failed"
    if not recovery_text:
        recovery_text = "retry with corrected inputs"
    return f"{reason_text}. Recovery: {recovery_text}."


def require_non_empty_str(value: object, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(
            format_with_recovery(
                f"Missing required value for '{field}'",
                f"provide a non-empty '{field}' and retry",
            )
        )
    return text


def require_mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError(
        format_with_recovery(
            f"Invalid '{field}' payload type",
            f"provide '{field}' as an object/dict and retry",
        )
    )


def has_template_marker(value: object) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    return "{{" in text and "}}" in text
