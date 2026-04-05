from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import web3.exceptions
from pydantic import ValidationError

from core.security.guardrails import RiskViolationError
from core.utils.errors import (
    NonRetryableError,
    RouteExpiredError,
    SlippageExceededError,
)


@dataclass(frozen=True)
class ApiErrorMapping:
    status_code: int
    error: str
    retryable: bool
    default_message: str


def map_turn_error(exc: Exception) -> ApiErrorMapping:
    """
    Map runtime exceptions to HTTP-facing error payload metadata.

    Order matters: specific exceptions must be checked before broad parents.
    """
    if isinstance(exc, ValidationError):
        return ApiErrorMapping(
            status_code=400,
            error="validation_error",
            retryable=False,
            default_message="Invalid request payload.",
        )

    if isinstance(exc, RiskViolationError):
        return ApiErrorMapping(
            status_code=403,
            error="guardrail_violation",
            retryable=False,
            default_message="Request blocked by security guardrails.",
        )

    if isinstance(exc, SlippageExceededError):
        return ApiErrorMapping(
            status_code=409,
            error="slippage_exceeded",
            retryable=True,
            default_message="Execution output fell below the configured minimum.",
        )

    if isinstance(exc, RouteExpiredError):
        return ApiErrorMapping(
            status_code=409,
            error="route_expired",
            retryable=True,
            default_message="Quoted route expired before execution.",
        )

    if isinstance(exc, NonRetryableError):
        return ApiErrorMapping(
            status_code=422,
            error="non_retryable_error",
            retryable=False,
            default_message="Request failed with a non-retryable execution error.",
        )

    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return ApiErrorMapping(
            status_code=504,
            error="timeout",
            retryable=True,
            default_message="Request timed out while waiting for execution.",
        )

    if isinstance(exc, (ConnectionError, web3.exceptions.Web3Exception)):
        return ApiErrorMapping(
            status_code=502,
            error="upstream_error",
            retryable=True,
            default_message="Upstream blockchain/provider request failed.",
        )

    if isinstance(exc, (ValueError, TypeError, KeyError)):
        return ApiErrorMapping(
            status_code=400,
            error="bad_request",
            retryable=False,
            default_message="Request could not be processed.",
        )

    return ApiErrorMapping(
        status_code=500,
        error="internal_error",
        retryable=True,
        default_message="Internal server error.",
    )


def build_error_response_body(
    *,
    mapping: ApiErrorMapping,
    message: str | None = None,
    details: Any | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "error": mapping.error,
        "message": message or mapping.default_message,
        "retryable": mapping.retryable,
    }
    if details is not None:
        body["details"] = details
    return body
