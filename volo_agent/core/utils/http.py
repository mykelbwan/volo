from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Optional

import httpx
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.utils.telemetry import record_external_call
from core.utils.timeouts import EXTERNAL_HTTP_TIMEOUT_SECONDS

_RETRY = Retry(
    total=3,
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods={"GET", "POST"},
    raise_on_status=False,
)

_SESSION = requests.Session()
_SESSION.mount("https://", HTTPAdapter(max_retries=_RETRY))
_SESSION.mount("http://", HTTPAdapter(max_retries=_RETRY))

_ASYNC_CLIENTS: dict[int, httpx.AsyncClient] = {}
_ASYNC_CLIENTS_LOCK = threading.Lock()
_ASYNC_LIMITS = httpx.Limits(
    max_connections=256,
    max_keepalive_connections=64,
    keepalive_expiry=30.0,
)


def _format_body_excerpt(body: str, *, max_chars: int = 400) -> str:
    text = (body or "").strip()
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}..."


def _build_async_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(limits=_ASYNC_LIMITS)


async def _get_shared_async_client() -> httpx.AsyncClient:
    loop_id = id(asyncio.get_running_loop())
    with _ASYNC_CLIENTS_LOCK:
        client = _ASYNC_CLIENTS.get(loop_id)
        if client is None or client.is_closed:
            client = _build_async_client()
            _ASYNC_CLIENTS[loop_id] = client
        return client


async def close_shared_async_http_clients() -> None:
    """
    Close pooled async HTTP clients (mainly useful in graceful shutdown/tests).
    """
    with _ASYNC_CLIENTS_LOCK:
        clients = list(_ASYNC_CLIENTS.values())
        _ASYNC_CLIENTS.clear()
    if not clients:
        return
    await asyncio.gather(*(client.aclose() for client in clients), return_exceptions=True)


class ExternalServiceError(RuntimeError):
    def __init__(self, service: str, status_code: Optional[int], body: str) -> None:
        self.service = service
        self.status_code = status_code
        self.body = body
        excerpt = _format_body_excerpt(body)
        message = (
            f"{service} request failed with HTTP {status_code}. "
            "Recovery path: retry shortly; if this persists, verify upstream status/API credentials. "
            f"Response excerpt: {excerpt}"
        )
        super().__init__(message)


def raise_for_status(response: requests.Response, service: str) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        status = response.status_code if response is not None else None
        body = response.text if response is not None else ""
        raise ExternalServiceError(service, status, body) from exc


def request_json(
    method: str,
    url: str,
    *,
    timeout: float = EXTERNAL_HTTP_TIMEOUT_SECONDS,
    service: str | None = None,
    **kwargs: Any,
) -> requests.Response:
    """
    Make an HTTP request with retries and a default timeout.
    Returns the raw response so callers can handle error formatting.
    """
    start = time.perf_counter()
    if "timeout" not in kwargs:
        kwargs["timeout"] = timeout
    try:
        response = _SESSION.request(method, url, **kwargs)
    except requests.RequestException as exc:
        duration_ms = (time.perf_counter() - start) * 1000
        record_external_call(
            service=service,
            method=method,
            url=url,
            duration_ms=duration_ms,
            error=exc,
        )
        raise

    duration_ms = (time.perf_counter() - start) * 1000
    record_external_call(
        service=service,
        method=method,
        url=url,
        duration_ms=duration_ms,
        status_code=response.status_code,
    )
    return response


async def async_raise_for_status(response: httpx.Response, service: str) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status = response.status_code if response is not None else None
        body = response.text if response is not None else ""
        raise ExternalServiceError(service, status, body) from exc


async def async_request_json(
    method: str,
    url: str,
    *,
    timeout: float = EXTERNAL_HTTP_TIMEOUT_SECONDS,
    service: str | None = None,
    **kwargs: Any,
) -> httpx.Response:
    """
    Async HTTP request with retries and a default timeout.
    Returns the raw response so callers can handle error formatting.
    """
    start = time.perf_counter()
    method = str(method or "").upper().strip()
    if not method:
        raise ValueError(
            "Invalid HTTP method for async request. Recovery path: pass a non-empty HTTP method such as GET or POST."
        )
    if not isinstance(url, str) or not url.strip():
        raise ValueError(
            "Invalid URL for async request. Recovery path: provide a non-empty URL string."
        )

    if "timeout" not in kwargs:
        kwargs["timeout"] = timeout

    client = await _get_shared_async_client()
    last_exc: Exception | None = None
    for attempt in range(1, 4):
        try:
            response = await client.request(method, url, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt < 3:
                await asyncio.sleep(0.3 * attempt)
                continue
            duration_ms = (time.perf_counter() - start) * 1000
            record_external_call(
                service=service,
                method=method,
                url=url,
                duration_ms=duration_ms,
                error=exc,
            )
            raise

        duration_ms = (time.perf_counter() - start) * 1000
        record_external_call(
            service=service,
            method=method,
            url=url,
            duration_ms=duration_ms,
            status_code=response.status_code,
        )

        if response.status_code in {429, 500, 502, 503, 504} and attempt < 3:
            await asyncio.sleep(0.3 * attempt)
            continue

        return response

    if last_exc:
        raise last_exc
    raise RuntimeError(
        "Async HTTP request failed unexpectedly without an error. "
        "Recovery path: retry once; if this repeats, capture request telemetry and inspect upstream service health."
    )
