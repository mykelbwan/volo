from __future__ import annotations

import asyncio
import inspect
import logging
import time
from functools import wraps
from typing import Any, Callable, Optional

from core.utils.timeouts import NODE_TIMEOUT_SECONDS

_NODE_LOGGER = logging.getLogger("volo.telemetry")
_EXTERNAL_LOGGER = logging.getLogger("volo.external")


def record_external_call(
    *,
    service: Optional[str],
    method: str,
    url: str,
    duration_ms: float,
    status_code: Optional[int] = None,
    error: Optional[BaseException] = None,
) -> None:
    status = status_code if status_code is not None else "unknown"
    service_label = service or "unknown"
    if error is None:
        _EXTERNAL_LOGGER.info(
            "external_call service=%s method=%s url=%s status=%s duration_ms=%.2f",
            service_label,
            method,
            url,
            status,
            duration_ms,
        )
    else:
        _EXTERNAL_LOGGER.warning(
            "external_call_failed service=%s method=%s url=%s status=%s duration_ms=%.2f error=%s",
            service_label,
            method,
            url,
            status,
            duration_ms,
            error,
        )


def wrap_node(
    node_name: str,
    node_fn: Callable[..., Any],
    *,
    timeout_seconds: float = NODE_TIMEOUT_SECONDS,
) -> Callable[..., Any]:
    @wraps(node_fn)
    async def _wrapped(*args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        status = "ok"
        try:
            result = node_fn(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await asyncio.wait_for(result, timeout=timeout_seconds)
            return result
        except asyncio.TimeoutError:
            status = "timeout"
            _NODE_LOGGER.warning(
                "node_timeout node=%s timeout_s=%.2f",
                node_name,
                timeout_seconds,
            )
            raise
        except asyncio.CancelledError:
            status = "cancelled"
            _NODE_LOGGER.warning("node_cancelled node=%s", node_name)
            raise
        except Exception:
            status = "error"
            _NODE_LOGGER.exception("node_error node=%s", node_name)
            raise
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            _NODE_LOGGER.info(
                "node_execution node=%s status=%s duration_ms=%.2f",
                node_name,
                status,
                duration_ms,
            )

    return _wrapped
