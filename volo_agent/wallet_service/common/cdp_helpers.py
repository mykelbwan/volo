from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Awaitable, Protocol, TypeVar, cast, runtime_checkable

from core.utils.async_resources import get_current_async_resource_scope
from wallet_service.common.messages import format_with_recovery, require_non_empty_str

_DEFAULT_CDP_CALL_TIMEOUT_SECONDS = 45.0
_T = TypeVar("_T")


@runtime_checkable
class CdpClientProtocol(Protocol):
    def __init__(
        self,
        api_key_id: str,
        api_key_secret: str,
        wallet_secret: str | None = None,
    ) -> None: ...

    async def close(self) -> None: ...


@dataclass(frozen=True)
class CdpClientConfig:
    api_key_id: str
    api_key_secret: str
    wallet_secret: str | None
    disable_usage_tracking: bool
    disable_error_reporting: bool

    def __repr__(self) -> str:
        return (
            f"CdpClientConfig(api_key_id={self.api_key_id!r}, "
            f"api_key_secret='***', wallet_secret={'***' if self.wallet_secret else 'None'}, "
            f"disable_usage_tracking={self.disable_usage_tracking}, "
            f"disable_error_reporting={self.disable_error_reporting})"
        )


def _cdp_client_cls() -> type[CdpClientProtocol]:
    from cdp import CdpClient

    return cast(type[CdpClientProtocol], CdpClient)


def _read_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return None


# Module-level cache for configuration and timeout to avoid repeated os.getenv calls in hot loops.
_CACHED_CONFIG: CdpClientConfig | None = None
_CACHED_TIMEOUT: float | None = None


def get_cdp_client_config() -> CdpClientConfig:
    """
    Load CDP credentials and local policy flags without mutating ``os.environ``.
    """
    global _CACHED_CONFIG
    if _CACHED_CONFIG is not None:
        return _CACHED_CONFIG

    api_key_id = _read_env("CDP_API_KEY_ID", "COINBASE_API_KEY_ID")
    api_key_secret = _read_env("CDP_API_KEY_SECRET", "COINBASE_SECRET_KEY")
    wallet_secret = _read_env("CDP_WALLET_SECRET", "COINBASE_SERVER_WALLET")

    if not api_key_id or not api_key_secret:
        raise ValueError(
            format_with_recovery(
                "Missing CDP API credentials",
                "set CDP_API_KEY_ID/CDP_API_KEY_SECRET or Coinbase-compatible equivalents, then retry",
            )
        )

    usage_tracking = os.getenv("DISABLE_CDP_USAGE_TRACKING", "").strip().lower()
    error_reporting = os.getenv("DISABLE_CDP_ERROR_REPORTING", "").strip().lower()

    _CACHED_CONFIG = CdpClientConfig(
        api_key_id=api_key_id,
        api_key_secret=api_key_secret,
        wallet_secret=wallet_secret,
        # Default to disabled locally, but do not write back into global env.
        disable_usage_tracking=usage_tracking != "false",
        disable_error_reporting=error_reporting != "false",
    )
    return _CACHED_CONFIG


def run_async(coro):
    """
    Bridge sync callers to async CDP helpers.

    Sync wrappers must never leak ``Task`` objects to callers. When invoked from
    an active event loop we fail fast so the caller crosses the async boundary
    explicitly instead of silently receiving a scheduled task.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    if inspect.iscoroutine(coro):
        coro.close()
    raise RuntimeError(
        "run_async() cannot be used from a running event loop. "
        "Await the async CDP helper directly instead."
    )


def build_deterministic_account_name(volo_user_id: str) -> str:
    user_id = require_non_empty_str(volo_user_id, field="volo_user_id")
    digest = hashlib.sha256(user_id.encode("utf-8")).hexdigest()
    return f"volo-{digest}"


def cdp_call_timeout_seconds() -> float:
    global _CACHED_TIMEOUT
    if _CACHED_TIMEOUT is not None:
        return _CACHED_TIMEOUT

    raw = os.getenv("CDP_CALL_TIMEOUT_SECONDS", "").strip()
    if not raw:
        _CACHED_TIMEOUT = _DEFAULT_CDP_CALL_TIMEOUT_SECONDS
    else:
        try:
            value = float(raw)
            _CACHED_TIMEOUT = value if value > 0 else _DEFAULT_CDP_CALL_TIMEOUT_SECONDS
        except ValueError:
            _CACHED_TIMEOUT = _DEFAULT_CDP_CALL_TIMEOUT_SECONDS
    return _CACHED_TIMEOUT


async def await_cdp_call(
    awaitable: Awaitable[_T],
    *,
    operation: str,
    timeout_seconds: float | None = None,
) -> _T:
    timeout = timeout_seconds or cdp_call_timeout_seconds()
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(
            f"CDP timeout while {operation} after {timeout:.1f}s. "
            "Please retry. If this keeps happening, check network connectivity or CDP status."
        ) from exc


@asynccontextmanager
async def managed_cdp_client():
    config = get_cdp_client_config()
    client_cls = _cdp_client_cls()
    scope = get_current_async_resource_scope()

    def _build_client() -> CdpClientProtocol:
        # Pass credentials explicitly so secrets never flow through global env.
        return client_cls(
            api_key_id=config.api_key_id,
            api_key_secret=config.api_key_secret,
            wallet_secret=config.wallet_secret,
        )

    if scope is not None:
        client = await scope.get_or_create_cdp_client(_build_client)
        yield client
        # Resource scope is responsible for closing the client if it manages it.
        return

    client = _build_client()
    try:
        yield client
    finally:
        # Ensure cleanup for ad-hoc clients.
        close = getattr(client, "close", None)
        if callable(close):
            with contextlib.suppress(Exception):
                maybe_awaitable = close()
                if inspect.isawaitable(maybe_awaitable):
                    await asyncio.shield(cast(Awaitable[object], maybe_awaitable))
