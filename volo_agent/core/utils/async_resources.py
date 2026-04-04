from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import logging
from contextlib import asynccontextmanager
from typing import Any, Callable, Optional

_LOGGER = logging.getLogger("volo.async_resources")
_RESOURCE_SCOPE: contextvars.ContextVar["AsyncResourceScope | None"] = (
    contextvars.ContextVar("volo_async_resource_scope", default=None)
)


class AsyncResourceScope:
    """
    Tracks async network clients created during one logical unit of work.
    """

    def __init__(self) -> None:
        self._cdp_client: Any = None
        self._cdp_client_lock = asyncio.Lock()
        self._web3_providers: list[Any] = []

    def register_web3_provider(self, provider: Any) -> None:
        if any(existing is provider for existing in self._web3_providers):
            return
        self._web3_providers.append(provider)

    async def get_or_create_cdp_client(self, factory: Callable[[], Any]) -> Any:
        client = self._cdp_client
        if client is not None and not getattr(client, "_closed", False):
            return client
        async with self._cdp_client_lock:
            client = self._cdp_client
            if client is not None and not getattr(client, "_closed", False):
                return client
            client = factory()
            self._cdp_client = client
            return client

    async def aclose(self) -> None:
        providers = tuple(self._web3_providers)
        self._web3_providers.clear()
        cdp_client = self._cdp_client
        self._cdp_client = None

        coroutines = []
        for provider in providers:
            disconnect = getattr(provider, "disconnect", None)
            if not callable(disconnect):
                continue
            try:
                result = disconnect()
            except Exception:
                _LOGGER.warning("async_resource_web3_disconnect_failed", exc_info=True)
                continue
            if inspect.isawaitable(result):
                coroutines.append(asyncio.shield(result))

        if cdp_client is not None:
            close = getattr(cdp_client, "close", None)
            if callable(close):
                try:
                    result = close()
                except Exception:
                    _LOGGER.warning("async_resource_cdp_close_failed", exc_info=True)
                else:
                    if inspect.isawaitable(result):
                        coroutines.append(asyncio.shield(result))

        if coroutines:
            results = await asyncio.gather(*coroutines, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    _LOGGER.warning(
                        "async_resource_cleanup_failed",
                        exc_info=(type(result), result, result.__traceback__),
                    )


def get_current_async_resource_scope() -> Optional[AsyncResourceScope]:
    return _RESOURCE_SCOPE.get()


@asynccontextmanager
async def async_resource_scope():
    existing_scope = get_current_async_resource_scope()
    if existing_scope is not None:
        yield existing_scope
        return

    scope = AsyncResourceScope()
    token = _RESOURCE_SCOPE.set(scope)
    try:
        yield scope
    finally:
        _RESOURCE_SCOPE.reset(token)
        with contextlib.suppress(Exception):
            await scope.aclose()
