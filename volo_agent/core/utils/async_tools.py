from __future__ import annotations

import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Callable, TypeVar
from weakref import WeakKeyDictionary, finalize

from core.utils.timeouts import TOOL_TIMEOUT_SECONDS

_T = TypeVar("_T")
_DEFAULT_MAX_WORKERS = 16
_LOOP_EXECUTORS: WeakKeyDictionary[asyncio.AbstractEventLoop, ThreadPoolExecutor] = (
    WeakKeyDictionary()
)


def _shutdown_executor(executor: ThreadPoolExecutor) -> None:
    executor.shutdown(wait=False, cancel_futures=True)


def _get_loop_executor(loop: asyncio.AbstractEventLoop) -> ThreadPoolExecutor:
    cached = _LOOP_EXECUTORS.get(loop)
    if cached is not None:
        return cached
    # Keep executor ownership per event loop to avoid cross-loop timeout stalls.
    created = ThreadPoolExecutor(max_workers=_DEFAULT_MAX_WORKERS)
    _LOOP_EXECUTORS[loop] = created
    # Ensure threads are cleaned up when the loop object is released.
    finalize(loop, _shutdown_executor, created)
    return created


def _is_async_callable(func: Callable[..., object]) -> bool:
    if inspect.iscoroutinefunction(func):
        return True
    call = getattr(func, "__call__", None)
    return inspect.iscoroutinefunction(call)


def _ensure_non_awaitable_result(result: _T) -> _T:
    if inspect.isawaitable(result):
        # Avoid "coroutine was never awaited" warnings for bad call paths.
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError(
            "run_blocking() cannot run async callables. "
            "Pass a synchronous function, or await the async callable directly."
        )
    return result


async def run_blocking(
    func: Callable[..., _T],
    *args,
    timeout: float | None = TOOL_TIMEOUT_SECONDS,
    executor: ThreadPoolExecutor | None = None,
) -> _T:
    loop = asyncio.get_running_loop()
    if timeout is not None and timeout <= 0:
        raise ValueError(
            f"Invalid timeout={timeout!r}. Provide a positive number of seconds or None."
        )
    if _is_async_callable(func):
        raise TypeError(
            "run_blocking() cannot run async callables. "
            "Pass a synchronous function, or await the async callable directly."
        )
    exec_to_use = executor or _get_loop_executor(loop)
    task = loop.run_in_executor(exec_to_use, partial(func, *args))
    try:
        if timeout is None:
            return _ensure_non_awaitable_result(await task)
        result = await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        return _ensure_non_awaitable_result(result)
    except asyncio.TimeoutError as exc:
        if task.done():
            return _ensure_non_awaitable_result(task.result())
        task.cancel()
        raise asyncio.TimeoutError(
            f"Blocking operation exceeded timeout after {timeout:.3f}s."
        ) from exc
    except asyncio.CancelledError:
        task.cancel()
        raise
