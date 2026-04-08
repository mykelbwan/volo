from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

from config.chains import ChainConfig, get_chain_by_id, get_chain_by_name
from core.utils.evm_async import get_shared_async_web3
from core.utils.upstash_client import get_async_redis

# Cache TTL in seconds — gas price is refreshed at most once every 50 seconds
_CACHE_TTL_SECONDS = 50
_VOLATILE_CACHE_TTL_SECONDS = 10
_REDIS_KEY_PREFIX = "gas_price"
_MIN_PRIORITY_FEE_WEI = 1_500_000_000
_DEFAULT_PRIORITY_FEE_WEI = 2_000_000_000
_MAX_GAS_PRICE_WEI = 5000 * 10**9  # 5,000 Gwei — sanity cap for safety.


def _parse_int(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, bytes):
        try:
            return int(value.decode("utf-8"), 0)
        except Exception:
            return None
    if isinstance(value, str):
        try:
            return int(value, 0)
        except Exception:
            return None
    try:
        return int(value)  # type: ignore[arg-type]
    except Exception:
        return None


@dataclass
class _CacheEntry:
    gas_price_wei: int
    fetched_at: float  # Unix timestamp
    ttl_seconds: float = _CACHE_TTL_SECONDS


@dataclass
class _RefreshEntry:
    event: asyncio.Event


@dataclass
class GasPriceCache:
    _entries: dict[int, _CacheEntry] = field(default_factory=dict)
    _state_lock: threading.Lock = field(default_factory=threading.Lock)
    _refreshing: dict[tuple[int, int], _RefreshEntry] = field(default_factory=dict)

    def _resolve_chain(
        self,
        chain_id: int | None,
        chain_name: str | None,
    ) -> ChainConfig:
        if chain_id is not None:
            return get_chain_by_id(chain_id)
        if chain_name is not None:
            return get_chain_by_name(chain_name)
        raise ValueError("Provide either chain_id or chain_name.")

    def _is_stale(self, entry: _CacheEntry) -> bool:
        return (time.monotonic() - entry.fetched_at) >= entry.ttl_seconds

    def _get_fresh_entry(self, chain_id: int) -> _CacheEntry | None:
        with self._state_lock:
            entry = self._entries.get(chain_id)
            if entry is None or self._is_stale(entry):
                return None
            return entry

    def _store_entry(self, chain_id: int, gas_price_wei: int) -> None:
        with self._state_lock:
            ttl_seconds = _CACHE_TTL_SECONDS
            previous = self._entries.get(chain_id)
            if previous is not None and previous.gas_price_wei > 0:
                delta = (
                    abs(previous.gas_price_wei - gas_price_wei) / previous.gas_price_wei
                )
                if delta >= 0.125:
                    # Refresh faster during volatility so retries do not cling
                    # to stale fees while the market is moving quickly.
                    ttl_seconds = _VOLATILE_CACHE_TTL_SECONDS
            self._entries[chain_id] = _CacheEntry(
                gas_price_wei=gas_price_wei,
                fetched_at=time.monotonic(),
                ttl_seconds=ttl_seconds,
            )

    def _begin_refresh(self, chain_id: int, loop_id: int) -> tuple[bool, asyncio.Event]:
        with self._state_lock:
            entry = self._entries.get(chain_id)
            if entry is not None and not self._is_stale(entry):
                ready = asyncio.Event()
                ready.set()
                return False, ready

            refresh_key = (chain_id, loop_id)
            refresh_entry = self._refreshing.get(refresh_key)
            if refresh_entry is None:
                event = asyncio.Event()
                self._refreshing[refresh_key] = _RefreshEntry(event=event)
                return True, event
            return False, refresh_entry.event

    def _finish_refresh(self, chain_id: int, loop_id: int) -> None:
        with self._state_lock:
            refresh_entry = self._refreshing.pop((chain_id, loop_id), None)
            if refresh_entry is not None:
                refresh_entry.event.set()

    @staticmethod
    def _redis_key(chain_id: int) -> str:
        return f"{_REDIS_KEY_PREFIX}:{int(chain_id)}"

    async def _fetch_async(self, chain: ChainConfig) -> int:
        w3 = await get_shared_async_web3(chain.rpc_url)
        result = w3.eth.gas_price
        if inspect.isawaitable(result):
            gas_price_wei = _parse_int(await result) or 0
        elif callable(result):
            gas_price_wei = _parse_int(result()) or 0
        else:
            gas_price_wei = _parse_int(result) or 0

        # Sanity cap for safety.
        if gas_price_wei > _MAX_GAS_PRICE_WEI:
            # cap to prevent catastrophic overpayment.
            return _MAX_GAS_PRICE_WEI
        return max(gas_price_wei, 0)

    async def get_wei(
        self,
        chain_id: int | None = None,
        chain_name: str | None = None,
    ) -> int:
        chain = self._resolve_chain(chain_id, chain_name)
        loop_id = id(asyncio.get_running_loop())

        # Local Memory Cache (Fastest)
        entry = self._get_fresh_entry(chain.chain_id)
        if entry is not None:
            return entry.gas_price_wei

        # Redis Cache (Network Bound)
        redis = await get_async_redis()
        if redis is not None:
            try:
                cached = await redis.get(self._redis_key(chain.chain_id))
                if inspect.isawaitable(cached):
                    cached = await cached
                parsed = _parse_int(cached)
                if parsed is not None:
                    # Keep in-memory cache warm as well.
                    self._store_entry(chain.chain_id, parsed)
                    return parsed
            except Exception:
                # Redis is best-effort only.
                pass

        # RPC Fetch
        while True:
            should_refresh, refresh_event = self._begin_refresh(chain.chain_id, loop_id)
            if should_refresh:
                try:
                    gas_price_wei = await self._fetch_async(chain)
                    self._store_entry(chain.chain_id, gas_price_wei)
                    if redis is not None:
                        try:
                            await redis.set(
                                self._redis_key(chain.chain_id),
                                str(gas_price_wei),
                                ex=_CACHE_TTL_SECONDS,
                            )
                        except Exception:
                            pass
                    return gas_price_wei
                finally:
                    # Never leave waiters blocked if the RPC fetch fails.
                    self._finish_refresh(chain.chain_id, loop_id)

            await refresh_event.wait()
            entry = self._get_fresh_entry(chain.chain_id)
            if entry is not None:
                return entry.gas_price_wei

    async def get_gwei(
        self,
        chain_id: int | None = None,
        chain_name: str | None = None,
    ) -> Decimal:
        wei = await self.get_wei(chain_id=chain_id, chain_name=chain_name)
        return Decimal(wei) / Decimal(10**9)

    def get_wei_sync(
        self,
        chain_id: int | None = None,
        chain_name: str | None = None,
    ) -> int:
        chain = self._resolve_chain(chain_id, chain_name)

        # Optimization: Check local cache synchronously first to avoid
        # the overhead of creating/entering an event loop for warm hits.
        entry = self._get_fresh_entry(chain.chain_id)
        if entry is not None:
            return entry.gas_price_wei

        coro = self.get_wei(chain_id=chain_id, chain_name=chain_name)
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # A loop is already running in this thread — we cannot use
                # asyncio.run() or run_until_complete().
                # We must run the coroutine in a separate thread.

                # We use a one-off executor here for safety to avoid global
                # state issues, but we should consider a shared pool if
                # this path is high-frequency.
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(asyncio.run, coro)
                    return future.result()
        except RuntimeError:
            # No running event loop in this thread — safe to use asyncio.run()
            return asyncio.run(coro)

        # Fallback for edge cases where get_running_loop() might behave unexpectedly.
        return asyncio.run(coro)

    def get_gwei_sync(
        self,
        chain_id: int | None = None,
        chain_name: str | None = None,
    ) -> Decimal:
        wei = self.get_wei_sync(chain_id=chain_id, chain_name=chain_name)
        return Decimal(wei) / Decimal(10**9)

    def invalidate(
        self,
        chain_id: int | None = None,
        chain_name: str | None = None,
    ) -> None:
        chain = self._resolve_chain(chain_id, chain_name)
        with self._state_lock:
            self._entries.pop(chain.chain_id, None)

    async def invalidate_async(
        self,
        chain_id: int | None = None,
        chain_name: str | None = None,
    ) -> None:
        chain = self._resolve_chain(chain_id, chain_name)
        self.invalidate(chain_id=chain.chain_id)
        redis = await get_async_redis()
        if redis is None:
            return
        try:
            deleted = redis.delete(self._redis_key(chain.chain_id))
            if inspect.isawaitable(deleted):
                await deleted
        except Exception:
            pass

    def cached_at(
        self,
        chain_id: int | None = None,
        chain_name: str | None = None,
    ) -> float | None:
        chain = self._resolve_chain(chain_id, chain_name)
        with self._state_lock:
            entry = self._entries.get(chain.chain_id)
        return entry.fetched_at if entry is not None else None

    def seconds_until_refresh(
        self,
        chain_id: int | None = None,
        chain_name: str | None = None,
    ) -> float:
        chain = self._resolve_chain(chain_id, chain_name)
        with self._state_lock:
            entry = self._entries.get(chain.chain_id)
        if entry is None or self._is_stale(entry):
            return 0.0
        return max(0.0, entry.ttl_seconds - (time.monotonic() - entry.fetched_at))


gas_price_cache = GasPriceCache()


def to_eip1559_fees(gas_price_wei: int) -> tuple[int, int]:
    gas_price = min(max(int(gas_price_wei), 0), _MAX_GAS_PRICE_WEI)
    if gas_price <= 0:
        return _DEFAULT_PRIORITY_FEE_WEI * 2, _DEFAULT_PRIORITY_FEE_WEI

    priority_fee = min(
        _DEFAULT_PRIORITY_FEE_WEI,
        max(_MIN_PRIORITY_FEE_WEI, gas_price),
    )
    base_fee = max(gas_price - priority_fee, 0)
    buffer = max(base_fee // 4, priority_fee)
    max_fee = max(base_fee + priority_fee + buffer, priority_fee)

    # Sanity cap for final fee to prevent catastrophic overpayment.
    max_fee = min(max_fee, _MAX_GAS_PRICE_WEI)
    return max_fee, priority_fee


async def estimate_eip1559_fees(w3: Any, gas_price_wei: int) -> tuple[int, int]:
    try:
        latest_block = w3.eth.get_block("latest")
        if inspect.isawaitable(latest_block):
            latest_block = await latest_block
        base_fee_raw = latest_block.get("baseFeePerGas") if latest_block else None
        base_fee = _parse_int(base_fee_raw) or 0
    except Exception:
        base_fee = 0

    try:
        priority_result = getattr(w3.eth, "max_priority_fee")
        if inspect.isawaitable(priority_result):
            priority_result = await priority_result
        elif callable(priority_result):
            priority_result = priority_result()
            if inspect.isawaitable(priority_result):
                priority_result = await priority_result
        priority_fee = _parse_int(priority_result) or _DEFAULT_PRIORITY_FEE_WEI
    except Exception:
        priority_fee = _DEFAULT_PRIORITY_FEE_WEI

    priority_fee = min(
        _DEFAULT_PRIORITY_FEE_WEI,
        max(_MIN_PRIORITY_FEE_WEI, priority_fee),
    )

    if base_fee <= 0:
        return to_eip1559_fees(gas_price_wei)

    buffer = max(base_fee // 4, priority_fee)
    max_fee = min(base_fee + priority_fee + buffer, _MAX_GAS_PRICE_WEI)
    return max(max_fee, priority_fee), priority_fee
