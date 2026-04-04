import asyncio
import time
from unittest.mock import AsyncMock

from wallet_service.evm.gas_price import GasPriceCache, _CacheEntry


class _DummyChain:
    def __init__(self, chain_id=1):
        self.chain_id = chain_id
        self.rpc_url = "http://example"


def test_gas_price_cache_uses_cache(monkeypatch):
    cache = GasPriceCache()
    chain = _DummyChain()
    calls = {"count": 0}

    async def _fake_fetch(_chain):
        calls["count"] += 1
        cache._entries[_chain.chain_id] = _CacheEntry(
            gas_price_wei=100, fetched_at=time.monotonic()
        )
        return 100

    monkeypatch.setattr(
        "wallet_service.evm.gas_price.get_async_redis",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(cache, "_resolve_chain", lambda *_args, **_kwargs: chain)
    monkeypatch.setattr(cache, "_fetch_async", _fake_fetch)

    result1 = asyncio.run(cache.get_wei(chain_id=1))
    result2 = asyncio.run(cache.get_wei(chain_id=1))

    assert result1 == 100
    assert result2 == 100
    assert calls["count"] == 1


def test_gas_price_cache_invalidate(monkeypatch):
    cache = GasPriceCache()
    chain = _DummyChain()
    calls = {"count": 0}

    async def _fake_fetch(_chain):
        calls["count"] += 1
        cache._entries[_chain.chain_id] = _CacheEntry(
            gas_price_wei=200, fetched_at=0.0
        )
        return 200

    monkeypatch.setattr(
        "wallet_service.evm.gas_price.get_async_redis",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(cache, "_resolve_chain", lambda *_args, **_kwargs: chain)
    monkeypatch.setattr(cache, "_fetch_async", _fake_fetch)

    asyncio.run(cache.get_wei(chain_id=1))
    cache.invalidate(chain_id=1)
    asyncio.run(cache.get_wei(chain_id=1))

    assert calls["count"] == 2
