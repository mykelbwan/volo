from __future__ import annotations

from types import SimpleNamespace

import pytest


class _FakeAsyncRegistry:
    def __init__(self) -> None:
        self.get_calls: list[tuple[str, int]] = []
        self.get_by_alias_calls: list[tuple[str, int]] = []

    async def get(self, symbol: str, chain_id: int):
        self.get_calls.append((symbol, chain_id))
        return SimpleNamespace(decimals=6)

    async def get_by_alias(self, symbol: str, chain_id: int):
        self.get_by_alias_calls.append((symbol, chain_id))
        return None


class _FakeAsyncAddressRegistry:
    def __init__(self) -> None:
        self.get_by_address_calls: list[tuple[str, int]] = []
        self.get_calls: list[tuple[str, int]] = []
        self.upserted: list[object] = []

    async def get_by_address(self, address: str, chain_id: int):
        self.get_by_address_calls.append((address, chain_id))
        return None

    async def get(self, symbol: str, chain_id: int):
        self.get_calls.append((symbol, chain_id))
        return None

    async def upsert(self, entry: object):
        self.upserted.append(entry)


@pytest.mark.asyncio
async def test_async_registry_symbol_lookup_reuses_cached_result(
    monkeypatch: pytest.MonkeyPatch,
):
    import core.token_security.registry_lookup as registry_lookup

    registry_lookup._LOOKUP_CACHE.clear()
    fake_registry = _FakeAsyncRegistry()
    monkeypatch.setattr(
        registry_lookup,
        "get_async_token_registry",
        lambda: fake_registry,
    )

    first = await registry_lookup.get_registry_decimals_by_symbol_async("USDC", 8453)
    second = await registry_lookup.get_registry_decimals_by_symbol_async("USDC", 8453)
    third = await registry_lookup.get_registry_decimals_by_symbol_async("USDC", 8453)

    assert first == 6
    assert second == 6
    assert third == 6
    assert fake_registry.get_calls == [("USDC", 8453)]
    assert fake_registry.get_by_alias_calls == []


@pytest.mark.asyncio
async def test_async_registry_symbol_lookup_misses_cache_for_different_inputs(
    monkeypatch: pytest.MonkeyPatch,
):
    import core.token_security.registry_lookup as registry_lookup

    registry_lookup._LOOKUP_CACHE.clear()
    fake_registry = _FakeAsyncRegistry()
    monkeypatch.setattr(
        registry_lookup,
        "get_async_token_registry",
        lambda: fake_registry,
    )

    first = await registry_lookup.get_registry_decimals_by_symbol_async("USDC", 8453)
    second = await registry_lookup.get_registry_decimals_by_symbol_async("WETH", 8453)

    assert first == 6
    assert second == 6
    assert fake_registry.get_calls == [("USDC", 8453), ("WETH", 8453)]
    assert fake_registry.get_by_alias_calls == []


@pytest.mark.asyncio
async def test_async_registry_symbol_lookup_expires_after_ttl(
    monkeypatch: pytest.MonkeyPatch,
):
    import core.token_security.registry_lookup as registry_lookup

    registry_lookup._LOOKUP_CACHE.clear()
    fake_registry = _FakeAsyncRegistry()
    clock = {"now": 1_000.0}
    monkeypatch.setattr(
        registry_lookup,
        "get_async_token_registry",
        lambda: fake_registry,
    )
    monkeypatch.setattr(registry_lookup.time, "monotonic", lambda: clock["now"])

    first = await registry_lookup.get_registry_decimals_by_symbol_async("USDC", 8453)
    clock["now"] += registry_lookup._CACHE_TTL_SECONDS - 1
    second = await registry_lookup.get_registry_decimals_by_symbol_async("USDC", 8453)
    clock["now"] += 2
    third = await registry_lookup.get_registry_decimals_by_symbol_async("USDC", 8453)

    assert first == 6
    assert second == 6
    assert third == 6
    assert fake_registry.get_calls == [("USDC", 8453), ("USDC", 8453)]
    assert fake_registry.get_by_alias_calls == []


@pytest.mark.asyncio
async def test_async_address_lookup_falls_back_to_onchain_and_upserts(
    monkeypatch: pytest.MonkeyPatch,
):
    import core.token_security.registry_lookup as registry_lookup

    registry_lookup._LOOKUP_CACHE.clear()
    fake_registry = _FakeAsyncAddressRegistry()
    fallback_calls: list[tuple[str, int]] = []

    async def _fake_onchain(address: str, chain_id: int):
        fallback_calls.append((address, chain_id))
        return registry_lookup._OnchainTokenMetadata(
            address="0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            decimals=6,
            symbol="USDC",
        )

    monkeypatch.setattr(
        registry_lookup,
        "get_async_token_registry",
        lambda: fake_registry,
    )
    monkeypatch.setattr(
        registry_lookup,
        "_fetch_onchain_token_metadata_async",
        _fake_onchain,
    )

    first = await registry_lookup.get_registry_decimals_by_address_async(
        "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 8453
    )
    second = await registry_lookup.get_registry_decimals_by_address_async(
        "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 8453
    )

    assert first == 6
    assert second == 6
    assert fallback_calls == [("0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 8453)]
    assert fake_registry.get_by_address_calls == [
        ("0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 8453)
    ]
    assert fake_registry.get_calls == [("USDC", 8453)]
    assert len(fake_registry.upserted) == 1

    entry = fake_registry.upserted[0]
    assert getattr(entry, "symbol") == "USDC"
    assert int(getattr(entry, "decimals")) == 6
    assert getattr(entry, "source") == "onchain_fallback"
