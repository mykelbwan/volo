import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from core.utils.async_resources import async_resource_scope


def test_async_resource_scope_closes_registered_web3_providers():
    provider = SimpleNamespace(disconnect=AsyncMock(return_value=None))

    async def _run():
        async with async_resource_scope() as scope:
            scope.register_web3_provider(provider)

    asyncio.run(_run())

    provider.disconnect.assert_awaited_once()


def test_async_resource_scope_reuses_single_cdp_client():
    created = []

    def _factory():
        client = SimpleNamespace(close=AsyncMock(return_value=None), _closed=False)
        created.append(client)
        return client

    async def _run():
        async with async_resource_scope() as scope:
            first = await scope.get_or_create_cdp_client(_factory)
            second = await scope.get_or_create_cdp_client(_factory)
            assert first is second
            return first

    client = asyncio.run(_run())

    assert len(created) == 1
    client.close.assert_awaited_once()
