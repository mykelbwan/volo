from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest
from solders.hash import Hash
from solders.keypair import Keypair

from wallet_service.solana import rpc_client, spl_transfer

from tests.unit.wallet_service._stress_helpers import sleep_ticks

ASYNC_TEST_TIMEOUT = 10.0
FIRST_BLOCKHASH = Hash.from_string("8qbHbw2BbbTHBW1s7WcTLs8x2Wc6N8qF8a5n1Yp8wQkX")
SECOND_BLOCKHASH = Hash.from_string("4vJ9JU1bJJHzAq4xV7hW4Y3mJrLrM8VgHkL8V5x6wZ7G")


@pytest.fixture(autouse=True)
def _clear_solana_caches() -> None:
    with rpc_client._SHARED_SOLANA_CLIENTS_LOCK:
        rpc_client._SHARED_SOLANA_CLIENTS.clear()
    with rpc_client._BLOCKHASH_CACHE_LOCK:
        rpc_client._BLOCKHASH_CACHE.clear()
        rpc_client._BLOCKHASH_IN_FLIGHT.clear()
    with spl_transfer._TOKEN_DECIMALS_CACHE_LOCK:
        spl_transfer._TOKEN_DECIMALS_CACHE.clear()
        spl_transfer._TOKEN_DECIMALS_IN_FLIGHT.clear()


@pytest.mark.asyncio
async def test_blockhash_cache_ttl_expiry_refreshes_once_after_stampede(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SequencedBlockhashClient:
        call_count = 0
        outcomes = [FIRST_BLOCKHASH, SECOND_BLOCKHASH]

        def __init__(self, rpc_url: str) -> None:
            self.rpc_url = rpc_url

        async def get_latest_blockhash(self) -> SimpleNamespace:
            index = type(self).call_count
            type(self).call_count += 1
            await sleep_ticks(3)
            blockhash = type(self).outcomes[min(index, len(type(self).outcomes) - 1)]
            return SimpleNamespace(value=SimpleNamespace(blockhash=blockhash))

    async def exercise() -> None:
        monkeypatch.setattr("solana.rpc.async_api.AsyncClient", _SequencedBlockhashClient)

        warm_results = await asyncio.gather(
            *[rpc_client.get_cached_latest_blockhash("https://rpc.test") for _ in range(40)]
        )
        assert warm_results == [FIRST_BLOCKHASH] * 40
        assert _SequencedBlockhashClient.call_count == 1

        with rpc_client._BLOCKHASH_CACHE_LOCK:
            rpc_client._BLOCKHASH_CACHE["https://rpc.test"] = rpc_client._BlockhashCacheEntry(
                blockhash=FIRST_BLOCKHASH,
                fetched_at=time.monotonic() - rpc_client._BLOCKHASH_TTL_SECONDS - 1,
            )

        refreshed_results = await asyncio.gather(
            *[rpc_client.get_cached_latest_blockhash("https://rpc.test") for _ in range(40)]
        )

        assert refreshed_results == [SECOND_BLOCKHASH] * 40
        assert _SequencedBlockhashClient.call_count == 2
        assert rpc_client._BLOCKHASH_IN_FLIGHT == {}

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_blockhash_invalidation_triggers_single_refresh_for_many_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SequencedBlockhashClient:
        call_count = 0
        outcomes = [FIRST_BLOCKHASH, SECOND_BLOCKHASH]

        def __init__(self, rpc_url: str) -> None:
            self.rpc_url = rpc_url

        async def get_latest_blockhash(self) -> SimpleNamespace:
            index = type(self).call_count
            type(self).call_count += 1
            await sleep_ticks(2)
            blockhash = type(self).outcomes[min(index, len(type(self).outcomes) - 1)]
            return SimpleNamespace(value=SimpleNamespace(blockhash=blockhash))

    async def exercise() -> None:
        monkeypatch.setattr("solana.rpc.async_api.AsyncClient", _SequencedBlockhashClient)

        assert await rpc_client.get_cached_latest_blockhash("https://rpc.test") == FIRST_BLOCKHASH
        rpc_client.invalidate_cached_blockhash("https://rpc.test")

        results = await asyncio.gather(
            *[rpc_client.get_cached_latest_blockhash("https://rpc.test") for _ in range(32)]
        )

        assert results == [SECOND_BLOCKHASH] * 32
        assert _SequencedBlockhashClient.call_count == 2
        assert rpc_client._BLOCKHASH_IN_FLIGHT == {}

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_blockhash_failed_refresh_releases_waiters_and_recovers_next_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FlakyBlockhashClient:
        call_count = 0

        def __init__(self, rpc_url: str) -> None:
            self.rpc_url = rpc_url

        async def get_latest_blockhash(self) -> SimpleNamespace:
            type(self).call_count += 1
            await sleep_ticks(2)
            if type(self).call_count == 1:
                raise RuntimeError("blockhash rpc timeout")
            return SimpleNamespace(value=SimpleNamespace(blockhash=SECOND_BLOCKHASH))

    async def exercise() -> None:
        monkeypatch.setattr("solana.rpc.async_api.AsyncClient", _FlakyBlockhashClient)

        first_wave = await asyncio.gather(
            *[rpc_client.get_cached_latest_blockhash("https://rpc.test") for _ in range(24)],
            return_exceptions=True,
        )

        assert all(isinstance(item, RuntimeError) for item in first_wave)
        assert rpc_client._BLOCKHASH_IN_FLIGHT == {}
        assert rpc_client._BLOCKHASH_CACHE == {}

        second_wave = await asyncio.gather(
            *[rpc_client.get_cached_latest_blockhash("https://rpc.test") for _ in range(24)]
        )

        assert second_wave == [SECOND_BLOCKHASH] * 24
        assert _FlakyBlockhashClient.call_count == 2
        assert rpc_client._BLOCKHASH_IN_FLIGHT == {}

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)


@pytest.mark.asyncio
async def test_mint_decimals_failed_refresh_cleans_up_inflight_and_recovers() -> None:
    class _FlakyMintClient:
        def __init__(self) -> None:
            self.call_count = 0

        async def get_account_info(self, _pubkey: object) -> object:
            self.call_count += 1
            await sleep_ticks(2)
            if self.call_count == 1:
                raise RuntimeError("mint rpc timeout")
            raw = bytearray(82)
            raw[44] = 9
            return SimpleNamespace(value=SimpleNamespace(data=bytes(raw)))

    async def exercise() -> None:
        client = _FlakyMintClient()
        mint_address = str(Keypair().pubkey())

        first_wave = await asyncio.gather(
            *[
                spl_transfer._get_cached_mint_decimals(client, object(), mint_address)
                for _ in range(24)
            ],
            return_exceptions=True,
        )

        assert all(isinstance(item, RuntimeError) for item in first_wave)
        assert spl_transfer._TOKEN_DECIMALS_IN_FLIGHT == {}
        assert spl_transfer._TOKEN_DECIMALS_CACHE == {}

        second_wave = await asyncio.gather(
            *[
                spl_transfer._get_cached_mint_decimals(client, object(), mint_address)
                for _ in range(24)
            ]
        )

        assert second_wave == [9] * 24
        assert client.call_count == 2
        assert spl_transfer._TOKEN_DECIMALS_IN_FLIGHT == {}
        assert spl_transfer._TOKEN_DECIMALS_CACHE[mint_address] == 9

    await asyncio.wait_for(exercise(), timeout=ASYNC_TEST_TIMEOUT)
