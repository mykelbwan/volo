from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from core.volume.flusher import _atomic_getdel, _flush_once, _is_testnet_chain, _scan_primary_keys
from core.volume.redis_keys import hour_bucket, volume_key


def test_is_testnet_chain_detects_evm_and_solana_testnets():
    assert _is_testnet_chain("base_sepolia") is True
    assert _is_testnet_chain("solana_devnet") is True
    assert _is_testnet_chain("ethereum") is False


def test_flush_once_skips_testnet_price_resolution_but_records_volume():
    bucket = hour_bucket(datetime(2025, 3, 21, 10, 0, tzinfo=timezone.utc))
    testnet_key = volume_key("swap", "base sepolia", "USDC", bucket)
    mainnet_key = volume_key("swap", "ethereum", "ETH", bucket)

    redis = MagicMock()
    redis.getdel = AsyncMock(
        side_effect=lambda key: {
            testnet_key: "12.5",
            mainnet_key: "2.0",
            testnet_key.replace("volume:", "vol_cnt:"): "1",
            mainnet_key.replace("volume:", "vol_cnt:"): "3",
        }.get(key)
    )
    redis.get = AsyncMock()
    redis.delete = AsyncMock()

    watcher = MagicMock()
    watcher.register_volume_symbols = MagicMock()
    collection = MagicMock()
    collection.update_one = AsyncMock()

    async def run():
        with patch("core.volume.flusher._scan_primary_keys", AsyncMock(return_value=[testnet_key, mainnet_key])), patch(
            "core.volume.flusher._resolve_prices",
            AsyncMock(return_value={"ETH": 3000.0}),
        ) as mock_resolve, patch(
            "core.volume.flusher.AsyncMongoDB.get_collection",
            return_value=collection,
        ):
            written = await _flush_once(redis, watcher)
            return written, mock_resolve

    written, mock_resolve = asyncio.run(run())

    assert written == 2
    watcher.register_volume_symbols.assert_called_once_with(["ETH"])
    mock_resolve.assert_awaited_once()
    assert mock_resolve.await_args.args[0] == {"ETH"}

    testnet_update = collection.update_one.await_args_list[0].args[1]["$inc"]
    mainnet_update = collection.update_one.await_args_list[1].args[1]["$inc"]
    assert "usd_volume" not in testnet_update
    assert testnet_update["normalized_volume"] == 12.5
    assert testnet_update["execution_count"] == 1
    assert mainnet_update["usd_volume"] == 6000.0
    assert mainnet_update["execution_count"] == 3


def test_scan_primary_keys_deduplicates_scan_batches():
    redis = MagicMock()
    redis.scan = AsyncMock(
        side_effect=[
            (1, ["volume:swap:ethereum:eth:2025032110", "volume:swap:ethereum:eth:2025032110"]),
            (0, ["volume:swap:base:usdc:2025032110"]),
        ]
    )

    keys = asyncio.run(_scan_primary_keys(redis))

    assert keys == [
        "volume:swap:ethereum:eth:2025032110",
        "volume:swap:base:usdc:2025032110",
    ]


def test_atomic_getdel_falls_back_to_transaction_pipeline():
    pipe = MagicMock()
    pipe.get = MagicMock(return_value=pipe)
    pipe.delete = MagicMock(return_value=pipe)
    pipe.exec = AsyncMock(return_value=["12.5", 1])

    redis = MagicMock()
    del redis.getdel
    redis.multi = MagicMock(return_value=pipe)

    value = asyncio.run(_atomic_getdel(redis, "volume:swap:ethereum:eth:2025032110"))

    assert value == "12.5"
    redis.multi.assert_called_once_with()
    pipe.get.assert_called_once_with("volume:swap:ethereum:eth:2025032110")
    pipe.delete.assert_called_once_with("volume:swap:ethereum:eth:2025032110")
    pipe.exec.assert_awaited_once()
