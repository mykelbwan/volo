import os
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.utils.async_resources import async_resource_scope
from wallet_service.common.cdp_helpers import CdpClientConfig, get_cdp_client_config
from wallet_service.evm.cdp_utils import (
    clear_evm_account_cache,
    get_evm_account,
    normalize_evm_network,
    sign_evm_transaction_by_name_async,
)


@pytest.fixture(autouse=True)
def clean_cache(monkeypatch):
    clear_evm_account_cache()
    # Reset the cached config in cdp_helpers to ensure environment tests are fresh
    monkeypatch.setattr("wallet_service.common.cdp_helpers._CACHED_CONFIG", None)
    monkeypatch.setattr("wallet_service.common.cdp_helpers._CACHED_TIMEOUT", None)
    
    # Ensure no external env vars leak into tests
    for var in [
        "CDP_API_KEY_ID", "COINBASE_API_KEY_ID",
        "CDP_API_KEY_SECRET", "COINBASE_SECRET_KEY",
        "CDP_WALLET_SECRET", "COINBASE_SERVER_WALLET",
        "DISABLE_CDP_USAGE_TRACKING", "DISABLE_CDP_ERROR_REPORTING",
        "CDP_CALL_TIMEOUT_SECONDS"
    ]:
        monkeypatch.delenv(var, raising=False)
        
    yield
    clear_evm_account_cache()


def _test_cdp_config() -> CdpClientConfig:
    return CdpClientConfig(
        api_key_id="key-id",
        api_key_secret="secret",
        wallet_secret="wallet",
        disable_usage_tracking=True,
        disable_error_reporting=True,
    )


def test_sign_evm_transaction_by_name_async_closes_client_on_success():
    account = SimpleNamespace(
        address="0xabc",
        sign_transaction=AsyncMock(
            return_value=SimpleNamespace(raw_transaction="0xsigned")
        ),
    )
    client = SimpleNamespace(
        evm=SimpleNamespace(get_account=AsyncMock(return_value=account)),
        close=AsyncMock(return_value=None),
    )

    with patch("wallet_service.common.cdp_helpers.get_cdp_client_config", return_value=_test_cdp_config()), patch(
        "wallet_service.common.cdp_helpers._cdp_client_cls", return_value=MagicMock(return_value=client)
    ):
        result = asyncio.run(
            sign_evm_transaction_by_name_async(
                "sub-org",
                {"to": "0x123", "nonce": 1, "gas": 21000},
                sign_with="0xabc",
            )
        )

    assert result == "0xsigned"
    client.close.assert_awaited_once()


def test_sign_evm_transaction_by_name_async_closes_client_on_error():
    account = SimpleNamespace(
        address="0xabc",
        sign_transaction=AsyncMock(side_effect=RuntimeError("boom")),
    )
    client = SimpleNamespace(
        evm=SimpleNamespace(get_account=AsyncMock(return_value=account)),
        close=AsyncMock(return_value=None),
    )

    with patch("wallet_service.common.cdp_helpers.get_cdp_client_config", return_value=_test_cdp_config()), patch(
        "wallet_service.common.cdp_helpers._cdp_client_cls", return_value=MagicMock(return_value=client)
    ):
        try:
            asyncio.run(
                sign_evm_transaction_by_name_async(
                    "sub-org",
                    {"to": "0x123", "nonce": 1, "gas": 21000},
                    sign_with="0xabc",
                )
            )
            assert False, "Expected RuntimeError"
        except RuntimeError as exc:
            assert str(exc) == "boom"

    client.close.assert_awaited_once()


def test_sign_evm_transaction_reuses_scoped_cdp_client():
    account = SimpleNamespace(
        address="0xabc",
        sign_transaction=AsyncMock(
            return_value=SimpleNamespace(raw_transaction="0xsigned")
        ),
    )
    client = SimpleNamespace(
        evm=SimpleNamespace(get_account=AsyncMock(return_value=account)),
        close=AsyncMock(return_value=None),
        _closed=False,
    )

    async def _run():
        async with async_resource_scope():
            first = await sign_evm_transaction_by_name_async(
                "sub-org",
                {"to": "0x123", "nonce": 1, "gas": 21000},
                sign_with="0xabc",
            )
            second = await sign_evm_transaction_by_name_async(
                "sub-org",
                {"to": "0x123", "nonce": 2, "gas": 21000},
                sign_with="0xabc",
            )
            return first, second

    with patch("wallet_service.common.cdp_helpers.get_cdp_client_config", return_value=_test_cdp_config()), patch(
        "wallet_service.common.cdp_helpers._cdp_client_cls", return_value=MagicMock(return_value=client)
    ) as client_ctor:
        first, second = asyncio.run(_run())

    assert first == "0xsigned"
    assert second == "0xsigned"
    # We call _cdp_client_cls() twice (once per transaction) to get the "class",
    # but the scope handles reusing the same client instance.
    assert client_ctor.call_count == 2
    client.close.assert_awaited_once()


def test_sign_evm_transaction_times_out_fast_on_cdp_stall():
    async def _slow_get_account(*_args, **_kwargs):
        await asyncio.sleep(0.05)
        return SimpleNamespace(address="0xabc")

    client = SimpleNamespace(
        evm=SimpleNamespace(get_account=AsyncMock(side_effect=_slow_get_account)),
        close=AsyncMock(return_value=None),
    )

    with patch("wallet_service.common.cdp_helpers.get_cdp_client_config", return_value=_test_cdp_config()), patch(
        "wallet_service.common.cdp_helpers._cdp_client_cls", return_value=MagicMock(return_value=client)
    ), patch(
        "wallet_service.common.cdp_helpers.cdp_call_timeout_seconds",
        return_value=0.01,
    ):
        with pytest.raises(TimeoutError) as exc_info:
            asyncio.run(
                sign_evm_transaction_by_name_async(
                    "sub-org",
                    {"to": "0x123", "nonce": 1, "gas": 21000},
                    sign_with="0xabc",
                )
            )

    assert "CDP timeout while loading EVM account" in str(exc_info.value)
    client.close.assert_awaited_once()


def test_get_cdp_client_config_requires_resolvable_credentials(monkeypatch):
    monkeypatch.setenv("COINBASE_API_KEY_ID", "coinbase-key")
    monkeypatch.setenv("COINBASE_SECRET_KEY", "coinbase-secret")
    monkeypatch.setenv("COINBASE_SERVER_WALLET", "coinbase-wallet")

    config = get_cdp_client_config()

    assert config.api_key_id == "coinbase-key"
    assert config.api_key_secret == "coinbase-secret"
    assert config.wallet_secret == "coinbase-wallet"
    assert config.disable_usage_tracking is True # Default
    assert config.disable_error_reporting is True # Default


def test_get_cdp_client_config_preserves_explicit_telemetry_override(monkeypatch):
    monkeypatch.setenv("COINBASE_API_KEY_ID", "coinbase-key")
    monkeypatch.setenv("COINBASE_SECRET_KEY", "coinbase-secret")
    monkeypatch.setenv("COINBASE_SERVER_WALLET", "coinbase-wallet")
    monkeypatch.setenv("DISABLE_CDP_USAGE_TRACKING", "false")
    monkeypatch.setenv("DISABLE_CDP_ERROR_REPORTING", "false")

    config = get_cdp_client_config()

    assert config.disable_usage_tracking is False
    assert config.disable_error_reporting is False


def test_normalize_evm_network_supports_base_sepolia_alias():
    assert normalize_evm_network("base sepolia") == "base-sepolia"
    assert normalize_evm_network("base") == "base"
    assert normalize_evm_network("ethereum") == "ethereum"


def test_normalize_evm_network_rejects_unsupported_chain():
    with pytest.raises(ValueError):
        normalize_evm_network("optimism")


@pytest.mark.asyncio
async def test_sync_cdp_bridge_does_not_spawn_threads(monkeypatch):
    with pytest.raises(RuntimeError, match="running event loop"):
        get_evm_account("sub-org")
