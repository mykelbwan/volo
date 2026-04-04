from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from decimal import Decimal

import pytest
import httpx

from core.utils.http import ExternalServiceError
from wallet_service.evm.get_all_bal import get_wallet_balances


class _DummyResponse:
    def __init__(self, json_data, *, status_code=200, text="ok", raise_exc=None):
        self._json = json_data
        self.status_code = status_code
        self.text = text
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc
        return None

    def json(self):
        return self._json


@pytest.mark.anyio
async def test_get_wallet_balances_http_error_raises():
    response = _DummyResponse(
        {"error": "boom"},
        status_code=500,
        text="boom",
        raise_exc=httpx.HTTPStatusError("boom", request=None, response=None),
    )

    fake_chain = SimpleNamespace(rpc_url="https://alchemy.example", name="Optimism", is_testnet=False)
    with (
        patch("wallet_service.evm.get_all_bal.get_chain_by_name", return_value=fake_chain),
        patch("wallet_service.evm.get_all_bal._build_native_entry", AsyncMock(return_value=None)),
        patch("wallet_service.evm.get_all_bal.async_request_json", AsyncMock(return_value=response)),
    ):
        with pytest.raises(ExternalServiceError):
            await get_wallet_balances("0x0000000000000000000000000000000000000000", "optimism")


@pytest.mark.anyio
async def test_get_wallet_balances_prefers_cdp_on_supported_network():
    cdp_result = SimpleNamespace(
        balances=[
            SimpleNamespace(
                token=SimpleNamespace(
                    contract_address="0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
                    symbol="ETH",
                    name="Ether",
                ),
                amount=SimpleNamespace(amount=2000000000000000000, decimals=18),
            ),
            SimpleNamespace(
                token=SimpleNamespace(
                    contract_address="0x036CbD53842c5426634e7929541eC2318f3dCF7e",
                    symbol="USDC",
                    name="USD Coin",
                ),
                amount=SimpleNamespace(amount=5000000, decimals=6),
            ),
        ]
    )

    native_entry = {
        "name": "Base Sepolia Native",
        "symbol": "ETH",
        "decimals": 18,
        "balance": None,
        "balance_formatted": "2.0",
        "token_address": None,
    }

    with (
        patch("wallet_service.evm.get_all_bal._cdp_list_token_balances_async", AsyncMock(return_value=cdp_result)) as cdp_call,
        patch("wallet_service.evm.get_all_bal._fetch_balances_alchemy", AsyncMock()) as alchemy_call,
        patch("wallet_service.evm.get_all_bal._build_native_entry", AsyncMock(return_value=native_entry)) as native_call,
    ):
        result = await get_wallet_balances(
            "0x0000000000000000000000000000000000000000",
            "base sepolia",
        )

    cdp_call.assert_called_once()
    alchemy_call.assert_not_called()
    native_call.assert_called_once()
    # Native entry is prepended, and the duplicate ETH from CDP is filtered out
    assert result[0]["symbol"] == "ETH"
    assert result[0]["token_address"] is None
    assert result[1]["symbol"] == "USDC"


@pytest.mark.anyio
async def test_get_wallet_balances_hides_suspicious_testnet_tokens_but_keeps_trusted_assets():
    cdp_result = SimpleNamespace(
        balances=[
            SimpleNamespace(
                token=SimpleNamespace(
                    contract_address="0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
                    symbol="ETH",
                    name="Ether",
                ),
                amount=SimpleNamespace(amount=2000000000000000000, decimals=18),
            ),
            SimpleNamespace(
                token=SimpleNamespace(
                    contract_address="0x036CbD53842c5426634e7929541eC2318f3dCF7e",
                    symbol="USDC",
                    name="USD Coin",
                ),
                amount=SimpleNamespace(amount=5000000, decimals=6),
            ),
            SimpleNamespace(
                token=SimpleNamespace(
                    contract_address="0x0000000000000000000000000000000000000001",
                    symbol="DYN1914",
                    name="DYN1914 (TEST)",
                ),
                amount=SimpleNamespace(amount=300000000000000000000, decimals=18),
            ),
        ]
    )

    with (
        patch("wallet_service.evm.get_all_bal._cdp_list_token_balances_async", AsyncMock(return_value=cdp_result)),
        patch("wallet_service.evm.get_all_bal._build_native_entry", AsyncMock(return_value=None)),
    ):
        # We simulate the filter by letting it run. USDC will stay because it's not suspicious.
        result = await get_wallet_balances(
            "0x0000000000000000000000000000000000000000",
            "base sepolia",
        )

    symbols = [entry["symbol"] for entry in result]
    assert "USDC" in symbols
    assert "ETH" in symbols
    assert "DYN1914" not in symbols


@pytest.mark.anyio
async def test_get_wallet_balances_keeps_unknown_mainnet_tokens_when_not_spam():
    cdp_result = SimpleNamespace(
        balances=[
            SimpleNamespace(
                token=SimpleNamespace(
                    contract_address="0x0000000000000000000000000000000000000002",
                    symbol="FOO",
                    name="Foo Token",
                ),
                amount=SimpleNamespace(amount=250000000000000000, decimals=18),
            ),
        ]
    )

    native_entry = {
        "name": "Base Native",
        "symbol": "ETH",
        "decimals": 18,
        "balance": None,
        "balance_formatted": "1.0",
        "token_address": None,
    }

    with (
        patch("wallet_service.evm.get_all_bal._cdp_list_token_balances_async", AsyncMock(return_value=cdp_result)),
        patch("wallet_service.evm.get_all_bal._build_native_entry", AsyncMock(return_value=native_entry)),
    ):
        result = await get_wallet_balances(
            "0x0000000000000000000000000000000000000000",
            "base",
        )

    assert [entry["symbol"] for entry in result] == ["ETH", "FOO"]


@pytest.mark.anyio
async def test_get_wallet_balances_falls_back_to_alchemy_for_unsupported_cdp_network():
    fake_chain = SimpleNamespace(
        rpc_url="https://alchemy.example", 
        name="Optimism",
        native_token_aliases=["0x0000000000000000000000000000000000000000"]
    )
    with (
        patch("wallet_service.evm.get_all_bal.get_chain_by_name", return_value=fake_chain),
        patch("wallet_service.evm.get_all_bal._fetch_balances_alchemy", AsyncMock(return_value=[])) as alchemy_call,
        patch("wallet_service.evm.get_all_bal._build_native_entry", AsyncMock(return_value=None)),
        patch("wallet_service.evm.get_all_bal._cdp_list_token_balances_async", AsyncMock()) as cdp_call,
    ):
        await get_wallet_balances(
            "0x0000000000000000000000000000000000000000",
            "optimism",
        )

    cdp_call.assert_not_called()
    alchemy_call.assert_called_once()
