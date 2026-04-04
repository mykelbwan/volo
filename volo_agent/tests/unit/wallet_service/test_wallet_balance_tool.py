import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

import tool_nodes.wallet.balance as balance_module
from tool_nodes.wallet.balance import check_balance
from core.utils.http import ExternalServiceError


@pytest.fixture(autouse=True)
def _disable_price_network_calls():
    with patch(
        "tool_nodes.wallet.balance.fetch_prices_batch_coingecko",
        new=AsyncMock(return_value={}),
    ):
        with patch(
            "tool_nodes.wallet.balance.fetch_prices_dexscreener",
            new=AsyncMock(return_value={}),
        ):
            yield


def test_check_balance_missing_params_raises():
    with pytest.raises(ValueError):
        asyncio.run(check_balance({"sender": "0xabc"}))


def test_check_balance_no_tokens_falls_back_to_native():
    with patch("tool_nodes.wallet.balance.get_wallet_balances", return_value=[]):
        with patch("tool_nodes.wallet.balance.get_chain_by_name") as get_chain:
            get_chain.return_value = type(
                "C",
                (),
                {"rpc_url": "http://example", "native_symbol": "ETH", "name": "Ethereum"},
            )()
            with patch("tool_nodes.wallet.balance.get_native_balance", return_value=Decimal("2")):
                result = asyncio.run(check_balance({"sender": "0xabc", "chain": "ethereum"}))

    assert result["status"] == "success"
    assert "2" in result["message"]
    assert "ETH" in result["message"]


def test_check_balance_formats_balances():
    balances = [
        {
            "balance_formatted": "1.23",
            "symbol": "ETH",
            "name": "Ethereum",
        }
    ]

    with patch("tool_nodes.wallet.balance.get_wallet_balances", return_value=balances):
        result = asyncio.run(check_balance({"sender": "0xabcdef", "chain": "ethereum"}))

    assert result["status"] == "success"
    assert "Balances on Ethereum" in result["message"]
    assert "1.23 ETH" in result["message"]


def test_check_balance_external_error_falls_back_to_native():
    err = ExternalServiceError("alchemy", 404, "not found")
    with patch("tool_nodes.wallet.balance.get_wallet_balances", side_effect=err):
        with patch("tool_nodes.wallet.balance.get_chain_by_name") as get_chain:
            get_chain.return_value = type(
                "C",
                (),
                {"rpc_url": "http://example", "native_symbol": "STT", "name": "Somnia Testnet"},
            )()
            with patch("tool_nodes.wallet.balance.get_native_balance", return_value=Decimal("2")):
                result = asyncio.run(check_balance({"sender": "0xabc", "chain": "somnia testnet"}))

    assert result["status"] == "success"
    assert "2" in result["message"]
    assert "STT" in result["message"]


def test_check_balance_solana_uses_solana_sender():
    with patch(
        "tool_nodes.wallet.balance.get_solana_wallet_balances",
        return_value=[{"balance_formatted": "3.5", "symbol": "SOL", "name": "Solana"}],
    ):
        result = asyncio.run(
            check_balance(
                {
                    "sender": "0xabc",
                    "solana_sender": "So11111111111111111111111111111111111111112",
                    "chain": "solana",
                }
            )
        )

    assert result["status"] == "success"
    assert "Balances on Solana" in result["message"]
    assert "3.5 SOL" in result["message"]


def test_check_balance_all_supported_returns_aggregated_message():
    with patch(
        "tool_nodes.wallet.balance.list_supported_balance_chain_specs",
        return_value=[],
    ):
        result = asyncio.run(
            check_balance({"sender": "0xabc", "chain": "all_supported"})
        )

    assert result["status"] == "error"
    assert "No supported chains are configured" in result["message"]


def test_fetch_evm_chain_balances_uses_full_wallet_timeout():
    balances = [
        {
            "balance_formatted": "1.23",
            "symbol": "ETH",
            "name": "Ethereum",
        }
    ]
    run_io = AsyncMock(return_value=balances)

    async def _run():
        with (
            patch.object(balance_module, "_FULL_WALLET_TIMEOUT_SECONDS", 12.0),
            patch.object(balance_module, "_run_io", run_io),
        ):
            result = await balance_module._fetch_evm_chain_balances(
                chain_name="ethereum",
                wallet_address="0xabc",
            )
        return result

    result = asyncio.run(_run())

    assert result[0]["symbol"] == "ETH"
    assert run_io.await_args.kwargs["timeout"] == 12.0


def test_check_balance_timeout_message_uses_full_wallet_timeout():
    async def _run():
        with (
            patch.object(balance_module, "_FULL_WALLET_TIMEOUT_SECONDS", 12.0),
            patch(
                "tool_nodes.wallet.balance.get_wallet_balances",
                side_effect=TimeoutError("slow"),
            ),
        ):
            return await check_balance({"sender": "0xabc", "chain": "ethereum"})

    result = asyncio.run(_run())

    assert result["status"] == "error"
    assert ">12s" in result["message"]
