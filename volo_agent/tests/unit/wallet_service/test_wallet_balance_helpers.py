import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from wallet_service.evm.get_native_bal import get_native_balance_async
from wallet_service.evm.get_single_erc20_token_bal import get_token_balance_async


@pytest.mark.anyio
async def test_get_native_balance_async():
    mock_w3 = MagicMock()
    mock_w3.to_checksum_address.side_effect = lambda x: x
    # mock_w3.eth.get_balance is called with await, so it must return a coroutine or be an AsyncMock
    mock_w3.eth.get_balance = AsyncMock(return_value=2 * 10**18)
    mock_w3.from_wei.side_effect = lambda val, unit: Decimal(val) / Decimal(10**18)

    with patch("wallet_service.evm.get_native_bal.make_async_web3", return_value=mock_w3):
        balance = await get_native_balance_async("0xabc", "http://example")
    
    assert balance == Decimal("2")
    mock_w3.eth.get_balance.assert_called_once_with("0xabc")


@pytest.mark.anyio
async def test_get_token_balance_async():
    mock_w3 = MagicMock()
    mock_w3.to_checksum_address.side_effect = lambda x: x
    
    mock_contract = MagicMock()
    # contract.functions.balanceOf(addr).call() is awaited
    mock_call = AsyncMock(return_value=5000)
    mock_contract.functions.balanceOf.return_value.call = mock_call
    
    mock_w3.eth.contract.return_value = mock_contract

    with patch("wallet_service.evm.get_single_erc20_token_bal.make_async_web3", return_value=mock_w3):
        balance = await get_token_balance_async("0xabc", "0xtoken", 3, "http://example")
    
    assert balance == Decimal("5")
    mock_contract.functions.balanceOf.assert_called_once_with("0xabc")
