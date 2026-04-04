import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from decimal import Decimal
from wallet_service.solana.get_native_bal import get_native_balance_async

@pytest.mark.asyncio
async def test_get_native_balance_async_success():
    # A valid-looking base58 public key
    wallet_address = "6S4VAsT89M8H8H8H8H8H8H8H8H8H8H8H8H8H8H8H" 
    # Wait, the previous one was 40 chars. Let's use a known valid one for simplicity or just mock Pubkey if we want to be safe,
    # but since we are testing the integration with Pubkey, let's use a real one.
    wallet_address = "vines1vzrYbzduYv64MvYvBx6uVv665QeWf1B8f28p8" # Random valid pubkey
    mock_rpc_url = "https://mock-solana-rpc.com"
    
    mock_chain_config = MagicMock()
    mock_chain_config.rpc_url = mock_rpc_url
    
    # Mocking get_solana_chain
    with patch("wallet_service.solana.get_native_bal.get_solana_chain", return_value=mock_chain_config) as mock_get_chain:
        # Mocking AsyncClient
        with patch("wallet_service.solana.get_native_bal.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            # Mock get_balance response
            mock_response = MagicMock()
            mock_response.value = 1_500_000_000 # 1.5 SOL in lamports
            mock_client.get_balance.return_value = mock_response
            
            balance = await get_native_balance_async(wallet_address)
            
            assert isinstance(balance, Decimal)
            assert balance == Decimal("1.5")
            
            mock_get_chain.assert_called_with("solana")
            mock_client_class.assert_called_with(mock_rpc_url)
            mock_client.get_balance.assert_called()

@pytest.mark.asyncio
async def test_get_native_balance_async_with_network():
    wallet_address = "vines1vzrYbzduYv64MvYvBx6uVv665QeWf1B8f28p8"
    network = "solana-devnet"
    mock_rpc_url = "https://api.devnet.solana.com"
    
    mock_chain_config = MagicMock()
    mock_chain_config.rpc_url = mock_rpc_url
    
    with patch("wallet_service.solana.get_native_bal.get_solana_chain", return_value=mock_chain_config) as mock_get_chain:
        with patch("wallet_service.solana.get_native_bal.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            mock_response = MagicMock()
            mock_response.value = 500_000_000 # 0.5 SOL
            mock_client.get_balance.return_value = mock_response
            
            balance = await get_native_balance_async(wallet_address, network=network)
            
            assert balance == Decimal("0.5")
            mock_get_chain.assert_called_with(network)

@pytest.mark.asyncio
async def test_get_native_balance_async_invalid_address():
    with pytest.raises(ValueError, match="Missing required value for 'wallet_address'"):
        await get_native_balance_async("")
