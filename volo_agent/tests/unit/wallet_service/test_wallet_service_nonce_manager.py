import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from wallet_service.evm.nonce_manager import is_nonce_error, reset_on_error_async


def test_is_nonce_error_matches_known_hints():
    assert is_nonce_error(Exception("nonce too low"))
    assert is_nonce_error(Exception("Nonce too high"))
    assert is_nonce_error(Exception("replacement transaction underpriced"))
    assert is_nonce_error(Exception("already known"))
    assert is_nonce_error(Exception("invalid nonce"))


def test_is_nonce_error_false_for_unrelated_errors():
    assert not is_nonce_error(Exception("insufficient funds"))
    assert not is_nonce_error(Exception("gas price too low"))


def test_reset_on_error_invokes_reset_for_nonce_errors():
    manager = MagicMock()
    manager.reset = AsyncMock()
    w3 = MagicMock()
    with patch(
        "wallet_service.evm.nonce_manager.get_async_nonce_manager",
        return_value=manager,
    ):
        did_reset = asyncio.run(
            reset_on_error_async(
                Exception("nonce too low"),
                sender="0xSender",
                chain_id=1,
                w3=w3,
            )
        )

    assert did_reset is True
    manager.reset.assert_called_once_with("0xSender", 1, w3)


def test_reset_on_error_skips_non_nonce_errors():
    manager = MagicMock()
    manager.reset = AsyncMock()
    w3 = MagicMock()
    with patch(
        "wallet_service.evm.nonce_manager.get_async_nonce_manager",
        return_value=manager,
    ):
        did_reset = asyncio.run(
            reset_on_error_async(
                Exception("insufficient funds"),
                sender="0xSender",
                chain_id=1,
                w3=w3,
            )
        )

    assert did_reset is False
    manager.reset.assert_not_called()
