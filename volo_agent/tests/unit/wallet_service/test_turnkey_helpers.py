import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wallet_service.evm.create_sub_org import create_sub_org
from wallet_service.evm.sign_tx import sign_transaction_async


def test_create_sub_org_requires_user_id():
    with pytest.raises(ValueError):
        create_sub_org("")


def test_create_sub_org_parses_response(monkeypatch):
    mock_account = MagicMock()
    mock_account.address = "0xabc"

    with patch(
        "wallet_service.evm.create_sub_org.create_evm_account",
        return_value=mock_account,
    ):
        result = create_sub_org("user-1")

    assert result["address"] == "0xabc"
    assert result["sub_org_id"].startswith("volo-")


def test_sign_transaction_returns_signed_tx(monkeypatch):
    with patch(
        "wallet_service.evm.sign_tx.sign_evm_transaction_by_name_async",
        new=AsyncMock(return_value="0xsigned"),
    ):
        signed = asyncio.run(
            sign_transaction_async(
                "sub",
                {
                    "nonce": 1,
                    "gas": 21000,
                    "maxFeePerGas": 1,
                    "maxPriorityFeePerGas": 1,
                    "type": "0x2",
                    "to": "0x0000000000000000000000000000000000000000",
                    "value": 0,
                    "data": "0x",
                    "chainId": 1,
                },
                "0xsigner",
            )
        )

    assert signed == "0xsigned"
