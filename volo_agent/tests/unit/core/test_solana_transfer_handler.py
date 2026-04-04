import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from core.transfers.chains import get_transfer_chain_spec
from core.transfers.models import NormalizedTransferRequest
from core.transfers.solana_handler import SOLANA_TRANSFER_HANDLER

_SOL_NATIVE_REF = "So11111111111111111111111111111111111111112"
_SOL_MINT_USDC_DEVNET = "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU"
_SENDER = "9xQeWvG816bUx9EPjHmaT23yvVMcFVN8Y6GsB7mQ7X7G"
_RECIPIENT = "H3qWmD5MZx7Sg1H4C4r4Y6xWm4kD1a2o3p4q5r6s7t8"


def _solana_request(
    *,
    asset_symbol: str = "SOL",
    asset_ref: str | None = _SOL_NATIVE_REF,
    amount: str = "0.1",
    decimals: int | None = None,
    requested_network: str | None = "solana-devnet",
    idempotency_key: str | None = None,
) -> NormalizedTransferRequest:
    return NormalizedTransferRequest(
        asset_ref=asset_ref,
        asset_symbol=asset_symbol,
        amount=Decimal(amount),
        recipient=_RECIPIENT,
        network="solana-devnet",
        requested_network=requested_network,
        sender=_SENDER,
        sub_org_id="sub",
        decimals=decimals,
        idempotency_key=idempotency_key,
    )


def test_solana_handler_native_transfer_parity():
    request = _solana_request()
    chain_spec = get_transfer_chain_spec("solana-devnet")

    with patch(
        "core.transfers.solana_handler.execute_native_transfer",
        new=AsyncMock(return_value="solsig"),
    ) as execute_native:
        result = asyncio.run(
            SOLANA_TRANSFER_HANDLER.execute_transfer(request, chain_spec)
        )

    execute_native.assert_awaited_once_with(
        sender=_SENDER,
        sub_org_id="sub",
        recipient=_RECIPIENT,
        amount_native=Decimal("0.1"),
        network="solana-devnet",
        idempotency_key=None,
    )
    assert result.status == "success"
    assert result.tx_hash == "solsig"
    assert result.asset_symbol == "SOL"
    assert result.network == "solana-devnet"
    assert result.message.startswith("Transfer submitted: 0.1 SOL")
    assert "https://solscan.io/tx/solsig?cluster=devnet" in result.message


def test_solana_handler_spl_transfer_parity():
    request = _solana_request(
        asset_symbol="USDC",
        asset_ref=_SOL_MINT_USDC_DEVNET,
        amount="5",
        decimals=6,
        idempotency_key="req-123",
    )
    chain_spec = get_transfer_chain_spec("solana-devnet")

    with patch(
        "core.transfers.solana_handler.get_solana_chain",
        return_value=SimpleNamespace(rpc_url="https://rpc.test"),
    ):
        with patch(
            "core.transfers.solana_handler.execute_spl_transfer",
            new=AsyncMock(return_value="splsig"),
        ) as execute_spl:
            result = asyncio.run(
                SOLANA_TRANSFER_HANDLER.execute_transfer(request, chain_spec)
            )

    execute_spl.assert_awaited_once_with(
        sender=_SENDER,
        sub_org_id="sub",
        recipient=_RECIPIENT,
        mint_address=_SOL_MINT_USDC_DEVNET,
        amount=Decimal("5"),
        rpc_url="https://rpc.test",
        network="solana-devnet",
        decimals=6,
        idempotency_key="req-123",
    )
    assert result.status == "success"
    assert result.tx_hash == "splsig"
    assert result.asset_symbol == "USDC"
    assert result.recipient == _RECIPIENT


def test_solana_handler_accepts_missing_asset_ref_for_native_symbol_compat():
    request = _solana_request(
        asset_ref=None,
    )
    chain_spec = get_transfer_chain_spec("solana-devnet")

    with patch(
        "core.transfers.solana_handler.execute_native_transfer",
        new=AsyncMock(return_value="solsig"),
    ) as execute_native:
        result = asyncio.run(
            SOLANA_TRANSFER_HANDLER.execute_transfer(request, chain_spec)
        )

    execute_native.assert_awaited_once()
    assert result.tx_hash == "solsig"


def test_solana_handler_rejects_missing_asset_ref_for_non_native_symbol():
    request = _solana_request(
        asset_symbol="USDC",
        asset_ref=None,
    )
    chain_spec = get_transfer_chain_spec("solana-devnet")

    with pytest.raises(ValueError, match="explicit mint address"):
        asyncio.run(SOLANA_TRANSFER_HANDLER.execute_transfer(request, chain_spec))


def test_solana_handler_rejects_native_asset_ref_with_non_native_symbol():
    request = _solana_request(asset_symbol="WSOL")
    chain_spec = get_transfer_chain_spec("solana-devnet")

    with pytest.raises(ValueError, match="native asset reference requires"):
        asyncio.run(SOLANA_TRANSFER_HANDLER.execute_transfer(request, chain_spec))


def test_solana_handler_rejects_non_solana_family():
    request = _solana_request()
    chain_spec = get_transfer_chain_spec("ethereum")

    with pytest.raises(ValueError, match="cannot execute family"):
        asyncio.run(SOLANA_TRANSFER_HANDLER.execute_transfer(request, chain_spec))
