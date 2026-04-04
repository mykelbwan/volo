from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from core.fees.fee_collector import FeeCollectionError, collect_fee
from core.fees.models import FeeQuote


def _evm_quote() -> FeeQuote:
    return FeeQuote(
        node_id="step_0",
        tool="swap",
        chain="Ethereum",
        native_symbol="ETH",
        base_fee_bps=20,
        discount_bps=0,
        final_fee_bps=20,
        fee_amount_native=Decimal("0.001"),
        fee_recipient="0x000000000000000000000000000000000000dEaD",
        chain_family="evm",
        chain_network="Ethereum",
        is_native_tx=True,
    )


def _solana_quote() -> FeeQuote:
    return FeeQuote(
        node_id="step_0",
        tool="solana_swap",
        chain="Solana",
        native_symbol="SOL",
        base_fee_bps=20,
        discount_bps=0,
        final_fee_bps=20,
        fee_amount_native=Decimal("0.001"),
        fee_recipient="11111111111111111111111111111111",
        chain_family="solana",
        chain_network="solana",
        is_native_tx=True,
    )


def test_collect_fee_dispatches_to_evm_transfer():
    quote = _evm_quote()
    with patch(
        "wallet_service.evm.native_transfer.execute_native_transfer",
        new=AsyncMock(return_value="0xfeehash"),
    ) as mock_transfer:
        tx_hash = asyncio.run(
            collect_fee(
                quote,
                {"sender": "0xabc", "sub_org_id": "sub-123"},
            )
        )

    assert tx_hash == "0xfeehash"
    mock_transfer.assert_awaited_once_with(
        to=quote.fee_recipient,
        amount_native=quote.fee_amount_native,
        chain_name=quote.chain,
        sender="0xabc",
        sub_org_id="sub-123",
    )


def test_collect_fee_dispatches_to_solana_transfer():
    quote = _solana_quote()
    with patch(
        "wallet_service.solana.native_transfer.execute_native_transfer",
        new=AsyncMock(return_value="solsig123"),
    ) as mock_transfer:
        tx_hash = asyncio.run(
            collect_fee(
                quote,
                {
                    "sender": "So1sender111111111111111111111111111111111",
                    "sub_org_id": "sol-sub-123",
                },
            )
        )

    assert tx_hash == "solsig123"
    mock_transfer.assert_awaited_once_with(
        sender="So1sender111111111111111111111111111111111",
        sub_org_id="sol-sub-123",
        recipient=quote.fee_recipient,
        amount_native=quote.fee_amount_native,
        network="solana",
    )


def test_collect_fee_raises_clear_error_when_solana_sender_missing():
    quote = _solana_quote()
    try:
        asyncio.run(collect_fee(quote, {}))
    except FeeCollectionError as exc:
        message = str(exc).lower()
    else:
        raise AssertionError("expected FeeCollectionError")

    assert "solana wallet details were incomplete" in message
    assert "retry after wallet setup finishes" in message
