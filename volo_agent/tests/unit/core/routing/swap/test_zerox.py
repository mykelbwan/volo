from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from core.routing.swap.zerox import ZeroXAggregator
from core.utils.http import ExternalServiceError

@pytest.mark.asyncio
async def test_zerox_get_quote_success():
    aggregator = ZeroXAggregator()
    
    mock_data = {
        "buyAmount": "1000000000000000000", # 1.0
        "estimatedGas": "150000",
        "estimatedGasPrice": "25000000000",
        "transaction": {
            "to": "0x123",
            "data": "0xdeadbeef"
        }
    }

    with patch("core.routing.swap.zerox._api_key", return_value="fake-key"), \
         patch("core.routing.swap.zerox.get_chain_by_id") as mock_get_chain, \
         patch("core.routing.swap.zerox._resolve_decimals", AsyncMock(side_effect=[18, 18])), \
         patch("core.routing.swap.zerox.async_request_json") as mock_req, \
         patch("core.routing.swap.zerox.async_raise_for_status", AsyncMock()):
        
        # async_request_json is async, so await mock_req returns its return_value
        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_data
        mock_req.return_value = mock_resp

        quote = await aggregator.get_quote(
            chain_id=1,
            token_in="0x0",
            token_out="0x1",
            amount_in=Decimal("1.0"),
            slippage_pct=0.5,
            sender="0xsender"
        )

        request_headers = mock_req.await_args.kwargs["headers"]
        assert request_headers["0x-api-key"] == "fake-key"
        assert request_headers["0x-version"] == "v2"
        assert quote is not None
        assert quote.aggregator == "0x"
        assert quote.amount_out == Decimal("1.0")
        assert quote.gas_estimate == 150000
        assert quote.gas_cost_usd is None  # Verified fix
        assert quote.to == "0x123"
        assert quote.calldata == "0xdeadbeef"

@pytest.mark.asyncio
async def test_zerox_get_quote_unsupported_chain():
    aggregator = ZeroXAggregator()

    with patch("core.routing.swap.zerox._api_key", return_value="fake-key"), \
         patch("core.routing.swap.zerox.get_chain_by_id", side_effect=KeyError("Chain not found")):
        
        quote = await aggregator.get_quote(
            chain_id=999,
            token_in="0x0",
            token_out="0x1",
            amount_in=Decimal("1.0"),
            slippage_pct=0.5,
            sender="0xsender"
        )

        assert quote is None

@pytest.mark.asyncio
async def test_zerox_get_quote_api_error():
    aggregator = ZeroXAggregator()

    # ExternalServiceError(service, status_code, body)
    err = ExternalServiceError("zerox", 500, "Internal Server Error")

    with patch("core.routing.swap.zerox._api_key", return_value="fake-key"), \
         patch("core.routing.swap.zerox.get_chain_by_id"), \
         patch("core.routing.swap.zerox._resolve_decimals", AsyncMock(return_value=18)), \
         patch("core.routing.swap.zerox.async_request_json", side_effect=err):
        
        quote = await aggregator.get_quote(
            chain_id=1,
            token_in="0x0",
            token_out="0x1",
            amount_in=Decimal("1.0"),
            slippage_pct=0.5,
            sender="0xsender"
        )

        assert quote is None
