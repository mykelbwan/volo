import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from core.transfers.chains import TransferChainSpec
from core.transfers.models import (
    NormalizedTransferRequest,
    TransferExecutionResult,
)
from tool_nodes.wallet.transfer import transfer_token


class _FakeHandler:
    def __init__(self, result: TransferExecutionResult):
        self.execute_transfer = AsyncMock(return_value=result)


def _normalized_request(
    *,
    network: str = "ethereum",
    requested_network: str | None = "ethereum",
    asset_ref: str | None = "0x0000000000000000000000000000000000000000",
) -> NormalizedTransferRequest:
    return NormalizedTransferRequest(
        asset_ref=asset_ref,
        asset_symbol="ETH",
        amount=Decimal("1"),
        recipient="0xabc",
        network=network,
        requested_network=requested_network,
        sender="0xsender",
        sub_org_id="sub",
        decimals=None,
        idempotency_key=None,
    )


def _chain_spec(
    *,
    family: str = "evm",
    network: str = "ethereum",
    display_name: str = "Ethereum",
) -> TransferChainSpec:
    return TransferChainSpec(
        family=family,
        network=network,
        display_name=display_name,
        native_symbol="ETH" if family == "evm" else "SOL",
        explorer_url="https://example.test",
        is_testnet=False,
        native_asset_ref=(
            "0x0000000000000000000000000000000000000000"
            if family == "evm"
            else "So11111111111111111111111111111111111111112"
        ),
    )


def test_transfer_missing_params_raises():
    with pytest.raises(ValueError):
        asyncio.run(transfer_token({"token_symbol": "ETH"}))


def test_transfer_routes_normalized_request_through_family_handler():
    handler_result = TransferExecutionResult(
        status="success",
        tx_hash="0xhash",
        asset_symbol="ETH",
        amount=Decimal("1"),
        recipient="0xabc",
        network="ethereum",
        message="Transfer submitted: 1 ETH to 0xabc on ethereum. tx: 0xhash",
    )
    handler = _FakeHandler(handler_result)
    request = _normalized_request()
    chain_spec = _chain_spec()

    with patch(
        "tool_nodes.wallet.transfer.normalize_transfer_request",
        return_value=request,
    ) as normalize_request:
        with patch(
            "tool_nodes.wallet.transfer.get_transfer_chain_spec",
            return_value=chain_spec,
        ) as get_chain_spec:
            with patch(
                "tool_nodes.wallet.transfer.get_transfer_handler",
                return_value=handler,
            ) as get_handler:
                result = asyncio.run(transfer_token({"chain": "ethereum"}))

    normalize_request.assert_called_once_with({"chain": "ethereum"})
    get_chain_spec.assert_any_call("ethereum")
    get_handler.assert_called_once_with("evm")
    handler.execute_transfer.assert_awaited_once_with(request, chain_spec)
    assert result["tx_hash"] == "0xhash"
    assert result["chain"] == "ethereum"


def test_transfer_dispatches_to_family_handler_and_preserves_response_shape():
    handler_result = TransferExecutionResult(
        status="success",
        tx_hash="0xhash",
        asset_symbol="ETH",
        amount=Decimal("1"),
        recipient="0xabc",
        network="ethereum",
        message="Transfer submitted: 1 ETH to 0xabc on ethereum. tx: 0xhash",
    )
    handler = _FakeHandler(handler_result)

    with patch("tool_nodes.wallet.transfer.get_transfer_handler", return_value=handler):
        result = asyncio.run(
            transfer_token(
                {
                    "token_symbol": "ETH",
                    "token_address": "0x0000000000000000000000000000000000000000",
                    "amount": "1",
                    "recipient": "0xabc",
                    "chain": "ethereum",
                    "sub_org_id": "sub",
                    "sender": "0xsender",
                }
            )
        )

    handler.execute_transfer.assert_awaited_once()
    assert result == {
        "status": "success",
        "tx_hash": "0xhash",
        "asset_symbol": "ETH",
        "token_symbol": "ETH",
        "amount": "1",
        "recipient": "0xabc",
        "network": "ethereum",
        "chain": "ethereum",
        "message": "Transfer submitted: 1 ETH to 0xabc on ethereum. tx: 0xhash",
    }


def test_transfer_accepts_asset_ref_and_network_aliases():
    handler_result = TransferExecutionResult(
        status="success",
        tx_hash="0xhash",
        asset_symbol="ETH",
        amount=Decimal("1"),
        recipient="0xabc",
        network="ethereum",
        message="Transfer submitted: 1 ETH to 0xabc on ethereum. tx: 0xhash",
    )
    handler = _FakeHandler(handler_result)

    with patch("tool_nodes.wallet.transfer.get_transfer_handler", return_value=handler):
        result = asyncio.run(
            transfer_token(
                {
                    "asset_symbol": "ETH",
                    "asset_ref": "0x0000000000000000000000000000000000000000",
                    "amount": "1",
                    "recipient": "0xabc",
                    "network": "ethereum",
                    "sub_org_id": "sub",
                    "sender": "0xsender",
                }
            )
        )

    handler.execute_transfer.assert_awaited_once()
    assert result["tx_hash"] == "0xhash"
    assert result["asset_symbol"] == "ETH"
    assert result["network"] == "ethereum"
    assert result["chain"] == "ethereum"
    assert result["token_symbol"] == "ETH"


def test_transfer_rejects_conflicting_chain_and_network_aliases():
    params = {
        "token_symbol": "ETH",
        "amount": "1",
        "recipient": "0xabc",
        "chain": "ethereum",
        "network": "solana-devnet",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }

    with pytest.raises(ValueError, match="Conflicting transfer network inputs"):
        asyncio.run(transfer_token(params))


def test_transfer_supports_solana_family_through_shared_dispatcher():
    params = {
        "token_symbol": "SOL",
        "token_address": "So11111111111111111111111111111111111111112",
        "amount": "0.5",
        "recipient": "9xQeWvG816bUx9EPjHmaT23yvVMcFVN8Y6GsB7mQ7X7G",
        "chain": "solana-devnet",
        "sub_org_id": "sub",
        "sender": "H3qWmD5MZx7Sg1H4C4r4Y6xWm4kD1a2o3p4q5r6s7t8",
    }

    with patch(
        "core.transfers.solana_handler.execute_native_transfer",
        new=AsyncMock(return_value="solsig"),
    ) as execute_native:
        result = asyncio.run(transfer_token(params))

    execute_native.assert_awaited_once()
    assert result["tx_hash"] == "solsig"
    assert result["asset_symbol"] == "SOL"
    assert result["network"] == "solana-devnet"
    assert result["chain"] == "solana-devnet"
    assert result["token_symbol"] == "SOL"


def test_transfer_supports_solana_spl_through_shared_dispatcher():
    params = {
        "token_symbol": "USDC",
        "token_address": "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU",
        "amount": "2",
        "recipient": "9xQeWvG816bUx9EPjHmaT23yvVMcFVN8Y6GsB7mQ7X7G",
        "chain": "solana-devnet",
        "sub_org_id": "sub",
        "sender": "H3qWmD5MZx7Sg1H4C4r4Y6xWm4kD1a2o3p4q5r6s7t8",
        "decimals": 6,
    }

    with patch(
        "core.transfers.solana_handler.get_solana_chain",
        return_value=SimpleNamespace(rpc_url="https://rpc.test"),
    ):
        with patch(
            "core.transfers.solana_handler.execute_spl_transfer",
            new=AsyncMock(return_value="splsig"),
        ) as execute_spl:
            result = asyncio.run(transfer_token(params))

    execute_spl.assert_awaited_once()
    assert result["tx_hash"] == "splsig"
    assert result["asset_symbol"] == "USDC"
    assert result["network"] == "solana-devnet"
    assert result["chain"] == "solana-devnet"
    assert result["token_symbol"] == "USDC"


def test_transfer_rejects_malformed_solana_spl_without_asset_ref():
    params = {
        "token_symbol": "USDC",
        "amount": "0.5",
        "recipient": "9xQeWvG816bUx9EPjHmaT23yvVMcFVN8Y6GsB7mQ7X7G",
        "chain": "solana-devnet",
        "sub_org_id": "sub",
        "sender": "H3qWmD5MZx7Sg1H4C4r4Y6xWm4kD1a2o3p4q5r6s7t8",
    }

    with pytest.raises(ValueError, match="explicit mint address"):
        asyncio.run(transfer_token(params))


def test_transfer_rejects_malformed_normalized_request_before_dispatch():
    with patch(
        "tool_nodes.wallet.transfer.normalize_transfer_request",
        return_value=object(),
    ):
        with patch("tool_nodes.wallet.transfer.get_transfer_handler") as get_handler:
            with pytest.raises(ValueError, match="Malformed normalized transfer request"):
                asyncio.run(transfer_token({"chain": "ethereum"}))

    get_handler.assert_not_called()


def test_transfer_rejects_inconsistent_normalized_request_and_chain_spec():
    request = _normalized_request(requested_network="solana-devnet")

    with patch(
        "tool_nodes.wallet.transfer.normalize_transfer_request",
        return_value=request,
    ):
        with pytest.raises(ValueError, match="Malformed normalized transfer request"):
            asyncio.run(transfer_token({"chain": "ethereum"}))


def test_transfer_dispatch_does_not_execute_family_specific_logic_in_tool_module():
    handler_result = TransferExecutionResult(
        status="success",
        tx_hash="0xhash",
        asset_symbol="ETH",
        amount=Decimal("1"),
        recipient="0xabc",
        network="ethereum",
        message="Transfer submitted: 1 ETH to 0xabc on ethereum. tx: 0xhash",
    )
    handler = _FakeHandler(handler_result)

    with patch("tool_nodes.wallet.transfer.get_transfer_handler", return_value=handler):
        with patch(
            "core.transfers.evm_handler.execute_native_transfer",
            new=AsyncMock(side_effect=AssertionError("dispatcher should not execute transfers")),
        ):
            result = asyncio.run(
                transfer_token(
                    {
                        "token_symbol": "ETH",
                        "token_address": "0x0000000000000000000000000000000000000000",
                        "amount": "1",
                        "recipient": "0xabc",
                        "chain": "ethereum",
                        "sub_org_id": "sub",
                        "sender": "0xsender",
                    }
                )
            )

    assert result["tx_hash"] == "0xhash"
