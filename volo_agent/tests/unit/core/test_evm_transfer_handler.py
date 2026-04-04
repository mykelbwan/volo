import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from core.transfers.chains import get_transfer_chain_spec
from core.transfers.evm_handler import EVM_TRANSFER_HANDLER
from core.transfers.models import NormalizedTransferRequest


class _DummyChain:
    def __init__(self):
        self.chain_id = 1
        self.rpc_url = "http://example"
        self.explorer_url = "https://etherscan.io"


class _DummyWeb3:
    def to_checksum_address(self, addr):
        return addr


class _DummyNonceManager:
    async def allocate_safe(self, *_args, **_kwargs):
        return 1


@asynccontextmanager
async def _noop_wallet_lock(*_args, **_kwargs):
    class _Lock:
        async def ensure_held(self) -> None:
            return None

    yield _Lock()


def _evm_request(
    *,
    asset_symbol: str = "ETH",
    asset_ref: str | None = "0x0000000000000000000000000000000000000000",
    amount: str = "1",
    decimals: int | None = None,
    requested_network: str | None = "ethereum",
) -> NormalizedTransferRequest:
    return NormalizedTransferRequest(
        asset_ref=asset_ref,
        asset_symbol=asset_symbol,
        amount=Decimal(amount),
        recipient="0xabc",
        network="ethereum",
        requested_network=requested_network,
        sender="0xsender",
        sub_org_id="sub",
        decimals=decimals,
        idempotency_key=None,
    )


def test_evm_handler_native_transfer_parity():
    request = _evm_request()
    chain_spec = get_transfer_chain_spec("ethereum")

    with patch(
        "core.transfers.evm_handler.execute_native_transfer",
        new=AsyncMock(return_value="0xhash"),
    ):
        with patch(
            "core.transfers.evm_handler.get_chain_by_name",
            return_value=_DummyChain(),
        ):
            result = asyncio.run(EVM_TRANSFER_HANDLER.execute_transfer(request, chain_spec))

    assert result.status == "success"
    assert result.tx_hash == "0xhash"
    assert result.asset_symbol == "ETH"
    assert result.network == "ethereum"
    assert result.message.startswith("Transfer submitted: 1 ETH")


def test_evm_handler_preserves_requested_network_alias_in_response():
    request = _evm_request(requested_network="Ethereum")
    chain_spec = get_transfer_chain_spec("ethereum")

    with patch(
        "core.transfers.evm_handler.execute_native_transfer",
        new=AsyncMock(return_value="0xhash"),
    ):
        with patch(
            "core.transfers.evm_handler.get_chain_by_name",
            return_value=_DummyChain(),
        ):
            result = asyncio.run(EVM_TRANSFER_HANDLER.execute_transfer(request, chain_spec))

    assert result.network == "Ethereum"
    assert " on Ethereum. " in result.message


def test_evm_handler_erc20_transfer_parity():
    request = _evm_request(
        asset_symbol="USDC",
        asset_ref="0xusdc",
        amount="5",
        decimals=6,
    )
    chain_spec = get_transfer_chain_spec("ethereum")

    with patch(
        "core.transfers.evm_handler.get_chain_by_name",
        return_value=_DummyChain(),
    ):
        with patch(
            "core.transfers.evm_handler.gas_price_cache.get_wei",
            new=AsyncMock(return_value=1_000_000_000),
        ):
            with patch("core.transfers.evm_handler.wallet_lock", new=_noop_wallet_lock):
                with patch(
                    "core.transfers.evm_handler.make_async_web3",
                    return_value=_DummyWeb3(),
                ):
                    with patch(
                        "core.transfers.evm_handler.build_erc20_tx",
                        return_value={"tx": "data"},
                    ):
                        with patch(
                            "core.transfers.evm_handler.sign_transaction_async",
                            new=AsyncMock(return_value="0xdeadbeef"),
                        ):
                            with patch(
                                "core.transfers.evm_handler.get_async_nonce_manager",
                                new=AsyncMock(return_value=_DummyNonceManager()),
                            ):
                                with patch(
                                    "core.transfers.evm_handler.async_broadcast_evm",
                                    new=AsyncMock(return_value="0x1234"),
                                ):
                                    result = asyncio.run(
                                        EVM_TRANSFER_HANDLER.execute_transfer(
                                            request, chain_spec
                                        )
                                    )

    assert result.status == "success"
    assert result.tx_hash == "0x1234"
    assert result.asset_symbol == "USDC"
    assert result.recipient == "0xabc"


def test_evm_handler_erc20_preserves_exact_decimal_amount():
    request = _evm_request(
        asset_symbol="USDC",
        asset_ref="0xusdc",
        amount="123456789.123456",
        decimals=6,
    )
    chain_spec = get_transfer_chain_spec("ethereum")
    captured_amounts: list[Decimal] = []

    def _build_erc20_tx(**kwargs):
        captured_amounts.append(kwargs["amount"])
        return {"tx": "data"}

    with patch(
        "core.transfers.evm_handler.get_chain_by_name",
        return_value=_DummyChain(),
    ):
        with patch(
            "core.transfers.evm_handler.gas_price_cache.get_wei",
            new=AsyncMock(return_value=1_000_000_000),
        ):
            with patch("core.transfers.evm_handler.wallet_lock", new=_noop_wallet_lock):
                with patch(
                    "core.transfers.evm_handler.make_async_web3",
                    return_value=_DummyWeb3(),
                ):
                    with patch(
                        "core.transfers.evm_handler.build_erc20_tx",
                        new=_build_erc20_tx,
                    ):
                        with patch(
                            "core.transfers.evm_handler.sign_transaction_async",
                            new=AsyncMock(return_value="0xdeadbeef"),
                        ):
                            with patch(
                                "core.transfers.evm_handler.get_async_nonce_manager",
                                new=AsyncMock(return_value=_DummyNonceManager()),
                            ):
                                with patch(
                                    "core.transfers.evm_handler.async_broadcast_evm",
                                    new=AsyncMock(return_value="0x1234"),
                                ):
                                    result = asyncio.run(
                                        EVM_TRANSFER_HANDLER.execute_transfer(
                                            request, chain_spec
                                        )
                                    )

    assert captured_amounts == [Decimal("123456789.123456")]
    assert result.amount == Decimal("123456789.123456")


def test_evm_handler_rejects_non_evm_family():
    request = _evm_request(asset_symbol="SOL", asset_ref="native", amount="0.5")
    chain_spec = get_transfer_chain_spec("solana-devnet")

    with pytest.raises(ValueError, match="cannot execute family"):
        asyncio.run(EVM_TRANSFER_HANDLER.execute_transfer(request, chain_spec))
