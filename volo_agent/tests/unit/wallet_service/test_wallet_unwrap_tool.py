import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from tool_nodes.wallet.unwrap import unwrap_token


class _DummyWalletLock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def ensure_held(self):
        return None


class _FakeCall:
    def __init__(self, value: int):
        self._value = value

    async def call(self):
        return self._value


class _FakeFunctions:
    def __init__(self, balance_raw: int):
        self._balance_raw = balance_raw

    def balanceOf(self, _wallet: str):
        return _FakeCall(self._balance_raw)


class _FakeContract:
    def __init__(self, balance_raw: int):
        self.functions = _FakeFunctions(balance_raw)


class _FakeEth:
    def __init__(self, balance_raw: int):
        self._balance_raw = balance_raw

    def contract(self, address, abi):
        return _FakeContract(self._balance_raw)

    async def estimate_gas(self, tx):
        return 90_000


class _FakeWeb3:
    def __init__(self, balance_raw: int):
        self.eth = _FakeEth(balance_raw)

    def to_checksum_address(self, value: str):
        return value


def _run(coro):
    return asyncio.run(coro)


def test_unwrap_token_uses_full_wrapped_balance_when_amount_missing():
    fake_chain = SimpleNamespace(
        chain_id=84532,
        rpc_url="https://rpc.test",
        name="Base Sepolia",
        explorer_url="https://sepolia.basescan.org",
    )
    fake_nonce_manager = SimpleNamespace(allocate_safe=AsyncMock(return_value=7))
    encode_mock = Mock(return_value="0xdeadbeef")
    broadcast_mock = AsyncMock(return_value="0xtx")

    with patch("tool_nodes.wallet.unwrap.get_chain_by_name", return_value=fake_chain):
        with patch(
            "tool_nodes.wallet.unwrap.make_async_web3",
            return_value=_FakeWeb3(2_000_000_000_000_000_000),
        ):
            with patch("tool_nodes.wallet.unwrap.encode_contract_call", encode_mock):
                with patch(
                    "tool_nodes.wallet.unwrap.gas_price_cache.get_wei",
                    new=AsyncMock(return_value=1_000_000_000),
                ):
                    with patch(
                        "tool_nodes.wallet.unwrap.estimate_eip1559_fees",
                        new=AsyncMock(return_value=(2_000_000_000, 1_000_000_000)),
                    ):
                        with patch(
                            "tool_nodes.wallet.unwrap.get_async_nonce_manager",
                            new=AsyncMock(return_value=fake_nonce_manager),
                        ):
                            with patch(
                                "tool_nodes.wallet.unwrap.sign_transaction_async",
                                new=AsyncMock(return_value="0xsigned"),
                            ):
                                with patch(
                                    "tool_nodes.wallet.unwrap.wallet_lock",
                                    return_value=_DummyWalletLock(),
                                ):
                                    with patch(
                                        "tool_nodes.wallet.unwrap.async_broadcast_evm",
                                        broadcast_mock,
                                    ):
                                        result = _run(
                                            unwrap_token(
                                                {
                                                    "token_symbol": "ETH",
                                                    "token_address": "0x4200000000000000000000000000000000000006",
                                                    "chain": "base sepolia",
                                                    "sub_org_id": "sub",
                                                    "sender": "0xabc",
                                                }
                                            )
                                        )

    assert result["status"] == "success"
    assert result["amount"] == "2"
    assert result["wrapped_token_symbol"] == "WETH"
    assert result["tx_hash"] == "0xtx"
    broadcast_mock.assert_awaited_once()
    assert encode_mock.call_args[0][1] == "withdraw"
    assert encode_mock.call_args[0][2] == [2_000_000_000_000_000_000]


def test_unwrap_token_fails_when_requested_amount_exceeds_wrapped_balance():
    fake_chain = SimpleNamespace(
        chain_id=84532,
        rpc_url="https://rpc.test",
        name="Base Sepolia",
        explorer_url="https://sepolia.basescan.org",
    )

    with patch("tool_nodes.wallet.unwrap.get_chain_by_name", return_value=fake_chain):
        with patch(
            "tool_nodes.wallet.unwrap.make_async_web3",
            return_value=_FakeWeb3(1_000_000_000_000_000_000),
        ):
            with pytest.raises(ValueError, match="available 1 WETH"):
                _run(
                    unwrap_token(
                        {
                            "token_symbol": "ETH",
                            "token_address": "0x4200000000000000000000000000000000000006",
                            "amount": "2",
                            "chain": "base sepolia",
                            "sub_org_id": "sub",
                            "sender": "0xabc",
                        }
                    )
                )


def test_unwrap_token_fails_with_clear_zero_balance_feedback():
    fake_chain = SimpleNamespace(
        chain_id=84532,
        rpc_url="https://rpc.test",
        name="Base Sepolia",
        explorer_url="https://sepolia.basescan.org",
    )

    with patch("tool_nodes.wallet.unwrap.get_chain_by_name", return_value=fake_chain):
        with patch(
            "tool_nodes.wallet.unwrap.make_async_web3",
            return_value=_FakeWeb3(0),
        ):
            with pytest.raises(ValueError, match="wrapped balance is 0 WETH"):
                _run(
                    unwrap_token(
                        {
                            "token_symbol": "ETH",
                            "token_address": "0x4200000000000000000000000000000000000006",
                            "chain": "base sepolia",
                            "sub_org_id": "sub",
                            "sender": "0xabc",
                        }
                    )
                )
