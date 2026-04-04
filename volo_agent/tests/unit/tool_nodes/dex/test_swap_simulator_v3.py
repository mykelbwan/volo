import asyncio
from unittest.mock import AsyncMock, patch

from tool_nodes.dex.swap_simulator_v3 import SimulationError, simulate_swap


class _DummyChain:
    def __init__(
        self,
        *,
        v3_quoter="0xquoter",
        v3_router="0xrouter",
        wrapped_native="0xwrap",
        supports_native_swaps=True,
    ):
        self.v3_quoter = v3_quoter
        self.v3_router = v3_router
        self.wrapped_native = wrapped_native
        self.supports_native_swaps = supports_native_swaps
        self.name = "Ethereum"
        self.chain_id = 1


class _DummyEth:
    def contract(self, *args, **kwargs):
        return object()


class _DummyWeb3:
    eth = _DummyEth()

    def to_checksum_address(self, addr):
        return addr


def test_simulate_v3_no_quoter():
    chain = _DummyChain(v3_quoter=None, v3_router="0xrouter")
    with patch("tool_nodes.dex.swap_simulator_v3._resolve_chain", return_value=chain):
        result = asyncio.run(
            simulate_swap(
                token_in="0xaaa",
                token_out="0xbbb",
                amount_in=1,
                sender="0xsender",
                chain_name="ethereum",
            )
        )
    assert isinstance(result, SimulationError)
    assert result.reason == "NO_QUOTER"


def test_simulate_v3_no_router():
    chain = _DummyChain(v3_quoter="0xquoter", v3_router=None)
    with patch("tool_nodes.dex.swap_simulator_v3._resolve_chain", return_value=chain):
        result = asyncio.run(
            simulate_swap(
                token_in="0xaaa",
                token_out="0xbbb",
                amount_in=1,
                sender="0xsender",
                chain_name="ethereum",
            )
        )
    assert isinstance(result, SimulationError)
    assert result.reason == "NO_ROUTER"


def test_simulate_v3_same_token():
    chain = _DummyChain()
    with patch("tool_nodes.dex.swap_simulator_v3._resolve_chain", return_value=chain):
        with patch("tool_nodes.dex.swap_simulator_v3._get_web3", return_value=_DummyWeb3()):
            with patch("tool_nodes.dex.swap_simulator_v3._resolve_token_address", return_value="0xsame"):
                result = asyncio.run(
                    simulate_swap(
                        token_in="0xaaa",
                        token_out="0xbbb",
                        amount_in=1,
                        sender="0xsender",
                        chain_name="ethereum",
                    )
                )
    assert isinstance(result, SimulationError)
    assert result.reason == "SAME_TOKEN"


def test_simulate_v3_native_requires_approval_when_router_no_native():
    chain = _DummyChain(supports_native_swaps=False)
    with patch("tool_nodes.dex.swap_simulator_v3._resolve_chain", return_value=chain):
        with patch("tool_nodes.dex.swap_simulator_v3._get_web3", return_value=_DummyWeb3()):
            with patch(
                "tool_nodes.dex.swap_simulator_v3._resolve_token_address",
                side_effect=["0xwrap", "0xout"],
            ):
                with patch(
                    "tool_nodes.dex.swap_simulator_v3._get_token_decimals",
                    new=AsyncMock(return_value=18),
                ):
                    with patch(
                        "tool_nodes.dex.swap_simulator_v3._try_quote_single",
                        new=AsyncMock(return_value=(2 * 10**18, 100_000)),
                    ):
                        with patch(
                            "tool_nodes.dex.swap_simulator_v3._get_allowance",
                            new=AsyncMock(return_value=0),
                        ):
                            result = asyncio.run(
                                simulate_swap(
                                    token_in="0x0000000000000000000000000000000000000000",
                                    token_out="0xout",
                                    amount_in=1,
                                    sender="0xsender",
                                    chain_name="ethereum",
                                )
                            )
    assert not isinstance(result, SimulationError)
    assert result.needs_approval is True


def test_simulate_v3_wrapped_input_requires_approval_even_with_native_support():
    chain = _DummyChain(supports_native_swaps=True)
    with patch("tool_nodes.dex.swap_simulator_v3._resolve_chain", return_value=chain):
        with patch("tool_nodes.dex.swap_simulator_v3._get_web3", return_value=_DummyWeb3()):
            with patch(
                "tool_nodes.dex.swap_simulator_v3._resolve_token_address",
                side_effect=["0xwrap", "0xout"],
            ):
                with patch(
                    "tool_nodes.dex.swap_simulator_v3._get_token_decimals",
                    new=AsyncMock(return_value=18),
                ):
                    with patch(
                        "tool_nodes.dex.swap_simulator_v3._try_quote_single",
                        new=AsyncMock(return_value=(2 * 10**18, 100_000)),
                    ):
                        with patch(
                            "tool_nodes.dex.swap_simulator_v3._get_allowance",
                            new=AsyncMock(return_value=0),
                        ):
                            result = asyncio.run(
                                simulate_swap(
                                    token_in="0xwrap",
                                    token_out="0xout",
                                    amount_in=1,
                                    sender="0xsender",
                                    chain_name="ethereum",
                                )
                            )
    assert not isinstance(result, SimulationError)
    assert result.needs_approval is True
