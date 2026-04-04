import asyncio
from unittest.mock import AsyncMock, patch

from tool_nodes.dex.swap_simulator_v2 import SimulationErrorV2, simulate_swap_v2


class _DummyChain:
    def __init__(
        self,
        *,
        v2_router="0xrouter",
        v2_factory="0xfactory",
        wrapped_native="0xwrap",
        supports_native_swaps=True,
    ):
        self.v2_router = v2_router
        self.v2_factory = v2_factory
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


def test_simulate_v2_no_router():
    chain = _DummyChain(v2_router=None, v2_factory="0xfactory")
    with patch("tool_nodes.dex.swap_simulator_v2._resolve_chain", return_value=chain):
        result = asyncio.run(
            simulate_swap_v2(
                token_in="0xaaa",
                token_out="0xbbb",
                amount_in=1,
                sender="0xsender",
                chain_name="ethereum",
            )
        )
    assert isinstance(result, SimulationErrorV2)
    assert result.reason == "NO_ROUTER"


def test_simulate_v2_no_factory():
    chain = _DummyChain(v2_router="0xrouter", v2_factory=None)
    with patch("tool_nodes.dex.swap_simulator_v2._resolve_chain", return_value=chain):
        result = asyncio.run(
            simulate_swap_v2(
                token_in="0xaaa",
                token_out="0xbbb",
                amount_in=1,
                sender="0xsender",
                chain_name="ethereum",
            )
        )
    assert isinstance(result, SimulationErrorV2)
    assert result.reason == "NO_FACTORY"


def test_simulate_v2_same_token():
    chain = _DummyChain()
    with patch("tool_nodes.dex.swap_simulator_v2._resolve_chain", return_value=chain):
        with patch("tool_nodes.dex.swap_simulator_v2._get_web3", return_value=_DummyWeb3()):
            with patch("tool_nodes.dex.swap_simulator_v2._resolve_to_wrapped", return_value="0xsame"):
                result = asyncio.run(
                    simulate_swap_v2(
                        token_in="0xaaa",
                        token_out="0xbbb",
                        amount_in=1,
                        sender="0xsender",
                        chain_name="ethereum",
                    )
                )
    assert isinstance(result, SimulationErrorV2)
    assert result.reason == "SAME_TOKEN"


def test_simulate_v2_native_requires_approval_when_router_no_native():
    chain = _DummyChain(supports_native_swaps=False)
    with patch("tool_nodes.dex.swap_simulator_v2._resolve_chain", return_value=chain):
        with patch("tool_nodes.dex.swap_simulator_v2._get_web3", return_value=_DummyWeb3()):
            with patch(
                "tool_nodes.dex.swap_simulator_v2._resolve_to_wrapped",
                side_effect=["0xwrap", "0xout"],
            ):
                with patch(
                    "tool_nodes.dex.swap_simulator_v2._get_token_decimals",
                    new=AsyncMock(return_value=18),
                ):
                    with patch(
                        "tool_nodes.dex.swap_simulator_v2._resolve_path",
                        new=AsyncMock(return_value=["0xwrap", "0xout"]),
                    ):
                        with patch(
                            "tool_nodes.dex.swap_simulator_v2._get_amounts_out",
                            new=AsyncMock(return_value=[10**18, 2 * 10**18]),
                        ):
                            with patch(
                                "tool_nodes.dex.swap_simulator_v2._estimate_gas",
                                new=AsyncMock(return_value=130_000),
                            ):
                                with patch(
                                    "tool_nodes.dex.swap_simulator_v2._get_allowance",
                                    new=AsyncMock(return_value=0),
                                ):
                                    result = asyncio.run(
                                        simulate_swap_v2(
                                            token_in="0x0000000000000000000000000000000000000000",
                                            token_out="0xout",
                                            amount_in=1,
                                            sender="0xsender",
                                            chain_name="ethereum",
                                        )
                                    )
    assert not isinstance(result, SimulationErrorV2)
    assert result.needs_approval is True


def test_simulate_v2_wrapped_input_requires_approval_even_with_native_support():
    chain = _DummyChain(supports_native_swaps=True)
    with patch("tool_nodes.dex.swap_simulator_v2._resolve_chain", return_value=chain):
        with patch("tool_nodes.dex.swap_simulator_v2._get_web3", return_value=_DummyWeb3()):
            with patch(
                "tool_nodes.dex.swap_simulator_v2._resolve_to_wrapped",
                side_effect=["0xwrap", "0xout"],
            ):
                with patch(
                    "tool_nodes.dex.swap_simulator_v2._get_token_decimals",
                    new=AsyncMock(return_value=18),
                ):
                    with patch(
                        "tool_nodes.dex.swap_simulator_v2._resolve_path",
                        new=AsyncMock(return_value=["0xwrap", "0xout"]),
                    ):
                        with patch(
                            "tool_nodes.dex.swap_simulator_v2._get_amounts_out",
                            new=AsyncMock(return_value=[10**18, 2 * 10**18]),
                        ):
                            with patch(
                                "tool_nodes.dex.swap_simulator_v2._estimate_gas",
                                new=AsyncMock(return_value=130_000),
                            ):
                                with patch(
                                    "tool_nodes.dex.swap_simulator_v2._get_allowance",
                                    new=AsyncMock(return_value=0),
                                ):
                                    result = asyncio.run(
                                        simulate_swap_v2(
                                            token_in="0xwrap",
                                            token_out="0xout",
                                            amount_in=1,
                                            sender="0xsender",
                                            chain_name="ethereum",
                                        )
                                    )
    assert not isinstance(result, SimulationErrorV2)
    assert result.needs_approval is True
