from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from config.bridge_registry import BridgeRoute
from tool_nodes.bridge.simulators.across_simulator import (
    AcrossSimulationError,
    fetch_across_available_routes,
    simulate_across_bridge,
)


_ROUTE = BridgeRoute(
    protocol="across",
    source_chain_id=1,
    dest_chain_id=8453,
    token_symbol="USDC",
    source_contract="0xsource",
    dest_contract="0xdest",
    input_token="0xinput",
    output_token="0xoutput",
)


def test_across_simulator_missing_sender_raises():
    try:
        simulate_across_bridge(_ROUTE, amount=1, sender="")
    except ValueError as exc:
        assert "sender address is required" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_across_simulator_zero_amount_returns_error():
    result = simulate_across_bridge(_ROUTE, amount=0, sender="0xsender")
    assert isinstance(result, AcrossSimulationError)
    assert result.reason == "ZERO_AMOUNT"


def test_across_simulator_timeout_returns_error():
    with patch(
        "tool_nodes.bridge.simulators.across_simulator.request_json",
        side_effect=requests.Timeout,
    ):
        result = simulate_across_bridge(_ROUTE, amount=1, sender="0xsender")

    assert isinstance(result, AcrossSimulationError)
    assert result.reason == "API_TIMEOUT"


def test_across_simulator_http_error_returns_error():
    class _DummyResponse:
        status_code = 500
        text = "boom"

        def raise_for_status(self):
            raise requests.HTTPError("boom")

        def json(self):
            return {"error": "boom"}

    with patch(
        "tool_nodes.bridge.simulators.across_simulator.request_json",
        return_value=_DummyResponse(),
    ):
        result = simulate_across_bridge(_ROUTE, amount=1, sender="0xsender")

    assert isinstance(result, AcrossSimulationError)
    assert result.reason == "API_ERROR"


def test_fetch_across_available_routes_filters_and_builds():
    class _DummyResponse:
        status_code = 200
        text = ""

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "availableRoutes": [
                    {
                        "originChainId": 1,
                        "destinationChainId": 8453,
                        "originTokenSymbol": "USDC",
                        "destinationTokenSymbol": "USDC",
                        "isNative": False,
                        "originToken": "0x1111111111111111111111111111111111111111",
                        "destinationToken": "0x2222222222222222222222222222222222222222",
                    },
                    {
                        "originChainId": 1,
                        "destinationChainId": 10,
                        "originTokenSymbol": "USDC",
                        "destinationTokenSymbol": "USDC",
                        "isNative": False,
                        "originToken": "0x3333333333333333333333333333333333333333",
                        "destinationToken": "0x4444444444444444444444444444444444444444",
                    },
                ]
            }

    source = SimpleNamespace(
        is_testnet=False,
        native_symbol="ETH",
        wrapped_native="0x4200000000000000000000000000000000000006",
    )
    dest = SimpleNamespace(
        is_testnet=False,
        native_symbol="ETH",
        wrapped_native="0x4200000000000000000000000000000000000007",
    )

    with patch(
        "tool_nodes.bridge.simulators.across_simulator.get_chain_by_id",
        side_effect=[source, dest],
    ):
        with patch(
            "tool_nodes.bridge.simulators.across_simulator.request_json",
            return_value=_DummyResponse(),
        ):
            routes = fetch_across_available_routes(1, 8453, "USDC")

    assert len(routes) == 1
    route = routes[0]
    assert route.input_token == "0x1111111111111111111111111111111111111111"
    assert route.output_token == "0x2222222222222222222222222222222222222222"
    assert route.is_native_input is False


def test_fetch_across_available_routes_allows_native_symbol_mismatch():
    class _DummyResponse:
        status_code = 200
        text = ""

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "availableRoutes": [
                    {
                        "originChainId": 1,
                        "destinationChainId": 10,
                        "originTokenSymbol": "WETH",
                        "destinationTokenSymbol": "WETH",
                        "isNative": True,
                        "originToken": "",
                        "destinationToken": "",
                    }
                ]
            }

    source = SimpleNamespace(
        is_testnet=False,
        native_symbol="ETH",
        wrapped_native="0x4200000000000000000000000000000000000006",
    )
    dest = SimpleNamespace(
        is_testnet=False,
        native_symbol="ETH",
        wrapped_native="0x4200000000000000000000000000000000000007",
    )

    with patch(
        "tool_nodes.bridge.simulators.across_simulator.get_chain_by_id",
        side_effect=[source, dest],
    ):
        with patch(
            "tool_nodes.bridge.simulators.across_simulator.request_json",
            return_value=_DummyResponse(),
        ):
            routes = fetch_across_available_routes(1, 10, "ETH")

    assert len(routes) == 1
    route = routes[0]
    assert route.is_native_input is True
    assert route.input_token == "0x4200000000000000000000000000000000000006"
    assert route.output_token == "0x4200000000000000000000000000000000000007"


def test_fetch_across_available_routes_uses_testnet_base_url():
    class _DummyResponse:
        status_code = 200
        text = ""

        def raise_for_status(self):
            return None

        def json(self):
            return {"availableRoutes": []}

    source = SimpleNamespace(is_testnet=True, native_symbol="ETH")
    dest = SimpleNamespace(is_testnet=True, native_symbol="ETH")

    request_mock = MagicMock(return_value=_DummyResponse())
    with patch(
        "tool_nodes.bridge.simulators.across_simulator.get_chain_by_id",
        side_effect=[source, dest],
    ):
        with patch(
            "tool_nodes.bridge.simulators.across_simulator.request_json",
            new=request_mock,
        ):
            fetch_across_available_routes(11155111, 84532, "USDC")

    assert request_mock.call_count == 1
    url = request_mock.call_args[0][1]
    assert url.startswith("https://testnet.across.to/api/available-routes")
