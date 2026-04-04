import asyncio
import threading
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from config.bridge_registry import BridgeRoute
from core.utils.errors import DeterminismViolationError
from core.utils.errors import NonRetryableError
from tool_nodes.bridge.bridge_tool import bridge_token
from tool_nodes.bridge.executors.across_executor import AcrossBridgeResult
from tool_nodes.bridge.executors.relay_executor import RelayBridgeResult
from tool_nodes.bridge.simulators.across_simulator import (
    AcrossBridgeQuote,
    AcrossSimulationError,
)
from tests.unit.wallet_service._wallet_security_helpers import InMemoryIdempotencyStore
from wallet_service.common import transfer_idempotency as transfer_idempotency_module


@asynccontextmanager
async def _noop_wallet_lock(*_args, **_kwargs):
    yield None


@pytest.fixture(autouse=True)
def _patch_bridge_idempotency(monkeypatch: pytest.MonkeyPatch) -> None:
    store = InMemoryIdempotencyStore()

    async def _claim(**kwargs):
        return await transfer_idempotency_module.claim_transfer_idempotency(
            store=store, **kwargs
        )

    async def _load(claim, **kwargs):
        return await transfer_idempotency_module.load_transfer_idempotency_claim(
            claim, store=store, **kwargs
        )

    async def _mark_success(claim, *, tx_hash: str, result=None):
        await transfer_idempotency_module.mark_transfer_success(
            claim, tx_hash=tx_hash, result=result, store=store
        )

    async def _mark_failed(claim, *, error: str):
        await transfer_idempotency_module.mark_transfer_failed(
            claim, error=error, store=store
        )

    monkeypatch.setattr("tool_nodes.bridge.bridge_tool.claim_transfer_idempotency", _claim)
    monkeypatch.setattr("tool_nodes.bridge.bridge_tool.load_transfer_idempotency_claim", _load)
    monkeypatch.setattr("tool_nodes.bridge.bridge_tool.mark_transfer_success", _mark_success)
    monkeypatch.setattr("tool_nodes.bridge.bridge_tool.mark_transfer_failed", _mark_failed)


class _DummyChain:
    def __init__(self, chain_id, name):
        self.chain_id = chain_id
        self.name = name
        self.explorer_url = "https://etherscan.io"
        self.is_testnet = False


def _quote(output_amount: Decimal) -> AcrossBridgeQuote:
    return AcrossBridgeQuote(
        protocol="across",
        token_symbol="USDC",
        input_token="0xinput",
        output_token="0xoutput",
        source_chain_id=1,
        dest_chain_id=8453,
        source_chain_name="Ethereum",
        dest_chain_name="Base",
        input_amount=Decimal("100"),
        output_amount=output_amount,
        total_fee=Decimal("1"),
        total_fee_pct=Decimal("0.01"),
        lp_fee=Decimal("0.5"),
        relayer_fee=Decimal("0.4"),
        gas_fee=Decimal("0.1"),
        input_decimals=6,
        output_decimals=6,
        quote_timestamp=1,
        fill_deadline=2,
        exclusivity_deadline=3,
        exclusive_relayer="0xrelayer",
        spoke_pool="0xpool",
        is_native_input=False,
        avg_fill_time_seconds=120,
    )


def _run_async(coro, timeout: float = 5.0):
    """
    Run an async coroutine in a fresh event loop with a hard timeout.

    Avoids pytest/loop shutdown hangs by stepping the loop in small slices.
    """
    try:
        asyncio.get_running_loop()
        has_running_loop = True
    except RuntimeError:
        has_running_loop = False

    if not has_running_loop:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            task = loop.create_task(coro)
            deadline = time.monotonic() + timeout
            while not task.done():
                if time.monotonic() > deadline:
                    raise TimeoutError("Async task did not complete in time.")
                loop.run_until_complete(asyncio.sleep(0.01))
            return task.result()
        finally:
            try:
                loop.stop()
            finally:
                loop.close()
                asyncio.set_event_loop(None)

    result: dict[str, object] = {}
    error: dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:  # noqa: BLE001 - surfacing test failures
            error["value"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TimeoutError("Async task did not complete in time.")
    if "value" in error:
        raise error["value"]
    return result.get("value")


def test_bridge_missing_params_raises():
    with pytest.raises(ValueError):
        _run_async(bridge_token({"token_symbol": "USDC"}))


def test_bridge_selects_best_quote_and_executes(monkeypatch):
    params = {
        "token_symbol": "USDC",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }

    route1 = BridgeRoute(
        protocol="across",
        source_chain_id=1,
        dest_chain_id=8453,
        token_symbol="USDC",
        source_contract="0xsource1",
        dest_contract="0xdest1",
        input_token="0x1111111111111111111111111111111111111111",
        output_token="0x2222222222222222222222222222222222222222",
    )
    route2 = BridgeRoute(
        protocol="across",
        source_chain_id=1,
        dest_chain_id=8453,
        token_symbol="USDC",
        source_contract="0xsource2",
        dest_contract="0xdest2",
        input_token="0x3333333333333333333333333333333333333333",
        output_token="0x4444444444444444444444444444444444444444",
    )
    routes = [("cfg", route1), ("cfg", route2)]

    async def _fake_sim(route_tuple, *_args, **_kwargs):
        if route_tuple[1] is route1:
            return _quote(Decimal("99"))
        return _quote(Decimal("98"))

    with patch("tool_nodes.bridge.bridge_tool.get_chain_by_name", side_effect=[_DummyChain(1, "Ethereum"), _DummyChain(8453, "Base")]):
        with patch("tool_nodes.bridge.bridge_tool.get_routes", return_value=routes):
            with patch("tool_nodes.bridge.bridge_tool.get_dynamic_protocols", return_value=[]):
                with patch(
                    "tool_nodes.bridge.bridge_tool.fetch_across_available_routes_async",
                    new=AsyncMock(return_value=[]),
                ):
                    with patch("tool_nodes.bridge.bridge_tool._simulate_route_async", new=_fake_sim):
                        with patch("tool_nodes.bridge.bridge_tool.get_registry_decimals_by_address", return_value=None):
                            with patch("tool_nodes.bridge.bridge_tool.get_registry_decimals_by_symbol", return_value=None):
                                with patch("tool_nodes.bridge.bridge_tool.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
                                    with patch(
                                        "tool_nodes.bridge.bridge_tool.execute_across_bridge",
                                        return_value=AcrossBridgeResult(
                                            tx_hash="0xhash",
                                            approve_hash=None,
                                            source_chain_id=1,
                                            dest_chain_id=8453,
                                            source_chain_name="Ethereum",
                                            dest_chain_name="Base",
                                            token_symbol="USDC",
                                            input_amount=Decimal("100"),
                                            output_amount=Decimal("99"),
                                            recipient="0xsender",
                                            spoke_pool="0xpool",
                                            fill_deadline=2,
                                            estimated_fill_time="~2 minutes",
                                            status="pending",
                                        ),
                                    ):
                                        with patch("tool_nodes.bridge.bridge_tool.wallet_lock", new=_noop_wallet_lock):
                                            result = _run_async(bridge_token(params))

    assert result["protocol"] == "across"
    assert result["output_amount"] == "99"
    assert result["bridge_status"] == "pending"
    assert "Bridge started" in result["message"]


def test_bridge_errors_when_all_simulations_fail():
    params = {
        "token_symbol": "USDC",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }

    route1 = BridgeRoute(
        protocol="across",
        source_chain_id=1,
        dest_chain_id=8453,
        token_symbol="USDC",
        source_contract="0xsource1",
        dest_contract="0xdest1",
        input_token="0x1111111111111111111111111111111111111111",
        output_token="0x2222222222222222222222222222222222222222",
    )
    routes = [("cfg", route1)]

    async def _fake_sim(_route_tuple, *_args, **_kwargs):
        return AcrossSimulationError(reason="NO_LIQ", message="no liquidity")

    with patch("tool_nodes.bridge.bridge_tool.get_chain_by_name", side_effect=[_DummyChain(1, "Ethereum"), _DummyChain(8453, "Base")]):
        with patch("tool_nodes.bridge.bridge_tool.get_routes", return_value=routes):
            with patch("tool_nodes.bridge.bridge_tool.get_dynamic_protocols", return_value=[]):
                with patch(
                    "tool_nodes.bridge.bridge_tool.fetch_across_available_routes_async",
                    new=AsyncMock(return_value=[]),
                ):
                    with patch("tool_nodes.bridge.bridge_tool._simulate_route_async", new=_fake_sim):
                        with patch("tool_nodes.bridge.bridge_tool.get_registry_decimals_by_address", return_value=None):
                            with patch("tool_nodes.bridge.bridge_tool.get_registry_decimals_by_symbol", return_value=None):
                                with pytest.raises(RuntimeError):
                                    _run_async(bridge_token(params))


def test_bridge_relay_status_in_response(monkeypatch):
    params = {
        "token_symbol": "USDC",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }

    route1 = BridgeRoute(
        protocol="across",
        source_chain_id=1,
        dest_chain_id=8453,
        token_symbol="USDC",
        source_contract="0xsource1",
        dest_contract="0xdest1",
        input_token="0x1111111111111111111111111111111111111111",
        output_token="0x2222222222222222222222222222222222222222",
    )
    routes = [("cfg", route1)]

    async def _fake_sim(_route_tuple, *_args, **_kwargs):
        quote = _quote(Decimal("99"))
        quote.protocol = "relay"
        quote.fees = {"total": "0.1"}
        return quote

    with patch("tool_nodes.bridge.bridge_tool.get_chain_by_name", side_effect=[_DummyChain(1, "Ethereum"), _DummyChain(8453, "Base")]):
        with patch("tool_nodes.bridge.bridge_tool.get_routes", return_value=routes):
            with patch("tool_nodes.bridge.bridge_tool.get_dynamic_protocols", return_value=[]):
                with patch(
                    "tool_nodes.bridge.bridge_tool.fetch_across_available_routes_async",
                    new=AsyncMock(return_value=[]),
                ):
                    with patch("tool_nodes.bridge.bridge_tool._simulate_route_async", new=_fake_sim):
                        with patch("tool_nodes.bridge.bridge_tool.get_registry_decimals_by_address", return_value=None):
                            with patch("tool_nodes.bridge.bridge_tool.get_registry_decimals_by_symbol", return_value=None):
                                with patch(
                                    "tool_nodes.bridge.bridge_tool.execute_relay_bridge",
                                    return_value=RelayBridgeResult(
                                        protocol="relay",
                                        tx_hash="0xhash",
                                        tx_hashes=["0xhash"],
                                        request_id="req-1",
                                        token_symbol="USDC",
                                        input_amount=Decimal("100"),
                                        output_amount=Decimal("99"),
                                        source_chain_name="Ethereum",
                                        dest_chain_name="Base",
                                        recipient="0xsender",
                                        status="pending",
                                        relay_status="pending",
                                    ),
                                ):
                                    with patch("tool_nodes.bridge.bridge_tool.wallet_lock", new=_noop_wallet_lock):
                                        result = _run_async(bridge_token(params))

    assert result["protocol"] == "relay"
    assert result["bridge_status"] == "pending"
    assert result["relay_status"] == "pending"


def test_bridge_rejects_execution_fallback_without_policy():
    params = {
        "token_symbol": "USDC",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }

    route1 = BridgeRoute(
        protocol="across",
        source_chain_id=1,
        dest_chain_id=8453,
        token_symbol="USDC",
        source_contract="0xsource1",
        dest_contract="0xdest1",
        input_token="0x1111111111111111111111111111111111111111",
        output_token="0x2222222222222222222222222222222222222222",
    )
    route2 = BridgeRoute(
        protocol="relay",
        source_chain_id=1,
        dest_chain_id=8453,
        token_symbol="USDC",
        source_contract="0xsource2",
        dest_contract="0xdest2",
        input_token="0x3333333333333333333333333333333333333333",
        output_token="0x4444444444444444444444444444444444444444",
    )
    routes = [("cfg", route1), ("cfg", route2)]

    async def _fake_sim(route_tuple, *_args, **_kwargs):
        if route_tuple[1] is route1:
            return _quote(Decimal("99"))
        quote = _quote(Decimal("98"))
        quote.protocol = "relay"
        quote.fees = {"total": "0.1"}
        return quote

    with patch("tool_nodes.bridge.bridge_tool.get_chain_by_name", side_effect=[_DummyChain(1, "Ethereum"), _DummyChain(8453, "Base")]):
        with patch("tool_nodes.bridge.bridge_tool.get_routes", return_value=routes):
            with patch("tool_nodes.bridge.bridge_tool.get_dynamic_protocols", return_value=[]):
                with patch(
                    "tool_nodes.bridge.bridge_tool.fetch_across_available_routes_async",
                    new=AsyncMock(return_value=[]),
                ):
                    with patch("tool_nodes.bridge.bridge_tool._simulate_route_async", new=_fake_sim):
                        with patch("tool_nodes.bridge.bridge_tool.get_registry_decimals_by_address", return_value=None):
                            with patch("tool_nodes.bridge.bridge_tool.get_registry_decimals_by_symbol", return_value=None):
                                with patch("tool_nodes.bridge.bridge_tool.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
                                    with patch(
                                        "tool_nodes.bridge.bridge_tool.execute_across_bridge",
                                        new=AsyncMock(side_effect=RuntimeError("across failed")),
                                    ):
                                        with patch(
                                            "tool_nodes.bridge.bridge_tool.execute_relay_bridge",
                                            new=AsyncMock(),
                                        ) as exec_relay:
                                            with pytest.raises(DeterminismViolationError):
                                                with patch("tool_nodes.bridge.bridge_tool.wallet_lock", new=_noop_wallet_lock):
                                                    _run_async(bridge_token(params))

    exec_relay.assert_not_called()


def test_bridge_uses_planned_across_quote_without_resimulation():
    params = {
        "token_symbol": "USDC",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": "0xsender",
        "_route_meta": {
            "aggregator": "across",
            "token_symbol": "USDC",
            "source_chain_id": 1,
            "dest_chain_id": 8453,
            "source_chain": "Ethereum",
            "target_chain": "Base",
            "output_amount": "99",
            "total_fee": "1",
            "total_fee_pct": "0.01",
            "fill_time_seconds": 120,
            "tool_data": {
                "planned_quote": {
                    "protocol": "across",
                    "token_symbol": "USDC",
                    "input_token": "0xinput",
                    "output_token": "0xoutput",
                    "source_chain_id": 1,
                    "dest_chain_id": 8453,
                    "source_chain_name": "Ethereum",
                    "dest_chain_name": "Base",
                    "input_amount": "100",
                    "output_amount": "99",
                    "total_fee": "1",
                    "total_fee_pct": "0.01",
                    "lp_fee": "0.5",
                    "relayer_fee": "0.4",
                    "gas_fee": "0.1",
                    "input_decimals": 6,
                    "output_decimals": 6,
                    "quote_timestamp": 1,
                    "fill_deadline": 2,
                    "exclusivity_deadline": 3,
                    "exclusive_relayer": "0xrelayer",
                    "spoke_pool": "0xpool",
                    "is_native_input": False,
                    "avg_fill_time_seconds": 120,
                }
            },
        },
    }

    with patch("tool_nodes.bridge.bridge_tool.get_chain_by_name", side_effect=[_DummyChain(1, "Ethereum"), _DummyChain(8453, "Base")]):
        with patch("tool_nodes.bridge.bridge_tool.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
            with patch(
                "tool_nodes.bridge.bridge_tool.execute_across_bridge",
                new=AsyncMock(
                    return_value=AcrossBridgeResult(
                        tx_hash="0xhash",
                        approve_hash=None,
                        source_chain_id=1,
                        dest_chain_id=8453,
                        source_chain_name="Ethereum",
                        dest_chain_name="Base",
                        token_symbol="USDC",
                        input_amount=Decimal("100"),
                        output_amount=Decimal("99"),
                        recipient="0xsender",
                        spoke_pool="0xpool",
                        fill_deadline=2,
                        estimated_fill_time="~2 minutes",
                        status="pending",
                    )
                ),
            ) as exec_across:
                with patch("tool_nodes.bridge.bridge_tool.wallet_lock", new=_noop_wallet_lock):
                    result = _run_async(bridge_token(params))

    exec_across.assert_called_once()
    assert result["protocol"] == "across"
    assert result["route_meta_used"] is True


def test_bridge_rejects_untrusted_relay_route_meta():
    params = {
        "token_symbol": "USDC",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": "0xsender",
        "_route_meta": {
            "aggregator": "relay",
            "token_symbol": "USDC",
            "source_chain_id": 1,
            "dest_chain_id": 8453,
            "tool_data": {
                "planned_quote": {
                    "protocol": "relay",
                    "token_symbol": "USDC",
                    "source_chain_id": 1,
                    "dest_chain_id": 8453,
                    "input_amount": "100",
                    "output_amount": "99",
                    "total_fee": "1",
                    "total_fee_pct": "0.01",
                    "steps": [{"items": [{"data": {"to": "0xrouter", "data": "0xdead"}}]}],
                }
            },
        },
    }

    with patch("tool_nodes.bridge.bridge_tool.get_chain_by_name", side_effect=[_DummyChain(1, "Ethereum"), _DummyChain(8453, "Base")]):
        with pytest.raises(
            NonRetryableError,
            match="Untrusted precomputed transaction data is not allowed",
        ):
            _run_async(bridge_token(params))


def test_bridge_rejects_shadow_planned_quote_mismatch():
    params = {
        "token_symbol": "USDC",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": "0xsender",
        "_route_meta": {
            "aggregator": "across",
            "token_symbol": "USDC",
            "source_chain_id": 1,
            "dest_chain_id": 8453,
            "input_amount": "100",
            "recipient": "0xsender",
            "tool_data": {
                "planned_quote": {
                    "protocol": "across",
                    "token_symbol": "USDC",
                    "input_token": "0xinput",
                    "output_token": "0xoutput",
                    "source_chain_id": 1,
                    "dest_chain_id": 8453,
                    "source_chain_name": "Ethereum",
                    "dest_chain_name": "Base",
                    "input_amount": "1000000",
                    "output_amount": "99",
                    "total_fee": "1",
                    "total_fee_pct": "0.01",
                    "lp_fee": "0.5",
                    "relayer_fee": "0.4",
                    "gas_fee": "0.1",
                    "input_decimals": 6,
                    "output_decimals": 6,
                    "quote_timestamp": 1,
                    "fill_deadline": 2,
                    "exclusivity_deadline": 3,
                    "exclusive_relayer": "0xrelayer",
                    "spoke_pool": "0xpool",
                    "is_native_input": False,
                    "avg_fill_time_seconds": 120,
                }
            },
        },
    }

    with patch(
        "tool_nodes.bridge.bridge_tool.get_chain_by_name",
        side_effect=[_DummyChain(1, "Ethereum"), _DummyChain(8453, "Base")],
    ):
        with patch("tool_nodes.bridge.bridge_tool.execute_across_bridge", new=AsyncMock()) as exec_across:
            with pytest.raises(
                NonRetryableError,
                match="planned bridge metadata does not match the requested amount",
            ):
                _run_async(bridge_token(params))

    exec_across.assert_not_called()


def test_bridge_rejects_unverifiable_mayan_route_meta():
    params = {
        "token_symbol": "USDC",
        "source_chain": "solana",
        "target_chain": "ethereum",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": "sender1111111111111111111111111111111111111",
        "recipient": "0x00000000000000000000000000000000000000aa",
        "_route_meta": {
            "aggregator": "mayan",
            "token_symbol": "USDC",
            "source_chain_id": 101,
            "dest_chain_id": 1,
            "input_amount": "100",
            "output_amount": "99",
            "tool_data": {
                "quoteHash": "attacker-quote-hash",
                "routeType": "SWIFT",
                "fromToken": "AttackerMint111111111111111111111111111111111",
                "toToken": "0xattacker",
                "fromChain": "solana",
                "toChain": "ethereum",
            },
        },
    }

    def _get_chain(name: str):
        if str(name).lower() == "ethereum":
            return _DummyChain(1, "Ethereum")
        raise KeyError(name)

    with patch(
        "tool_nodes.bridge.bridge_tool.get_chain_by_name",
        side_effect=_get_chain,
    ):
        with patch(
            "tool_nodes.bridge.bridge_tool.get_solana_chain",
            return_value=type(
                "_SolanaChain",
                (),
                {"chain_id": 101, "name": "Solana", "network": "solana"},
            )(),
        ):
            with pytest.raises(
                NonRetryableError,
                match="planned Mayan route metadata cannot be executed safely",
            ):
                _run_async(bridge_token(params))


def test_bridge_identical_requests_without_external_idempotency_key_deduplicate():
    params = {
        "token_symbol": "USDC",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }
    route = BridgeRoute(
        protocol="across",
        source_chain_id=1,
        dest_chain_id=8453,
        token_symbol="USDC",
        source_contract="0xsource1",
        dest_contract="0xdest1",
        input_token="0x1111111111111111111111111111111111111111",
        output_token="0x2222222222222222222222222222222222222222",
    )
    routes = [("cfg", route)]
    execute_mock = AsyncMock(
        side_effect=[
            AcrossBridgeResult(
                tx_hash="0xone",
                approve_hash=None,
                source_chain_id=1,
                dest_chain_id=8453,
                source_chain_name="Ethereum",
                dest_chain_name="Base",
                token_symbol="USDC",
                input_amount=Decimal("100"),
                output_amount=Decimal("99"),
                recipient="0xsender",
                spoke_pool="0xpool",
                fill_deadline=2,
                estimated_fill_time="~2 minutes",
                status="pending",
            ),
            AcrossBridgeResult(
                tx_hash="0xtwo",
                approve_hash=None,
                source_chain_id=1,
                dest_chain_id=8453,
                source_chain_name="Ethereum",
                dest_chain_name="Base",
                token_symbol="USDC",
                input_amount=Decimal("100"),
                output_amount=Decimal("99"),
                recipient="0xsender",
                spoke_pool="0xpool",
                fill_deadline=2,
                estimated_fill_time="~2 minutes",
                status="pending",
            ),
        ]
    )

    with patch("tool_nodes.bridge.bridge_tool.get_chain_by_name", side_effect=[_DummyChain(1, "Ethereum"), _DummyChain(8453, "Base"), _DummyChain(1, "Ethereum"), _DummyChain(8453, "Base")]):
        with patch("tool_nodes.bridge.bridge_tool.get_routes", return_value=routes):
            with patch("tool_nodes.bridge.bridge_tool.get_dynamic_protocols", return_value=[]):
                with patch(
                    "tool_nodes.bridge.bridge_tool.fetch_across_available_routes_async",
                    new=AsyncMock(return_value=[]),
                ):
                    with patch("tool_nodes.bridge.bridge_tool._simulate_route_async", new=AsyncMock(return_value=_quote(Decimal("99")))):
                        with patch("tool_nodes.bridge.bridge_tool.get_registry_decimals_by_address", return_value=None):
                            with patch("tool_nodes.bridge.bridge_tool.get_registry_decimals_by_symbol", return_value=None):
                                with patch("tool_nodes.bridge.bridge_tool.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
                                    with patch("tool_nodes.bridge.bridge_tool.execute_across_bridge", new=execute_mock):
                                        with patch("tool_nodes.bridge.bridge_tool.wallet_lock", new=_noop_wallet_lock):
                                            first = _run_async(bridge_token(dict(params)))
                                            second = _run_async(bridge_token(dict(params)))

    assert first["tx_hash"] == "0xone"
    assert second["tx_hash"] == "0xone"
    assert execute_mock.await_count == 1


def test_bridge_normalizes_equivalent_decimal_and_address_inputs_for_deduplication():
    params = {
        "token_symbol": "usdc",
        "source_chain": "ethereum",
        "target_chain": "base",
        "amount": "100.0",
        "sub_org_id": "sub",
        "sender": "0xSender",
        "recipient": "0xRecipient",
    }
    route = BridgeRoute(
        protocol="across",
        source_chain_id=1,
        dest_chain_id=8453,
        token_symbol="USDC",
        source_contract="0xsource1",
        dest_contract="0xdest1",
        input_token="0x1111111111111111111111111111111111111111",
        output_token="0x2222222222222222222222222222222222222222",
    )
    routes = [("cfg", route)]
    execute_mock = AsyncMock(
        return_value=AcrossBridgeResult(
            tx_hash="0xone",
            approve_hash=None,
            source_chain_id=1,
            dest_chain_id=8453,
            source_chain_name="Ethereum",
            dest_chain_name="Base",
            token_symbol="USDC",
            input_amount=Decimal("100"),
            output_amount=Decimal("99"),
            recipient="0xrecipient",
            spoke_pool="0xpool",
            fill_deadline=2,
            estimated_fill_time="~2 minutes",
            status="pending",
        )
    )

    with patch(
        "tool_nodes.bridge.bridge_tool.get_chain_by_name",
        side_effect=[
            _DummyChain(1, "Ethereum"),
            _DummyChain(8453, "Base"),
            _DummyChain(1, "Ethereum"),
            _DummyChain(8453, "Base"),
        ],
    ):
        with patch("tool_nodes.bridge.bridge_tool.get_routes", return_value=routes):
            with patch("tool_nodes.bridge.bridge_tool.get_dynamic_protocols", return_value=[]):
                with patch(
                    "tool_nodes.bridge.bridge_tool.fetch_across_available_routes_async",
                    new=AsyncMock(return_value=[]),
                ):
                    with patch(
                        "tool_nodes.bridge.bridge_tool._simulate_route_async",
                        new=AsyncMock(return_value=_quote(Decimal("99"))),
                    ):
                        with patch(
                            "tool_nodes.bridge.bridge_tool.get_registry_decimals_by_address",
                            return_value=None,
                        ):
                            with patch(
                                "tool_nodes.bridge.bridge_tool.get_registry_decimals_by_symbol",
                                return_value=None,
                            ):
                                with patch(
                                    "tool_nodes.bridge.bridge_tool.gas_price_cache.get_gwei",
                                    new=AsyncMock(return_value=5),
                                ):
                                    with patch(
                                        "tool_nodes.bridge.bridge_tool.execute_across_bridge",
                                        new=execute_mock,
                                    ):
                                        with patch(
                                            "tool_nodes.bridge.bridge_tool.wallet_lock",
                                            new=_noop_wallet_lock,
                                        ):
                                            first = _run_async(bridge_token(dict(params)))
                                            second = _run_async(
                                                bridge_token(
                                                    {
                                                        **params,
                                                        "amount": "100.00",
                                                        "sender": "0xsender",
                                                        "recipient": "0xrecipient",
                                                    }
                                                )
                                            )

    assert first["tx_hash"] == "0xone"
    assert second["tx_hash"] == "0xone"
    assert execute_mock.await_count == 1
