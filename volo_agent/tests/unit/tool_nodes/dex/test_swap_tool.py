import asyncio
import os
from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.utils.errors import NonRetryableError
from core.utils.errors import DeterminismViolationError
from core.utils.timeouts import resolve_tool_timeout
from tests.unit.wallet_service._wallet_security_helpers import InMemoryIdempotencyStore
from tool_nodes.dex import swap as swap_module
from tool_nodes.dex.swap import swap_token
from tool_nodes.dex.swap_simulator_v3 import SimulationError
from wallet_service.common import transfer_idempotency as transfer_idempotency_module


class _DummyChain:
    def __init__(self, v3=True, v2=True):
        self.chain_id = 1
        self.name = "Ethereum"
        self.v3_quoter = "0xquoter" if v3 else None
        self.v3_router = "0xrouter" if v3 else None
        self.v2_router = "0xv2router" if v2 else None
        self.v2_factory = "0xv2factory" if v2 else None
        self.explorer_url = "https://etherscan.io"


@dataclass
class _DummyQuote:
    route: str = "single-hop"
    path: list[str] = None
    amount_out: Decimal = Decimal("1")
    amount_out_minimum: Decimal = Decimal("0.9")
    amount_in: Decimal = Decimal("1")
    chain_name: str = "Ethereum"
    protocol: str = "v3"


@dataclass
class _DummyResult:
    protocol: str
    tx_hash: str
    approve_hash: str | None
    amount_in: Decimal
    amount_out_minimum: Decimal
    chain_name: str


@pytest.fixture(autouse=True)
def _patch_idempotency(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(swap_module, "claim_transfer_idempotency", AsyncMock(return_value=None))
    monkeypatch.setattr(
        swap_module,
        "load_transfer_idempotency_claim",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(swap_module, "mark_transfer_inflight", AsyncMock(return_value=None))
    monkeypatch.setattr(swap_module, "mark_transfer_success", AsyncMock(return_value=None))
    monkeypatch.setattr(swap_module, "mark_transfer_failed", AsyncMock(return_value=None))


def _patch_real_idempotency(
    monkeypatch: pytest.MonkeyPatch,
    store: InMemoryIdempotencyStore,
) -> None:
    async def _claim(**kwargs):
        return await transfer_idempotency_module.claim_transfer_idempotency(
            store=store, **kwargs
        )

    async def _load(claim, **kwargs):
        return await transfer_idempotency_module.load_transfer_idempotency_claim(
            claim, store=store, **kwargs
        )

    async def _mark_inflight(claim, *, tx_hash: str, result=None):
        await transfer_idempotency_module.mark_transfer_inflight(
            claim, tx_hash=tx_hash, result=result, store=store
        )

    async def _mark_success(claim, *, tx_hash: str, result=None):
        await transfer_idempotency_module.mark_transfer_success(
            claim, tx_hash=tx_hash, result=result, store=store
        )

    async def _mark_failed(claim, *, error: str):
        await transfer_idempotency_module.mark_transfer_failed(
            claim, error=error, store=store
        )

    monkeypatch.setattr(swap_module, "claim_transfer_idempotency", _claim)
    monkeypatch.setattr(swap_module, "load_transfer_idempotency_claim", _load)
    monkeypatch.setattr(swap_module, "mark_transfer_inflight", _mark_inflight)
    monkeypatch.setattr(swap_module, "mark_transfer_success", _mark_success)
    monkeypatch.setattr(swap_module, "mark_transfer_failed", _mark_failed)


def test_swap_missing_params_raises():
    with pytest.raises(ValueError):
        asyncio.run(swap_token({"chain": "ethereum"}))


def test_swap_prefers_v3_when_available():
    params = {
        "token_in_address": "0xaaa",
        "token_out_address": "0xbbb",
        "amount_in": 1,
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": "0xsender",
        "token_in_symbol": "ETH",
        "token_out_symbol": "USDC",
    }

    with patch("tool_nodes.dex.swap.get_chain_by_name", return_value=_DummyChain(v3=True, v2=True)):
        with patch("tool_nodes.dex.swap.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
            with patch(
                "tool_nodes.dex.swap.simulate_swap",
                new=AsyncMock(
                    return_value=_DummyQuote(route="single-hop", path=["0xaaa", "0xbbb"])
                ),
            ) as sim_v3:
                with patch(
                    "tool_nodes.dex.swap.execute_swap",
                    new=AsyncMock(
                        return_value=_DummyResult(
                            protocol="v3",
                            tx_hash="0xhash",
                            approve_hash=None,
                            amount_in=Decimal("1"),
                            amount_out_minimum=Decimal("0.9"),
                            chain_name="Ethereum",
                        )
                    ),
                ):
                    result = asyncio.run(swap_token(params))

    sim_v3.assert_called_once()
    assert result["protocol"] == "v3"
    assert result["route"] == "single-hop"
    assert result["path"] == ["0xaaa", "0xbbb"]


def test_swap_rejects_v2_fallback_without_policy():
    params = {
        "token_in_address": "0xaaa",
        "token_out_address": "0xbbb",
        "amount_in": 1,
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }

    with patch("tool_nodes.dex.swap.get_chain_by_name", return_value=_DummyChain(v3=True, v2=True)):
        with patch("tool_nodes.dex.swap.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
            with patch(
                "tool_nodes.dex.swap.simulate_swap",
                new=AsyncMock(return_value=SimulationError(reason="NO_LIQ", message="no")),
            ):
                with patch(
                    "tool_nodes.dex.swap.simulate_swap_v2",
                    new=AsyncMock(),
                ) as sim_v2:
                    with pytest.raises(DeterminismViolationError):
                        asyncio.run(swap_token(params))

    sim_v2.assert_not_called()


def test_swap_rejects_untrusted_precomputed_route_meta():
    params = {
        "token_in_address": "0xaaa",
        "token_out_address": "0xbbb",
        "amount_in": 1,
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": "0xsender",
        "_route_meta": {
            "aggregator": "0x",
            "chain_id": 1,
            "token_in": "0xaaa",
            "token_out": "0xbbb",
            "amount_out_min": "0.9",
            "calldata": "0xfeed",
            "to": "0xrouter",
        },
    }

    with patch("tool_nodes.dex.swap.get_chain_by_name", return_value=_DummyChain(v3=True, v2=True)):
        with patch("tool_nodes.dex.swap.simulate_swap", new=AsyncMock()) as sim_v3:
            with patch("tool_nodes.dex.swap.simulate_swap_v2", new=AsyncMock()) as sim_v2:
                with pytest.raises(
                    NonRetryableError,
                    match="Untrusted precomputed transaction data is not allowed",
                ):
                    asyncio.run(swap_token(params))

    sim_v3.assert_not_called()
    sim_v2.assert_not_called()


def test_swap_metadata_only_route_meta_still_resimulates():
    params = {
        "token_in_address": "0xaaa",
        "token_out_address": "0xbbb",
        "amount_in": 1,
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": "0xsender",
        "_route_meta": {
            "aggregator": "uniswap_v3",
            "chain_id": 1,
            "token_in": "0xaaa",
            "token_out": "0xbbb",
            "amount_in": "1",
        },
    }

    with patch("tool_nodes.dex.swap.get_chain_by_name", return_value=_DummyChain(v3=True, v2=True)):
        with patch("tool_nodes.dex.swap.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
            with patch(
                "tool_nodes.dex.swap.simulate_swap",
                new=AsyncMock(
                    return_value=_DummyQuote(route="single-hop", path=["0xaaa", "0xbbb"])
                ),
            ) as sim_v3:
                with patch("tool_nodes.dex.swap.simulate_swap_v2", new=AsyncMock()) as sim_v2:
                    with patch(
                        "tool_nodes.dex.swap.execute_swap",
                        new=AsyncMock(
                            return_value=_DummyResult(
                                protocol="v3",
                                tx_hash="0xhash",
                                approve_hash=None,
                                amount_in=Decimal("1"),
                                amount_out_minimum=Decimal("0.9"),
                                chain_name="Ethereum",
                            )
                        ),
                    ) as exec_swap:
                        result = asyncio.run(swap_token(params))

    sim_v3.assert_called_once()
    sim_v2.assert_not_called()
    exec_swap.assert_called_once()
    assert result["protocol"] == "v3"
    assert result["route_meta_used"] is True


def test_swap_passes_exact_decimal_amount_to_simulators():
    amount_in = "123456789.123456789123456789"
    params = {
        "token_in_address": "0xaaa",
        "token_out_address": "0xbbb",
        "amount_in": amount_in,
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }

    async def _simulate_swap(**kwargs):
        assert kwargs["amount_in"] == Decimal(amount_in)
        return _DummyQuote(route="single-hop", path=["0xaaa", "0xbbb"])

    with patch("tool_nodes.dex.swap.get_chain_by_name", return_value=_DummyChain(v3=True, v2=False)):
        with patch("tool_nodes.dex.swap.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
            with patch("tool_nodes.dex.swap.simulate_swap", new=_simulate_swap):
                with patch(
                    "tool_nodes.dex.swap.execute_swap",
                    new=AsyncMock(
                        return_value=_DummyResult(
                            protocol="v3",
                            tx_hash="0xhash",
                            approve_hash=None,
                            amount_in=Decimal(amount_in),
                            amount_out_minimum=Decimal("0.9"),
                            chain_name="Ethereum",
                        )
                    ),
                ):
                    result = asyncio.run(swap_token(params))

    assert result["amount_in"] == amount_in


def test_swap_timeout_covers_receipt_wait_window():
    with patch.dict(os.environ, {}, clear=True):
        assert resolve_tool_timeout("swap", None) == 300.0


def test_swap_identical_requests_without_external_idempotency_key_deduplicate(
    monkeypatch: pytest.MonkeyPatch,
):
    store = InMemoryIdempotencyStore()
    _patch_real_idempotency(monkeypatch, store)
    params = {
        "token_in_address": "0xaaa",
        "token_out_address": "0xbbb",
        "amount_in": "1",
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }
    execute_mock = AsyncMock(
        side_effect=[
            _DummyResult(
                protocol="v3",
                tx_hash="0xhash-1",
                approve_hash=None,
                amount_in=Decimal("1"),
                amount_out_minimum=Decimal("0.9"),
                chain_name="Ethereum",
            ),
            _DummyResult(
                protocol="v3",
                tx_hash="0xhash-2",
                approve_hash=None,
                amount_in=Decimal("1"),
                amount_out_minimum=Decimal("0.9"),
                chain_name="Ethereum",
            ),
        ]
    )

    with patch("tool_nodes.dex.swap.get_chain_by_name", return_value=_DummyChain(v3=True, v2=False)):
        with patch("tool_nodes.dex.swap.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
            with patch(
                "tool_nodes.dex.swap.simulate_swap",
                new=AsyncMock(
                    return_value=_DummyQuote(route="single-hop", path=["0xaaa", "0xbbb"])
                ),
            ):
                with patch("tool_nodes.dex.swap.execute_swap", new=execute_mock):
                    first = asyncio.run(swap_token(dict(params)))
                    second = asyncio.run(swap_token(dict(params)))

    assert first["tx_hash"] == "0xhash-1"
    assert second["tx_hash"] == "0xhash-1"
    assert execute_mock.await_count == 1


def test_swap_normalizes_equivalent_amount_and_address_inputs_for_deduplication(
    monkeypatch: pytest.MonkeyPatch,
):
    store = InMemoryIdempotencyStore()
    _patch_real_idempotency(monkeypatch, store)
    execute_mock = AsyncMock(
        return_value=_DummyResult(
            protocol="v3",
            tx_hash="0xhash-1",
            approve_hash=None,
            amount_in=Decimal("1"),
            amount_out_minimum=Decimal("0.9"),
            chain_name="Ethereum",
        )
    )

    with patch("tool_nodes.dex.swap.get_chain_by_name", return_value=_DummyChain(v3=True, v2=False)):
        with patch("tool_nodes.dex.swap.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
            with patch(
                "tool_nodes.dex.swap.simulate_swap",
                new=AsyncMock(
                    return_value=_DummyQuote(route="single-hop", path=["0xaaa", "0xbbb"])
                ),
            ):
                with patch("tool_nodes.dex.swap.execute_swap", new=execute_mock):
                    first = asyncio.run(
                        swap_token(
                            {
                                "token_in_address": "0xAAA",
                                "token_out_address": "0xBBB",
                                "amount_in": "1.0",
                                "chain": "ethereum",
                                "sub_org_id": "sub",
                                "sender": "0xSender",
                            }
                        )
                    )
                    second = asyncio.run(
                        swap_token(
                            {
                                "token_in_address": "0xaaa",
                                "token_out_address": "0xbbb",
                                "amount_in": "1.00",
                                "chain": "ethereum",
                                "sub_org_id": "sub",
                                "sender": "0xsender",
                            }
                        )
                    )

    assert first["tx_hash"] == "0xhash-1"
    assert second["tx_hash"] == "0xhash-1"
    assert execute_mock.await_count == 1


def test_swap_resume_injects_legacy_claim_tx_hash_into_old_execution_state(
    monkeypatch: pytest.MonkeyPatch,
):
    store = InMemoryIdempotencyStore()
    _patch_real_idempotency(monkeypatch, store)
    params = {
        "token_in_address": "0xaaa",
        "token_out_address": "0xbbb",
        "amount_in": "1",
        "chain": "ethereum",
        "sub_org_id": "sub",
        "sender": "0xsender",
    }
    quote = _DummyQuote(route="single-hop", path=["0xaaa", "0xbbb"])

    async def _crash_mid_route(**kwargs):
        await kwargs["persist_execution_state"](
            {
                "current_step": "swap",
                "completion_status": "pending",
                "steps": {
                    "swap": {
                        "status": "completed",
                        "tx_hash": "0xdeadbeef",
                    }
                },
                "metadata": {},
            }
        )
        raise RuntimeError("simulated crash after persisted legacy swap state")

    async def _resume_with_legacy_metadata(**kwargs):
        execution_state = kwargs["execution_state"]
        assert execution_state["metadata"]["legacy_claim_tx_hash"] == "0xdeadbeef"
        return _DummyResult(
            protocol="v3",
            tx_hash="0xdeadbeef",
            approve_hash=None,
            amount_in=Decimal("1"),
            amount_out_minimum=Decimal("0.9"),
            chain_name="Ethereum",
        )

    with patch("tool_nodes.dex.swap.get_chain_by_name", return_value=_DummyChain(v3=True, v2=False)):
        with patch("tool_nodes.dex.swap.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
            with patch("tool_nodes.dex.swap.simulate_swap", new=AsyncMock(return_value=quote)):
                with patch("tool_nodes.dex.swap.execute_swap", new=AsyncMock(side_effect=_crash_mid_route)):
                    with pytest.raises(RuntimeError, match="simulated crash"):
                        asyncio.run(swap_token(dict(params)))

    with patch("tool_nodes.dex.swap.get_chain_by_name", return_value=_DummyChain(v3=True, v2=False)):
        with patch("tool_nodes.dex.swap.gas_price_cache.get_gwei", new=AsyncMock(return_value=5)):
            with patch("tool_nodes.dex.swap.simulate_swap", new=AsyncMock(return_value=quote)):
                with patch(
                    "tool_nodes.dex.swap.execute_swap",
                    new=AsyncMock(side_effect=_resume_with_legacy_metadata),
                ) as execute_mock:
                    result = asyncio.run(swap_token(dict(params)))

    assert result["tx_hash"] == "0xdeadbeef"
    assert execute_mock.await_count == 1
