import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from core.utils.evm_async import async_await_evm_receipt
from core.utils.errors import NonRetryableError
from tool_nodes.dex.swap_executor import _maybe_approve, execute_swap
from tool_nodes.dex.swap_simulator_v2 import SwapQuoteV2
from web3.exceptions import TimeExhausted


class _DummyW3:
    class Eth:
        async def wait_for_transaction_receipt(self, *_args, **_kwargs):
            return None

    eth = Eth()


class _Quote:
    def __init__(self, needs_approval: bool, token_in: str):
        self.needs_approval = needs_approval
        self.token_in = token_in
        self.chain_id = 1
        self.decimals_in = 18
        self.amount_in = Decimal("1")


def test_maybe_approve_skips_when_not_needed():
    quote = _Quote(needs_approval=False, token_in="0xabc")
    w3 = _DummyW3()

    with patch(
        "tool_nodes.dex.swap_executor._build_approve_tx", new=AsyncMock()
    ) as build_tx:
        with patch(
            "tool_nodes.dex.swap_executor.async_get_allowance",
            new=AsyncMock(return_value=0),
        ):
            nonce_manager = MagicMock()
            approve_hash = asyncio.run(
                _maybe_approve(
                    w3=w3,
                    quote=quote,
                    router_address="0xrouter",
                    sub_org_id="sub",
                    sender="0xsender",
                    gas_price=1,
                    chain=MagicMock(wrapped_native="0xwrap", chain_id=1),
                    nonce_manager=nonce_manager,
                )
            )

    assert approve_hash is None
    nonce_manager.allocate_safe.assert_not_called()
    build_tx.assert_not_called()


def test_maybe_approve_executes_when_needed():
    quote = _Quote(needs_approval=True, token_in="0xabc")
    w3 = _DummyW3()

    with patch(
        "tool_nodes.dex.swap_executor._build_approve_tx",
        new=AsyncMock(return_value={"tx": "data"}),
    ) as build_tx:
        with patch(
            "tool_nodes.dex.swap_executor.sign_transaction_async",
            new=AsyncMock(return_value="0xsigned"),
        ):
            with patch(
                "tool_nodes.dex.swap_executor.async_broadcast_evm",
                new=AsyncMock(return_value="0xhash"),
            ):
                with patch(
                    "tool_nodes.dex.swap_executor.async_get_allowance",
                    new=AsyncMock(return_value=0),
                ):
                    nonce_manager = MagicMock()
                    nonce_manager.allocate_safe = AsyncMock(return_value=1)
                    approve_hash = asyncio.run(
                        _maybe_approve(
                            w3=w3,
                            quote=quote,
                            router_address="0xrouter",
                            sub_org_id="sub",
                            sender="0xsender",
                            gas_price=1,
                            chain=MagicMock(wrapped_native="0xwrap", chain_id=1),
                            nonce_manager=nonce_manager,
                        )
                    )

    build_tx.assert_called_once()
    assert approve_hash == "0xhash"
    nonce_manager.allocate_safe.assert_called_once()


def test_await_receipt_raises_on_revert():
    class _Eth:
        async def wait_for_transaction_receipt(self, *_args, **_kwargs):
            return type("Receipt", (), {"status": 0})()

    w3 = type("W3", (), {"eth": _Eth()})()

    try:
        asyncio.run(async_await_evm_receipt(w3, "0xhash"))
        assert False, "Expected NonRetryableError"
    except NonRetryableError as exc:
        assert "rejected" in str(exc).lower()


def test_await_receipt_raises_on_timeout():
    class _Eth:
        async def wait_for_transaction_receipt(self, *_args, **_kwargs):
            raise TimeExhausted

    w3 = type("W3", (), {"eth": _Eth()})()

    try:
        asyncio.run(async_await_evm_receipt(w3, "0xhash", timeout=1))
        assert False, "Expected NonRetryableError"
    except NonRetryableError as exc:
        assert "pending" in str(exc).lower()


def test_execute_swap_resumes_after_mid_route_crash_without_repeating_completed_steps():
    quote = SwapQuoteV2(
        token_in="0x0000000000000000000000000000000000000000",
        token_out="0x0000000000000000000000000000000000000000",
        amount_in=Decimal("1"),
        amount_out=Decimal("1"),
        amount_out_minimum=Decimal("0.9"),
        decimals_in=18,
        decimals_out=18,
        slippage_pct=Decimal("0.5"),
        price_impact_pct=Decimal("0.1"),
        path=["0xwrap", "0xwrap"],
        gas_estimate=150_000,
        needs_approval=True,
        allowance=0,
        chain_id=1,
        chain_name="Ethereum",
        dex_name="Uniswap V2",
    )
    chain = MagicMock(
        chain_id=1,
        name="Ethereum",
        rpc_url="https://rpc.test",
        v2_router="0xrouter",
        v3_router=None,
        wrapped_native="0xwrap",
        supports_native_swaps=False,
    )
    nonce_manager = MagicMock()
    nonce_manager.pending = AsyncMock(return_value=7)
    nonce_manager.allocate_safe = AsyncMock(side_effect=[7, 8, 9, 10])
    execution_states: list[dict] = []
    wrap_calls = 0
    approve_calls = 0
    swap_build_calls = 0

    class _BalanceCall:
        def __init__(self, values: list[int]) -> None:
            self._values = values

        async def call(self) -> int:
            if len(self._values) > 1:
                return self._values.pop(0)
            return self._values[0]

    class _WrappedContract:
        def __init__(self) -> None:
            self.functions = self
            self._values = [0, 0, 10]

        def balanceOf(self, _sender: str):
            return _BalanceCall(self._values)

    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda value: value.lower()
    wrapped_contract = _WrappedContract()
    w3.eth.contract.return_value = wrapped_contract

    async def _persist_state(state: dict) -> None:
        execution_states.append(state)

    async def _build_wrap(*_args, **_kwargs):
        nonlocal wrap_calls
        wrap_calls += 1
        return {"kind": "wrap"}

    async def _approve(*_args, persist_step_submission=None, **_kwargs):
        nonlocal approve_calls
        approve_calls += 1
        assert persist_step_submission is not None
        await persist_step_submission("0xapprove")
        return "0xapprove"

    async def _build_swap(*_args, **_kwargs):
        nonlocal swap_build_calls
        swap_build_calls += 1
        if swap_build_calls == 1:
            raise RuntimeError("crash after wrap+approve")
        return {"kind": "swap"}

    async def _build_unwrap(*_args, **_kwargs):
        return {"kind": "unwrap"}

    async def _sign(_sub_org_id, tx, _sender):
        return tx["kind"]

    async def _broadcast(_w3, signed_tx):
        return {
            "wrap": "0xwrap",
            "swap": "0xswap",
            "unwrap": "0xunwrap",
        }[signed_tx]

    with patch("tool_nodes.dex.swap_executor.get_chain_by_id", return_value=chain):
        with patch("tool_nodes.dex.swap_executor.make_async_web3", return_value=w3):
            with patch("tool_nodes.dex.swap_executor.get_router_capabilities", return_value=MagicMock(supports_native_swaps=False, last_checked="2026-01-01T00:00:00")):
                with patch("tool_nodes.dex.swap_executor.get_async_nonce_manager", new=AsyncMock(return_value=nonce_manager)):
                    with patch("tool_nodes.dex.swap_executor._build_wrap_tx", new=_build_wrap):
                        with patch("tool_nodes.dex.swap_executor._maybe_approve", new=_approve):
                            with patch("tool_nodes.dex.swap_executor._build_v2_swap_tx", new=_build_swap):
                                with patch("tool_nodes.dex.swap_executor._build_unwrap_tx", new=_build_unwrap):
                                    with patch("tool_nodes.dex.swap_executor.sign_transaction_async", new=_sign):
                                        with patch("tool_nodes.dex.swap_executor.async_broadcast_evm", new=_broadcast):
                                            with patch("tool_nodes.dex.swap_executor.async_await_evm_receipt", new=AsyncMock(return_value=None)):
                                                with patch(
                                                    "tool_nodes.dex.swap_executor.wallet_lock",
                                                    return_value=MagicMock(
                                                        __aenter__=AsyncMock(
                                                            return_value=MagicMock(
                                                                ensure_held=AsyncMock(return_value=None)
                                                            )
                                                        ),
                                                        __aexit__=AsyncMock(return_value=None),
                                                    ),
                                                ):
                                                    try:
                                                        asyncio.run(
                                                            execute_swap(
                                                                quote=quote,
                                                                sub_org_id="sub",
                                                                sender="0xsender",
                                                                execution_state=None,
                                                                persist_execution_state=_persist_state,
                                                            )
                                                        )
                                                        assert False, "expected crash"
                                                    except RuntimeError as exc:
                                                        assert "crash after wrap+approve" in str(exc)

                                                    resumed = asyncio.run(
                                                        execute_swap(
                                                            quote=quote,
                                                            sub_org_id="sub",
                                                            sender="0xsender",
                                                            execution_state=execution_states[-1],
                                                            persist_execution_state=_persist_state,
                                                        )
                                                    )

    assert wrap_calls == 1
    assert approve_calls == 1
    assert resumed.tx_hash == "0xswap"
    assert resumed.unwrap_hash == "0xunwrap"


def test_execute_swap_rejects_tampered_completed_swap_state():
    quote = SwapQuoteV2(
        token_in="0x1111111111111111111111111111111111111111",
        token_out="0x2222222222222222222222222222222222222222",
        amount_in=Decimal("1"),
        amount_out=Decimal("1"),
        amount_out_minimum=Decimal("0.9"),
        decimals_in=18,
        decimals_out=18,
        slippage_pct=Decimal("0.5"),
        price_impact_pct=Decimal("0.1"),
        path=["0x1111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222"],
        gas_estimate=150_000,
        needs_approval=False,
        allowance=0,
        chain_id=1,
        chain_name="Ethereum",
        dex_name="Uniswap V2",
    )
    chain = MagicMock(
        chain_id=1,
        name="Ethereum",
        rpc_url="https://rpc.test",
        v2_router="0xrouter",
        v3_router=None,
        wrapped_native="0xwrap",
        supports_native_swaps=True,
    )
    nonce_manager = MagicMock()
    nonce_manager.pending = AsyncMock(return_value=7)
    nonce_manager.allocate_safe = AsyncMock(return_value=7)
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda value: value.lower()
    w3.eth.contract.return_value = MagicMock()

    tampered_state = {
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

    with patch("tool_nodes.dex.swap_executor.get_chain_by_id", return_value=chain):
        with patch("tool_nodes.dex.swap_executor.make_async_web3", return_value=w3):
            with patch(
                "tool_nodes.dex.swap_executor.get_router_capabilities",
                return_value=MagicMock(
                    supports_native_swaps=True,
                    last_checked="2026-01-01T00:00:00",
                ),
            ):
                with patch(
                    "tool_nodes.dex.swap_executor.get_async_nonce_manager",
                    new=AsyncMock(return_value=nonce_manager),
                ):
                    with patch(
                        "tool_nodes.dex.swap_executor._build_v2_swap_tx",
                        new=AsyncMock(return_value={"kind": "swap"}),
                    ) as build_swap:
                        with patch(
                            "tool_nodes.dex.swap_executor.sign_transaction_async",
                            new=AsyncMock(return_value="signed"),
                        ) as sign_tx:
                            with patch(
                                "tool_nodes.dex.swap_executor.async_broadcast_evm",
                                new=AsyncMock(return_value="0xhash"),
                            ) as broadcast:
                                with patch(
                                    "tool_nodes.dex.swap_executor.wallet_lock",
                                    return_value=MagicMock(
                                        __aenter__=AsyncMock(
                                            return_value=MagicMock(
                                                ensure_held=AsyncMock(return_value=None)
                                            )
                                        ),
                                        __aexit__=AsyncMock(return_value=None),
                                    ),
                                ):
                                    try:
                                        asyncio.run(
                                            execute_swap(
                                                quote=quote,
                                                sub_org_id="sub",
                                                sender="0xsender",
                                                execution_state=tampered_state,
                                            )
                                        )
                                        assert False, "expected tampered state to be rejected"
                                    except NonRetryableError as exc:
                                        assert "tampered" in str(exc).lower()

    build_swap.assert_not_called()
    sign_tx.assert_not_called()
    broadcast.assert_not_called()


def test_execute_swap_allows_legacy_completed_swap_state_with_matching_claim_tx_hash():
    quote = SwapQuoteV2(
        token_in="0x1111111111111111111111111111111111111111",
        token_out="0x2222222222222222222222222222222222222222",
        amount_in=Decimal("1"),
        amount_out=Decimal("1"),
        amount_out_minimum=Decimal("0.9"),
        decimals_in=18,
        decimals_out=18,
        slippage_pct=Decimal("0.5"),
        price_impact_pct=Decimal("0.1"),
        path=["0x1111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222"],
        gas_estimate=150_000,
        needs_approval=False,
        allowance=0,
        chain_id=1,
        chain_name="Ethereum",
        dex_name="Uniswap V2",
    )
    chain = MagicMock(
        chain_id=1,
        name="Ethereum",
        rpc_url="https://rpc.test",
        v2_router="0xrouter",
        v3_router=None,
        wrapped_native="0xwrap",
        supports_native_swaps=True,
    )
    nonce_manager = MagicMock()
    nonce_manager.pending = AsyncMock(return_value=7)
    nonce_manager.allocate_safe = AsyncMock(return_value=7)
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda value: value.lower()
    w3.eth.contract.return_value = MagicMock()
    persisted_states: list[dict] = []

    async def _persist_state(state: dict) -> None:
        persisted_states.append(state)

    legacy_state = {
        "current_step": "swap",
        "completion_status": "pending",
        "steps": {
            "swap": {
                "status": "completed",
                "tx_hash": "0xdeadbeef",
            }
        },
        "metadata": {
            "legacy_claim_tx_hash": "0xdeadbeef",
        },
    }

    with patch("tool_nodes.dex.swap_executor.get_chain_by_id", return_value=chain):
        with patch("tool_nodes.dex.swap_executor.make_async_web3", return_value=w3):
            with patch(
                "tool_nodes.dex.swap_executor.get_router_capabilities",
                return_value=MagicMock(
                    supports_native_swaps=True,
                    last_checked="2026-01-01T00:00:00",
                ),
            ):
                with patch(
                    "tool_nodes.dex.swap_executor.get_async_nonce_manager",
                    new=AsyncMock(return_value=nonce_manager),
                ):
                    with patch(
                        "tool_nodes.dex.swap_executor._build_v2_swap_tx",
                        new=AsyncMock(return_value={"kind": "swap"}),
                    ) as build_swap:
                        with patch(
                            "tool_nodes.dex.swap_executor.sign_transaction_async",
                            new=AsyncMock(return_value="signed"),
                        ) as sign_tx:
                            with patch(
                                "tool_nodes.dex.swap_executor.async_broadcast_evm",
                                new=AsyncMock(return_value="0xhash"),
                            ) as broadcast:
                                with patch(
                                    "tool_nodes.dex.swap_executor.wallet_lock",
                                    return_value=MagicMock(
                                        __aenter__=AsyncMock(
                                            return_value=MagicMock(
                                                ensure_held=AsyncMock(return_value=None)
                                            )
                                        ),
                                        __aexit__=AsyncMock(return_value=None),
                                    ),
                                ):
                                    result = asyncio.run(
                                        execute_swap(
                                            quote=quote,
                                            sub_org_id="sub",
                                            sender="0xsender",
                                            execution_state=legacy_state,
                                            persist_execution_state=_persist_state,
                                        )
                                    )

    assert result.tx_hash == "0xdeadbeef"
    assert persisted_states
    assert persisted_states[-1]["steps"]["swap"]["status"] == "completed"
    assert persisted_states[-1]["steps"]["swap"]["fingerprint"]
    build_swap.assert_not_called()
    sign_tx.assert_not_called()
    broadcast.assert_not_called()


def test_execute_swap_unwraps_wrapped_native_output_even_when_native_supported():
    quote = SwapQuoteV2(
        token_in="0x1111111111111111111111111111111111111111",
        token_out="0xwrap",
        amount_in=Decimal("1"),
        amount_out=Decimal("1"),
        amount_out_minimum=Decimal("0.9"),
        decimals_in=18,
        decimals_out=18,
        slippage_pct=Decimal("0.5"),
        price_impact_pct=Decimal("0.1"),
        path=["0x1111111111111111111111111111111111111111", "0xwrap"],
        gas_estimate=150_000,
        needs_approval=False,
        allowance=0,
        chain_id=1,
        chain_name="Ethereum",
        dex_name="Uniswap V2",
    )
    chain = MagicMock(
        chain_id=1,
        name="Ethereum",
        rpc_url="https://rpc.test",
        v2_router="0xrouter",
        v3_router=None,
        wrapped_native="0xwrap",
        supports_native_swaps=True,
    )
    nonce_manager = MagicMock()
    nonce_manager.pending = AsyncMock(return_value=7)
    nonce_manager.allocate_safe = AsyncMock(side_effect=[8, 9])

    class _BalanceCall:
        def __init__(self, values: list[int]) -> None:
            self._values = values

        async def call(self) -> int:
            if len(self._values) > 1:
                return self._values.pop(0)
            return self._values[0]

    class _WrappedContract:
        def __init__(self) -> None:
            self.functions = self
            self._values = [5, 25]

        def balanceOf(self, _sender: str):
            return _BalanceCall(self._values)

    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda value: value.lower()
    w3.eth.contract.return_value = _WrappedContract()

    async def _build_swap(*_args, **_kwargs):
        return {"kind": "swap"}

    async def _build_unwrap(*_args, **_kwargs):
        return {"kind": "unwrap"}

    async def _sign(_sub_org_id, tx, _sender):
        return tx["kind"]

    async def _broadcast(_w3, signed_tx):
        return {
            "swap": "0xswap",
            "unwrap": "0xunwrap",
        }[signed_tx]

    with patch("tool_nodes.dex.swap_executor.get_chain_by_id", return_value=chain):
        with patch("tool_nodes.dex.swap_executor.make_async_web3", return_value=w3):
            with patch(
                "tool_nodes.dex.swap_executor.get_router_capabilities",
                return_value=MagicMock(
                    supports_native_swaps=True,
                    last_checked="2026-01-01T00:00:00",
                ),
            ):
                with patch(
                    "tool_nodes.dex.swap_executor.get_async_nonce_manager",
                    new=AsyncMock(return_value=nonce_manager),
                ):
                    with patch(
                        "tool_nodes.dex.swap_executor._maybe_approve",
                        new=AsyncMock(return_value=None),
                    ):
                        with patch(
                            "tool_nodes.dex.swap_executor._build_v2_swap_tx",
                            new=_build_swap,
                        ):
                            with patch(
                                "tool_nodes.dex.swap_executor._build_unwrap_tx",
                                new=_build_unwrap,
                            ):
                                with patch(
                                    "tool_nodes.dex.swap_executor.sign_transaction_async",
                                    new=_sign,
                                ):
                                    with patch(
                                        "tool_nodes.dex.swap_executor.async_broadcast_evm",
                                        new=_broadcast,
                                    ):
                                        with patch(
                                            "tool_nodes.dex.swap_executor.async_await_evm_receipt",
                                            new=AsyncMock(return_value=None),
                                        ):
                                            with patch(
                                                "tool_nodes.dex.swap_executor.wallet_lock",
                                                return_value=MagicMock(
                                                    __aenter__=AsyncMock(
                                                        return_value=MagicMock(
                                                            ensure_held=AsyncMock(
                                                                return_value=None
                                                            )
                                                        )
                                                    ),
                                                    __aexit__=AsyncMock(return_value=None),
                                                ),
                                            ):
                                                result = asyncio.run(
                                                    execute_swap(
                                                        quote=quote,
                                                        sub_org_id="sub",
                                                        sender="0xsender",
                                                    )
                                                )

    assert result.tx_hash == "0xswap"
    assert result.unwrap_hash == "0xunwrap"
