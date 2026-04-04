from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.unit.wallet_service._wallet_security_helpers import (
    ASYNC_TEST_TIMEOUT,
    FakeAsyncWeb3,
    FakeChain,
    FakeEvmEth,
    StaticNonceManager,
    evm_transfer_params,
    fake_chain,
    AtomicFakeNonceManager,
)
from tool_nodes.wallet.transfer import transfer_token
from wallet_service.evm.gas_price import GasPriceCache, estimate_eip1559_fees
from wallet_service.evm.native_transfer import execute_native_transfer


@asynccontextmanager
async def _noop_wallet_lock(*_args, **_kwargs):
    class _Lock:
        async def ensure_held(self) -> None:
            return None

    yield _Lock()


def test_gas_price_cache_shortens_ttl_after_volatile_refresh() -> None:
    # Vulnerability: stale gas price cache survives large price moves and repeats failures.
    cache = GasPriceCache()
    chain_id = 8453

    cache._store_entry(chain_id, 10_000_000_000)
    first = cache._entries[chain_id]
    cache._store_entry(chain_id, 13_000_000_000)
    second = cache._entries[chain_id]

    assert first.ttl_seconds == 20
    assert second.ttl_seconds == 10


@pytest.mark.asyncio
async def test_estimate_eip1559_fees_caps_tip_and_keeps_max_fee_near_base_fee() -> None:
    # Vulnerability: unsafe EIP-1559 logic overpays the tip or inflates maxFee excessively.
    w3 = FakeAsyncWeb3(
        FakeEvmEth(
            pending_nonce=1,
            base_fee_per_gas=40_000_000_000,
            max_priority_fee=99_000_000_000,
        )
    )

    max_fee, priority_fee = await estimate_eip1559_fees(w3, 80_000_000_000)

    assert priority_fee == 2_000_000_000
    assert max_fee >= 40_000_000_000
    assert max_fee < 60_000_000_000


@pytest.mark.asyncio
async def test_broadcast_failure_invalidates_gas_cache_before_retry(
    monkeypatch: pytest.MonkeyPatch,
    fake_chain: FakeChain,
) -> None:
    # Vulnerability: stale cached gas survives an underpriced failure and poisons retries.
    manager = AtomicFakeNonceManager(initial_nonce=17)
    w3 = FakeAsyncWeb3(FakeEvmEth(pending_nonce=17))
    invalidate_mock = AsyncMock()

    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_chain_by_name",
        lambda _name: fake_chain,
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_shared_async_web3",
        AsyncMock(return_value=w3),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.get_async_nonce_manager",
        AsyncMock(return_value=manager),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.wallet_lock",
        _noop_wallet_lock,
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.gas_price_cache.get_wei",
        AsyncMock(return_value=15_000_000_000),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.gas_price_cache.invalidate_async",
        invalidate_mock,
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.sign_transaction_async",
        AsyncMock(return_value="signed"),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.reset_on_error_async",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        "wallet_service.evm.native_transfer.async_broadcast_evm",
        AsyncMock(side_effect=RuntimeError("replacement transaction underpriced")),
    )

    with pytest.raises(RuntimeError, match="couldn't broadcast"):
        await asyncio.wait_for(
            execute_native_transfer(
                to="0x00000000000000000000000000000000000000aa",
                amount_native=Decimal("0.0001"),
                chain_name="ethereum",
                sender="0x00000000000000000000000000000000000000bb",
                sub_org_id="sub-org",
            ),
            timeout=ASYNC_TEST_TIMEOUT,
        )

    invalidate_mock.assert_awaited_once_with(chain_id=fake_chain.chain_id)


@pytest.mark.parametrize(
    "asset_ref",
    [
        None,
        "",
        "native",
        "0x0000000000000000000000000000000000000000",
    ],
)
def test_transfer_tool_routes_supported_native_refs_to_native_transfer(
    monkeypatch: pytest.MonkeyPatch,
    fake_chain: FakeChain,
    asset_ref: str | None,
) -> None:
    # Vulnerability: native token placeholder resolution routes into the wrong send path.
    execute_native_mock = AsyncMock(return_value="0xnativehash")
    monkeypatch.setattr(
        "core.transfers.evm_handler.get_chain_by_name",
        lambda _name: fake_chain,
    )
    monkeypatch.setattr(
        "core.transfers.evm_handler.execute_native_transfer",
        execute_native_mock,
    )

    result = asyncio.run(transfer_token(evm_transfer_params(asset_ref=asset_ref)))

    assert result["tx_hash"] == "0xnativehash"
    execute_native_mock.assert_awaited_once()


def test_transfer_tool_does_not_treat_foreign_placeholder_as_native(
    monkeypatch: pytest.MonkeyPatch,
    fake_chain: FakeChain,
) -> None:
    # Vulnerability: foreign sentinels can trick the wallet into misrouting funds as native.
    execute_native_mock = AsyncMock()
    build_erc20_tx_mock = MagicMock(
        return_value={
            "to": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "value": 0,
            "data": "0x",
            "nonce": 5,
            "gas": 65_000,
            "maxFeePerGas": 10,
            "maxPriorityFeePerGas": 2,
            "type": "0x2",
            "chainId": fake_chain.chain_id,
        }
    )

    monkeypatch.setattr(
        "core.transfers.evm_handler.get_chain_by_name",
        lambda _name: fake_chain,
    )
    monkeypatch.setattr(
        "core.transfers.evm_handler.execute_native_transfer",
        execute_native_mock,
    )
    monkeypatch.setattr(
        "core.transfers.evm_handler.gas_price_cache.get_wei",
        AsyncMock(return_value=10_000_000_000),
    )
    monkeypatch.setattr(
        "core.transfers.evm_handler.estimate_eip1559_fees",
        AsyncMock(return_value=(12_000_000_000, 2_000_000_000)),
    )
    monkeypatch.setattr(
        "core.transfers.evm_handler.make_async_web3",
        lambda _rpc_url: FakeAsyncWeb3(FakeEvmEth(pending_nonce=5)),
    )
    monkeypatch.setattr(
        "core.transfers.evm_handler.wallet_lock",
        _noop_wallet_lock,
    )
    monkeypatch.setattr(
        "core.transfers.evm_handler.build_erc20_tx",
        build_erc20_tx_mock,
    )
    monkeypatch.setattr(
        "core.transfers.evm_handler.get_async_nonce_manager",
        AsyncMock(return_value=StaticNonceManager(5)),
    )
    monkeypatch.setattr(
        "core.transfers.evm_handler.sign_transaction_async",
        AsyncMock(return_value="0xsigned"),
    )
    monkeypatch.setattr(
        "core.transfers.evm_handler.async_broadcast_evm",
        AsyncMock(return_value="0xerc20hash"),
    )

    result = asyncio.run(
        transfer_token(
            evm_transfer_params(
                asset_ref="0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeeeE"
            )
        )
    )

    assert result["tx_hash"] == "0xerc20hash"
    execute_native_mock.assert_not_awaited()
    build_erc20_tx_mock.assert_called_once()
