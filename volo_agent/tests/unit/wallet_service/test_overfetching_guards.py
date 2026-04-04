from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from wallet_service.solana import get_single_token_bal

VALID_WALLET = "11111111111111111111111111111111"
VALID_MINT = "So11111111111111111111111111111111111111112"


def _parsed_token_account(mint: str, amount: str, decimals: int) -> SimpleNamespace:
    return SimpleNamespace(
        account=SimpleNamespace(
            data=SimpleNamespace(
                parsed={
                    "info": {
                        "mint": mint,
                        "tokenAmount": {
                            "amount": amount,
                            "decimals": decimals,
                        },
                    }
                }
            )
        )
    )


def _mock_client_response(
    monkeypatch: pytest.MonkeyPatch,
    response: object | Exception,
) -> tuple[AsyncMock, AsyncMock]:
    client = AsyncMock()
    if isinstance(response, Exception):
        client.get_token_accounts_by_owner_json_parsed = AsyncMock(side_effect=response)
    else:
        client.get_token_accounts_by_owner_json_parsed = AsyncMock(return_value=response)

    get_client = AsyncMock(return_value=client)
    monkeypatch.setattr(get_single_token_bal, "get_shared_solana_client", get_client)
    monkeypatch.setattr(
        get_single_token_bal,
        "get_solana_chain",
        lambda network: SimpleNamespace(
            rpc_url="https://rpc.test",
            network=network,
        ),
    )
    return get_client, client.get_token_accounts_by_owner_json_parsed


@pytest.mark.asyncio
async def test_single_token_balance_uses_typed_rpc_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_client, rpc_call = _mock_client_response(
        monkeypatch,
        SimpleNamespace(value=[_parsed_token_account(VALID_MINT, "5000000", 6)]),
    )

    balance = await get_single_token_bal.get_token_balance_async(
        VALID_WALLET,
        VALID_MINT,
        network="mainnet",
    )

    assert balance == Decimal("5")
    get_client.assert_awaited_once_with("https://rpc.test")
    rpc_call.assert_awaited_once()
    assert str(rpc_call.await_args.args[0]) == VALID_WALLET
    assert str(rpc_call.await_args.args[1].mint) == VALID_MINT
    assert rpc_call.await_args.kwargs == {"commitment": "confirmed"}


@pytest.mark.asyncio
async def test_single_token_balance_sums_multiple_accounts_for_same_mint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, rpc_call = _mock_client_response(
        monkeypatch,
        SimpleNamespace(
            value=[
                _parsed_token_account(VALID_MINT, "250", 2),
                _parsed_token_account(VALID_MINT, "1234500", 4),
            ]
        ),
    )

    balance = await get_single_token_bal.get_token_balance_async(VALID_WALLET, VALID_MINT)

    assert balance == Decimal("125.95")
    rpc_call.assert_awaited_once()


@pytest.mark.asyncio
async def test_single_token_balance_returns_zero_when_no_accounts_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_client_response(monkeypatch, SimpleNamespace(value=[]))

    balance = await get_single_token_bal.get_token_balance_async(VALID_WALLET, VALID_MINT)

    assert balance == Decimal(0)


@pytest.mark.asyncio
async def test_single_token_balance_rejects_invalid_wallet_before_rpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_client = AsyncMock()
    monkeypatch.setattr(get_single_token_bal, "get_shared_solana_client", get_client)

    with pytest.raises(ValueError, match="Invalid wallet_address"):
        await get_single_token_bal.get_token_balance_async("wallet-abc", VALID_MINT)

    get_client.assert_not_awaited()


@pytest.mark.asyncio
async def test_single_token_balance_rejects_invalid_mint_before_rpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_client = AsyncMock()
    monkeypatch.setattr(get_single_token_bal, "get_shared_solana_client", get_client)

    with pytest.raises(ValueError, match="Invalid token_mint"):
        await get_single_token_bal.get_token_balance_async(VALID_WALLET, "mint-xyz")

    get_client.assert_not_awaited()


@pytest.mark.asyncio
async def test_single_token_balance_raises_controlled_error_for_malformed_parsed_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, rpc_call = _mock_client_response(
        monkeypatch,
        SimpleNamespace(
            value=[
                SimpleNamespace(
                    account=SimpleNamespace(
                        data=SimpleNamespace(parsed={"info": {"mint": VALID_MINT}})
                    )
                )
            ]
        ),
    )

    with pytest.raises(RuntimeError, match="Unexpected Solana parsed token account shape"):
        await get_single_token_bal.get_token_balance_async(VALID_WALLET, VALID_MINT)

    rpc_call.assert_awaited_once()


@pytest.mark.asyncio
async def test_single_token_balance_propagates_rpc_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_client_response(monkeypatch, TimeoutError("rpc timed out"))

    with pytest.raises(TimeoutError, match="rpc timed out"):
        await get_single_token_bal.get_token_balance_async(VALID_WALLET, VALID_MINT)
