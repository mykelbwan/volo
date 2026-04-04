import asyncio

import pytest

from core.identity.wallet_bindings import UnsupportedWalletFamilyError
from core.transfers.chains import TransferChainSpec
from intent_hub.ontology.intent import Intent, IntentStatus
import intent_hub.resolver.transfer_resolver as transfer_resolver


async def _fake_get_token_data(symbol: str, chain: str):
    return {
        "symbol": symbol.upper(),
        "decimals": 6,
        "chains": {
            chain: {
                "address": f"{chain}-token-address",
            }
        },
    }


def _intent_for_chain(chain: str, recipient: str) -> Intent:
    return Intent(
        intent_type="transfer",
        slots={
            "token": {"symbol": "USDC"},
            "amount": "1",
            "recipient": recipient,
            "chain": chain,
        },
        missing_slots=[],
        constraints=None,
        confidence=0.9,
        status=IntentStatus.COMPLETE,
        raw_input=f"send 1 usdc on {chain}",
    )


def test_resolve_transfer_uses_evm_wallet_markers(monkeypatch):
    monkeypatch.setattr(transfer_resolver, "get_token_data_async", _fake_get_token_data)

    plan = asyncio.run(
        transfer_resolver.resolve_transfer(
            _intent_for_chain("base", "0x000000000000000000000000000000000000dead")
        )
    )

    assert plan.chain == "base"
    assert plan.parameters["network"] == "base"
    assert plan.parameters["sender"] == "{{EVM_ADDRESS}}"
    assert plan.parameters["sub_org_id"] == "{{EVM_SUB_ORG_ID}}"


def test_resolve_transfer_uses_solana_wallet_markers_and_not_evm(monkeypatch):
    monkeypatch.setattr(transfer_resolver, "get_token_data_async", _fake_get_token_data)

    plan = asyncio.run(
        transfer_resolver.resolve_transfer(
            _intent_for_chain(
                "solana-devnet",
                "9xQeWvG816bUx9EPjHmaT23yvVMcFVN8Y6GsB7mQ7X7G",
            )
        )
    )

    assert plan.chain == "solana-devnet"
    assert plan.parameters["network"] == "solana-devnet"
    assert plan.parameters["sender"] == "{{SOLANA_ADDRESS}}"
    assert plan.parameters["sub_org_id"] == "{{SOLANA_SUB_ORG_ID}}"
    assert plan.parameters["sender"] != "{{EVM_ADDRESS}}"


def test_resolve_transfer_fails_closed_for_unsupported_wallet_family(monkeypatch):
    monkeypatch.setattr(
        transfer_resolver,
        "get_transfer_chain_spec",
        lambda _: TransferChainSpec(
            family="cosmos",
            network="cosmoshub",
            display_name="Cosmos Hub",
            native_symbol="ATOM",
            explorer_url=None,
            is_testnet=False,
            native_asset_ref="uatom",
        ),
    )

    with pytest.raises(
        UnsupportedWalletFamilyError,
        match="Unsupported wallet binding family: 'cosmos'",
    ):
        asyncio.run(
            transfer_resolver.resolve_transfer(
                _intent_for_chain(
                    "cosmoshub",
                    "cosmos1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqnrql8a",
                )
            )
        )
