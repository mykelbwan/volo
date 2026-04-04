import pytest

from core.transfers.chains import (
    canonicalize_transfer_network,
    get_transfer_chain_spec,
    resolve_transfer_chain_spec,
)
from core.transfers.handlers import get_transfer_handler
from core.transfers.models import normalize_transfer_request
from core.transfers.planning import resolve_transfer_planning_metadata


def test_canonicalize_transfer_network_handles_evm_and_solana_aliases():
    assert canonicalize_transfer_network("eth") == "ethereum"
    assert canonicalize_transfer_network("solana devnet") == "solana-devnet"


def test_get_transfer_chain_spec_resolves_aliases_to_deterministic_families():
    evm_spec = get_transfer_chain_spec("eth")
    solana_spec = get_transfer_chain_spec("sol")

    assert evm_spec.family == "evm"
    assert evm_spec.network == "ethereum"
    assert solana_spec.family == "solana"
    assert solana_spec.network == "solana"


def test_resolve_transfer_chain_spec_returns_neutral_fields_for_evm():
    spec = resolve_transfer_chain_spec("ethereum")

    assert spec is not None
    assert spec.family == "evm"
    assert spec.network == "ethereum"
    assert spec.display_name == "Ethereum"
    assert spec.native_symbol == "ETH"
    assert spec.native_asset_ref == "0x0000000000000000000000000000000000000000"
    assert spec.is_testnet is False


def test_resolve_transfer_chain_spec_returns_neutral_fields_for_solana_devnet():
    spec = resolve_transfer_chain_spec("solana-devnet")

    assert spec is not None
    assert spec.family == "solana"
    assert spec.network == "solana-devnet"
    assert spec.display_name == "Solana Devnet"
    assert spec.native_symbol == "SOL"
    assert spec.native_asset_ref == "So11111111111111111111111111111111111111112"
    assert spec.is_testnet is True


def test_get_transfer_chain_spec_rejects_unsupported_network():
    with pytest.raises(KeyError, match="not registered"):
        get_transfer_chain_spec("definitely-not-a-chain")


def test_get_transfer_handler_returns_registered_handlers_and_fails_closed_for_missing_family():
    assert get_transfer_handler("evm") is not None
    assert get_transfer_handler("solana") is not None
    assert get_transfer_handler("not-a-family") is None


def test_normalize_transfer_request_accepts_legacy_fields():
    request = normalize_transfer_request(
        {
            "token_symbol": "usdc",
            "token_address": "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "amount": "1.25",
            "recipient": "0xabc",
            "chain": "Ethereum",
            "sub_org_id": "sub",
            "sender": "0xsender",
            "decimals": 6,
            "idempotency_key": " req-123 ",
        }
    )

    assert request.asset_symbol == "USDC"
    assert request.asset_ref == "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    assert str(request.amount) == "1.25"
    assert request.network == "ethereum"
    assert request.requested_network == "Ethereum"
    assert request.decimals == 6
    assert request.idempotency_key == "req-123"


def test_normalize_transfer_request_accepts_new_aliases():
    request = normalize_transfer_request(
        {
            "asset_symbol": "sol",
            "asset_ref": "So11111111111111111111111111111111111111112",
            "amount": "0.5",
            "recipient": "H3qWmD5MZx7Sg1H4C4r4Y6xWm4kD1a2o3p4q5r6s7t8",
            "network": "solana-devnet",
            "sub_org_id": "sub",
            "sender": "9xQeWvG816bUx9EPjHmaT23yvVMcFVN8Y6GsB7mQ7X7G",
        }
    )

    assert request.asset_symbol == "SOL"
    assert request.asset_ref == "So11111111111111111111111111111111111111112"
    assert str(request.amount) == "0.5"
    assert request.network == "solana-devnet"
    assert request.requested_network == "solana-devnet"


def test_normalize_transfer_request_maps_solana_native_alias_to_chain_native_asset_ref():
    request = normalize_transfer_request(
        {
            "asset_symbol": "sol",
            "asset_ref": "native",
            "amount": "0.5",
            "recipient": "H3qWmD5MZx7Sg1H4C4r4Y6xWm4kD1a2o3p4q5r6s7t8",
            "network": "solana-devnet",
            "sub_org_id": "sub",
            "sender": "9xQeWvG816bUx9EPjHmaT23yvVMcFVN8Y6GsB7mQ7X7G",
        }
    )

    assert request.asset_ref == "So11111111111111111111111111111111111111112"


def test_normalize_transfer_request_preserves_non_native_solana_asset_ref():
    request = normalize_transfer_request(
        {
            "asset_symbol": "usdc",
            "asset_ref": "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU",
            "amount": "1",
            "recipient": "H3qWmD5MZx7Sg1H4C4r4Y6xWm4kD1a2o3p4q5r6s7t8",
            "network": "solana-devnet",
            "sub_org_id": "sub",
            "sender": "9xQeWvG816bUx9EPjHmaT23yvVMcFVN8Y6GsB7mQ7X7G",
        }
    )

    assert request.asset_ref == "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU"


def test_normalize_transfer_request_accepts_matching_chain_and_network_aliases():
    request = normalize_transfer_request(
        {
            "token_symbol": "ETH",
            "token_address": "native",
            "amount": "1",
            "recipient": "0xabc",
            "chain": "Ethereum",
            "network": "eth",
            "sub_org_id": "sub",
            "sender": "0xsender",
        }
    )

    assert request.network == "ethereum"
    assert request.requested_network == "eth"
    assert request.asset_ref == "0x0000000000000000000000000000000000000000"


def test_normalize_transfer_request_rejects_unsupported_network():
    with pytest.raises(ValueError, match="Unsupported transfer network"):
        normalize_transfer_request(
            {
                "token_symbol": "ETH",
                "amount": "1",
                "recipient": "0xabc",
                "chain": "definitely-not-a-chain",
                "sub_org_id": "sub",
                "sender": "0xsender",
            }
        )


def test_normalize_transfer_request_rejects_conflicting_chain_and_network():
    with pytest.raises(ValueError, match="Conflicting transfer network inputs"):
        normalize_transfer_request(
            {
                "token_symbol": "ETH",
                "amount": "1",
                "recipient": "0xabc",
                "chain": "ethereum",
                "network": "solana-devnet",
                "sub_org_id": "sub",
                "sender": "0xsender",
            }
        )


def test_normalize_transfer_request_rejects_conflicting_asset_refs():
    with pytest.raises(ValueError, match="Conflicting transfer asset inputs"):
        normalize_transfer_request(
            {
                "token_symbol": "USDC",
                "token_address": "0x1111111111111111111111111111111111111111",
                "asset_ref": "0x2222222222222222222222222222222222222222",
                "amount": "1",
                "recipient": "0xabc",
                "chain": "ethereum",
                "sub_org_id": "sub",
                "sender": "0xsender",
            }
        )


def test_normalize_transfer_request_rejects_conflicting_asset_symbols():
    with pytest.raises(ValueError, match="Conflicting transfer asset symbol inputs"):
        normalize_transfer_request(
            {
                "token_symbol": "USDC",
                "asset_symbol": "ETH",
                "amount": "1",
                "recipient": "0xabc",
                "chain": "ethereum",
                "sub_org_id": "sub",
                "sender": "0xsender",
            }
        )


def test_resolve_transfer_planning_metadata_classifies_evm_native_and_token():
    native = resolve_transfer_planning_metadata(
        {
            "asset_symbol": "ETH",
            "asset_ref": "native",
            "network": "ethereum",
        }
    )
    token = resolve_transfer_planning_metadata(
        {
            "asset_symbol": "USDC",
            "asset_ref": "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "network": "ethereum",
        }
    )

    assert native.family == "evm"
    assert native.asset_kind == "native"
    assert native.asset_ref == "0x0000000000000000000000000000000000000000"
    assert token.family == "evm"
    assert token.asset_kind == "token"


def test_resolve_transfer_planning_metadata_classifies_solana_native_and_spl():
    native = resolve_transfer_planning_metadata(
        {
            "asset_symbol": "SOL",
            "asset_ref": "native",
            "network": "solana-devnet",
        }
    )
    token = resolve_transfer_planning_metadata(
        {
            "asset_symbol": "USDC",
            "asset_ref": "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU",
            "network": "solana-devnet",
        }
    )

    assert native.family == "solana"
    assert native.asset_kind == "native"
    assert native.asset_ref == "So11111111111111111111111111111111111111112"
    assert token.family == "solana"
    assert token.asset_kind == "token"


def test_resolve_transfer_planning_metadata_rejects_ambiguous_evm_token_without_asset_ref():
    with pytest.raises(ValueError, match="explicit asset reference"):
        resolve_transfer_planning_metadata(
            {
                "asset_symbol": "USDC",
                "network": "ethereum",
            }
        )


def test_resolve_transfer_planning_metadata_rejects_conflicting_network_aliases():
    with pytest.raises(ValueError, match="Conflicting transfer network inputs"):
        resolve_transfer_planning_metadata(
            {
                "asset_symbol": "USDC",
                "asset_ref": "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "network": "ethereum",
                "chain": "solana-devnet",
            }
        )


def test_resolve_transfer_planning_metadata_rejects_conflicting_asset_refs():
    with pytest.raises(ValueError, match="Conflicting transfer asset inputs"):
        resolve_transfer_planning_metadata(
            {
                "asset_symbol": "USDC",
                "asset_ref": "0x1111111111111111111111111111111111111111",
                "token_address": "0x2222222222222222222222222222222222222222",
                "network": "ethereum",
            }
        )


def test_resolve_transfer_planning_metadata_rejects_conflicting_asset_symbols():
    with pytest.raises(ValueError, match="Conflicting transfer asset symbol inputs"):
        resolve_transfer_planning_metadata(
            {
                "asset_symbol": "USDC",
                "token_symbol": "ETH",
                "asset_ref": "native",
                "network": "ethereum",
            }
        )
