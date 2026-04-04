from core.utils.balance_chains import (
    ALL_SUPPORTED_CHAIN_KEY,
    canonicalize_balance_chain,
    is_all_supported_chain_request,
    list_supported_balance_chain_specs,
    resolve_balance_chain_spec,
)
from config.solana_chains import SOLANA_CHAINS


def test_canonicalize_balance_chain_handles_aliases():
    assert canonicalize_balance_chain("somnia") == "somnia testnet"
    assert canonicalize_balance_chain("sol") == "solana"
    assert canonicalize_balance_chain("arbitrum") == "arbitrum one"


def test_canonicalize_balance_chain_handles_all_supported_markers():
    assert is_all_supported_chain_request("all chains") is True
    assert canonicalize_balance_chain("all chains") == ALL_SUPPORTED_CHAIN_KEY


def test_resolve_balance_chain_spec_includes_family_and_testnet():
    evm_spec = resolve_balance_chain_spec("somnia")
    assert evm_spec is not None
    assert evm_spec.family == "evm"
    assert evm_spec.is_testnet is True

    sol_spec = resolve_balance_chain_spec("solana")
    assert sol_spec is not None
    assert sol_spec.family == "solana"


def test_list_supported_balance_chain_specs_includes_configured_solana_networks():
    specs = list_supported_balance_chain_specs(
        include_testnets=True,
        include_solana_devnet=True,
    )
    keys = {spec.key for spec in specs}
    expected = set(SOLANA_CHAINS.keys())
    assert expected.issubset(keys)


def test_list_supported_balance_chain_specs_can_exclude_solana_devnet():
    specs = list_supported_balance_chain_specs(
        include_testnets=True,
        include_solana_devnet=False,
    )
    keys = {spec.key for spec in specs}
    assert "solana-devnet" not in keys
