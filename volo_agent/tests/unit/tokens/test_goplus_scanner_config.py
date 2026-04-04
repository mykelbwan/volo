from config.chains import CHAINS
from config.solana_chains import SOLANA_CHAINS, get_solana_chain
from core.token_security.goplus_scanner import GOPLUS_SUPPORTED_CHAIN_IDS


def test_goplus_supported_chain_ids_are_derived_from_non_testnet_configs():
    expected_evm = {chain.chain_id for chain in CHAINS.values() if not chain.is_testnet}
    expected_solana = {
        chain.chain_id for chain in SOLANA_CHAINS.values() if not chain.is_testnet
    }

    assert GOPLUS_SUPPORTED_CHAIN_IDS == frozenset(expected_evm | expected_solana)


def test_goplus_supported_chain_ids_exclude_testnets():
    assert 84532 not in GOPLUS_SUPPORTED_CHAIN_IDS
    assert 50312 not in GOPLUS_SUPPORTED_CHAIN_IDS
    assert 11155111 not in GOPLUS_SUPPORTED_CHAIN_IDS
    assert get_solana_chain("solana-devnet").chain_id not in GOPLUS_SUPPORTED_CHAIN_IDS


def test_goplus_supported_chain_ids_include_solana_mainnet():
    assert get_solana_chain("solana").chain_id in GOPLUS_SUPPORTED_CHAIN_IDS
