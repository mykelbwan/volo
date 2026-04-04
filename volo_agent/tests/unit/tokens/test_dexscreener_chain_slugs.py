from config.solana_chains import (
    get_solana_chain,
    get_solana_chain_by_id,
    is_solana_chain_id,
)
from core.observer.price_keys import normalize_chain_name
from core.observer.price_observer import _normalize_chain_and_slug
from core.token_security.dexscreener import (
    CHAIN_ID_TO_DEX_SLUG,
    get_dexscreener_slug_for_chain_name,
)


def test_chain_id_to_dex_slug_is_built_from_registered_chain_config():
    solana_chain_id = get_solana_chain("solana").chain_id

    assert CHAIN_ID_TO_DEX_SLUG[1] == "ethereum"
    assert CHAIN_ID_TO_DEX_SLUG[56] == "bsc"
    assert CHAIN_ID_TO_DEX_SLUG[137] == "polygon"
    assert CHAIN_ID_TO_DEX_SLUG[42161] == "arbitrum"
    assert CHAIN_ID_TO_DEX_SLUG[10] == "optimism"
    assert CHAIN_ID_TO_DEX_SLUG[8453] == "base"
    assert CHAIN_ID_TO_DEX_SLUG[43114] == "avalanche"
    assert CHAIN_ID_TO_DEX_SLUG[solana_chain_id] == "solana"


def test_chain_id_to_dex_slug_excludes_testnets_and_unverified_networks():
    solana_devnet_id = get_solana_chain("solana-devnet").chain_id

    assert 84532 not in CHAIN_ID_TO_DEX_SLUG
    assert 50312 not in CHAIN_ID_TO_DEX_SLUG
    assert 11155111 not in CHAIN_ID_TO_DEX_SLUG
    assert solana_devnet_id not in CHAIN_ID_TO_DEX_SLUG
    assert get_dexscreener_slug_for_chain_name("solana-devnet") is None


def test_get_dexscreener_slug_for_chain_name_handles_aliases_and_solana():
    assert get_dexscreener_slug_for_chain_name("bsc") == "bsc"
    assert get_dexscreener_slug_for_chain_name("Arbitrum") == "arbitrum"
    assert get_dexscreener_slug_for_chain_name("Solana") == "solana"


def test_normalize_chain_name_and_observer_slug_resolution_use_static_metadata():
    assert normalize_chain_name("bsc") == "bnb smart chain"
    assert normalize_chain_name("Base") == "base"
    assert _normalize_chain_and_slug("solana") == ("solana", "solana")
    assert _normalize_chain_and_slug("solana-devnet") == (None, None)


def test_solana_chain_ids_resolve_via_config_helpers():
    mainnet = get_solana_chain("solana")
    devnet = get_solana_chain("solana-devnet")

    assert is_solana_chain_id(mainnet.chain_id) is True
    assert is_solana_chain_id(devnet.chain_id) is True
    assert get_solana_chain_by_id(mainnet.chain_id).network == "solana"
    assert get_solana_chain_by_id(devnet.chain_id).network == "solana-devnet"
