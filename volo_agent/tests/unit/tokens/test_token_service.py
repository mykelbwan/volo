import json
import os
from pathlib import Path

from config.solana_chains import SOL_DECIMALS, get_solana_chain
from intent_hub.registry import token_service
from config.chains import get_chain_by_name


def test_get_token_data_native_shortcut(monkeypatch):
    chain_cfg = get_chain_by_name("ethereum")
    data = token_service.get_token_data("ETH", "ethereum")
    assert data["symbol"] == "ETH"
    assert data["chains"]["ethereum"]["address"] == chain_cfg.wrapped_native


def test_get_token_data_solana_native_shortcut():
    token_service._get_token_data_cached.cache_clear()
    chain_cfg = get_solana_chain("solana-devnet")

    data = token_service.get_token_data("SOL", "solana-devnet")

    assert data["symbol"] == "SOL"
    assert data["decimals"] == SOL_DECIMALS
    assert data["chains"]["solana-devnet"]["address"] == chain_cfg.native_mint


def test_resolve_chain_id_supports_solana_networks():
    token_service._resolve_chain_id.cache_clear()

    assert token_service._resolve_chain_id("solana-devnet") == get_solana_chain(
        "solana-devnet"
    ).chain_id


def test_get_token_data_local_fallback(monkeypatch, tmp_path: Path):
    registry = {
        "NIA": {
            "decimals": 18,
            "chains": {"somnia testnet": {"address": "0xF2F773753cEbEFaF9b68b841d80C083b18C69311"}},
        }
    }
    path = tmp_path / "tokens.json"
    path.write_text(json.dumps(registry))

    monkeypatch.setenv("TOKEN_REGISTRY_FALLBACK_PATH", str(path))
    monkeypatch.setattr(token_service, "_LOCAL_REGISTRY_CACHE", None)
    monkeypatch.setattr(token_service, "_registry_lookup", lambda *_args, **_kwargs: None)

    data = token_service.get_token_data("NIA", "somnia testnet")
    assert data["symbol"] == "NIA"
    assert data["decimals"] == 18
    assert data["chains"]["somnia testnet"]["address"] == "0xF2F773753cEbEFaF9b68b841d80C083b18C69311"


def test_get_address_for_chain_normalizes_hyphenated_keys():
    token_data = {
        "symbol": "NIA",
        "decimals": 18,
        "chains": {"somnia-testnet": {"address": "0xabc"}},
    }

    assert token_service.get_address_for_chain(token_data, "somnia testnet") == "0xabc"


def test_get_chain_by_name_supports_somnia_aliases():
    chain = get_chain_by_name("somnia")
    assert chain.name == "Somnia Testnet"
    chain = get_chain_by_name("somnia network")
    assert chain.name == "Somnia Testnet"


def test_skip_healthcheck_does_not_skip_registry_lookup(monkeypatch):
    called = {"count": 0}

    def fake_registry_lookup(symbol: str, chain_name: str, chain_id: int):
        called["count"] += 1
        return {
            "symbol": symbol,
            "decimals": 6,
            "chains": {chain_name: {"address": "0xabc"}},
        }

    monkeypatch.setenv("SKIP_MONGODB_HEALTHCHECK", "1")
    monkeypatch.delenv("SKIP_MONGODB_REGISTRY", raising=False)
    token_service._get_token_data_cached.cache_clear()
    monkeypatch.setattr(token_service, "_registry_lookup", fake_registry_lookup)

    data = token_service.get_token_data("usdc", "base")

    assert called["count"] == 1
    assert data["chains"]["base"]["address"] == "0xabc"


def test_skip_registry_env_still_disables_registry_lookup(monkeypatch):
    called = {"count": 0}

    def fake_registry_lookup(*_args, **_kwargs):
        called["count"] += 1
        return None

    monkeypatch.delenv("SKIP_MONGODB_HEALTHCHECK", raising=False)
    monkeypatch.setenv("SKIP_MONGODB_REGISTRY", "1")
    token_service._get_token_data_cached.cache_clear()
    monkeypatch.setattr(token_service, "_registry_lookup", fake_registry_lookup)

    data = token_service.get_token_data("usdc", "base")

    assert called["count"] == 0
    assert data["symbol"] == "USDC"
