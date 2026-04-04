"""
tests/conftest.py
─────────────────
Pytest session bootstrap.

config/env.py is imported transitively by almost every module under test.
It raises ValueError at module load time if required environment variables
are absent.  This conftest stubs every mandatory variable with safe dummy
values *before* any test module is imported, so the full test suite runs
without a real .env file.

The stubs are injected via os.environ so they are visible to os.getenv()
calls inside the production code.  They are set once for the entire session
and never cleaned up (tests run in an isolated process anyway).
"""

import os

import pytest

# ── Mandatory env vars checked at import time in config/env.py ────────────────

_STUBS = {
    # CDP credentials (any non-empty string satisfies the None-check)
    "CDP_API_KEY_ID": "test_cdp_key_id",
    "CDP_API_KEY_SECRET": "test_cdp_key_secret",
    "CDP_WALLET_SECRET": "test_cdp_wallet_secret",
    # Ormi indexer
    # Gemini LLM keys  (config/env.py checks keys 1-6 individually)
    "GEMINI_API_KEY1": "test_gemini_key_1",
    "GEMINI_API_KEY2": "test_gemini_key_2",
    "GEMINI_API_KEY3": "test_gemini_key_3",
    "GEMINI_API_KEY4": "test_gemini_key_4",
    "GEMINI_API_KEY5": "test_gemini_key_5",
    "GEMINI_API_KEY6": "test_gemini_key_6",
    "COHERE_API_KEY": "test_cohere_key",
    "HUGGINGFACE_API_KEY": "test_huggingface_key",
    # MongoDB – point at a local URI so MongoClient doesn't try Atlas DNS
    "MONGODB_URI": "mongodb://localhost:27017/test_auraagent",
    # Optional but referenced by several modules
    "FEE_TREASURY_ADDRESS": "0x000000000000000000000000000000000000dead",
    "FEE_TREASURY_SOLANA_ADDRESS": "11111111111111111111111111111111",
    "SOMNIA_TESTNET_RPC_URL": "https://dream-rpc.somnia.network",
    "GOPLUS_API_KEY": "",
    "GOPLUS_SECURITY_KEY": "",
    # RPC URLs (used by config/chains.py via os.getenv with fallbacks,
    # but explicit stubs prevent accidental live calls in CI)
    "ETH_RPC_URL": "https://eth.llamarpc.com",
    "ARBITRUM_RPC_URL": "https://arb1.arbitrum.io/rpc",
    "OPTIMISM_RPC_URL": "https://mainnet.optimism.io",
    "BASE_RPC_URL": "https://mainnet.base.org",
    "BASE_SEPOLIA": "https://sepolia.base.org",
    "POLYGON_RPC_URL": "https://polygon-rpc.com",
    "BSC_RPC_URL": "https://bsc-dataseed1.binance.org",
    "AVALANCHE_RPC_URL": "https://api.avax.network/ext/bc/C/rpc",
    "SEPOLIA_RPC_URL": "https://rpc.sepolia.org",
}

for _key, _value in _STUBS.items():
    os.environ.setdefault(_key, _value)


class _FakePolicyStore:
    def __init__(self) -> None:
        pass

    def get_default_policy(self):
        return None

    def get_user_policy(self, volo_user_id: str):
        return None

    def get_effective_policy(self, volo_user_id: str):
        return None

    async def aget_default_policy(self):
        return None

    async def aget_user_policy(self, volo_user_id: str):
        return None

    async def aget_effective_policy(self, volo_user_id: str):
        return None


def pytest_configure(config: pytest.Config) -> None:
    import core.security.policy_store as policy_store

    policy_store.PolicyStore = _FakePolicyStore
