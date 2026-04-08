from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Dict, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

WSOL_MINT: str = "So11111111111111111111111111111111111111112"

SOL_DECIMALS: int = 9

_KNOWN_DECIMALS: Dict[str, int] = {
    # Wrapped SOL
    WSOL_MINT: 9,
    # USDC (mainnet)
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": 6,
    # USDT (mainnet)
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": 6,
    # USDC (devnet — Circle test token)
    "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU": 6,
    # RAY (Raydium governance token)
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R": 6,
    # Bonk
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263": 5,
    # JTO (Jito governance token)
    "jtojtomepa8beP8AuQc6eL9H6gdzTiqxLpfDuuKKM4c": 9,
    # JUP (Jupiter governance token)
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN": 6,
    # WIF (dogwifhat)
    "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm": 6,
    # PYTH
    "HZ1JovNiVvGrk6Kyy2k7o5MLKhLNxRb5DJTM2kbHcfPo": 6,
}

# Public fallback RPCs (rate-limited — use a dedicated provider in production).
_MAINNET_PUBLIC_RPC = "https://api.mainnet-beta.solana.com"
_DEVNET_PUBLIC_RPC = "https://api.devnet.solana.com"


@dataclass(frozen=True)
class SolanaChainConfig:
    chain_id: int
    network: str
    name: str
    rpc_url: str
    native_symbol: str
    native_mint: str
    dexscreener_slug: Optional[str]
    explorer_url: str
    is_testnet: bool


def _mainnet_rpc() -> str:
    url = os.getenv("SOLANA_RPC_URL", "").strip()
    if url:
        return url
    logger.warning("SOLANA_RPC_URL is not set falling back to the public Solana RPC. ")
    return _MAINNET_PUBLIC_RPC


def _devnet_rpc() -> str:
    url = os.getenv("SOLANA_TESTNET_RPC_URL", "").strip()
    if url:
        return url
    logger.warning("SOLANA_TESTNET_RPC_URL is not set falling back to the default. ")

    return _DEVNET_PUBLIC_RPC


SOLANA_CHAINS: Dict[str, SolanaChainConfig] = {
    "solana": SolanaChainConfig(
        chain_id=900_000_001,
        network="solana",
        name="Solana",
        rpc_url=_mainnet_rpc(),
        native_symbol="SOL",
        native_mint=WSOL_MINT,
        dexscreener_slug="solana",
        explorer_url="https://solscan.io",
        is_testnet=False,
    ),
    "solana-devnet": SolanaChainConfig(
        chain_id=900_000_002,
        network="solana-devnet",
        name="Solana Devnet",
        rpc_url=_devnet_rpc(),
        native_symbol="SOL",
        native_mint=WSOL_MINT,
        dexscreener_slug=None,
        explorer_url="https://solscan.io/?cluster=devnet",
        is_testnet=True,
    ),
}

_CHAIN_ID_INDEX: Dict[int, SolanaChainConfig] = {}
for _chain in SOLANA_CHAINS.values():
    if _chain.chain_id in _CHAIN_ID_INDEX:
        raise ValueError(f"Duplicate Solana chain_id detected: {_chain.chain_id}")
    _CHAIN_ID_INDEX[_chain.chain_id] = _chain

_CHAIN_ALIASES: Dict[str, str] = {
    # Canonical keys
    "solana": "solana",
    "solana-devnet": "solana-devnet",
    # Display names (lowercase)
    "solana mainnet": "solana",
    "solana mainnet-beta": "solana",
    "solana devnet": "solana-devnet",
    "solana testnet": "solana-devnet",
    # Short forms
    "sol": "solana",
    "sol-devnet": "solana-devnet",
}


def get_solana_chain(network: str) -> SolanaChainConfig:
    key = network.strip().lower()
    canonical = _CHAIN_ALIASES.get(key)
    if canonical and canonical in SOLANA_CHAINS:
        return SOLANA_CHAINS[canonical]
    raise KeyError(
        f"Solana network {network!r} is not registered. "
        "Use 'solana' for mainnet or 'solana-devnet' for devnet."
    )


def is_solana_network(network: str) -> bool:
    try:
        get_solana_chain(network)
        return True
    except KeyError:
        return False


def is_solana_testnet(network: str) -> bool:
    try:
        return get_solana_chain(network).is_testnet
    except KeyError:
        return False


def normalize_solana_mint(mint: str) -> str:
    m = mint.strip().lower()
    if m in ("native", "sol", WSOL_MINT.lower()):
        return WSOL_MINT
    return mint.strip()


def get_solana_chain_by_id(chain_id: int) -> SolanaChainConfig:
    if chain_id in _CHAIN_ID_INDEX:
        return _CHAIN_ID_INDEX[chain_id]
    raise KeyError(
        f"Solana chain_id {chain_id!r} is not registered. "
        f"Known Solana chain_ids: {sorted(_CHAIN_ID_INDEX.keys())}"
    )


def is_solana_chain_id(chain_id: int) -> bool:
    return chain_id in _CHAIN_ID_INDEX


async def fetch_solana_token_decimals(
    mint: str,
    rpc_url: str,
    *,
    timeout: float = 5.0,
) -> int:
    from solders.pubkey import Pubkey

    from wallet_service.solana.rpc_client import get_shared_solana_client

    mint = normalize_solana_mint(mint)

    if mint == WSOL_MINT:
        return SOL_DECIMALS

    if mint in _KNOWN_DECIMALS:
        return _KNOWN_DECIMALS[mint]

    try:
        # Validate mint as a valid Pubkey
        mint_pubkey = Pubkey.from_string(mint)
        client = await get_shared_solana_client(rpc_url)
        # Note: We pass timeout here although the shared client might have its own;
        # some versions of solana-py AsyncClient allow passing timeout to get_token_supply.
        # If not supported, we rely on the client's internal budget.
        resp = await client.get_token_supply(mint_pubkey)
        # solana-py returns a RPCResponse object or similar; we check .value
        if resp and hasattr(resp, "value") and resp.value is not None:
            decimals = getattr(resp.value, "decimals", None)
            if decimals is not None:
                # Cache the result for this process lifetime so subsequent
                # calls to the same mint skip the network round-trip.
                _KNOWN_DECIMALS[mint] = int(decimals)
                return int(decimals)
    except Exception as exc:
        msg = f"fetch_solana_token_decimals: could not fetch decimals for {mint[:12]} (rpc={rpc_url[:40]}): {exc}"
        logger.error(msg)
        raise RuntimeError(msg) from exc

    msg = f"fetch_solana_token_decimals: RPC returned empty decimals for {mint[:12]}"
    logger.error(msg)
    raise RuntimeError(msg)
