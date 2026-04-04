import os
from dataclasses import dataclass, field, replace
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _native_token_aliases() -> tuple[str, ...]:
    configured = os.getenv("EVM_NATIVE_TOKEN_PLACEHOLDER", "").strip().lower()
    aliases = ["0x0000000000000000000000000000000000000000"]
    if configured:
        aliases.append(configured)
    else:
        # Keep provider-specific native sentinels in chain configuration rather
        # than scattering them through balance and transfer logic.
        aliases.append("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
    return tuple(dict.fromkeys(aliases))


@dataclass(frozen=True)
class ChainConfig:
    chain_id: int
    name: str
    rpc_url: str
    native_symbol: str
    wrapped_native: str
    dexscreener_slug: Optional[str] = None
    v2_router: Optional[str] = None
    v2_factory: Optional[str] = None
    v3_router: Optional[str] = None
    v3_quoter: Optional[str] = None
    v3_factory: Optional[str] = None
    explorer_url: Optional[str] = None
    is_testnet: bool = False
    supports_native_swaps: bool = True
    native_token_aliases: tuple[str, ...] = field(default_factory=_native_token_aliases)


def _rpc_env(var_name: str) -> str:
    return os.getenv(var_name, "").strip()


CHAINS: dict[int, ChainConfig] = {
    1: ChainConfig(
        chain_id=1,
        name="Ethereum",
        rpc_url=_rpc_env("ETH_RPC_URL"),
        native_symbol="ETH",
        wrapped_native="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        dexscreener_slug="ethereum",
        v2_router="0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
        v2_factory="0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
        v3_router="0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        v3_quoter="0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
        v3_factory="0x1F98431c8aD98523631AE4a59f267346ea31F984",
        explorer_url="https://etherscan.io",
    ),
    42161: ChainConfig(
        chain_id=42161,
        name="Arbitrum One",
        rpc_url=_rpc_env("ARBITRUM_RPC_URL"),
        native_symbol="ETH",
        wrapped_native="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        dexscreener_slug="arbitrum",
        v2_router="0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
        v2_factory="0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
        v3_router="0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        v3_quoter="0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
        v3_factory="0x1F98431c8aD98523631AE4a59f267346ea31F984",
        explorer_url="https://arbiscan.io",
    ),
    10: ChainConfig(
        chain_id=10,
        name="Optimism",
        rpc_url=_rpc_env("OPTIMISM_RPC_URL"),
        native_symbol="ETH",
        wrapped_native="0x4200000000000000000000000000000000000006",
        dexscreener_slug="optimism",
        v2_router="0x4C1f6fCBd233241bF2f4D02811E3bF8429BC27B8",
        v2_factory="0xFbc12984689e5f15626Bad03Ad60160Fe98B303C",
        v3_router="0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        v3_quoter="0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
        v3_factory="0x1F98431c8aD98523631AE4a59f267346ea31F984",
        explorer_url="https://optimistic.etherscan.io",
    ),
    8453: ChainConfig(
        chain_id=8453,
        name="Base",
        rpc_url=_rpc_env("BASE_RPC_URL"),
        native_symbol="ETH",
        wrapped_native="0x4200000000000000000000000000000000000006",
        dexscreener_slug="base",
        v2_router="0xfCD3842f85ed87ba2889b4D35893403796e67FF1",
        v2_factory="0xFDa619b6d20975be80A10332cD39b9a4b0FAa8BB",
        v3_router="0x2626664c2603336E57B271c5C0b26F421741e481",
        v3_quoter="0x3d4e44Eb1374240CE5F1B136041212501e4a8ef1",
        v3_factory="0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        explorer_url="https://basescan.org",
    ),
    84532: ChainConfig(
        chain_id=84532,
        name="Base Sepolia",
        rpc_url=_rpc_env("BASE_SEPOLIA"),
        native_symbol="ETH",
        wrapped_native="0x4200000000000000000000000000000000000006",
        v2_router="0x1689E7B1F10000AE47eBfE339a4f69dECd19F602",
        v2_factory="0x7Ae58f10f7849cA6F5fB71b7f45CB416c9204b1e",
        v3_router="0x94cC0AaC535CCDB3C01d6787D6413C739ae12bc4",
        v3_quoter="0xC5290058841028F1614F3A6F0F5816cAd0df5E27",
        v3_factory="0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24",
        explorer_url="https://sepolia.basescan.org",
        is_testnet=True,
    ),
    137: ChainConfig(
        chain_id=137,
        name="Polygon",
        rpc_url=_rpc_env("POLYGON_RPC_URL"),
        native_symbol="MATIC",
        wrapped_native="0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        dexscreener_slug="polygon",
        v2_router="0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff",
        v2_factory="0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32",
        v3_router="0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
        v3_quoter="0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
        v3_factory="0x1F98431c8aD98523631AE4a59f267346ea31F984",
        explorer_url="https://polygonscan.com",
    ),
    56: ChainConfig(
        chain_id=56,
        name="BNB Smart Chain",
        rpc_url=_rpc_env("BSC_RPC_URL"),
        native_symbol="BNB",
        wrapped_native="0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        dexscreener_slug="bsc",
        v2_router="0x10ED43C718714eb63d5aA57B78B54704E256024E",
        v2_factory="0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73",
        v3_router="0x1b81D678ffb9C0263b24A97847620C99d213eB14",
        v3_quoter="0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
        v3_factory="0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        explorer_url="https://bscscan.com",
    ),
    43114: ChainConfig(
        chain_id=43114,
        name="Avalanche",
        rpc_url=_rpc_env("AVALANCHE_RPC_URL"),
        native_symbol="AVAX",
        wrapped_native="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        dexscreener_slug="avalanche",
        v2_router="0x60aE616a2155Ee3d9A68541Ba4544862310933d4",
        v2_factory="0x9Ad6C38BE94206cA50bb0d90783181662f0Cfa10",
        v3_router="0xbb00FF08d01D300023C629E8fFfFcb65A5a578cE",
        v3_quoter="0xbe0F5544EC67e9B3b2D979aaA43f18Fd87E6257F",
        v3_factory="0x740b1c1de25031C31FF4fC9A62f554A55cdC1baD",
        explorer_url="https://snowtrace.io",
    ),
    50312: ChainConfig(
        chain_id=50312,
        name="Somnia Testnet",
        rpc_url=_rpc_env("SOMNIA_TESTNET_RPC_URL"),
        native_symbol="STT",
        wrapped_native="0xF22eF0085f6511f70b01a68F360dCc56261F768a",
        v2_router="0xb98c15a0dC1e271132e341250703c7e94c059e8D",
        v2_factory="0x31015A978c5815EdE29D0F969a17e116BC1866B1",
        explorer_url="https://shannon-explorer.somnia.network",
        is_testnet=True,
        supports_native_swaps=False,
    ),
    11155111: ChainConfig(
        chain_id=11155111,
        name="Sepolia",
        rpc_url=_rpc_env("SEPOLIA_RPC_URL"),
        native_symbol="ETH",
        wrapped_native="0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14",
        v2_router="0xeE567Fe1712Faf6149d80dA1E6934E354124CfE3",
        v2_factory="0xF62c03E08ada871A0bEb309762E260a7a6a880E6",
        v3_router="0x3bFA4769FB09eefC5a80d6E87c3B9C650f7Ae48E",
        v3_quoter="0xEd1f6473345F45b75F8179591dd5bA1888cf2FB3",
        v3_factory="0x0227628f3F023bb0B980b67D528571c95c6DaC1c",
        explorer_url="https://sepolia.etherscan.io",
        is_testnet=True,
    ),
}

RPC_ENV_VARS: dict[int, str] = {
    1: "ETH_RPC_URL",
    42161: "ARBITRUM_RPC_URL",
    10: "OPTIMISM_RPC_URL",
    8453: "BASE_RPC_URL",
    84532: "BASE_SEPOLIA",
    137: "POLYGON_RPC_URL",
    56: "BSC_RPC_URL",
    43114: "AVALANCHE_RPC_URL",
    50312: "SOMNIA_TESTNET_RPC_URL",
    11155111: "SEPOLIA_RPC_URL",
}

# Lookup by lowercase name for convenience (e.g. "ethereum", "base", "arbitrum one")
_NAME_INDEX: dict[str, ChainConfig] = {c.name.lower(): c for c in CHAINS.values()}
_CHAIN_ALIASES: dict[str, str] = {
    # Common shorthand aliases
    "eth": "ethereum",
    "ethereum mainnet": "ethereum",
    "arbitrum": "arbitrum one",
    "arb": "arbitrum one",
    "op": "optimism",
    "optimism mainnet": "optimism",
    "bsc": "bnb smart chain",
    "binance smart chain": "bnb smart chain",
    "matic": "polygon",
    "avax": "avalanche",
    "somnia": "somnia testnet",
    "somnia network": "somnia testnet",
}


def _ensure_rpc(chain: ChainConfig) -> ChainConfig:
    if chain.rpc_url:
        return chain
    env_var = RPC_ENV_VARS.get(chain.chain_id)
    if env_var:
        rpc_url = os.getenv(env_var, "").strip()
        if rpc_url:
            return replace(chain, rpc_url=rpc_url)
        raise RuntimeError(
            f"RPC URL for {chain.name} is not configured. Set {env_var}."
        )
    raise RuntimeError(f"RPC URL for {chain.name} is not configured.")


def find_chain_by_id(chain_id: int) -> ChainConfig:
    if chain_id not in CHAINS:
        raise KeyError(
            f"Chain ID {chain_id} is not registered. "
            f"Known chain IDs: {sorted(CHAINS.keys())}"
        )
    return CHAINS[chain_id]


def get_chain_by_id(chain_id: int) -> ChainConfig:
    return _ensure_rpc(find_chain_by_id(chain_id))


def find_chain_by_name(name: str) -> ChainConfig:
    key = name.strip().lower()
    key = _CHAIN_ALIASES.get(key, key)
    if key not in _NAME_INDEX:
        raise KeyError(
            f"Chain {name!r} is not registered. "
            f"Known chains: {sorted(_NAME_INDEX.keys())}"
        )
    return _NAME_INDEX[key]


def get_chain_by_name(name: str) -> ChainConfig:
    return _ensure_rpc(find_chain_by_name(name))


def supported_chains() -> list[str]:
    return list(_NAME_INDEX.keys())


def get_all_chains() -> list[ChainConfig]:
    return [_ensure_rpc(chain) for chain in CHAINS.values()]
