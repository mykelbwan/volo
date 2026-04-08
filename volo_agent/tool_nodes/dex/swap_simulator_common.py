from __future__ import annotations

from typing import Any, Optional

from config.abi import ERC20_ABI
from config.chains import ChainConfig, get_chain_by_id, get_chain_by_name
from core.token_security.registry_lookup import get_registry_decimals_by_address
from core.utils.evm_async import make_async_web3

NATIVE_TOKEN_ADDRESS = "0x0000000000000000000000000000000000000000"


def _get_web3(chain: ChainConfig) -> Any:
    return make_async_web3(chain.rpc_url)


def _resolve_chain(chain_id: Optional[int], chain_name: Optional[str]) -> ChainConfig:
    if chain_id is not None:
        return get_chain_by_id(chain_id)
    if chain_name is not None:
        return get_chain_by_name(chain_name)
    raise ValueError("Provide either chain_id or chain_name.")


def _is_zero_native(address: str) -> bool:
    return address.lower() == NATIVE_TOKEN_ADDRESS


def _is_native(address: str, chain: ChainConfig) -> bool:
    _ = chain
    return _is_zero_native(address)


def _is_wrapped_native(address: str, chain: ChainConfig) -> bool:
    return (
        bool(chain.wrapped_native) and address.lower() == chain.wrapped_native.lower()
    )


async def _get_token_decimals(w3: Any, token_address: str, chain_id: int) -> int:
    cached = get_registry_decimals_by_address(token_address, chain_id)
    if cached is not None:
        return cached
    contract = w3.eth.contract(
        address=w3.to_checksum_address(token_address),
        abi=ERC20_ABI,
    )
    return int(await contract.functions.decimals().call())


async def _get_allowance(w3: Any, token_address: str, owner: str, spender: str) -> int:
    contract = w3.eth.contract(
        address=w3.to_checksum_address(token_address),
        abi=ERC20_ABI,
    )
    return int(
        await contract.functions.allowance(
            w3.to_checksum_address(owner),
            w3.to_checksum_address(spender),
        ).call()
    )
