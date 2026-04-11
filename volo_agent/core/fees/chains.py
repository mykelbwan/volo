from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from config.chains import get_chain_by_name
from config.solana_chains import get_solana_chain

_EVM_NATIVE = "0x0000000000000000000000000000000000000000"


@dataclass(frozen=True)
class FeeChain:
    family: str
    name: str
    network: str
    native_symbol: str
    is_testnet: bool
    native_token_ref: str
    wrapped_native_ref: Optional[str] = None


def resolve_fee_chain(node_args: dict[str, Any]) -> FeeChain | None:
    # For bridges, source_chain is the canonical chain where the transaction starts
    # and where fees are typically collected. We prioritize it over the generic 'chain'
    # which might be injected as a default for the entire plan.
    raw_chain = (
        node_args.get("source_chain")
        or node_args.get("chain")
        or node_args.get("network")
    )
    if not raw_chain:
        return None

    chain_name = str(raw_chain).strip()
    if not chain_name:
        return None

    try:
        chain = get_chain_by_name(chain_name)
        return FeeChain(
            family="evm",
            name=chain.name,
            network=chain.name,
            native_symbol=chain.native_symbol,
            is_testnet=bool(chain.is_testnet),
            native_token_ref=_EVM_NATIVE,
            wrapped_native_ref=chain.wrapped_native,
        )
    except KeyError:
        pass

    try:
        chain = get_solana_chain(chain_name)
        return FeeChain(
            family="solana",
            name=chain.name,
            network=chain.network,
            native_symbol=chain.native_symbol,
            is_testnet=bool(chain.is_testnet),
            native_token_ref=chain.native_mint,
            wrapped_native_ref=chain.native_mint,
        )
    except KeyError:
        return None


def is_native_token(token_ref: str | None, chain: FeeChain) -> bool:
    token = str(token_ref or "").strip().lower()
    if not token:
        return False

    if token == str(chain.native_token_ref).strip().lower():
        return True
    if chain.wrapped_native_ref and token == str(chain.wrapped_native_ref).strip().lower():
        return True
    return False
