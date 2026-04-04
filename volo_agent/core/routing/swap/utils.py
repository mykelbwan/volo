from __future__ import annotations

from core.token_security.registry_lookup import (
    get_native_decimals,
    get_registry_decimals_by_address_async,
)


async def resolve_decimals(
    token_address: str,
    chain_id: int,
    *,
    zero_address: str,
    native_token_address: str,
) -> int:
    addr = token_address.strip().lower()
    if addr in (zero_address.lower(), native_token_address.lower()):
        return get_native_decimals(chain_id)

    cached = await get_registry_decimals_by_address_async(token_address, chain_id)
    if cached is None:
        raise ValueError(f"Could not resolve decimals for token {token_address}")

    return int(cached)
