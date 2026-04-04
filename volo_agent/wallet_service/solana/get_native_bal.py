from decimal import Decimal

from solders.pubkey import Pubkey

from config.solana_chains import get_solana_chain
from wallet_service.common.messages import require_non_empty_str
from wallet_service.solana.rpc_client import get_shared_solana_client


async def get_native_balance_async(
    wallet_address: str, network: str | None = None
) -> Decimal:
    wallet = require_non_empty_str(wallet_address, field="wallet_address")
    chain_config = get_solana_chain(network or "solana")
    client = await get_shared_solana_client(chain_config.rpc_url)
    pubkey = Pubkey.from_string(wallet)
    response = await client.get_balance(pubkey)
    balance_lamports = response.value
    balance_sol = Decimal(balance_lamports) / Decimal(1_000_000_000)
    return balance_sol
