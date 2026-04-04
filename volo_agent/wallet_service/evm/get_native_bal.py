from decimal import Decimal

from core.utils.evm_async import get_shared_async_web3
from wallet_service.common.messages import format_with_recovery, require_non_empty_str


async def get_native_balance_async(wallet_address: str, http_provider: str) -> Decimal:
    provider = require_non_empty_str(http_provider, field="http_provider")
    address = require_non_empty_str(wallet_address, field="wallet_address")
    # Shared AsyncWeb3 transport keeps RPC connections warm across balance reads.
    w3 = await get_shared_async_web3(provider)
    try:
        checksum_address = w3.to_checksum_address(address)
    except Exception as exc:
        raise ValueError(
            format_with_recovery(
                f"Invalid Ethereum wallet address: {wallet_address!r}",
                "provide a valid hex address and retry",
            )
        ) from exc
    
    balance_wei = await w3.eth.get_balance(checksum_address)
    return Decimal(w3.from_wei(balance_wei, "ether"))
