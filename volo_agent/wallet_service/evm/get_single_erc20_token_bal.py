from decimal import Decimal

from config.abi import ERC20_ABI
from core.utils.evm_async import get_shared_async_web3
from wallet_service.common.messages import format_with_recovery, require_non_empty_str


async def get_token_balance_async(
    wallet_address: str, token_address: str, decimals: int, http_provider: str
) -> Decimal:
    provider = require_non_empty_str(http_provider, field="http_provider")
    address = require_non_empty_str(wallet_address, field="wallet_address")
    token = require_non_empty_str(token_address, field="token_address")
    if not isinstance(decimals, int) or decimals < 0:
        raise ValueError(
            format_with_recovery(
                "Invalid token decimals",
                "provide decimals as a non-negative integer and retry",
            )
        )
    # Reuse the shared AsyncWeb3 transport so frequent token lookups do not
    # rebuild HTTP connection pools on the hot path.
    w3 = await get_shared_async_web3(provider)
    try:
        checksum_wallet = w3.to_checksum_address(address)
        checksum_token = w3.to_checksum_address(token)
    except Exception as exc:
        raise ValueError(
            format_with_recovery(
                "Invalid wallet or token address",
                "provide valid hex addresses and retry",
            )
        ) from exc

    contract = w3.eth.contract(address=checksum_token, abi=ERC20_ABI)

    raw_balance = await contract.functions.balanceOf(checksum_wallet).call()

    if raw_balance == 0:
        return Decimal(0)

    return Decimal(raw_balance) / Decimal(10**decimals)
