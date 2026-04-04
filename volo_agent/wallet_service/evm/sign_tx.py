from typing import Any, Mapping

from wallet_service.common.messages import (
    format_with_recovery,
    require_mapping,
    require_non_empty_str,
)
from wallet_service.evm.cdp_utils import sign_evm_transaction_by_name_async


async def sign_transaction_async(
    sub_org_id: str, unsigned_tx: Mapping[str, Any], sign_with: str
) -> str:
    sub_org = require_non_empty_str(sub_org_id, field="sub_org_id")
    signer = require_non_empty_str(sign_with, field="sign_with")
    tx_fields = require_mapping(unsigned_tx, field="unsigned_tx")

    if "gasPrice" in tx_fields:
        raise ValueError(
            format_with_recovery(
                "Legacy gasPrice is not supported",
                "use EIP-1559 fields 'maxFeePerGas' and 'maxPriorityFeePerGas'",
            )
        )

    return await sign_evm_transaction_by_name_async(
        sub_org, dict(tx_fields), sign_with=signer
    )
