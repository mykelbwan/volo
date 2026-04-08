from typing import Any

from wallet_service.common.messages import require_non_empty_str
from wallet_service.solana.cdp_utils import sign_solana_transaction_by_name_async


async def sign_transaction_async(
    sub_org_id: str, transaction_b64: Any, sign_with: str | None = None
) -> str:
    sub_org = require_non_empty_str(sub_org_id, field="sub_org_id")
    tx_b64 = require_non_empty_str(transaction_b64, field="transaction_b64")
    return await sign_solana_transaction_by_name_async(
        sub_org, tx_b64, sign_with=sign_with
    )
