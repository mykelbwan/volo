from typing import Any, Dict

from wallet_service.common.cdp_helpers import (
    build_deterministic_account_name,
)
from wallet_service.common.messages import format_with_recovery, require_non_empty_str
from wallet_service.evm.cdp_utils import (
    create_evm_account,
    create_evm_account_async,
    get_evm_account,
    get_evm_account_async,
)


def _require_expected_account_name(account: Any, *, expected_name: str) -> None:
    actual_name = getattr(account, "name", None)
    if actual_name is None or not isinstance(actual_name, (str, bytes)):
        return
    if str(actual_name).strip() != expected_name:
        raise RuntimeError(
            format_with_recovery(
                "CDP returned an unexpected EVM account identifier",
                "verify CDP account isolation before retrying provisioning",
            )
        )


def create_sub_org(volo_user_id: str) -> Dict[str, Any]:
    account_name = build_deterministic_account_name(
        require_non_empty_str(volo_user_id, field="volo_user_id")
    )
    try:
        account = create_evm_account(account_name)
    except Exception as exc:
        message = str(exc).lower()
        if "already" in message and "exist" in message:
            account = get_evm_account(account_name)
        else:
            raise RuntimeError(
                format_with_recovery(
                    f"Could not create EVM sub-org account for '{account_name}'",
                    "retry; if it persists, verify CDP credentials and account naming policy",
                )
            ) from exc

    _require_expected_account_name(account, expected_name=account_name)
    address = getattr(account, "address", None)
    if not address:
        raise KeyError(
            format_with_recovery(
                "CDP account response missing address",
                "verify the account in CDP and retry creation",
            )
        )

    return {"sub_org_id": account_name, "address": address}


async def create_sub_org_async(volo_user_id: str) -> Dict[str, Any]:
    account_name = build_deterministic_account_name(
        require_non_empty_str(volo_user_id, field="volo_user_id")
    )
    try:
        account = await create_evm_account_async(account_name)
    except Exception as exc:
        message = str(exc).lower()
        if "already" in message and "exist" in message:
            account = await get_evm_account_async(account_name)
        else:
            raise RuntimeError(
                format_with_recovery(
                    f"Could not create EVM sub-org account for '{account_name}'",
                    "retry; if it persists, verify CDP credentials and account naming policy",
                )
            ) from exc

    _require_expected_account_name(account, expected_name=account_name)
    address = getattr(account, "address", None)
    if not address:
        raise KeyError(
            format_with_recovery(
                "CDP account response missing address",
                "verify the account in CDP and retry creation",
            )
        )

    return {"sub_org_id": account_name, "address": address}
