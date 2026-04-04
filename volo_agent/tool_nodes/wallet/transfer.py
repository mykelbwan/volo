from __future__ import annotations

from typing import Any, Dict, Optional

from core.memory.ledger import ErrorCategory
from core.transfers.chains import get_transfer_chain_spec
from core.transfers.handlers import get_transfer_handler
from core.transfers.models import (
    NormalizedTransferRequest,
    TransferExecutionResult,
    normalize_transfer_request,
)
from tool_nodes.common.input_utils import format_with_recovery


def _reject_malformed_request() -> None:
    raise ValueError(
        format_with_recovery(
            "Malformed normalized transfer request",
            "retry the transfer with complete supported inputs",
        )
    )


def _validate_normalized_request(
    request: Any,
) -> NormalizedTransferRequest:
    if not isinstance(request, NormalizedTransferRequest):
        _reject_malformed_request()

    required_fields = (
        request.asset_symbol,
        request.recipient,
        request.network,
        request.sender,
        request.sub_org_id,
    )
    if any(not str(value or "").strip() for value in required_fields):
        _reject_malformed_request()

    if request.amount <= 0:
        _reject_malformed_request()

    return request


def _resolve_dispatch_chain_spec(request: NormalizedTransferRequest):
    try:
        chain_spec = get_transfer_chain_spec(request.network)
    except KeyError as exc:
        raise ValueError(
            format_with_recovery(
                f"Unsupported transfer network: {request.network!r}",
                "use a supported network and retry",
            )
        ) from exc

    if chain_spec.network != request.network:
        _reject_malformed_request()

    if request.requested_network is not None:
        try:
            requested_chain_spec = get_transfer_chain_spec(request.requested_network)
        except KeyError as exc:
            raise ValueError(
                format_with_recovery(
                    "Malformed normalized transfer request",
                    "retry the transfer with complete supported inputs",
                )
            ) from exc
        if requested_chain_spec.network != chain_spec.network:
            _reject_malformed_request()

    return chain_spec


def _get_dispatch_handler(*, family: Any, display_name: str) -> Any:
    normalized_family = str(family or "").strip().lower()
    if not normalized_family:
        raise ValueError(
            format_with_recovery(
                "Malformed transfer chain specification",
                "retry the transfer on a supported network",
            )
        )

    handler = get_transfer_handler(normalized_family)
    if handler is None:
        raise ValueError(
            format_with_recovery(
                f"Transfers on {display_name} are not available in this flow yet",
                "retry on a supported network for this transfer flow",
            )
        )
    return handler


def _format_transfer_response(result: TransferExecutionResult) -> Dict[str, Any]:
    return {
        "status": result.status,
        "tx_hash": result.tx_hash,
        "asset_symbol": result.asset_symbol,
        "token_symbol": result.asset_symbol,
        "amount": str(result.amount),
        "recipient": result.recipient,
        "network": result.network,
        "chain": result.network,
        "message": result.message,
    }


async def transfer_token(parameters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a token transfer (native or token) on a specific network.
    """
    request = _validate_normalized_request(normalize_transfer_request(parameters))
    chain_spec = _resolve_dispatch_chain_spec(request)
    handler = _get_dispatch_handler(
        family=chain_spec.family,
        display_name=chain_spec.display_name,
    )
    result = await handler.execute_transfer(request, chain_spec)
    return _format_transfer_response(result)


def suggest_transfer_fix(
    category: ErrorCategory, args: Dict[str, Any], msg: str
) -> Optional[Dict[str, Any]]:
    """
    Suggests fixes for transfer failures.
    """
    if category == ErrorCategory.GAS:
        # Transfer gas errors usually just need a retry with fresh price
        return args.copy()

    return None
