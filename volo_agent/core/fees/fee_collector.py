from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, Optional

from core.fees.models import FeeQuote

logger = logging.getLogger(__name__)


class FeeCollectionError(Exception):
    """Raised when fee collection fails after the main step has succeeded."""


async def _collect_evm_fee(
    quote: FeeQuote,
    node_args: Dict[str, Any],
) -> str:
    sender: str = node_args.get("sender", "")
    sub_org_id: str = node_args.get("sub_org_id", "")
    if not sender or not sub_org_id:
        raise FeeCollectionError(
            "Fee collection skipped because the EVM wallet details were incomplete. "
            "Please retry after wallet setup finishes."
        )

    from wallet_service.evm.native_transfer import execute_native_transfer

    return await execute_native_transfer(
        to=quote.fee_recipient,
        amount_native=quote.fee_amount_native,
        chain_name=quote.chain,
        sender=sender,
        sub_org_id=sub_org_id,
    )


async def _collect_solana_fee(
    quote: FeeQuote,
    node_args: Dict[str, Any],
) -> str:
    sender: str = str(node_args.get("sender") or "").strip()
    sub_org_id: str = str(node_args.get("sub_org_id") or "").strip()
    if not sender or not sub_org_id:
        raise FeeCollectionError(
            "Fee collection skipped because the Solana wallet details were incomplete. "
            "Please retry after wallet setup finishes."
        )

    from wallet_service.solana.native_transfer import execute_native_transfer

    return await execute_native_transfer(
        sender=sender,
        sub_org_id=sub_org_id,
        recipient=quote.fee_recipient,
        amount_native=quote.fee_amount_native,
        network=quote.chain_network or "solana",
    )


_COLLECTORS: dict[str, Callable[[FeeQuote, Dict[str, Any]], Awaitable[str]]] = {
    "evm": _collect_evm_fee,
    "solana": _collect_solana_fee,
}


async def collect_fee(
    quote: FeeQuote,
    node_args: Dict[str, Any],
) -> Optional[str]:
    """
    Execute the platform fee as a native transfer to the treasury.

    Called inside execution_engine_node immediately after a main step
    succeeds.  Failures are intentionally non-fatal — the main transaction
    has already been committed on-chain, so we log the error and let the
    graph continue rather than surfacing a confusing failure to the user.

    Args:
        quote:      The FeeQuote computed by FeeEngine for this node.
        node_args:  The resolved args dict of the corresponding PlanNode.
                    Must contain "sender" and "sub_org_id".

    Returns:
        The fee transfer tx hash on success, or None if collection was
        skipped (treasury not configured, zero-fee, or missing wallet info).

    Raises:
        FeeCollectionError: If the transfer is attempted but fails
                            (caller should catch and log, not re-raise).
    """
    # ── Guard: nothing to collect ────────────────────────────────────────────
    if not quote.fee_recipient:
        logger.warning("[FEE] No treasury address configured — fee skipped.")
        return None

    if quote.fee_amount_native <= Decimal("0"):
        logger.debug("[FEE] Zero-fee quote for node %s — skipped.", quote.node_id)
        return None

    collector = _COLLECTORS.get(str(quote.chain_family or "").strip().lower())
    if collector is None:
        logger.error(
            "[FEE] Cannot collect fee for node %s: unsupported chain family %r.",
            quote.node_id,
            quote.chain_family,
        )
        return None

    try:
        tx_hash = await collector(quote, node_args)
        logger.info(
            "[FEE] Collected %s for node '%s' → tx: %s",
            quote.formatted_amount(),
            quote.node_id,
            tx_hash,
        )
        return tx_hash

    except FeeCollectionError:
        raise
    except Exception as exc:
        raise FeeCollectionError(
            f"Fee collection failed for node '{quote.node_id}' "
            f"({quote.formatted_amount()} on {quote.chain}): {exc}"
        ) from exc
