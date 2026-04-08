from __future__ import annotations

import asyncio
import logging
import os
import re
from decimal import Decimal
from typing import Any, Dict, List, Optional

from config.solana_chains import fetch_solana_token_decimals, get_solana_chain
from core.memory.ledger import ErrorCategory, get_ledger
from core.routing.models import SolanaSwapRouteQuote
from core.routing.route_meta import (
    coerce_fallback_policy,
    enforce_fallback_policy,
    route_meta_matches_node,
)
from core.routing.scorer import pick_best_solana_swap
from core.utils.errors import NonRetryableError
from tool_nodes.common.input_utils import (
    format_with_recovery,
    parse_decimal_field,
    parse_float_field,
    require_fields,
    safe_decimal,
)
from wallet_service.common.transfer_idempotency import (
    canonicalize_decimal_idempotency_value,
    claim_transfer_idempotency,
    load_transfer_idempotency_claim,
    mark_transfer_failed,
    mark_transfer_inflight,
    mark_transfer_success,
    resolve_transfer_idempotency,
)
from wallet_service.solana.cdp_utils import send_solana_transaction_async
from wallet_service.solana.sign_tx import sign_transaction_async

_LOGGER = logging.getLogger("volo.tool.solana_swap")
_AGGREGATORS: List[Any] | None = None


def _get_aggregators() -> List[Any]:
    global _AGGREGATORS
    if _AGGREGATORS is None:
        from core.routing.solana.jupiter import JupiterAggregator
        from core.routing.solana.raydium import RaydiumAggregator

        _AGGREGATORS = [
            JupiterAggregator(),
            RaydiumAggregator(),
        ]
    return _AGGREGATORS


def _get_env_float(key: str, default: float) -> float:
    raw = str(os.getenv(key, "")).strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if value < 0:
        return default
    return value


# Once we receive the first viable quote, keep listening briefly for
# competitors, then execute to reduce long-tail latency.
_QUOTE_GRACE_SECONDS: float = min(
    max(_get_env_float("SOLANA_QUOTE_GRACE_SECONDS", 0.35), 0.0),
    3.0,
)


async def _fetch_quote_with_timeout(
    aggregator: Any,
    *,
    network: str,
    rpc_url: str,
    input_mint: str,
    output_mint: str,
    amount_in: Decimal,
    input_decimals: int,
    output_decimals: int,
    slippage_pct: float,
    sender: str,
    timeout: float,
) -> Optional[SolanaSwapRouteQuote]:
    try:
        return await asyncio.wait_for(
            aggregator.get_quote(
                network=network,
                rpc_url=rpc_url,
                input_mint=input_mint,
                output_mint=output_mint,
                amount_in=amount_in,
                input_decimals=input_decimals,
                output_decimals=output_decimals,
                slippage_pct=slippage_pct,
                sender=sender,
            ),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        _LOGGER.warning(
            "[solana_swap] %s timed out after %.1fs", aggregator.name, timeout
        )
        return None
    except Exception as exc:
        _LOGGER.warning("[solana_swap] %s raised: %s", aggregator.name, exc)
        return None


# Normal tokens: step up cautiously, stop well below user-visible pain.
_NORMAL_SLIPPAGE_STEP: float = 0.5
_NORMAL_SLIPPAGE_MAX: float = 5.0

# Volatile tokens (memecoins, Pump.fun bonding curves, low-cap SPL tokens):
# jump immediately to a level that can survive fast price action, then
# keep stepping up until the absolute Solana cap.
_VOLATILE_SLIPPAGE_JUMP: float = 15.0
_VOLATILE_SLIPPAGE_STEP: float = 10.0
_VOLATILE_SLIPPAGE_MAX: float = 50.0

# Keywords whose presence in an error message signals a high-volatility swap.
# Checked case-insensitively against the full error string.
_VOLATILE_KEYWORDS: tuple[str, ...] = (
    "price impact",  # Jupiter high-impact warning
    "bonding curve",  # Pump.fun phase-1 AMM
    "pump",  # Pump.fun general reference
    "pumpswap",  # Pump.fun native DEX (post-graduation)
    "volatile",  # Generic volatility warning
    "insufficient output",  # Jupiter: got far less than expected
    "price change",  # Generic rapid movement
    "price movement",
    "amm error",  # Generic AMM-level failure — often volatility
    "simulation failed",  # Solana simulation failures on volatile pools
)

# Regex to extract any percentage figure from an error message.
# Matches patterns like "12%", "5.3 %", "exceeded by 18.7%", etc.
_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")


def _is_high_volatility_slippage(err_msg: str, current_slippage: float) -> bool:
    # already trying a medium-high slippage with no luck.
    if current_slippage >= 3.0:
        return True

    low = err_msg.lower()

    # explicit keyword match.
    if any(kw in low for kw in _VOLATILE_KEYWORDS):
        return True

    # large explicit percentage in the error text.
    for match in _PCT_RE.finditer(err_msg):
        try:
            if float(match.group(1)) >= 5.0:
                return True
        except ValueError:
            pass

    return False


def _classify_broadcast_error(err_msg: str) -> str:
    low = err_msg.lower()

    if any(k in low for k in ("slippage", "price", "exceeded", "tolerance")):
        return (
            "The price moved while we were placing your swap. "
            "We'll automatically try again with a slightly higher tolerance."
        )

    if any(k in low for k in ("insufficient", "balance", "funds", "lamport")):
        return (
            "Your wallet doesn't have enough SOL to cover this swap and the "
            "transaction fee. Add a little more SOL and try again."
        )

    if any(k in low for k in ("blockhash", "expired", "block")):
        return (
            "The transaction took too long to process and expired. "
            "Please try again — it usually goes through on the next attempt."
        )

    if any(k in low for k in ("timeout", "network", "connect", "unreachable")):
        return "Solana is having trouble right now. Please wait a moment and try again."

    return (
        "The swap didn't go through. "
        "Please try again, or use a smaller amount if the problem continues."
    )


def _route_meta_contains_untrusted_solana_tx(route_meta: Any) -> bool:
    if not isinstance(route_meta, dict):
        return False
    return bool(route_meta.get("swap_transaction") or route_meta.get("calldata"))


async def solana_swap_token(parameters: Dict[str, Any]) -> Dict[str, Any]:
    token_in_symbol: str = str(parameters.get("token_in_symbol") or "token")
    token_out_symbol: str = str(parameters.get("token_out_symbol") or "token")
    token_in_mint: str = str(parameters.get("token_in_mint") or "").strip()
    token_out_mint: str = str(parameters.get("token_out_mint") or "").strip()
    amount_in_raw = parameters.get("amount_in")
    network: str = str(parameters.get("network") or "solana").strip().lower()
    slippage_raw = parameters.get("slippage", 0.5)
    sub_org_id: str = str(parameters.get("sub_org_id") or "").strip()
    sender: str = str(parameters.get("sender") or "").strip()

    require_fields(
        {
            "token_in_mint": token_in_mint,
            "token_out_mint": token_out_mint,
            "amount_in": amount_in_raw,
            "sub_org_id": sub_org_id,
            "sender": sender,
        },
        ["token_in_mint", "token_out_mint", "amount_in", "sub_org_id", "sender"],
        context="solana swap",
        exception_cls=NonRetryableError,
    )

    amount_in = parse_decimal_field(
        amount_in_raw,
        field="amount_in",
        positive=True,
        exception_cls=NonRetryableError,
        invalid_recovery="enter a positive numeric amount (for example, 1.5)",
    )
    slippage = parse_float_field(
        slippage_raw,
        field="slippage",
        min_value=0.01,
        max_value=50.0,
        exception_cls=NonRetryableError,
        invalid_recovery="use a slippage value between 0.01 and 50",
    )
    if token_in_mint == token_out_mint:
        raise NonRetryableError(
            format_with_recovery(
                "Input and output token mints are the same",
                "choose different tokens and retry",
            )
        )

    try:
        chain = get_solana_chain(network)
    except KeyError:
        raise NonRetryableError(
            f"'{network}' isn't a recognised Solana network. "
            "Use 'solana' for mainnet or 'solana-devnet' for devnet."
        )

    idempotency_fields = {
        "sender": sender.strip(),
        "chain_id": int(chain.chain_id),
        "tool_name": "solana_swap",
        "token_in_mint": token_in_mint,
        "token_out_mint": token_out_mint,
        "amount_in": canonicalize_decimal_idempotency_value(amount_in),
    }
    idempotency_key, idempotency_fields, request_id = resolve_transfer_idempotency(
        tool_name="solana_swap",
        request_fields=idempotency_fields,
        external_key=parameters.get("idempotency_key"),
        request_id=parameters.get("request_id"),
    )
    claim = await claim_transfer_idempotency(
        operation="solana_swap",
        idempotency_key=idempotency_key,
        request_fields=idempotency_fields,
    )
    if claim is not None and claim.reused:
        if claim.result:
            return dict(claim.result)
        if claim.tx_hash:
            return {
                "status": "success" if claim.status == "success" else "pending",
                "tx_hash": claim.tx_hash,
                "signature": claim.tx_hash,
                "message": f"Swap already submitted. Transaction ID: {claim.tx_hash}.",
            }

    current_claim = await load_transfer_idempotency_claim(claim)
    if current_claim is not None and current_claim.tx_hash and current_claim.result:
        return dict(current_claim.result)

    route_meta: Dict[str, Any] = parameters.get("_route_meta") or {}
    fallback_policy = coerce_fallback_policy(parameters.get("_fallback_policy"))
    if route_meta.get("invalid") is True:
        raise NonRetryableError(
            format_with_recovery(
                "The planned Solana route is invalid",
                "request a fresh route and retry",
            )
        )
    if route_meta and not route_meta_matches_node(
        tool="solana_swap",
        route_meta=route_meta,
        resolved_args={
            "network": network,
            "token_in_mint": token_in_mint,
            "token_out_mint": token_out_mint,
        },
    ):
        raise NonRetryableError(
            format_with_recovery(
                "The planned Solana route metadata does not match the requested swap",
                "request a fresh route and retry",
            )
        )
    if _route_meta_contains_untrusted_solana_tx(route_meta):
        raise NonRetryableError("Untrusted precomputed transaction data is not allowed")

    swap_transaction: Optional[str] = None
    winning_aggregator: str = route_meta.get("aggregator") or ""
    actual_amount_out_str: Optional[str] = (
        str(route_meta.get("amount_out"))
        if route_meta.get("amount_out") is not None
        else None
    )
    amount_out_min_str: Optional[str] = route_meta.get("amount_out_min")

    if not swap_transaction:
        aggregators = _get_aggregators()
        if not aggregators:
            raise RuntimeError(
                format_with_recovery(
                    "No Solana quote providers are configured",
                    "enable at least one aggregator and retry",
                )
            )
        _LOGGER.info(
            "[solana_swap] no pre-built transaction — fetching live quotes for "
            "%s → %s on %s",
            token_in_symbol,
            token_out_symbol,
            chain.network,
        )

        # Fetch token decimals for both mints in parallel.
        try:
            input_decimals, output_decimals = await asyncio.gather(
                fetch_solana_token_decimals(token_in_mint, chain.rpc_url),
                fetch_solana_token_decimals(token_out_mint, chain.rpc_url),
            )
        except Exception as exc:
            raise RuntimeError(
                format_with_recovery(
                    "We couldn't verify the token decimals for this Solana swap",
                    "retry in a moment so we can fetch fresh token metadata",
                )
            ) from exc

        # Query all aggregators concurrently.
        quote_tasks = {
            asyncio.create_task(
                _fetch_quote_with_timeout(
                    agg,
                    network=chain.network,
                    rpc_url=chain.rpc_url,
                    input_mint=token_in_mint,
                    output_mint=token_out_mint,
                    amount_in=amount_in,
                    input_decimals=input_decimals,
                    output_decimals=output_decimals,
                    slippage_pct=slippage,
                    sender=sender,
                    timeout=agg.TIMEOUT_SECONDS,
                )
            )
            for agg in aggregators
        }

        quotes: List[SolanaSwapRouteQuote] = []
        loop = asyncio.get_running_loop()
        grace_deadline: float | None = None
        pending = set(quote_tasks)
        try:
            while pending:
                now = loop.time()
                if grace_deadline is not None and now >= grace_deadline:
                    break

                timeout = None
                if grace_deadline is not None:
                    timeout = max(0.0, grace_deadline - now)

                done, pending = await asyncio.wait(
                    pending,
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    break

                for task in done:
                    try:
                        result = task.result()
                    except Exception as exc:
                        _LOGGER.warning(
                            "[solana_swap] quote task failed unexpectedly: %s",
                            exc,
                        )
                        continue
                    if result is None:
                        continue
                    quotes.append(result)
                    if grace_deadline is None:
                        grace_deadline = loop.time() + _QUOTE_GRACE_SECONDS
        finally:
            if pending:
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        if not quotes:
            raise RuntimeError(
                format_with_recovery(
                    (
                        "We couldn't find a swap route for "
                        f"{token_in_symbol} → {token_out_symbol} on Solana right now"
                    ),
                    "retry in a moment, then try a smaller amount or higher slippage",
                )
            )

        # Score and select the best quote.
        ledger = get_ledger()
        best_result = pick_best_solana_swap(quotes, ledger)
        if best_result is None:
            raise RuntimeError(
                format_with_recovery(
                    "We found quotes but couldn't score them safely",
                    "retry in a moment; if it persists, request a fresh quote",
                )
            )

        best_quote, best_score = best_result
        winning_aggregator = best_quote.aggregator
        actual_amount_out_str = str(best_quote.amount_out)
        amount_out_min_str = str(best_quote.amount_out_min)
        swap_transaction = best_quote.swap_transaction

        _LOGGER.info(
            "[solana_swap] selected %s (score=%.4f, out=%s %s, impact=%s%%)",
            winning_aggregator,
            best_score,
            best_quote.amount_out,
            token_out_symbol,
            best_quote.price_impact_pct,
        )

        # If the winning quote has no transaction (e.g. rate-limited at build step),
        # try the runner-up quotes in score order.
        if not swap_transaction:
            fallback_candidates = sorted(
                (
                    q
                    for q in quotes
                    if q.aggregator != winning_aggregator and q.swap_transaction
                ),
                key=lambda q: q.amount_out,
                reverse=True,
            )
            for q in fallback_candidates:
                enforce_fallback_policy(
                    policy=fallback_policy,
                    detail=f"winning {winning_aggregator} quote had no transaction; attempting {q.aggregator}",
                )
                _LOGGER.info(
                    "[solana_swap] winner has no tx — using %s transaction instead",
                    q.aggregator,
                )
                swap_transaction = q.swap_transaction
                winning_aggregator = q.aggregator
                actual_amount_out_str = str(q.amount_out)
                amount_out_min_str = str(q.amount_out_min)
                break

        if not swap_transaction:
            raise RuntimeError(
                format_with_recovery(
                    "We found a good price but couldn't build the swap transaction",
                    "retry in a moment so providers can rebuild the transaction",
                )
            )

    try:
        signed_tx: str = await sign_transaction_async(
            sub_org_id,
            swap_transaction,
            sender,
        )
    except Exception as exc:
        _LOGGER.error("[solana_swap] signing failed: %s", exc)
        await mark_transfer_failed(claim, error=str(exc))
        raise NonRetryableError(
            format_with_recovery(
                "We weren't able to sign your transaction",
                "verify wallet setup/permissions, then retry",
            )
        ) from exc

    try:
        signature: str = await send_solana_transaction_async(
            signed_tx,
            chain.network,
        )
    except Exception as exc:
        err_msg = str(exc)
        _LOGGER.error("[solana_swap] broadcast failed: %s", err_msg)
        await mark_transfer_failed(claim, error=err_msg)
        plain_reason = _classify_broadcast_error(err_msg)
        raise RuntimeError(
            format_with_recovery(plain_reason, "retry in a moment")
        ) from exc

    await mark_transfer_inflight(
        claim,
        tx_hash=signature,
        result={
            "status": "pending",
            "tx_hash": signature,
            "signature": signature,
            "protocol": winning_aggregator,
            "network": chain.network,
            "chain": chain.name,
            "token_in_symbol": token_in_symbol,
            "token_out_symbol": token_out_symbol,
            "amount_in": str(amount_in),
            "amount_out": actual_amount_out_str or amount_out_min_str or "0",
            "amount_out_minimum": amount_out_min_str,
            "explorer_url": f"{chain.explorer_url}/tx/{signature}",
            "route_meta_used": bool(route_meta),
            "request_id": request_id,
            "message": f"Swap submitted. Transaction ID: {signature}.",
        },
    )

    amount_in_display = f"{amount_in:f}".rstrip("0").rstrip(".")
    message = (
        f"Done! Your swap of {amount_in_display} {token_in_symbol} "
        f"to {token_out_symbol} on Solana went through."
    )

    _LOGGER.info(
        "[solana_swap] success  sig=%s  aggregator=%s  amount_in=%s %s",
        signature[:16],
        winning_aggregator,
        amount_in_display,
        token_in_symbol,
    )

    response = {
        "status": "success",
        "tx_hash": signature,
        "signature": signature,
        "protocol": winning_aggregator,
        "network": chain.network,
        "chain": chain.name,
        "token_in_symbol": token_in_symbol,
        "token_out_symbol": token_out_symbol,
        "amount_in": str(amount_in),
        "amount_out": actual_amount_out_str or amount_out_min_str or "0",
        "amount_out_minimum": amount_out_min_str,
        "explorer_url": f"{chain.explorer_url}/tx/{signature}",
        "route_meta_used": bool(route_meta),
        "request_id": request_id,
        "message": message,
    }
    await mark_transfer_success(claim, tx_hash=signature, result=response)
    return response


def suggest_solana_swap_fix(
    category: ErrorCategory,
    args: Dict[str, Any],
    msg: str,
) -> Optional[Dict[str, Any]]:
    if category == ErrorCategory.SLIPPAGE:
        current = float(args.get("slippage", 0.5))

        if _is_high_volatility_slippage(msg, current):
            if current >= _VOLATILE_SLIPPAGE_MAX:
                # Already at the absolute Solana cap — stop retrying.
                return None

            if current < _VOLATILE_SLIPPAGE_JUMP:
                # First volatile retry: jump straight to 15 % regardless
                # of how low the current tolerance is.
                target = _VOLATILE_SLIPPAGE_JUMP
            else:
                # Subsequent volatile retries: step up by 10 % at a time.
                target = current + _VOLATILE_SLIPPAGE_STEP

            new_args = args.copy()
            new_args["slippage"] = round(min(target, _VOLATILE_SLIPPAGE_MAX), 2)
            _LOGGER.info(
                "[solana_swap] volatile slippage detected — retrying at %.1f%%  "
                "(was %.1f%%, max %.0f%%)",
                new_args["slippage"],
                current,
                _VOLATILE_SLIPPAGE_MAX,
            )
            return new_args

        else:
            if current >= _NORMAL_SLIPPAGE_MAX:
                # Hit the normal cap — stop retrying on this track.
                return None

            new_args = args.copy()
            new_args["slippage"] = round(
                min(current + _NORMAL_SLIPPAGE_STEP, _NORMAL_SLIPPAGE_MAX), 2
            )
            _LOGGER.info(
                "[solana_swap] normal slippage retry — %.1f%% → %.1f%%",
                current,
                new_args["slippage"],
            )
            return new_args

    if category == ErrorCategory.LIQUIDITY:
        current_amount = safe_decimal(args.get("amount_in"))
        if current_amount and current_amount > Decimal("0"):
            new_args = args.copy()
            new_args["amount_in"] = float(current_amount * Decimal("0.9"))
            return new_args

    return None
