from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

from core.volume.redis_keys import (
    VOLUME_KEY_TTL_SECONDS,
    hour_bucket,
    volume_count_key,
    volume_key,
)

logger = logging.getLogger(__name__)


def _safe_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


async def _incr_volume(
    exec_type: str,
    chain: str,
    token: str,
    normalized_amount: Decimal,
    bucket: str,
) -> None:
    from core.utils.upstash_client import get_async_redis

    redis = await get_async_redis()
    if redis is None:
        # Redis not configured for this deployment — silently skip.
        return

    try:
        vk = volume_key(exec_type, chain, token, bucket)
        ck = volume_count_key(exec_type, chain, token, bucket)

        # Primary volume bucket: cumulative normalised token amount.
        # INCRBYFLOAT is atomic on the Redis side, so concurrent tasks
        # writing to the same bucket key are safe.
        await redis.incrbyfloat(vk, float(normalized_amount))
        await redis.expire(vk, VOLUME_KEY_TTL_SECONDS)

        # Execution count — one integer increment per execution.
        await redis.incrby(ck, 1)
        await redis.expire(ck, VOLUME_KEY_TTL_SECONDS)

    except Exception as exc:
        logger.warning(
            "[VOLUME] Redis INCR failed for %s/%s/%s: %s",
            exec_type,
            chain,
            token,
            exc,
            exc_info=False,
        )


def track_volume(
    exec_type: str,
    chain: str,
    token_symbol: str,
    normalized_amount: Decimal,
    dt: Optional[datetime] = None,
) -> None:
    if not exec_type or not chain or not token_symbol:
        return
    if normalized_amount is None or normalized_amount <= Decimal("0"):
        return

    bucket = hour_bucket(dt)

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(
            _incr_volume(
                exec_type=exec_type.strip().lower(),
                chain=chain.strip().lower(),
                token=token_symbol.strip().upper(),
                normalized_amount=normalized_amount,
                bucket=bucket,
            ),
            name=f"vol_incr:{exec_type}:{chain}:{token_symbol}",
        )
    except RuntimeError:
        # No running event loop — metrics are non-critical, discard safely.
        logger.debug(
            "[VOLUME] No running event loop — volume not tracked for %s/%s/%s.",
            exec_type,
            chain,
            token_symbol,
        )


def track_execution_volume(
    tool: str,
    resolved_args: Dict[str, Any],
    result: Dict[str, Any],
) -> None:
    try:
        if tool == "swap":
            token_symbol: str = (
                resolved_args.get("token_in_symbol")
                or resolved_args.get("token_out_symbol")
                or "UNKNOWN"
            )
            chain: str = result.get("chain") or resolved_args.get("chain") or "unknown"
            amount = _safe_decimal(
                result.get("amount_in") or resolved_args.get("amount_in")
            )

        elif tool == "bridge":
            token_symbol = (
                result.get("token_symbol")
                or resolved_args.get("token_symbol")
                or "UNKNOWN"
            )
            chain = (
                result.get("source_chain")
                or result.get("source_chain_name")
                or resolved_args.get("source_chain")
                or "unknown"
            )
            amount = _safe_decimal(
                result.get("input_amount") or resolved_args.get("amount")
            )

        elif tool == "solana_swap":
            token_symbol = (
                resolved_args.get("token_in_symbol")
                or resolved_args.get("token_out_symbol")
                or "UNKNOWN"
            )
            # Use the human-readable chain name from the result, falling back
            # to the network identifier from args.
            chain = (
                result.get("chain")
                or result.get("network")
                or resolved_args.get("network")
                or "solana"
            )
            amount = _safe_decimal(
                result.get("amount_in") or resolved_args.get("amount_in")
            )

        else:
            return

        if amount is None or amount <= Decimal("0"):
            return

        track_volume(
            exec_type=tool,
            chain=chain,
            token_symbol=token_symbol,
            normalized_amount=amount,
        )

    except Exception as exc:
        # Metrics must never affect the execution result under any circumstances.
        logger.debug("[VOLUME] track_execution_volume error: %s", exc)
