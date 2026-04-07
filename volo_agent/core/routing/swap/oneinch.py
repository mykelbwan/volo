from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional

from config.chains import CHAINS
from core.routing.models import SwapRouteQuote
from core.routing.swap.base import SwapAggregator
from core.routing.swap.utils import resolve_decimals as _resolve_decimals
from core.utils.http import (
    ExternalServiceError,
    async_raise_for_status,
    async_request_json,
)
from core.utils.timeouts import EXTERNAL_HTTP_TIMEOUT_SECONDS

_LOGGER = logging.getLogger("volo.routing.swap.1inch")
_API_BASE_URL = "https://api.1inch.dev/swap/v6.0"

# 1inch uses this sentinel address instead of the zero-address for native tokens.
_NATIVE_TOKEN_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeeeE"
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _api_key() -> Optional[str]:
    return os.getenv("ONEINCH_API_KEY", "").strip() or None


def _auth_headers() -> Dict[str, str]:
    key = _api_key()
    if not key:
        return {}
    return {"Authorization": f"Bearer {key}"}


def _normalise_token_address(address: str, chain_id: int) -> str:
    addr = address.strip().lower()

    # Check against our chain-specific aliases (e.g. 0x0, 0xeee).
    chain = CHAINS.get(chain_id)
    if chain:
        aliases = [a.lower() for a in chain.native_token_aliases]
        if addr in aliases:
            return _NATIVE_TOKEN_ADDRESS

    if addr == _ZERO_ADDRESS:
        return _NATIVE_TOKEN_ADDRESS
    return address


def _to_wei(amount: Decimal, decimals: int) -> int:
    """Convert a human-readable token amount to integer wei."""
    return int(amount * Decimal(10**decimals))


def _from_wei(amount_wei: int, decimals: int) -> Decimal:
    """Convert an integer wei amount to human-readable token units."""
    return Decimal(amount_wei) / Decimal(10**decimals)


async def _fetch_quote(
    chain_id: int,
    src: str,
    dst: str,
    amount_wei: int,
    timeout: float,
) -> Dict[str, Any]:
    """
    Call the 1inch /quote endpoint.
    Returns the parsed JSON response dict.
    Raises on HTTP error so the caller can handle it.
    """
    url = f"{_API_BASE_URL}/{chain_id}/quote"
    params = {
        "src": src,
        "dst": dst,
        "amount": str(amount_wei),
        "includeGas": "true",
        "includePriceImpact": "true",
    }
    resp = await async_request_json(
        "GET",
        url,
        params=params,
        headers=_auth_headers(),
        timeout=timeout,
        service="1inch-quote",
    )
    await async_raise_for_status(resp, "1inch-quote")
    return resp.json()


async def _fetch_swap(
    chain_id: int,
    src: str,
    dst: str,
    amount_wei: int,
    sender: str,
    slippage_pct: float,
    timeout: float,
) -> Dict[str, Any]:
    url = f"{_API_BASE_URL}/{chain_id}/swap"
    params = {
        "src": src,
        "dst": dst,
        "amount": str(amount_wei),
        "from": sender,
        # 1inch expects slippage as a plain percentage, e.g. "1" for 1 %.
        "slippage": str(slippage_pct),
        "disableEstimate": "false",
        "includeGas": "true",
        "includePriceImpact": "true",
    }
    resp = await async_request_json(
        "GET",
        url,
        params=params,
        headers=_auth_headers(),
        timeout=timeout,
        service="1inch-swap",
    )
    await async_raise_for_status(resp, "1inch-swap")
    return resp.json()


class OneInchAggregator(SwapAggregator):
    name: str = "1inch"
    TIMEOUT_SECONDS: float = 5.0

    async def get_quote(
        self,
        *,
        chain_id: int,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_pct: float,
        sender: str,
    ) -> Optional[SwapRouteQuote]:

        if not _api_key():
            self._log_debug("ONEINCH_API_KEY not set — skipping")
            return None

        # ── Guard: check if chain is known/supported ──────────────────────
        chain = CHAINS.get(chain_id)
        if not chain:
            self._log_debug(f"chain_id {chain_id} not in global config")
            return None

        if chain.is_testnet:
            # 1inch v6 generally only supports mainnets.
            self._log_debug(f"chain_id {chain_id} is a testnet — 1inch skip")
            return None

        # ── Normalise token addresses ─────────────────────────────────────
        src = _normalise_token_address(token_in, chain_id)
        dst = _normalise_token_address(token_out, chain_id)

        # ── Resolve decimals ──────────────────────────────────────────────
        try:
            in_decimals = await _resolve_decimals(
                token_in,
                chain_id,
                zero_address=_ZERO_ADDRESS,
                native_token_address=_NATIVE_TOKEN_ADDRESS,
            )
            out_decimals = await _resolve_decimals(
                token_out,
                chain_id,
                zero_address=_ZERO_ADDRESS,
                native_token_address=_NATIVE_TOKEN_ADDRESS,
            )
        except Exception as exc:
            self._log_failure("decimal resolution failed", exc)
            return None

        amount_wei = _to_wei(amount_in, in_decimals)
        timeout = min(self.TIMEOUT_SECONDS, EXTERNAL_HTTP_TIMEOUT_SECONDS)

        # ── Phase 1: quote ────────────────────────────────────────────────
        try:
            quote_data = await _fetch_quote(
                chain_id,
                src,
                dst,
                amount_wei,
                timeout,
            )
        except ExternalServiceError as exc:
            # 400/404 often means the chain or token pair is not supported.
            self._log_debug(f"quote API error for {src}/{dst} on {chain_id}: {exc}")
            return None
        except Exception as exc:
            self._log_failure("quote unexpected error", exc)
            return None

        raw_out = quote_data.get("toAmount") or quote_data.get("dstAmount")
        if not raw_out:
            self._log_failure("quote response missing toAmount")
            return None

        try:
            out_wei = int(raw_out)
        except (ValueError, TypeError) as exc:
            self._log_failure("could not parse toAmount", exc)
            return None

        amount_out = _from_wei(out_wei, out_decimals)

        # Apply slippage to derive the minimum accepted output.
        slippage_factor = Decimal(str(1.0 - slippage_pct / 100.0))
        amount_out_min = amount_out * slippage_factor

        gas_estimate = int(quote_data.get("gas", 0))

        # ── Price impact ──────────────────────────────────────────────────
        price_impact_pct = Decimal("0")
        raw_impact = quote_data.get("priceImpact")
        if raw_impact is not None:
            try:
                price_impact_pct = Decimal(str(raw_impact))
            except Exception:
                pass

        # ── Phase 2: build calldata (swap endpoint) ────────────────────────
        # We call /swap unconditionally because the RoutePlanner picks the
        # winner *after* all quotes are gathered, and we need the calldata
        # ready.
        calldata: Optional[str] = None
        to_address: Optional[str] = None
        approval_address: Optional[str] = None
        swap_data: Dict[str, Any] | None = None

        try:
            swap_data = await _fetch_swap(
                chain_id,
                src,
                dst,
                amount_wei,
                sender,
                slippage_pct,
                timeout,
            )
            tx = swap_data.get("tx") or {}
            calldata = tx.get("data")
            to_address = tx.get("to")
            # The spender for ERC-20 approval is typically the router itself.
            # 1inch uses its own AggregationRouterV6 as the approval target.
            approval_address = to_address

            # Refine gas estimate from the swap response if available.
            if tx.get("gas"):
                try:
                    gas_estimate = int(tx["gas"])
                except (ValueError, TypeError):
                    pass

            # Refine price impact if available.
            if swap_data.get("priceImpact") is not None:
                try:
                    price_impact_pct = Decimal(str(swap_data["priceImpact"]))
                except Exception:
                    pass

        except ExternalServiceError as exc:
            # Calldata build failed — we still have a valid quote.
            self._log_debug(
                f"swap calldata build failed for 1inch (quote still valid): {exc}"
            )
        except Exception as exc:
            self._log_failure("swap calldata unexpected error", exc)

        raw_payload: Dict[str, Any] = dict(quote_data)
        if isinstance(swap_data, dict) and swap_data:
            tx_obj = swap_data.get("tx")
            if isinstance(tx_obj, dict) and tx_obj:
                raw_payload["transaction"] = tx_obj

        return SwapRouteQuote(
            aggregator=self.name,
            chain_id=chain_id,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            amount_out_min=amount_out_min,
            gas_estimate=gas_estimate,
            gas_cost_usd=None,  # 1inch v6 /quote does not return USD gas cost
            price_impact_pct=price_impact_pct,
            calldata=calldata,
            to=to_address,
            approval_address=approval_address,
            raw=raw_payload,
        )
