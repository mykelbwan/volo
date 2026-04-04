from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional

from config.chains import CHAINS, get_chain_by_id
from core.routing.models import SwapRouteQuote
from core.routing.swap.base import SwapAggregator
from core.routing.swap.utils import resolve_decimals as _resolve_decimals
from core.utils.http import (
    ExternalServiceError,
    async_raise_for_status,
    async_request_json,
)
from core.utils.timeouts import EXTERNAL_HTTP_TIMEOUT_SECONDS

_LOGGER = logging.getLogger("volo.routing.swap.paraswap")

_API_BASE_URL = "https://api.paraswap.io"
_PRICES_ENDPOINT = "/prices"
_TX_ENDPOINT = "/transactions/{network_id}"

# ParaSwap uses this fixed sentinel for native tokens across all EVM chains.
_PARASWAP_NATIVE = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeeeE"
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# ParaSwap swap side — we always quote the exact sell amount.
_SIDE_SELL = "SELL"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _partner() -> Optional[str]:
    return os.getenv("PARASWAP_PARTNER", "").strip() or None


def _partner_fee_wallet() -> Optional[str]:
    return os.getenv("PARASWAP_PARTNER_FEE_WALLET", "").strip() or None


def _normalise_token_address(address: str, chain_id: int) -> str:
    """Map any native-token alias to the ParaSwap native-token sentinel."""
    addr = address.strip().lower()
    # ParaSwap specifically requires 0xeee... for native tokens.
    # We check against our chain-specific aliases (e.g. 0x0, 0xeee).
    try:
        chain = CHAINS.get(chain_id)
        if chain:
            aliases = [a.lower() for a in chain.native_token_aliases]
            if addr in aliases:
                return _PARASWAP_NATIVE
    except Exception:
        pass

    if addr == _ZERO_ADDRESS:
        return _PARASWAP_NATIVE
    return address


def _to_wei(amount: Decimal, decimals: int) -> int:
    return int(amount * Decimal(10**decimals))


def _from_wei(amount_wei: int, decimals: int) -> Decimal:
    return Decimal(amount_wei) / Decimal(10**decimals)


# ---------------------------------------------------------------------------
# Async HTTP helpers
# ---------------------------------------------------------------------------


async def _fetch_prices(
    chain_id: int,
    src_token: str,
    dest_token: str,
    src_decimals: int,
    dest_decimals: int,
    amount_wei: int,
    user_address: str,
    timeout: float,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "srcToken": src_token,
        "destToken": dest_token,
        "srcDecimals": src_decimals,
        "destDecimals": dest_decimals,
        "amount": str(amount_wei),
        "side": _SIDE_SELL,
        "network": chain_id,
        "userAddress": user_address,
        "includeDEXS": "",  # empty = use all available DEXes
        "includeContractMethods": "simpleSwap,multiSwap,megaSwap",
    }

    partner = _partner()
    if partner:
        params["partner"] = partner

    resp = await async_request_json(
        "GET",
        f"{_API_BASE_URL}{_PRICES_ENDPOINT}",
        params=params,
        timeout=timeout,
        service="paraswap-prices",
    )
    await async_raise_for_status(resp, "paraswap-prices")
    return resp.json()


async def _fetch_transaction(
    chain_id: int,
    src_token: str,
    dest_token: str,
    src_decimals: int,
    dest_decimals: int,
    src_amount_wei: int,
    dest_amount_min_wei: int,
    price_route: Dict[str, Any],
    user_address: str,
    slippage_pct: float,
    timeout: float,
) -> Dict[str, Any]:
    """
    Call the ParaSwap /transactions endpoint to build execution calldata.
    """
    url = f"{_API_BASE_URL}{_TX_ENDPOINT.format(network_id=chain_id)}"

    payload: Dict[str, Any] = {
        "srcToken": src_token,
        "destToken": dest_token,
        "srcDecimals": src_decimals,
        "destDecimals": dest_decimals,
        "srcAmount": str(src_amount_wei),
        "destAmount": str(dest_amount_min_wei),
        "priceRoute": price_route,
        "userAddress": user_address,
        "slippage": int(round(slippage_pct * 100)),  # basis points
    }

    partner = _partner()
    fee_wallet = _partner_fee_wallet()
    if partner and fee_wallet:
        payload["partner"] = partner
        payload["partnerAddress"] = fee_wallet

    resp = await async_request_json(
        "POST",
        url,
        json=payload,
        timeout=timeout,
        service="paraswap-tx",
    )
    await async_raise_for_status(resp, "paraswap-tx")
    return resp.json()


class ParaSwapAggregator(SwapAggregator):
    name: str = "paraswap"
    TIMEOUT_SECONDS: float = 6.0

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
        """
        Fetch a ParaSwap quote and build execution calldata.
        """
        # ── Guard: check if chain is known/supported ──────────────────────
        try:
            get_chain_by_id(chain_id)
        except KeyError:
            self._log_debug(f"chain_id {chain_id} not in global config")
            return None

        # ── Normalise token addresses ─────────────────────────────────────
        src_token = _normalise_token_address(token_in, chain_id)
        dest_token = _normalise_token_address(token_out, chain_id)

        # ── Resolve decimals ──────────────────────────────────────────────
        try:
            # We resolve decimals using normalized addresses so native tokens
            # are correctly identified via the ParaSwap sentinel or zero address.
            src_decimals = await _resolve_decimals(
                src_token,
                chain_id,
                zero_address=_ZERO_ADDRESS,
                native_token_address=_PARASWAP_NATIVE,
            )
            dest_decimals = await _resolve_decimals(
                dest_token,
                chain_id,
                zero_address=_ZERO_ADDRESS,
                native_token_address=_PARASWAP_NATIVE,
            )
        except Exception as exc:
            self._log_failure("decimal resolution failed", exc)
            return None

        src_amount_wei = _to_wei(amount_in, src_decimals)
        timeout = min(self.TIMEOUT_SECONDS, EXTERNAL_HTTP_TIMEOUT_SECONDS)

        # ── Phase 1: prices ───────────────────────────────────────────────
        try:
            prices_data = await _fetch_prices(
                chain_id,
                src_token,
                dest_token,
                src_decimals,
                dest_decimals,
                src_amount_wei,
                sender,
                timeout,
            )
        except ExternalServiceError as exc:
            # 400/404 often means the chain or token pair is not supported.
            self._log_debug(f"ParaSwap API error for {src_token}/{dest_token}: {exc}")
            return None
        except Exception as exc:
            self._log_failure("prices unexpected error", exc)
            return None

        # ── Parse price route ─────────────────────────────────────────────
        price_route = prices_data.get("priceRoute") or {}
        if not price_route:
            self._log_failure("prices response missing priceRoute")
            return None

        raw_dest_amount = price_route.get("destAmount")
        if not raw_dest_amount:
            self._log_failure("priceRoute missing destAmount")
            return None

        try:
            dest_amount_wei = int(raw_dest_amount)
        except (ValueError, TypeError) as exc:
            self._log_failure("could not parse destAmount", exc)
            return None

        amount_out = _from_wei(dest_amount_wei, dest_decimals)

        # Minimum accepted output: apply slippage to the expected output.
        slippage_factor = Decimal(str(1.0 - slippage_pct / 100.0))
        amount_out_min = amount_out * slippage_factor
        dest_amount_min_wei = _to_wei(amount_out_min, dest_decimals)

        # ── Gas estimate ──────────────────────────────────────────────────
        gas_estimate = 0
        try:
            gas_estimate = int(price_route.get("gasCost", 0) or 0)
        except (ValueError, TypeError):
            pass

        # ── Gas cost in USD ───────────────────────────────────────────────
        gas_cost_usd: Optional[Decimal] = None
        raw_gas_usd = price_route.get("gasCostUSD")
        if raw_gas_usd:
            try:
                gas_cost_usd = Decimal(str(raw_gas_usd))
            except Exception:
                pass

        # ── Price impact ──────────────────────────────────────────────────
        price_impact_pct = Decimal("0")
        # Try specific impact fields first, then fallback to USD value delta.
        raw_impact = price_route.get("priceImpact") or price_route.get("maxImpactReached")
        if raw_impact is not None:
            try:
                price_impact_pct = Decimal(str(raw_impact))
            except Exception:
                pass
        else:
            src_usd = price_route.get("srcUSD")
            dest_usd = price_route.get("destUSD")
            if src_usd and dest_usd:
                try:
                    src_v = Decimal(str(src_usd))
                    dst_v = Decimal(str(dest_usd))
                    if src_v > 0:
                        price_impact_pct = ((src_v - dst_v) / src_v) * Decimal("100")
                        if price_impact_pct < 0:
                            price_impact_pct = Decimal("0")
                except Exception:
                    pass

        # ── Token transfer proxy (approval address) ───────────────────────
        approval_address: Optional[str] = price_route.get("tokenTransferProxy") or None

        # ── Phase 2: build transaction calldata ───────────────────────────
        calldata: Optional[str] = None
        to_address: Optional[str] = None

        try:
            tx_data = await _fetch_transaction(
                chain_id,
                src_token,
                dest_token,
                src_decimals,
                dest_decimals,
                src_amount_wei,
                dest_amount_min_wei,
                price_route,
                sender,
                slippage_pct,
                timeout,
            )
            calldata = tx_data.get("data") or None
            to_address = tx_data.get("to") or None

        except ExternalServiceError as exc:
            self._log_debug(f"transaction build failed for ParaSwap (quote still valid): {exc}")
        except Exception as exc:
            self._log_failure("transaction build unexpected error", exc)

        self._log_debug(
            f"quote ok chain={chain_id} out={amount_out:.6f} "
            f"impact={price_impact_pct:.4f}% gas={gas_estimate} "
            f"calldata={'yes' if calldata else 'no'}"
        )

        return SwapRouteQuote(
            aggregator=self.name,
            chain_id=chain_id,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            amount_out_min=amount_out_min,
            gas_estimate=gas_estimate,
            gas_cost_usd=gas_cost_usd,
            price_impact_pct=price_impact_pct,
            calldata=calldata,
            to=to_address,
            approval_address=approval_address,
            raw=prices_data,
        )
