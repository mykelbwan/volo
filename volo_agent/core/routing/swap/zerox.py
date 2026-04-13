from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional

from config.chains import get_chain_by_id
from core.routing.models import SwapRouteQuote
from core.routing.swap.base import SwapAggregator
from core.routing.swap.utils import resolve_decimals as _resolve_decimals
from core.utils.http import (
    ExternalServiceError,
    async_raise_for_status,
    async_request_json,
)
from core.utils.timeouts import EXTERNAL_HTTP_TIMEOUT_SECONDS

_LOGGER = logging.getLogger("volo.routing.swap.0x")
_API_BASE_URL = "https://api.0x.org"
_QUOTE_ENDPOINT = "/swap/permit2/quote"
_ZEROX_API_VERSION = "v2"

# 0x uses the zero-address to represent native tokens.
_NATIVE_TOKEN_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeeeE"
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _api_key() -> Optional[str]:
    return os.getenv("ZEROX_API_KEY", "").strip() or None


def _auth_headers() -> Dict[str, str]: 
    headers: Dict[str, str] = {"0x-version": _ZEROX_API_VERSION}
    key = _api_key()
    if key:
        headers["0x-api-key"] = key
    return headers


def _normalise_token_address(address: str) -> str:
    if address.strip().lower() == _ZERO_ADDRESS:
        return _NATIVE_TOKEN_ADDRESS
    return address


def _normalise_taker_address(address: str) -> Optional[str]:
    candidate = str(address or "").strip()
    if (
        candidate
        and "{{" not in candidate
        and candidate.startswith("0x")
        and len(candidate) == 42
    ):
        return candidate
    return None


def _to_wei(amount: Decimal, decimals: int) -> int:
    return int(amount * Decimal(10**decimals))


def _from_wei(amount_wei: int, decimals: int) -> Decimal:
    return Decimal(amount_wei) / Decimal(10**decimals)


async def _fetch_quote(
    chain_id: int,
    sell_token: str,
    buy_token: str,
    sell_amount_wei: int,
    taker_address: str,
    slippage_pct: float,
    timeout: float,
) -> Dict[str, Any]:
    url = f"{_API_BASE_URL}{_QUOTE_ENDPOINT}"
    # 0x v2 expects slippage in basis points (1 bps = 0.01 %).
    slippage_bps = int(round(slippage_pct * 100))
    params: Dict[str, Any] = {
        "chainId": chain_id,
        "sellToken": sell_token,
        "buyToken": buy_token,
        "sellAmount": str(sell_amount_wei),
        # 0x Permit2 quote now validates `taker`; keep `takerAddress` for
        # backward compatibility across gateway versions.
        "taker": taker_address,
        "takerAddress": taker_address,
        "slippageBps": slippage_bps,
    }
    resp = await async_request_json(
        "GET",
        url,
        params=params,
        headers=_auth_headers(),
        timeout=timeout,
        service="0x-quote",
    )
    await async_raise_for_status(resp, "0x-quote")
    return resp.json()


class ZeroXAggregator(SwapAggregator):
    name: str = "0x"
    TIMEOUT_SECONDS: float = 60.0

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
        # Guard: API key 
        if not _api_key():
            self._log_debug("ZEROX_API_KEY not set — skipping")
            return None

        # Guard: check if chain is known/supported 
        try:
            get_chain_by_id(chain_id)
        except KeyError:
            self._log_debug(f"chain_id {chain_id} not in global config")
            return None

        # Normalise token addresses 
        sell_token = _normalise_token_address(token_in)
        buy_token = _normalise_token_address(token_out)

        # Resolve decimals 
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
        sell_amount_wei = _to_wei(amount_in, in_decimals)
        taker = _normalise_taker_address(sender)
        if not taker:
            self._log_debug("sender is missing/invalid for required 0x 'taker' field — skipping")
            return None

        timeout = min(self.TIMEOUT_SECONDS, EXTERNAL_HTTP_TIMEOUT_SECONDS)

        # Fetch quote + calldata in one request 
        try:
            data = await _fetch_quote(
                chain_id,
                sell_token,
                buy_token,
                sell_amount_wei,
                taker,
                slippage_pct,
                timeout,
            )
        except ExternalServiceError as exc:
            self._log_failure("API error", exc)
            raise RuntimeError(f"0x quote API error: {exc}") from exc
        except Exception as exc:
            self._log_failure("unexpected error", exc)
            return None

        # Parse output amount 
        raw_buy = data.get("buyAmount")
        if not raw_buy:
            self._log_failure("response missing buyAmount")
            return None

        try:
            buy_wei = int(raw_buy)
        except (ValueError, TypeError) as exc:
            self._log_failure("could not parse buyAmount", exc)
            return None

        amount_out = _from_wei(buy_wei, out_decimals)

        # Minimum output: 0x encodes this in the calldata but also surfaces
        # it as ``minBuyAmount`` in the response when present.
        raw_min = data.get("minBuyAmount")
        if raw_min:
            try:
                amount_out_min = _from_wei(int(raw_min), out_decimals)
            except (ValueError, TypeError):
                slippage_factor = Decimal(str(1.0 - slippage_pct / 100.0))
                amount_out_min = amount_out * slippage_factor
        else:
            slippage_factor = Decimal(str(1.0 - slippage_pct / 100.0))
            amount_out_min = amount_out * slippage_factor

        # Gas estimate 
        gas_estimate = 0
        try:
            gas_estimate = int(data.get("estimatedGas", 0) or 0)
        except (ValueError, TypeError):
            pass

        # 0x API v2 /quote does not return gas cost in USD.
        # estimatedGasPrice is in wei.
        gas_cost_usd: Optional[Decimal] = None

        # 0x v2 surfaces price impact under different keys depending on the
        # route type.  We try the most specific key first.
        price_impact_pct = Decimal("0")
        for key in ("estimatedPriceImpact", "priceImpact"):
            raw_impact = data.get(key)
            if raw_impact is not None:
                try:
                    price_impact_pct = Decimal(str(raw_impact))
                    break
                except Exception:
                    pass

        tx: Dict[str, Any] = data.get("transaction") or {}
        calldata: Optional[str] = tx.get("data") or None
        to_address: Optional[str] = tx.get("to") or None

        # If ``issues.allowance`` is present, the taker must approve the
        # specified spender (the Permit2 contract) for the sell token.
        approval_address: Optional[str] = None
        issues = data.get("issues") or {}
        allowance_issue = issues.get("allowance")
        if isinstance(allowance_issue, dict) and allowance_issue.get("spender"):
            approval_address = allowance_issue["spender"]

        self._log_debug(
            f"quote ok chain={chain_id} out={amount_out:.6f} "
            f"gas={gas_estimate} permit2_approval={'yes' if approval_address else 'no'}"
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
            raw=data,
        )
