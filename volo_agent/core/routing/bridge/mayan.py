from __future__ import annotations

import asyncio
import json
import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional

from config.chains import CHAINS as EVM_CHAINS
from config.solana_chains import SOLANA_CHAINS, is_solana_chain_id
from core.routing.bridge.base import BridgeAggregator
from core.routing.bridge.token_resolver import resolve_bridge_token
from core.routing.models import BridgeRouteQuote
from core.routing.utils import safe_decimal as _safe_decimal
from core.utils.http import ExternalServiceError, async_request_json

_LOGGER = logging.getLogger("volo.routing.bridge.mayan")
_DEFAULT_PRICE_API = "https://price-api.mayan.finance/v3"
_MAYAN_PROGRAM_ID = "FC4eXxkyrMPTjiYUpp4EAnkmwMbQyZ6NDCh1kfLn6vsf"
_MAYAN_FORWARDER_CONTRACT = "0x337685fdaB40D39bd02028545a4FfA7D287cC3E2"
_MAYAN_SDK_VERSION = "13_2_0"


def _price_api() -> str:
    return os.getenv("MAYAN_PRICE_API_URL", _DEFAULT_PRICE_API).rstrip("/")


def _default_slippage() -> float:
    try:
        pct = float(os.getenv("MAYAN_SLIPPAGE_PCT", "1.0"))
    except (ValueError, TypeError):
        pct = 1.0
    return max(1, pct * 100)


_ZERO = "0x0000000000000000000000000000000000000000"

# Route type preference order — higher index = lower preference.
_ROUTE_TYPE_PRIORITY: Dict[str, int] = {
    "FAST_MCTP": 0,
    "SWIFT": 1,
    "MCTP": 2,
    "WH": 3,
}


def _is_solana_chain(chain_id: int) -> bool:
    return is_solana_chain_id(chain_id)


def _resolve_mayan_chain(chain_id: int) -> Optional[str]:
    """Map EIP-155 / Solana chain IDs to Mayan's internal identifiers."""
    if _is_solana_chain(chain_id):
        return "solana"

    chain_cfg = EVM_CHAINS.get(chain_id)
    if chain_cfg and chain_cfg.dexscreener_slug:
        return chain_cfg.dexscreener_slug

    return None


def _get_display_name(chain_id: int, fallback: str) -> str:
    if _is_solana_chain(chain_id):
        sol_cfg = SOLANA_CHAINS.get("solana")
        return sol_cfg.name if sol_cfg else "Solana"

    evm_cfg = EVM_CHAINS.get(chain_id)
    return evm_cfg.name if evm_cfg else fallback


def _pick_best_route(routes: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not routes:
        return None

    def _sort_key(r: Dict[str, Any]) -> tuple:
        rtype = str(r.get("type", "WH")).upper()
        priority = _ROUTE_TYPE_PRIORITY.get(rtype, 99)
        try:
            out = float(r.get("expectedAmountOut") or r.get("minAmountOut") or 0)
        except (TypeError, ValueError):
            out = 0.0
        # Lower priority number = better; higher output = better.
        return (priority, -out)

    return sorted(routes, key=_sort_key)[0]


def _summarize_mayan_error(
    *,
    status_code: int | None,
    body: str,
    token_symbol: str,
    from_chain: str,
    to_chain: str,
) -> str:
    payload: Dict[str, Any] = {}
    try:
        parsed = json.loads(body or "")
        if isinstance(parsed, dict):
            payload = parsed
    except Exception:
        payload = {}

    code = str(payload.get("code") or payload.get("error") or "").strip().upper()
    msg = str(payload.get("msg") or payload.get("message") or "").strip()
    raw_data: Any = payload.get("data")
    data: Dict[str, Any] = raw_data if isinstance(raw_data, dict) else {}

    if code == "AMOUNT_TOO_SMALL":
        min_amount = data.get("minAmountIn")
        if min_amount is not None:
            return (
                f"Mayan requires a larger amount for {token_symbol} {from_chain}→{to_chain}. "
                f"Minimum is about {min_amount} {token_symbol}."
            )
        return (
            f"Mayan requires a larger amount for {token_symbol} {from_chain}→{to_chain}. "
            "Try a higher amount."
        )

    if msg:
        return f"Mayan quote error ({status_code}): {msg}"

    return (
        f"Mayan quote request failed for {token_symbol} {from_chain}→{to_chain} "
        f"(HTTP {status_code})."
    )


class MayanAggregator(BridgeAggregator):
    name: str = "mayan"
    TIMEOUT_SECONDS: float = 60.0

    def __init__(self) -> None:
        self.last_error: Optional[str] = None

    async def get_quote(
        self,
        *,
        token_symbol: str,
        source_chain_id: int,
        dest_chain_id: int,
        source_chain_name: str,
        dest_chain_name: str,
        amount: Decimal,
        sender: str,
        recipient: str,
    ) -> Optional[BridgeRouteQuote]:
        self.last_error = None

        mayan_from = _resolve_mayan_chain(source_chain_id)
        mayan_to = _resolve_mayan_chain(dest_chain_id)

        if not mayan_from or not mayan_to:
            self.last_error = (
                f"Mayan does not support {source_chain_name}→{dest_chain_name}."
            )
            self._log_failure(self.last_error)
            return None

        symbol = token_symbol.strip().upper()
        source_token, dest_token = await asyncio.gather(
            resolve_bridge_token(
                symbol,
                chain_id=source_chain_id,
                chain_name=source_chain_name,
            ),
            resolve_bridge_token(
                symbol,
                chain_id=dest_chain_id,
                chain_name=dest_chain_name,
            ),
        )

        if source_token is None or dest_token is None:
            self.last_error = (
                f"token {symbol!r} could not be resolved for Mayan route "
                f"{source_chain_name}→{dest_chain_name}"
            )
            self._log_failure(self.last_error)
            return None

        # Mayan uses the zero-address for native gas tokens on ALL chains (including Solana)
        from_token = _ZERO if source_token.is_native else source_token.address
        to_token = _ZERO if dest_token.is_native else dest_token.address

        # Convert human-readable amount to atomic units (integers)
        # Mayan's 'amountIn64' expects the raw integer value (e.g. wei for ETH).
        # We must use the source token's decimals to scale correctly.
        src_decimals = int(source_token.decimals)
        amount_atomic = int(amount * Decimal(10**src_decimals))

        if amount_atomic <= 0:
            self.last_error = f"amount too small after conversion for {symbol}."
            self._log_failure(self.last_error)
            return None

        slippage_bps = int(_default_slippage())

        try:
            resp = await async_request_json(
                "GET",
                f"{_price_api()}/quote",
                params={
                    # Match Mayan SDK v13.2.0 request shape.
                    "wormhole": "true",
                    "swift": "true",
                    "mctp": "true",
                    "shuttle": "false",
                    "fastMctp": "true",
                    "gasless": "false",
                    "onlyDirect": "false",
                    "fullList": "false",
                    "monoChain": "true",
                    "solanaProgram": _MAYAN_PROGRAM_ID,
                    "forwarderAddress": _MAYAN_FORWARDER_CONTRACT,
                    "amountIn64": str(amount_atomic),
                    "fromToken": from_token,
                    "toToken": to_token,
                    "fromChain": mayan_from,
                    "toChain": mayan_to,
                    "slippageBps": slippage_bps,
                    "destinationAddress": recipient,
                    "sdkVersion": _MAYAN_SDK_VERSION,
                },
                timeout=self.TIMEOUT_SECONDS,
                service="mayan-price",
            )

            if resp.status_code == 404:
                self.last_error = (
                    f"no route found (404) for {symbol} {mayan_from}→{mayan_to}"
                )
                self._log_failure(self.last_error)
                return None

            if resp.status_code == 429:
                self.last_error = "Mayan is rate-limiting quote requests right now."
                self._log_failure(self.last_error)
                return None

            if resp.status_code >= 400:
                self.last_error = _summarize_mayan_error(
                    status_code=resp.status_code,
                    body=resp.text,
                    token_symbol=symbol,
                    from_chain=mayan_from,
                    to_chain=mayan_to,
                )
                self._log_failure(self.last_error)
                return None

            data: Dict[str, Any] = resp.json()

        except ExternalServiceError as exc:
            self.last_error = _summarize_mayan_error(
                status_code=exc.status_code,
                body=exc.body,
                token_symbol=symbol,
                from_chain=mayan_from,
                to_chain=mayan_to,
            )
            self._log_failure(self.last_error, exc)
            return None
        except Exception as exc:
            self.last_error = "Could not reach Mayan quote service."
            self._log_failure(self.last_error, exc)
            return None

        raw_routes: Any = data.get("quotes") if isinstance(data, dict) else None
        routes: list[Dict[str, Any]] = (
            [r for r in raw_routes if isinstance(r, dict)]
            if isinstance(raw_routes, list)
            else []
        )

        if not routes:
            self.last_error = (
                f"Mayan returned no quotes for {symbol} {mayan_from}→{mayan_to}"
            )
            self._log_failure(self.last_error)
            return None

        best = _pick_best_route(routes)
        if not best:
            return None

        expected_out = _safe_decimal(
            best.get("expectedAmountOut") or best.get("minAmountOut"), "0"
        )
        min_out = _safe_decimal(best.get("minAmountOut"), "0")

        if expected_out <= 0:
            self.last_error = f"Mayan returned an invalid quote for {symbol}."
            self._log_failure(self.last_error)
            return None

        # Compute fee as difference between input and expected output.
        # Mayan also surfaces individual fee components; we use the simple
        # approach (input − output) so units are always consistent.
        bridge_fee = (
            _safe_decimal(best.get("bridgeFee", 0))
            + _safe_decimal(best.get("redeemRelayerFee", 0))
            + _safe_decimal(best.get("clientRelayerFeeSuccess", 0))
        )
        total_fee = (
            bridge_fee if bridge_fee > 0 else max(Decimal("0"), amount - expected_out)
        )

        total_fee_pct = (
            (total_fee / amount * Decimal("100")) if amount > 0 else Decimal("0")
        )
        total_fee_pct = max(Decimal("0"), min(Decimal("100"), total_fee_pct))

        eta = 0
        try:
            eta = int(float(best.get("eta", 0)))
        except (TypeError, ValueError):
            pass

        route_type = str(best.get("type", "WH")).upper()
        quote_id = str(best.get("quoteId") or "").strip()
        if not quote_id:
            self.last_error = (
                f"Mayan returned a non-executable quote for {symbol} {mayan_from}→{mayan_to}."
            )
            self._log_failure(self.last_error)
            return None

        self._log_debug(
            f"quote ok [{route_type}] {mayan_from}→{mayan_to} "
            f"out={expected_out:.6f} fee={total_fee_pct:.2f}% "
            f"eta≈{eta}s "
            "quote_ref=yes"
        )

        src_display = _get_display_name(source_chain_id, source_chain_name)
        dst_display = _get_display_name(dest_chain_id, dest_chain_name)

        return BridgeRouteQuote(
            aggregator=self.name,
            token_symbol=symbol,
            source_chain_id=source_chain_id,
            dest_chain_id=dest_chain_id,
            source_chain_name=src_display,
            dest_chain_name=dst_display,
            input_amount=amount,
            output_amount=expected_out,
            total_fee=total_fee,
            total_fee_pct=total_fee_pct,
            estimated_fill_time_seconds=eta,
            gas_cost_source=None,  # Mayan does not surface gas separately
            calldata=None,  # Calldata built at execution time
            to=None,
            tool_data={
                "quoteId": quote_id,
                "routeType": route_type,
                "fromToken": from_token,
                "toToken": to_token,
                "fromChain": mayan_from,
                "toChain": mayan_to,
                "srcIsSolana": _is_solana_chain(source_chain_id),
                "dstIsSolana": _is_solana_chain(dest_chain_id),
                "mayanForwarder": best.get("mayanForwarder"),
                "solanaProgram": best.get("solanaProgram"),
                "minAmountOut": str(min_out),
                "slippageBps": slippage_bps,
                "slippage": slippage_bps / 10_000,
                "rawRoute": best,
            },
            raw=data if isinstance(data, dict) else {"quotes": routes},
        )
