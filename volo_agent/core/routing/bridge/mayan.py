from __future__ import annotations

import asyncio
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


def _price_api() -> str:
    return os.getenv("MAYAN_PRICE_API_URL", _DEFAULT_PRICE_API).rstrip("/")


def _default_slippage() -> float:
    try:
        return float(os.getenv("MAYAN_SLIPPAGE_PCT", "1.0"))
    except (ValueError, TypeError):
        return 1.0


_ZERO = "0x0000000000000000000000000000000000000000"

# Route type preference order — higher index = lower preference.
_ROUTE_TYPE_PRIORITY: Dict[str, int] = {
    "SWIFT": 0,
    "MCTP": 1,
    "WH": 2,
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
    """Return a human-readable name for the chain."""
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


class MayanAggregator(BridgeAggregator):
    name: str = "mayan"
    TIMEOUT_SECONDS: float = 8.0

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
        # ── Resolve Mayan chain names ─────────────────────────────────────
        mayan_from = _resolve_mayan_chain(source_chain_id)
        mayan_to = _resolve_mayan_chain(dest_chain_id)

        if not mayan_from or not mayan_to:
            self._log_failure(
                f"unsupported chain pair ({source_chain_id}→{dest_chain_id}) for Mayan"
            )
            return None

        # ── Resolve token addresses via registry/Dexscreener ──────────────
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
            self._log_failure(
                f"token {symbol!r} could not be resolved for Mayan route "
                f"{source_chain_name}→{dest_chain_name}"
            )
            return None

        # Mayan uses the zero-address for native gas tokens on ALL chains (including Solana)
        from_token = _ZERO if source_token.is_native else source_token.address
        to_token = _ZERO if dest_token.is_native else dest_token.address

        # ── Call Mayan price API ──────────────────────────────────────────
        slippage = _default_slippage() / 100.0  # Mayan expects 0.01 for 1%

        try:
            resp = await async_request_json(
                "GET",
                f"{_price_api()}/quote",
                params={
                    "amount": str(amount),
                    "fromToken": from_token,
                    "toToken": to_token,
                    "fromChain": mayan_from,
                    "toChain": mayan_to,
                    "slippage": slippage,
                    "withMctpRoutes": "true",
                },
                timeout=self.TIMEOUT_SECONDS,
                service="mayan-price",
            )

            if resp.status_code == 404:
                self._log_failure(
                    f"no route found (404) for {symbol} {mayan_from}→{mayan_to}"
                )
                return None

            if resp.status_code == 429:
                self._log_failure("rate-limited by Mayan price API (429)")
                return None

            resp.raise_for_status()
            data: Dict[str, Any] = resp.json()

        except ExternalServiceError as exc:
            self._log_failure(
                f"Mayan API error: {exc.service} HTTP {exc.status_code}", exc
            )
            return None
        except Exception as exc:
            self._log_failure("unexpected error contacting Mayan price API", exc)
            return None

        # ── Parse response ────────────────────────────────────────────────
        routes: list[Dict[str, Any]] = data if isinstance(data, list) else []

        if not routes:
            # Also try nested key in case API shape changes.
            routes = data.get("routes") or [] if isinstance(data, dict) else []

        if not routes:
            self._log_failure(
                f"Mayan returned no routes for {symbol} {mayan_from}→{mayan_to}"
            )
            return None

        best = _pick_best_route(routes)
        if not best:
            return None

        # ── Extract quote fields ──────────────────────────────────────────
        expected_out = _safe_decimal(
            best.get("expectedAmountOut") or best.get("minAmountOut"), "0"
        )
        min_out = _safe_decimal(best.get("minAmountOut"), "0")

        if expected_out <= 0:
            self._log_failure(f"Mayan route has zero expectedAmountOut for {symbol}")
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
        quote_hash = best.get("quoteHash") or best.get("hash") or ""

        self._log_debug(
            f"quote ok [{route_type}] {mayan_from}→{mayan_to} "
            f"out={expected_out:.6f} fee={total_fee_pct:.2f}% "
            f"eta≈{eta}s "
            f"hash={'yes' if quote_hash else 'no'}"
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
                # Stored for the executor — it uses quoteHash to request
                # the unsigned transaction from Mayan's swap API.
                "quoteHash": quote_hash,
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
                "slippage": slippage,
                "rawRoute": best,
            },
            raw=data if isinstance(data, dict) else {"routes": routes},
        )
