from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional

import httpx

from core.routing.models import SolanaSwapRouteQuote
from core.routing.solana.base import SolanaSwapAggregator
from core.routing.solana.utils import (
    decimal_from_lamports as _decimal_from_lamports,
    lamports_from_decimal as _lamports_from_decimal,
    safe_int as _safe_int,
)
from core.routing.utils import safe_decimal as _safe_decimal
from config.solana_chains import get_solana_chain, normalize_solana_mint

_LOGGER = logging.getLogger("volo.routing.solana.jupiter")
_DEFAULT_API_BASE = "https://quote-api.jup.ag/v6"

_QUOTE_PATH = "/quote"
_SWAP_PATH = "/swap"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _api_base() -> str:
    return os.getenv("JUPITER_API_URL", _DEFAULT_API_BASE).rstrip("/")


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class JupiterAggregator(SolanaSwapAggregator):
    name: str = "jupiter"
    TIMEOUT_SECONDS: float = 8.0

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.TIMEOUT_SECONDS)
        return self._client

    async def get_quote(
        self,
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
    ) -> Optional[SolanaSwapRouteQuote]:
        try:
            chain = get_solana_chain(network)
        except KeyError:
            return None

        if chain.is_testnet:
            self._log_debug(
                f"network={network!r} is a testnet — Jupiter is mainnet-only"
            )
            return None

        # Normalize mints (convert 'sol'/'native' to WSOL mint)
        input_mint = normalize_solana_mint(input_mint)
        output_mint = normalize_solana_mint(output_mint)

        amount_lamports = _lamports_from_decimal(amount_in, input_decimals)
        if amount_lamports <= 0:
            self._log_failure("computed amount_lamports is zero or negative")
            return None

        # Jupiter accepts slippage in basis points (1 bps = 0.01 %).
        slippage_bps = max(1, int(round(slippage_pct * 100)))

        base = _api_base()

        try:
            client = await self._get_client()
            # ── Phase 1: GET /quote ───────────────────────────────────
            quote_resp = await client.get(
                f"{base}{_QUOTE_PATH}",
                params={
                    "inputMint": input_mint,
                    "outputMint": output_mint,
                    "amount": str(amount_lamports),
                    "slippageBps": slippage_bps,
                    # Allow multi-hop routes for better pricing.
                    "onlyDirectRoutes": "false",
                    # Use versioned transactions (required for Lookup Tables).
                    "asLegacyTransaction": "false",
                    # Include platform fee info in the response.
                    "platformFeeBps": "0",
                    # Passing userPublicKey here allows Jupiter to include
                    # platform fees (if any) and optimize for the specific user.
                    "userPublicKey": sender,
                },
            )

            if quote_resp.status_code == 400:
                body = quote_resp.text[:200]
                self._log_failure(f"no route found (400): {body}")
                return None

            if quote_resp.status_code == 429:
                self._log_failure("rate-limited by Jupiter API (429)")
                return None

            quote_resp.raise_for_status()
            quote_data: Dict[str, Any] = quote_resp.json()

            # Validate the quote response has the fields we need.
            if not quote_data.get("outAmount"):
                self._log_failure("quote response missing outAmount")
                return None

            out_lamports = _safe_int(quote_data.get("outAmount"))
            min_out_lamports = _safe_int(quote_data.get("otherAmountThreshold"))
            price_impact_pct = _safe_decimal(quote_data.get("priceImpactPct", "0"))

            amount_out = _decimal_from_lamports(out_lamports, output_decimals)
            amount_out_min = _decimal_from_lamports(
                min_out_lamports, output_decimals
            )

            # ── Phase 2: POST /swap ───────────────────────────────────
            # Build the unsigned VersionedTransaction from the quote.
            swap_resp = await client.post(
                f"{base}{_SWAP_PATH}",
                json={
                    "quoteResponse": quote_data,
                    "userPublicKey": sender,
                    # Automatically wrap SOL → WSOL on input and
                    # unwrap WSOL → SOL on output when applicable.
                    "wrapAndUnwrapSol": True,
                    # Let Jupiter dynamically set the compute unit limit
                    # based on the simulated transaction.
                    "dynamicComputeUnitLimit": True,
                    # Auto priority fee adapts to current network load.
                    "prioritizationFeeLamports": "auto",
                },
            )

            if swap_resp.status_code == 429:
                # Rate-limited at the swap step — still return price data
                # without a transaction so the scorer can compare against
                # Raydium, but execution will fall back to Raydium's tx.
                self._log_failure(
                    "rate-limited on /swap (429) — returning price-only quote"
                )
                return SolanaSwapRouteQuote(
                    aggregator=self.name,
                    network=network,
                    input_mint=input_mint,
                    output_mint=output_mint,
                    amount_in=amount_in,
                    amount_out=amount_out,
                    amount_out_min=amount_out_min,
                    amount_in_lamports=amount_lamports,
                    amount_out_lamports=out_lamports,
                    price_impact_pct=price_impact_pct,
                    swap_transaction=None,
                    raw=quote_data,
                )

            swap_resp.raise_for_status()
            swap_data: Dict[str, Any] = swap_resp.json()
            swap_transaction: Optional[str] = swap_data.get("swapTransaction")

            if not swap_transaction:
                self._log_failure("swap response missing swapTransaction field")
                # Return the price quote without a transaction — the
                # executor will rebuild it at runtime if this quote wins.
                swap_transaction = None

        except httpx.TimeoutException as exc:
            self._log_failure("request timed out", exc)
            return None
        except httpx.HTTPStatusError as exc:
            self._log_failure(f"HTTP {exc.response.status_code} from Jupiter API", exc)
            return None
        except httpx.RequestError as exc:
            self._log_failure("network error contacting Jupiter API", exc)
            return None
        except Exception as exc:
            self._log_failure("unexpected error", exc)
            return None

        self._log_debug(
            f"quote ok  out={amount_out:.6f}  impact={price_impact_pct}%  "
            f"tx={'yes' if swap_transaction else 'no'}"
        )

        return SolanaSwapRouteQuote(
            aggregator=self.name,
            network=network,
            input_mint=input_mint,
            output_mint=output_mint,
            amount_in=amount_in,
            amount_out=amount_out,
            amount_out_min=amount_out_min,
            amount_in_lamports=amount_lamports,
            amount_out_lamports=out_lamports,
            price_impact_pct=price_impact_pct,
            swap_transaction=swap_transaction,
            raw=quote_data,
        )
