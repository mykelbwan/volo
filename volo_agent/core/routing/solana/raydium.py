"""
core/routing/solana/raydium.py
------------------------------
Raydium AMM v3 adapter.

Raydium is the largest native AMM on Solana.  Unlike Jupiter (which routes
through Raydium and other DEXes), this adapter calls Raydium's API directly,
giving an independent second price point that the scorer can compare against
Jupiter's aggregated route.

API flow (two phases)
---------------------
Phase 1 – GET /compute/swap-base-in
    Computes the optimal route and expected output across all Raydium pools.
    Returns a ``swapResponse`` object (not a transaction) that must be passed
    to Phase 2.

Phase 2 – POST /transaction/swap-base-in
    Builds a fully-serialised, unsigned ``VersionedTransaction`` (V0) from
    the compute result.  Returns a Base-64 encoded transaction ready to be
    signed by CDP and broadcast.

Supported networks
------------------
Raydium v3 is mainnet-only.  Devnet pools exist but have very little
liquidity, making devnet quotes unreliable.  Calls for any network other
than ``"solana"`` return ``None`` immediately.

No API key required
-------------------
Raydium's transaction API is public and unauthenticated.  A custom base URL
can be configured via ``RAYDIUM_API_URL`` for private / load-balanced setups.

Priority fees
-------------
``computeUnitPriceMicroLamports`` is set to a configurable default (500 000
microlamports ≈ 0.0000005 SOL per compute unit).  This can be overridden via
the ``RAYDIUM_PRIORITY_FEE`` environment variable.  Raydium does not yet
support fully dynamic fee estimation via API (unlike Jupiter's "auto" mode).

Reference: https://docs.raydium.io/raydium/traders/trade-api
"""

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

_LOGGER = logging.getLogger("volo.routing.solana.raydium")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_API_BASE = "https://transaction-v1.raydium.io"

_COMPUTE_PATH = "/compute/swap-base-in"
_TRANSACTION_PATH = "/transaction/swap-base-in"

# Default priority fee in microlamports per compute unit.
# 500 000 microlamports ≈ 0.0000005 SOL per CU — competitive without
# over-paying on quiet networks.
_DEFAULT_PRIORITY_FEE = "500000"

# Raydium V0 (VersionedTransaction with Address Lookup Tables).
_TX_VERSION = "V0"

# Wrapped SOL mint — used to decide whether to wrap/unwrap SOL.
_WSOL_MINT = "So11111111111111111111111111111111111111112"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _api_base() -> str:
    return os.getenv("RAYDIUM_API_URL", _DEFAULT_API_BASE).rstrip("/")


def _priority_fee() -> str:
    return os.getenv("RAYDIUM_PRIORITY_FEE", _DEFAULT_PRIORITY_FEE).strip()


def _is_sol(mint: str) -> bool:
    """Return True for native SOL or the Wrapped SOL mint."""
    return mint.strip().lower() in (_WSOL_MINT.lower(), "native", "sol")


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class RaydiumAggregator(SolanaSwapAggregator):
    """
    Swap aggregator adapter for Raydium AMM v3.

    Raydium provides an independent second price point from Jupiter.  When
    Raydium's direct pool route offers a better net output (after applying the
    success-rate weight from the scorer), it wins the routing decision.  In
    practice Jupiter wins most of the time because it routes through Raydium
    itself plus other sources — but Raydium occasionally wins on highly
    liquid pairs where its concentrated liquidity pools quote tighter spreads.

    Wrap / unwrap SOL
    -----------------
    ``wrapSol`` and ``unwrapSol`` are set automatically based on whether the
    input / output mint is native SOL / Wrapped SOL, so callers always pass
    the WSOL mint address and the adapter handles the wrapping transparently.
    """

    name: str = "raydium"
    TIMEOUT_SECONDS: float = 8.0

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
        """
        Compute a Raydium route quote and build the swap transaction.

        Returns ``None`` on any failure without raising so the RoutePlanner
        can continue with Jupiter's quote transparently.
        """
        try:
            chain = get_solana_chain(network)
        except KeyError:
            return None

        if chain.is_testnet:
            self._log_debug(
                f"network={network!r} is a testnet — Raydium is mainnet-beta only"
            )
            return None

        # Normalize mints (convert 'sol'/'native' to WSOL mint)
        input_mint = normalize_solana_mint(input_mint)
        output_mint = normalize_solana_mint(output_mint)

        amount_lamports = _lamports_from_decimal(amount_in, input_decimals)
        if amount_lamports <= 0:
            self._log_failure("computed amount_lamports is zero or negative")
            return None

        # Raydium accepts slippage in basis points (1 bps = 0.01 %).
        slippage_bps = max(1, int(round(slippage_pct * 100)))

        base = _api_base()

        try:
            async with httpx.AsyncClient(timeout=self.TIMEOUT_SECONDS) as client:
                # ── Phase 1: GET /compute/swap-base-in ───────────────────
                compute_resp = await client.get(
                    f"{base}{_COMPUTE_PATH}",
                    params={
                        "inputMint": input_mint,
                        "outputMint": output_mint,
                        "amount": str(amount_lamports),
                        "slippageBps": slippage_bps,
                        "txVersion": _TX_VERSION,
                    },
                )

                if compute_resp.status_code == 400:
                    body = compute_resp.text[:200]
                    self._log_failure(f"no route found (400): {body}")
                    return None

                if compute_resp.status_code == 429:
                    self._log_failure("rate-limited by Raydium API (429)")
                    return None

                compute_resp.raise_for_status()
                compute_json: Dict[str, Any] = compute_resp.json()

                if not compute_json.get("success"):
                    msg = compute_json.get("msg") or "success=false from compute"
                    self._log_failure(f"compute endpoint returned error: {msg}")
                    return None

                swap_data: Dict[str, Any] = compute_json.get("data") or {}

                out_lamports = _safe_int(swap_data.get("outputAmount"))
                min_out_lamports = _safe_int(swap_data.get("otherAmountThreshold"))
                price_impact_raw = swap_data.get("priceImpactPct", 0)

                if out_lamports <= 0:
                    self._log_failure(
                        "compute returned zero outputAmount — no liquidity"
                    )
                    return None

                price_impact_pct = _safe_decimal(price_impact_raw)
                amount_out = _decimal_from_lamports(out_lamports, output_decimals)
                amount_out_min = _decimal_from_lamports(
                    min_out_lamports, output_decimals
                )

                # ── Phase 2: POST /transaction/swap-base-in ───────────────
                tx_resp = await client.post(
                    f"{base}{_TRANSACTION_PATH}",
                    json={
                        "computeUnitPriceMicroLamports": _priority_fee(),
                        "swapResponse": swap_data,
                        "txVersion": _TX_VERSION,
                        "wallet": sender,
                        # Automatically wrap SOL → WSOL for native SOL inputs.
                        "wrapSol": _is_sol(input_mint),
                        # Automatically unwrap WSOL → SOL for native SOL outputs.
                        "unwrapSol": _is_sol(output_mint),
                    },
                )

                if tx_resp.status_code == 429:
                    self._log_failure(
                        "rate-limited on /transaction (429) — returning price-only quote"
                    )
                    # Return the price data without a transaction so the
                    # scorer can still compare against Jupiter. Execution
                    # will skip this quote if swap_transaction is None.
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
                        raw=swap_data,
                    )

                tx_resp.raise_for_status()
                tx_json: Dict[str, Any] = tx_resp.json()

                if not tx_json.get("success"):
                    msg = tx_json.get("msg") or "success=false from transaction build"
                    self._log_failure(f"transaction build failed: {msg}")
                    # Price is still valid — return without a transaction.
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
                        raw=swap_data,
                    )

                # Raydium returns a list of transactions — for a simple swap
                # this is always a single-element list.
                tx_list: list = tx_json.get("data") or []
                swap_transaction: Optional[str] = (
                    tx_list[0].get("transaction") if tx_list else None
                )

                if not swap_transaction:
                    self._log_failure(
                        "transaction response contained no transaction data"
                    )
                    return None

        except httpx.TimeoutException as exc:
            self._log_failure("request timed out", exc)
            return None
        except httpx.HTTPStatusError as exc:
            self._log_failure(f"HTTP {exc.response.status_code} from Raydium API", exc)
            return None
        except httpx.RequestError as exc:
            self._log_failure("network error contacting Raydium API", exc)
            return None
        except Exception as exc:
            self._log_failure("unexpected error", exc)
            return None

        self._log_debug(
            f"quote ok  out={amount_out:.6f}  impact={price_impact_pct}%  "
            f"priority_fee={_priority_fee()} microlamports"
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
            raw=swap_data,
        )
