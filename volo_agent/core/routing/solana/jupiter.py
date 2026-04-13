from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional

from core.utils.http import async_request_json
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
# Jupiter migrated quote/swap routes to /swap/v1 on the current API hosts.
_DEFAULT_API_BASE = "https://lite-api.jup.ag/swap/v1"

_QUOTE_PATH = "/quote"
_SWAP_PATH = "/swap"

def _api_base() -> str:
    return os.getenv("JUPITER_API_URL", _DEFAULT_API_BASE).rstrip("/")

class JupiterAggregator(SolanaSwapAggregator):
    name: str = "jupiter"
    TIMEOUT_SECONDS: float = 60.0

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
            quote_resp = await async_request_json(
                "GET",
                f"{base}{_QUOTE_PATH}",
                service="jupiter-quote",
                timeout=self.TIMEOUT_SECONDS,
                params={
                    "inputMint": input_mint,
                    "outputMint": output_mint,
                    "amount": str(amount_lamports),
                    "slippageBps": slippage_bps,
                    "onlyDirectRoutes": "false",
                    "asLegacyTransaction": "false",
                    "platformFeeBps": "0",
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

            # Build the unsigned VersionedTransaction from the quote.
            swap_resp = await async_request_json(
                "POST",
                f"{base}{_SWAP_PATH}",
                service="jupiter-swap",
                timeout=self.TIMEOUT_SECONDS,
                json={
                    "quoteResponse": quote_data,
                    "userPublicKey": sender,
                    "wrapAndUnwrapSol": True,
                    "dynamicComputeUnitLimit": True,
                    "prioritizationFeeLamports": "auto",
                },
            )

            if swap_resp.status_code == 429:
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
                swap_transaction = None

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
