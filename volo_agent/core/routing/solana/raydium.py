from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional

from config.solana_chains import get_solana_chain, normalize_solana_mint
from core.routing.models import SolanaSwapRouteQuote
from core.routing.solana.base import SolanaSwapAggregator
from core.routing.solana.utils import (
    decimal_from_lamports as _decimal_from_lamports,
)
from core.routing.solana.utils import (
    lamports_from_decimal as _lamports_from_decimal,
)
from core.routing.solana.utils import (
    safe_int as _safe_int,
)
from core.routing.utils import safe_decimal as _safe_decimal
from core.utils.http import async_request_json

_LOGGER = logging.getLogger("volo.routing.solana.raydium")
_DEFAULT_API_BASE = "https://transaction-v1.raydium.io"

_COMPUTE_PATH = "/compute/swap-base-in"
_TRANSACTION_PATH = "/transaction/swap-base-in"
_DEFAULT_PRIORITY_FEE = "500000"
_TX_VERSION = "V0"
_WSOL_MINT = "So11111111111111111111111111111111111111112"


def _api_base() -> str:
    return os.getenv("RAYDIUM_API_URL", _DEFAULT_API_BASE).rstrip("/")


def _priority_fee() -> str:
    return os.getenv("RAYDIUM_PRIORITY_FEE", _DEFAULT_PRIORITY_FEE).strip()


def _is_sol(mint: str) -> bool:
    return mint.strip().lower() in (_WSOL_MINT.lower(), "native", "sol")


class RaydiumAggregator(SolanaSwapAggregator):
    name: str = "raydium"
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
            compute_resp = await async_request_json(
                "GET",
                f"{base}{_COMPUTE_PATH}",
                params={
                    "inputMint": input_mint,
                    "outputMint": output_mint,
                    "amount": str(amount_lamports),
                    "slippageBps": slippage_bps,
                    "txVersion": _TX_VERSION,
                },
                timeout=self.TIMEOUT_SECONDS,
                service="raydium-compute",
            )

            if compute_resp.status_code == 400:
                body = compute_resp.text[:200]
                self._log_failure(f"no route found (400): {body}")
                return None

            if compute_resp.status_code == 429:
                self._log_failure("rate-limited by Raydium API (429)")
                return None

            if compute_resp.status_code < 200 or compute_resp.status_code >= 300:
                self._log_failure(
                    f"HTTP {compute_resp.status_code} from Raydium compute API: "
                    f"{compute_resp.text[:200]}"
                )
                return None

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
                self._log_failure("compute returned zero outputAmount — no liquidity")
                return None

            price_impact_pct = _safe_decimal(price_impact_raw)
            amount_out = _decimal_from_lamports(out_lamports, output_decimals)
            amount_out_min = _decimal_from_lamports(min_out_lamports, output_decimals)

            tx_resp = await async_request_json(
                "POST",
                f"{base}{_TRANSACTION_PATH}",
                json={
                    "computeUnitPriceMicroLamports": _priority_fee(),
                    "swapResponse": compute_json,
                    "txVersion": _TX_VERSION,
                    "wallet": sender,
                    "wrapSol": _is_sol(input_mint),
                    "unwrapSol": _is_sol(output_mint),
                },
                timeout=self.TIMEOUT_SECONDS,
                service="raydium-transaction",
            )

            if tx_resp.status_code == 429:
                self._log_failure(
                    "rate-limited on /transaction (429) — returning price-only quote"
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
                    raw=swap_data,
                )

            if tx_resp.status_code < 200 or tx_resp.status_code >= 300:
                self._log_failure(
                    f"HTTP {tx_resp.status_code} from Raydium transaction API: "
                    f"{tx_resp.text[:200]}"
                )
                return None

            tx_json: Dict[str, Any] = tx_resp.json()

            if not tx_json.get("success"):
                msg = tx_json.get("msg") or "success=false from transaction build"
                self._log_failure(f"transaction build failed: {msg}")
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

            tx_list: list = tx_json.get("data") or []
            swap_transaction: Optional[str] = (
                tx_list[0].get("transaction") if tx_list else None
            )

            if not swap_transaction:
                self._log_failure("transaction response contained no transaction data")
                return None

        except Exception as exc:
            err = str(exc).lower()
            if "timeout" in err:
                self._log_failure("request timed out", exc)
                return None
            if "connection" in err or "network" in err or "dns" in err:
                self._log_failure("network error contacting Raydium API", exc)
                return None
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
