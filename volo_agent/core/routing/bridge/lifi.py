from __future__ import annotations

import asyncio
import logging
import os
import time
from decimal import Decimal
from functools import partial
from typing import Any, Dict, List, Optional

from config.solana_chains import is_solana_chain_id, is_solana_network
from core.routing.bridge.base import BridgeAggregator
from core.routing.bridge.token_resolver import resolve_bridge_token
from core.routing.models import BridgeRouteQuote
from core.token_security.registry_lookup import get_native_decimals
from core.utils.async_tools import run_blocking
from core.utils.http import ExternalServiceError, raise_for_status, request_json
from core.utils.timeouts import EXTERNAL_HTTP_TIMEOUT_SECONDS

_LOGGER = logging.getLogger("volo.routing.bridge.lifi")
_API_BASE_URL = "https://li.quest/v1"
_QUOTE_ENDPOINT = "/quote"
_CHAINS_ENDPOINT = "/chains"

# Li.Fi uses the zero-address to represent native tokens on any chain.
_NATIVE_TOKEN_ADDRESS = "0x0000000000000000000000000000000000000000"

_LIFI_CHAIN_CACHE: Dict[str, tuple[int, float]] = {}
_LIFI_CHAIN_CACHE_TTL_SECONDS: float = 3600.0


def _api_key() -> Optional[str]:
    return os.getenv("LIFI_API_KEY", "").strip() or None


def _headers() -> Dict[str, str]:
    h: Dict[str, str] = {"Accept": "application/json"}
    key = _api_key()
    if key:
        h["x-lifi-api-key"] = key
    return h


def _extract_chains_payload(data: Any) -> list[dict]:
    if isinstance(data, dict):
        chains = data.get("chains") or data.get("data") or data.get("result")
    else:
        chains = data
    return chains if isinstance(chains, list) else []


def _find_solana_chain_id(chains: list[dict]) -> Optional[int]:
    for chain in chains:
        if not isinstance(chain, dict):
            continue
        key_raw = (
            chain.get("key")
            or chain.get("chainKey")
            or chain.get("name")
            or chain.get("chain")
            or ""
        )
        key = str(key_raw).strip().lower()
        if key in {"sol", "solana"} or "solana" in key:
            raw_id = chain.get("id") or chain.get("chainId") or chain.get("chain_id")
            if raw_id is None:
                continue
            try:
                return int(raw_id)
            except (TypeError, ValueError):
                continue
    return None


def _get_cached_lifi_chain_id(key: str) -> Optional[int]:
    cached = _LIFI_CHAIN_CACHE.get(key)
    if not cached:
        return None
    value, ts = cached
    if time.time() - ts > _LIFI_CHAIN_CACHE_TTL_SECONDS:
        return None
    return value


def _set_cached_lifi_chain_id(key: str, value: int) -> None:
    _LIFI_CHAIN_CACHE[key] = (value, time.time())


def _fetch_lifi_chains() -> list[dict]:
    resp = request_json(
        "GET",
        f"{_API_BASE_URL}{_CHAINS_ENDPOINT}",
        headers=_headers(),
        service="lifi-chains",
    )
    raise_for_status(resp, "lifi-chains")
    data = resp.json()
    return _extract_chains_payload(data)


async def _resolve_lifi_chain_id(
    chain_id: int,
    chain_name: str,
) -> Optional[int]:
    if not is_solana_chain_id(chain_id) and not is_solana_network(chain_name):
        return chain_id

    cached = _get_cached_lifi_chain_id("solana")
    if cached is not None:
        return cached

    try:
        chains = await run_blocking(_fetch_lifi_chains)
    except Exception as exc:
        _LOGGER.warning("[bridge:lifi] failed to fetch /chains: %s", exc)
        return None

    solana_id = _find_solana_chain_id(chains)
    if solana_id is None:
        _LOGGER.warning("[bridge:lifi] Solana chain id not found in /chains")
        return None

    _set_cached_lifi_chain_id("solana", solana_id)
    return solana_id


def _from_smallest_unit(amount_raw: Any, decimals: int) -> Decimal:

    try:
        return Decimal(str(int(amount_raw))) / Decimal(10**decimals)
    except Exception:
        return Decimal("0")


def _sum_fee_costs(
    fee_costs: List[Dict[str, Any]],
    token_decimals: int,
) -> Decimal:
    total = Decimal("0")
    for fee in fee_costs or []:
        raw_amount = fee.get("amount") or fee.get("amountUSD")
        if not raw_amount:
            continue
        # If the fee cost has its own token info with decimals, prefer it.
        fee_token_info = fee.get("token") or {}
        fee_decimals = fee_token_info.get("decimals")
        if fee_decimals is not None:
            try:
                dec = int(fee_decimals)
                total += _from_smallest_unit(raw_amount, dec)
                continue
            except Exception:
                pass
        # Fall back to input token decimals.
        try:
            total += _from_smallest_unit(raw_amount, token_decimals)
        except Exception:
            pass
    return total


def _extract_gas_cost_source(
    gas_costs: List[Dict[str, Any]],
    chain_id: int,
) -> Optional[Decimal]:
    if not gas_costs:
        return None

    total = Decimal("0")
    found_any = False
    native_decimals = get_native_decimals(chain_id)
    for entry in gas_costs:
        # Li.Fi returns gas cost as ``amountInNativeToken`` (a decimal string,
        # e.g. "0.002") or as ``amount`` in the native token's smallest unit.
        native_str = entry.get("amountInNativeToken") or entry.get("amountNative")
        if native_str:
            try:
                total += Decimal(str(native_str))
                found_any = True
                continue
            except Exception:
                pass

        raw_amount = entry.get("amount")
        if raw_amount is not None:
            try:
                # Gas cost amounts are in native-token units.
                total += Decimal(str(int(raw_amount))) / Decimal(
                    10**native_decimals
                )
                found_any = True
            except Exception:
                pass

    return total if found_any else None


def _fetch_quote(
    from_chain_id: int,
    to_chain_id: int,
    from_token: str,
    to_token: str,
    from_amount_wei: int,
    from_address: str,
    to_address: str,
    slippage_pct: float,
    timeout: float,
) -> Dict[str, Any]:
    url = f"{_API_BASE_URL}{_QUOTE_ENDPOINT}"

    # Li.Fi slippage is expressed as a decimal fraction (not basis points).
    slippage_fraction = round(slippage_pct / 100.0, 6)

    params: Dict[str, Any] = {
        "fromChain": from_chain_id,
        "toChain": to_chain_id,
        "fromToken": from_token,
        "toToken": to_token,
        "fromAmount": str(from_amount_wei),
        "fromAddress": from_address,
        "toAddress": to_address,
        "slippage": slippage_fraction,
    }

    resp = request_json(
        "GET",
        url,
        params=params,
        headers=_headers(),
        timeout=timeout,
        service="lifi-quote",
    )
    raise_for_status(resp, "lifi-quote")
    return resp.json()

class LiFiAggregator(BridgeAggregator):
    name: str = "lifi"
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
        symbol = token_symbol.strip().upper()

        source_lifi_chain_id, dest_lifi_chain_id = await asyncio.gather(
            _resolve_lifi_chain_id(source_chain_id, source_chain_name),
            _resolve_lifi_chain_id(dest_chain_id, dest_chain_name),
        )
        if not source_lifi_chain_id or not dest_lifi_chain_id:
            self._log_failure("could not resolve Li.Fi chain id(s)")
            return None

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
            self._log_failure("token resolution failed for Li.Fi quote")
            return None

        source_is_solana = is_solana_chain_id(source_chain_id) or is_solana_network(
            source_chain_name
        )
        dest_is_solana = is_solana_chain_id(dest_chain_id) or is_solana_network(
            dest_chain_name
        )

        from_token = source_token.address
        if not source_is_solana and source_token.is_native:
            from_token = _NATIVE_TOKEN_ADDRESS

        to_token = dest_token.address
        if not dest_is_solana and dest_token.is_native:
            to_token = _NATIVE_TOKEN_ADDRESS

        src_decimals = int(source_token.decimals)
        dest_decimals = int(dest_token.decimals)

        # Convert to smallest unit (wei equivalent)
        from_amount_wei = int(amount * Decimal(10**src_decimals))
        if from_amount_wei <= 0:
            self._log_failure("from_amount_wei is zero or negative after conversion")
            return None

        timeout = min(self.TIMEOUT_SECONDS, EXTERNAL_HTTP_TIMEOUT_SECONDS)

        # Default slippage: 0.5 %.  We don't expose per-aggregator slippage in
        # the router interface, so we use a safe default.  This can be made
        # configurable via an env var if needed.
        slippage_pct = float(os.getenv("LIFI_DEFAULT_SLIPPAGE_PCT", "0.5"))

        try:
            data = await run_blocking(
                partial(
                    _fetch_quote,
                    source_lifi_chain_id,
                    dest_lifi_chain_id,
                    from_token,
                    to_token,
                    from_amount_wei,
                    sender,
                    recipient,
                    slippage_pct,
                    timeout,
                )
            )
        except ExternalServiceError as exc:
            self._log_failure("API error", exc)
            return None
        except Exception as exc:
            self._log_failure("unexpected error", exc)
            return None

        # ── Parse estimate ────────────────────────────────────────────────
        estimate: Dict[str, Any] = data.get("estimate") or {}
        if not estimate:
            self._log_failure("response missing estimate object")
            return None

        raw_to_amount = estimate.get("toAmount")
        if not raw_to_amount:
            self._log_failure("estimate missing toAmount")
            return None

        output_amount = _from_smallest_unit(raw_to_amount, dest_decimals)
        if output_amount <= 0:
            self._log_failure(
                f"output_amount parsed as zero or negative ({raw_to_amount})"
            )
            return None

        # ── Compute fees ──────────────────────────────────────────────────
        fee_costs: List[Dict[str, Any]] = estimate.get("feeCosts") or []
        total_fee = _sum_fee_costs(fee_costs, src_decimals)

        # Fee as percentage of input amount.
        if amount > 0:
            total_fee_pct = (total_fee / amount) * Decimal("100")
        else:
            total_fee_pct = Decimal("0")

        # Clamp fee_pct to [0, 100] to handle any edge-case data anomalies.
        total_fee_pct = max(Decimal("0"), min(Decimal("100"), total_fee_pct))

        # ── Fill time ─────────────────────────────────────────────────────
        fill_time_seconds = 0
        raw_duration = estimate.get("executionDuration")
        if raw_duration is not None:
            try:
                fill_time_seconds = int(float(str(raw_duration)))
            except (ValueError, TypeError):
                pass

        # ── Gas cost on source chain ──────────────────────────────────────
        gas_costs: List[Dict[str, Any]] = estimate.get("gasCosts") or []
        gas_cost_source = _extract_gas_cost_source(gas_costs, source_chain_id)

        # ── Transaction request (execution calldata) ──────────────────────
        tx_request: Optional[Dict[str, Any]] = data.get("transactionRequest") or None
        calldata: Optional[str] = None
        to_contract: Optional[str] = None

        if isinstance(tx_request, dict):
            calldata = tx_request.get("data") or None
            to_contract = tx_request.get("to") or None

        # ── Extract which bridge protocol Li.Fi chose ─────────────────────
        # Li.Fi routes through one or more steps.  For a direct bridge the
        # first step's ``toolDetails.name`` identifies the underlying bridge.
        # We append this to the aggregator name for PerformanceLedger keying.
        bridge_tool_name = ""
        steps: List[Dict[str, Any]] = data.get("includedSteps") or []
        if not steps:
            steps = data.get("steps") or []
        if steps:
            first_step = steps[0] if isinstance(steps[0], dict) else {}
            tool_details = first_step.get("toolDetails") or {}
            bridge_tool_name = tool_details.get("name", "")

        if bridge_tool_name:
            self._log_debug(
                f"quote ok via {bridge_tool_name}: "
                f"{source_chain_name}→{dest_chain_name} "
                f"out={output_amount:.6f} fee={total_fee_pct:.2f}% "
                f"fill≈{fill_time_seconds}s "
                f"calldata={'yes' if calldata else 'no'}"
            )
        else:
            self._log_debug(
                f"quote ok: {source_chain_name}→{dest_chain_name} "
                f"out={output_amount:.6f} fee={total_fee_pct:.2f}% "
                f"fill≈{fill_time_seconds}s "
                f"calldata={'yes' if calldata else 'no'}"
            )

        return BridgeRouteQuote(
            aggregator=self.name,
            token_symbol=symbol,
            source_chain_id=source_chain_id,
            dest_chain_id=dest_chain_id,
            source_chain_name=source_chain_name,
            dest_chain_name=dest_chain_name,
            input_amount=amount,
            output_amount=output_amount,
            total_fee=total_fee,
            total_fee_pct=total_fee_pct,
            estimated_fill_time_seconds=fill_time_seconds,
            gas_cost_source=gas_cost_source,
            calldata=calldata,
            to=to_contract,
            # Store the full transactionRequest so the executor can submit it
            # directly.  The steps list is also stored for audit / debugging.
            tool_data={
                "transactionRequest": tx_request,
                "steps": steps,
                "bridge": bridge_tool_name,
                "fromAmountUSD": estimate.get("fromAmountUSD"),
                "toAmountUSD": estimate.get("toAmountUSD"),
            },
            raw=data,
        )
