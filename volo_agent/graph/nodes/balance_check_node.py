from __future__ import annotations

import asyncio
import logging
import os
import time
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Tuple

from langchain_core.messages import AIMessage

from config.chains import get_chain_by_name
from config.solana_chains import fetch_solana_token_decimals, get_solana_chain
from core.fees.chains import FeeChain, is_native_token, resolve_fee_chain
from core.fees.fee_engine import FeeEngine
from core.fees.fee_reducer import FeeContext
from core.memory.ledger import get_ledger
from core.planning.execution_plan import resolve_dynamic_args
from core.planning.vws_execution import VWSFailure, simulate_execution_plan
from core.reservations.common import normalize_wallet_scope, resource_key
from core.reservations.service import get_reservation_service
from core.token_security.registry_lookup import (
    get_registry_decimals_by_address_async,
)
from core.token_security.token_db import (
    TokenRegistryEntry,
    get_async_token_registry,
)
from core.transfers.planning import resolve_transfer_planning_metadata
from graph.agent_state import AgentState
from wallet_service.evm.get_native_bal import get_native_balance_async
from wallet_service.evm.get_single_erc20_token_bal import get_token_balance_async
from wallet_service.solana.get_native_bal import (
    get_native_balance_async as get_native_balance_solana_async,
)
from wallet_service.solana.get_single_token_bal import (
    get_token_balance_async as get_token_balance_solana_async,
)

logger = logging.getLogger(__name__)


# Backwards-compatible aliases used by existing tests and targeted patches.
get_native_balance = None
get_token_balance = None
get_solana_native_balance = None
get_solana_token_balance = None

_EVM_NATIVE_BALANCE_FN = None
_EVM_TOKEN_BALANCE_FN = None
_SOLANA_NATIVE_BALANCE_FN = None
_SOLANA_TOKEN_BALANCE_FN = None

# Zero address — used as the native token placeholder
_NATIVE = "0x0000000000000000000000000000000000000000"


@dataclass(frozen=True)
class _BalanceChainContext:
    fee_chain: FeeChain
    chain_obj: Any | None = None

    @property
    def family(self) -> str:
        return self.fee_chain.family

    @property
    def display_name(self) -> str:
        return self.fee_chain.name

    @property
    def cache_name(self) -> str:
        return self.fee_chain.network

    @property
    def native_symbol(self) -> str:
        return self.fee_chain.native_symbol

    @property
    def native_token_ref(self) -> str:
        return self.fee_chain.native_token_ref


def _is_native(address: str, chain) -> bool:
    return str(address or "").strip().lower() == _NATIVE


def _resolve_balance_chain(args: Dict[str, Any], tool: str | None = None) -> _BalanceChainContext | None:
    fee_chain = resolve_fee_chain(args, tool=tool)
    if fee_chain is None:
        return None
    if fee_chain.family == "evm":
        return _BalanceChainContext(
            fee_chain=fee_chain,
            chain_obj=get_chain_by_name(fee_chain.name),
        )
    if fee_chain.family == "solana":
        return _BalanceChainContext(
            fee_chain=fee_chain,
            chain_obj=get_solana_chain(fee_chain.network),
        )
    return None


def _balance_key(sender: str, chain_name: str, token_ref: str) -> str:
    return resource_key(sender, chain_name, token_ref)


def _normalize_token_key(token_ref: str, chain_ctx: _BalanceChainContext) -> str:
    if chain_ctx.family == "evm":
        if str(token_ref or "").strip().lower() == _NATIVE:
            return _NATIVE
        return str(token_ref).strip().lower()
    if is_native_token(token_ref, chain_ctx.fee_chain):
        return chain_ctx.native_token_ref
    return str(token_ref).strip().lower()


def _native_decimals(chain_ctx: _BalanceChainContext) -> int:
    return 9 if chain_ctx.family == "solana" else 18


def _solana_wallet_key(sender: str, network: str) -> Tuple[str, str]:
    return str(sender).strip().lower(), str(network).strip().lower()


def _to_base_units_non_negative(amount: Decimal, decimals: int) -> int:
    if decimals < 0:
        return 0
    try:
        value = Decimal(str(amount))
    except Exception:
        return 0
    if not value.is_finite() or value < 0:
        return 0
    scaled = value * (Decimal(10) ** int(decimals))
    try:
        return max(0, int(scaled))
    except Exception:
        return 0


def _record_resource_snapshot(
    resource_snapshots: Dict[str, Dict[str, Any]] | None,
    *,
    resource_key_value: str,
    wallet_scope: str | None,
    sender: str,
    chain_name: str,
    token_ref: str,
    symbol: str,
    decimals: int,
    available: Decimal,
    chain_family: str,
    reserved: Decimal = Decimal("0"),
    net_available: Decimal | None = None,
) -> None:
    if resource_snapshots is None or not wallet_scope:
        return
    reserved_amount = max(Decimal("0"), reserved)
    net_available_amount = (
        max(Decimal("0"), net_available)
        if net_available is not None
        else max(Decimal("0"), available - reserved_amount)
    )
    resource_snapshots[resource_key_value] = {
        "resource_key": resource_key_value,
        "wallet_scope": str(wallet_scope).strip().lower(),
        "sender": str(sender).strip().lower(),
        "chain": str(chain_name).strip().lower(),
        "token_ref": str(token_ref).strip().lower(),
        "symbol": str(symbol).strip().upper(),
        "decimals": int(decimals),
        "available": str(available),
        "available_base_units": str(
            _to_base_units_non_negative(available, int(decimals))
        ),
        "reserved": str(reserved_amount),
        "reserved_base_units": str(
            _to_base_units_non_negative(reserved_amount, int(decimals))
        ),
        "net_available": str(net_available_amount),
        "net_available_base_units": str(
            _to_base_units_non_negative(net_available_amount, int(decimals))
        ),
        "observed_at": str(int(time.time())),
        "chain_family": str(chain_family).strip().lower(),
    }


def _add_reservation_requirement(
    reservation_requirements: Dict[str, List[Dict[str, Any]]],
    *,
    node_id: str,
    resource_snapshot: Dict[str, Any] | None,
    required: Decimal,
    kind: str,
) -> None:
    if not isinstance(resource_snapshot, dict):
        return
    if required <= 0:
        return
    resource_key_value = (
        str(resource_snapshot.get("resource_key") or "").strip().lower()
    )
    wallet_scope = str(resource_snapshot.get("wallet_scope") or "").strip().lower()
    if not resource_key_value or not wallet_scope:
        return
    decimals = int(resource_snapshot.get("decimals") or 0)
    required_base_units = _to_base_units_non_negative(required, decimals)
    if required_base_units <= 0:
        return

    requirements = reservation_requirements.setdefault(str(node_id), [])
    for entry in requirements:
        if str(entry.get("resource_key") or "").strip().lower() != resource_key_value:
            continue
        existing_required = _to_decimal(entry.get("required")) or Decimal("0")
        entry["required"] = str(existing_required + required)
        entry["required_base_units"] = str(
            int(entry.get("required_base_units") or 0) + required_base_units
        )
        existing_kind = str(entry.get("kind") or "").strip().lower()
        if kind not in existing_kind.split("+"):
            entry["kind"] = f"{existing_kind}+{kind}" if existing_kind else kind
        return

    requirements.append(
        {
            "resource_key": resource_key_value,
            "wallet_scope": wallet_scope,
            "sender": resource_snapshot.get("sender"),
            "chain": resource_snapshot.get("chain"),
            "token_ref": resource_snapshot.get("token_ref"),
            "symbol": resource_snapshot.get("symbol"),
            "decimals": decimals,
            "required": str(required),
            "required_base_units": str(required_base_units),
            "kind": str(kind).strip().lower(),
        }
    )


async def _get_native_balance_for_chain(
    *,
    sender: str,
    chain_ctx: _BalanceChainContext,
    native_balance_cache: Dict[str, Decimal],
    balance_snapshot: Dict[str, str],
    resource_snapshots: Dict[str, Dict[str, Any]] | None = None,
    wallet_scope: str | None = None,
) -> tuple[str, Decimal]:
    native_balance_key = _balance_key(
        sender, chain_ctx.cache_name, chain_ctx.native_token_ref
    )
    if native_balance_key in native_balance_cache:
        native_balance = native_balance_cache[native_balance_key]
    elif chain_ctx.family == "solana":
        native_balance = await get_native_balance_solana_async(
            sender, network=chain_ctx.fee_chain.network
        )
        native_balance_cache[native_balance_key] = native_balance
    else:
        chain_obj = chain_ctx.chain_obj
        if chain_obj is None:
            raise RuntimeError(
                "EVM chain configuration unavailable for balance lookup."
            )
        native_balance = await get_native_balance_async(
            sender,
            chain_obj.rpc_url,
        )
        native_balance_cache[native_balance_key] = native_balance
    balance_snapshot.setdefault(native_balance_key, str(native_balance))
    _record_resource_snapshot(
        resource_snapshots,
        resource_key_value=native_balance_key,
        wallet_scope=wallet_scope,
        sender=sender,
        chain_name=chain_ctx.cache_name,
        token_ref=chain_ctx.native_token_ref,
        symbol=chain_ctx.native_symbol,
        decimals=_native_decimals(chain_ctx),
        available=native_balance,
        chain_family=chain_ctx.family,
    )
    return native_balance_key, native_balance


def _extract_solana_wallet_balances(
    balances: Iterable[Any],
) -> Dict[str, tuple[Decimal, int]]:
    from wallet_service.common.balance_utils import parse_decimal as _parse_decimal

    extracted: Dict[str, tuple[Decimal, int]] = {}
    for item in balances:
        token = getattr(item, "token", None)
        amount = getattr(item, "amount", None)
        mint_address = str(getattr(token, "mint_address", None) or "").strip().lower()
        if not mint_address:
            continue
        raw_amount = _parse_decimal(getattr(amount, "amount", None) if amount else None)
        decimals = _parse_decimal(getattr(amount, "decimals", None) if amount else None)
        if raw_amount is None or decimals is None:
            continue
        try:
            decimals_int = int(decimals)
        except Exception:
            continue
        if decimals_int < 0 or decimals_int > 36:
            continue
        extracted[mint_address] = (
            raw_amount / (Decimal(10) ** decimals_int),
            decimals_int,
        )
    return extracted


async def _prefetch_solana_wallet_balances(
    wallet_requests: Iterable[Tuple[str, str]],
    *,
    logs: List[str],
) -> Dict[Tuple[str, str], Dict[str, tuple[Decimal, int]]]:
    from wallet_service.solana.cdp_utils import list_token_balances_async

    unique_requests = sorted(set(wallet_requests))
    if not unique_requests:
        return {}

    logs.append(
        f"[VWS] Prefetching Solana token balances for {len(unique_requests)} wallet(s)."
    )
    results = await asyncio.gather(
        *(
            list_token_balances_async(sender, network=network)
            for sender, network in unique_requests
        ),
        return_exceptions=True,
    )

    prefetched: Dict[Tuple[str, str], Dict[str, tuple[Decimal, int]]] = {}
    for (sender, network), result in zip(unique_requests, results):
        if isinstance(result, BaseException):
            logs.append(
                f"[VWS] Solana token prefetch failed for {sender} on {network}; falling back to per-token reads."
            )
            continue
        prefetched[(sender, network)] = _extract_solana_wallet_balances(result)
    return prefetched


async def _get_solana_token_balance_and_decimals(
    *,
    sender: str,
    token_ref: str,
    network: str,
    wallet_balances: Dict[str, tuple[Decimal, int]] | None = None,
) -> tuple[Decimal, int]:
    target = str(token_ref).strip().lower()
    cached_wallet_balance = (wallet_balances or {}).get(target)
    if cached_wallet_balance is not None:
        return cached_wallet_balance

    available = await get_token_balance_solana_async(
        sender,
        token_ref,
        network=network,
    )

    chain_cfg = get_solana_chain(network)
    chain_id = chain_cfg.chain_id

    cached = await get_registry_decimals_by_address_async(token_ref, chain_id)
    if cached is not None:
        return available, int(cached)

    decimals_int = await fetch_solana_token_decimals(token_ref, chain_cfg.rpc_url)

    registry = get_async_token_registry()
    if registry is not None:
        try:
            entry = TokenRegistryEntry(
                symbol=token_ref[:12].upper(),
                chain_name=chain_cfg.name.lower(),
                chain_id=chain_id,
                address=token_ref,
                decimals=decimals_int,
                source="onchain_fallback",
            )
            await registry.upsert(entry)
        except Exception as exc:
            logger.warning(
                "_get_solana_token_balance_and_decimals: failed to persist decimals for %s: %s",
                token_ref,
                exc,
            )

    return available, decimals_int


async def _get_token_balance_for_chain(
    *,
    sender: str,
    token_ref: str,
    token_symbol_hint: str | None,
    chain_ctx: _BalanceChainContext,
    native_balance_cache: Dict[str, Decimal],
    balance_snapshot: Dict[str, str],
    token_balance_cache: Dict[str, Decimal] | None = None,
    solana_wallet_balances: Dict[Tuple[str, str], Dict[str, tuple[Decimal, int]]]
    | None = None,
    resource_snapshots: Dict[str, Dict[str, Any]] | None = None,
    wallet_scope: str | None = None,
) -> tuple[str, str, Decimal]:
    balance_key = _balance_key(
        sender,
        chain_ctx.cache_name,
        _normalize_token_key(token_ref, chain_ctx),
    )
    if token_balance_cache is not None and balance_key in token_balance_cache:
        available = token_balance_cache[balance_key]
        symbol = str(token_symbol_hint or token_ref[:8])
        return balance_key, symbol, available

    if chain_ctx.family == "evm":
        chain_obj = chain_ctx.chain_obj
        if chain_obj is None:
            raise RuntimeError(
                "EVM chain configuration unavailable for balance lookup."
            )
        if _is_native(token_ref, chain_obj):
            balance_key_native, available = await _get_native_balance_for_chain(
                sender=sender,
                chain_ctx=chain_ctx,
                native_balance_cache=native_balance_cache,
                balance_snapshot=balance_snapshot,
                resource_snapshots=resource_snapshots,
                wallet_scope=wallet_scope,
            )
            if token_balance_cache is not None:
                token_balance_cache[balance_key_native] = available
            return balance_key_native, chain_ctx.native_symbol, available

        decimals = await get_registry_decimals_by_address_async(
            token_ref,
            chain_obj.chain_id,
        )
        if decimals is None:
            logger.error(
                "balance_check_node: could not resolve decimals for %s on chain %s "
                "even after on-chain fallback. check RPC connectivity or token address.",
                token_ref,
                chain_obj.chain_id,
            )
            # We cannot accurately check balance without decimals.
            raise ValueError(f"Could not resolve decimals for token {token_ref}")

        available = await get_token_balance_async(
            sender,
            token_ref,
            decimals,
            chain_obj.rpc_url,
        )
        if token_balance_cache is not None:
            token_balance_cache[balance_key] = available
        symbol = str(token_symbol_hint or token_ref[:8])
        balance_snapshot.setdefault(balance_key, str(available))
        _record_resource_snapshot(
            resource_snapshots,
            resource_key_value=balance_key,
            wallet_scope=wallet_scope,
            sender=sender,
            chain_name=chain_ctx.cache_name,
            token_ref=_normalize_token_key(token_ref, chain_ctx),
            symbol=symbol,
            decimals=decimals,
            available=available,
            chain_family=chain_ctx.family,
        )
        return balance_key, symbol, available

    if is_native_token(token_ref, chain_ctx.fee_chain):
        balance_key_native, available = await _get_native_balance_for_chain(
            sender=sender,
            chain_ctx=chain_ctx,
            native_balance_cache=native_balance_cache,
            balance_snapshot=balance_snapshot,
            resource_snapshots=resource_snapshots,
            wallet_scope=wallet_scope,
        )
        if token_balance_cache is not None:
            token_balance_cache[balance_key_native] = available
        return balance_key_native, chain_ctx.native_symbol, available

    available, decimals = await _get_solana_token_balance_and_decimals(
        sender=sender,
        token_ref=token_ref,
        network=chain_ctx.fee_chain.network,
        wallet_balances=(solana_wallet_balances or {}).get(
            _solana_wallet_key(sender, chain_ctx.fee_chain.network)
        ),
    )
    if token_balance_cache is not None:
        token_balance_cache[balance_key] = available
    symbol = str(token_symbol_hint or token_ref[:8])
    balance_snapshot.setdefault(balance_key, str(available))
    _record_resource_snapshot(
        resource_snapshots,
        resource_key_value=balance_key,
        wallet_scope=wallet_scope,
        sender=sender,
        chain_name=chain_ctx.cache_name,
        token_ref=_normalize_token_key(token_ref, chain_ctx),
        symbol=symbol,
        decimals=decimals,
        available=available,
        chain_family=chain_ctx.family,
    )
    return balance_key, symbol, available


def _to_decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except Exception:
        return None


async def _apply_reserved_balances(
    *,
    balance_snapshot: Dict[str, str],
    resource_snapshots: Dict[str, Dict[str, Any]],
    logs: List[str],
) -> None:
    if not resource_snapshots:
        return

    wallet_resource_keys: Dict[str, List[str]] = {}
    for resource_key_value, snapshot in resource_snapshots.items():
        wallet_scope = str(snapshot.get("wallet_scope") or "").strip().lower()
        if not wallet_scope:
            continue
        wallet_resource_keys.setdefault(wallet_scope, []).append(resource_key_value)

    if not wallet_resource_keys:
        return

    try:
        reservation_service = await get_reservation_service()
    except Exception as exc:
        # Spend preflight must fail closed if the global reservation service is
        # unavailable. Continuing with raw on-chain balances would reintroduce
        # the same oversubscription race the reservation layer is meant to stop.
        raise RuntimeError(
            "Reservation service unavailable during spend preflight."
        ) from exc
    if reservation_service is None:
        raise RuntimeError("Reservation service unavailable during spend preflight.")

    lookup_items = list(wallet_resource_keys.items())
    # Reservation totals are independent lookups, so gather them to avoid
    # serial round-trips when a plan touches multiple wallet scopes.
    lookup_results: List[Dict[str, int] | BaseException] = await asyncio.gather(
        *(
            reservation_service.get_reserved_totals(
                wallet_scope=wallet_scope,
                resource_keys=resource_keys,
            )
            for wallet_scope, resource_keys in lookup_items
        ),
        return_exceptions=True,
    )

    adjusted_resources = 0
    for (wallet_scope, resource_keys), reserved_totals in zip(
        lookup_items, lookup_results
    ):
        if isinstance(reserved_totals, BaseException):
            raise RuntimeError(
                f"Reservation totals unavailable for wallet scope {wallet_scope}."
            ) from reserved_totals

        for resource_key_value in resource_keys:
            snapshot = resource_snapshots.get(resource_key_value)
            if not isinstance(snapshot, dict):
                continue
            decimals = int(snapshot.get("decimals") or 0)
            available = _to_decimal(snapshot.get("available")) or Decimal("0")
            reserved_base_units = max(
                0, int(reserved_totals.get(resource_key_value, 0))
            )
            reserved = (
                Decimal(reserved_base_units) / (Decimal(10) ** decimals)
                if decimals >= 0
                else Decimal("0")
            )
            net_available = max(Decimal("0"), available - reserved)
            snapshot["reserved"] = str(reserved)
            snapshot["reserved_base_units"] = str(reserved_base_units)
            snapshot["net_available"] = str(net_available)
            snapshot["net_available_base_units"] = str(
                _to_base_units_non_negative(net_available, decimals)
            )
            balance_snapshot[resource_key_value] = str(net_available)
            adjusted_resources += 1

    if adjusted_resources > 0:
        logs.append(
            f"[VWS] Applied reserved-balance deductions to {adjusted_resources} resource(s)."
        )


def _serialize_vws_failure(failure: VWSFailure | None) -> Dict[str, Any] | None:
    if failure is None:
        return None
    payload = {
        "node_id": failure.node_id,
        "tool": failure.tool,
        "category": failure.category,
        "reason": failure.reason,
    }
    if failure.path:
        payload["path"] = failure.path
    return payload


def _build_vws_failure_message(failure: VWSFailure) -> str:
    if failure.category == "gas_shortfall":
        prefix = "Insufficient gas"
    elif failure.category == "insufficient_funds":
        prefix = "Insufficient balance"
    elif failure.category == "route_meta_validation_failure":
        prefix = "Route validation failed"
    elif failure.category == "route_expired":
        prefix = "Route expired"
    elif failure.category == "slippage_exceeded":
        prefix = "Minimum output check failed"
    else:
        prefix = "Dependency resolution failure"
    if failure.path:
        return f"{prefix} for {failure.node_id}: {failure.reason} ({failure.path})."
    return f"{prefix} for {failure.node_id}: {failure.reason}."


def _has_vws_payload(state: AgentState) -> bool:
    return any(
        key in state and state.get(key) is not None
        for key in ("vws_simulation", "vws_failure", "projected_deltas")
    )


_VWS_ASSERT_ENV = "VOLO_ASSERT_BALANCE_CHECK_VWS"
_VWS_PASSTHROUGH_KEYS = (
    "plan_history",
    "balance_snapshot",
    "resource_snapshots",
    "native_requirements",
    "reservation_requirements",
    "projected_deltas",
    "preflight_estimates",
    "vws_simulation",
    "vws_failure",
)


def _default_vws_value(key: str) -> Any:
    if key == "vws_failure":
        return None
    return [] if key == "plan_history" else {}


def _extract_vws_passthrough(payload: Dict[str, Any]) -> Dict[str, Any]:
    extracted: Dict[str, Any] = {}
    for key in _VWS_PASSTHROUGH_KEYS:
        value = payload.get(key)
        if value is None:
            extracted[key] = _default_vws_value(key)
        elif isinstance(value, dict):
            extracted[key] = dict(value)
        elif isinstance(value, list):
            extracted[key] = list(value)
        else:
            extracted[key] = value
    return extracted


def _assert_vws_passthrough(
    balance_check_output: Dict[str, Any],
    vws_output: Dict[str, Any],
) -> None:
    enabled = str(os.getenv(_VWS_ASSERT_ENV, "")).strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return
    assert _extract_vws_passthrough(balance_check_output) == _extract_vws_passthrough(
        vws_output
    )


def _quote_plan_fees(plan) -> tuple[List[Dict[str, Any]], Dict[str, Decimal]]:
    fee_engine = FeeEngine()
    ledger = get_ledger()
    default_sender = ""
    for node in plan.nodes.values():
        sender = node.args.get("sender", "")
        if sender:
            default_sender = sender
            break
    fee_context = FeeContext(
        sender=default_sender,
        total_lifetime_txs=ledger.get_total_lifetime_txs(),
        monthly_volume_usd=Decimal("0"),
        platform_token_balance=Decimal("0"),
        is_referral=False,
        referral_code="",
    )
    fee_quotes = fee_engine.quote_plan(plan, fee_context)
    platform_fee_native_by_node: Dict[str, Decimal] = {}
    for quote in fee_quotes:
        fee_amount = _to_decimal(getattr(quote, "fee_amount_native", None))
        if fee_amount is None or fee_amount <= 0:
            continue
        platform_fee_native_by_node[str(quote.node_id)] = fee_amount
    return [quote.to_dict() for quote in fee_quotes], platform_fee_native_by_node


def _copy_vws_payload_from_state(state: AgentState) -> Dict[str, Any]:
    return {
        "plan_history": [],
        "reasoning_logs": [],
        "route_decision": state.get("route_decision"),
        "messages": list(state.get("messages") or []),
        "balance_snapshot": dict(state.get("balance_snapshot") or {}),
        "resource_snapshots": dict(state.get("resource_snapshots") or {}),
        "native_requirements": dict(state.get("native_requirements") or {}),
        "reservation_requirements": dict(state.get("reservation_requirements") or {}),
        "projected_deltas": dict(state.get("projected_deltas") or {}),
        "preflight_estimates": dict(state.get("preflight_estimates") or {}),
        "vws_simulation": dict(state.get("vws_simulation") or {}),
        "vws_failure": state.get("vws_failure"),
        "fee_quotes": list(state.get("fee_quotes") or []),
    }


def _build_vws_exception_payload(
    *,
    exc: Exception,
    logs: List[str],
    balance_snapshot: Dict[str, str],
    resource_snapshots: Dict[str, Dict[str, Any]],
    preflight_estimates: Dict[str, Dict[str, Any]],
    fee_quotes: List[Dict[str, Any]],
) -> Dict[str, Any]:
    failure = {
        "node_id": "vws",
        "tool": "vws",
        "category": "vws_internal_error",
        "reason": str(exc),
    }
    logs.append(f"[VWS] Internal error: {type(exc).__name__} — {exc}")
    return {
        "route_decision": "end",
        "reasoning_logs": logs,
        "messages": [
            AIMessage(
                content=(
                    "VWS validation failed before confirmation. "
                    "Recovery path: retry so the plan can be re-simulated."
                )
            )
        ],
        "balance_snapshot": balance_snapshot,
        "resource_snapshots": resource_snapshots,
        "native_requirements": {},
        "reservation_requirements": {},
        "projected_deltas": {},
        "preflight_estimates": preflight_estimates,
        "vws_simulation": {},
        "vws_failure": failure,
        "fee_quotes": fee_quotes,
    }


async def run_vws_preflight(state: AgentState) -> Dict[str, Any]:
    plan_history = state.get("plan_history") or []
    if not plan_history:
        return {}

    plan = plan_history[-1]
    execution_state = state.get("execution_state")
    logs: List[str] = []
    balance_snapshot: Dict[str, str] = {}
    resource_snapshots: Dict[str, Dict[str, Any]] = {}
    native_balance_cache: Dict[str, Decimal] = {}
    token_balance_cache: Dict[str, Decimal] = {}
    preflight_estimates: Dict[str, Dict[str, Any]] = dict(
        state.get("preflight_estimates") or {}
    )
    fee_quotes, platform_fee_native_by_node = _quote_plan_fees(plan)

    balance_reqs = set()
    gas_reqs = set()
    solana_wallet_reqs: set[Tuple[str, str]] = set()
    for node in plan.nodes.values():
        if node.tool == "check_balance":
            continue
        args = (
            resolve_dynamic_args(
                node.args,
                execution_state,
                context=state.get("artifacts"),
            )
            if execution_state
            else node.args
        )
        sender = args.get("sender")
        if not sender:
            continue
        try:
            chain_ctx = _resolve_balance_chain(args, tool=node.tool)
        except Exception:
            chain_ctx = None
        transfer_meta = None
        if node.tool == "transfer":
            try:
                transfer_meta = resolve_transfer_planning_metadata(args)
                chain_ctx = _resolve_balance_chain({"network": transfer_meta.network}, tool=node.tool)
            except ValueError:
                continue
            except Exception:
                continue
            if chain_ctx is None:
                continue
        elif chain_ctx is None:
            continue
        wallet_scope = normalize_wallet_scope(
            sender=sender,
            sub_org_id=args.get("sub_org_id"),
        )
        if transfer_meta is not None:
            token_ref = transfer_meta.asset_ref
            token_is_native = transfer_meta.asset_kind == "native"
            token_symbol = args.get("asset_symbol") or args.get("token_symbol")
        else:
            token_ref = (
                args.get("token_in_address")
                or args.get("token_address")
                or args.get("asset_ref")
                or args.get("source_address")
                or args.get("token_in_mint")
                or args.get("token_mint")
                or args.get("mint")
                or args.get("source_mint")
                or args.get("input_token")
            )
            token_is_native = False
            if token_ref:
                try:
                    token_is_native = (
                        _normalize_token_key(str(token_ref), chain_ctx)
                        == chain_ctx.native_token_ref
                    )
                except Exception:
                    token_is_native = False
            token_symbol = (
                args.get("token_in_symbol")
                or args.get("token_symbol")
                or args.get("asset_symbol")
                or args.get("symbol")
            )
        if not token_is_native:
            gas_reqs.add((sender, chain_ctx, wallet_scope))
        if token_ref:
            balance_reqs.add(
                (
                    sender,
                    str(token_ref),
                    token_symbol,
                    chain_ctx,
                    wallet_scope,
                )
            )
            if chain_ctx.family == "solana" and not token_is_native:
                solana_wallet_reqs.add(
                    _solana_wallet_key(sender, chain_ctx.fee_chain.network)
                )

    solana_wallet_balances = await _prefetch_solana_wallet_balances(
        solana_wallet_reqs,
        logs=logs,
    )

    fetch_tasks = []
    for sender, chain_ctx, wallet_scope in gas_reqs:
        fetch_tasks.append(
            _get_native_balance_for_chain(
                sender=sender,
                chain_ctx=chain_ctx,
                native_balance_cache=native_balance_cache,
                balance_snapshot=balance_snapshot,
                resource_snapshots=resource_snapshots,
                wallet_scope=wallet_scope,
            )
        )
    for sender, token_ref, symbol_hint, chain_ctx, wallet_scope in balance_reqs:
        fetch_tasks.append(
            _get_token_balance_for_chain(
                sender=sender,
                token_ref=token_ref,
                token_symbol_hint=symbol_hint,
                chain_ctx=chain_ctx,
                native_balance_cache=native_balance_cache,
                balance_snapshot=balance_snapshot,
                token_balance_cache=token_balance_cache,
                solana_wallet_balances=solana_wallet_balances,
                resource_snapshots=resource_snapshots,
                wallet_scope=wallet_scope,
            )
        )
    if fetch_tasks:
        logs.append(f"[VWS] Fetching {len(fetch_tasks)} balance snapshot entries.")
        fetch_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        for idx, res in enumerate(fetch_results):
            if isinstance(res, Exception):
                logs.append(f"[VWS] Balance fetch task {idx} failed: {res}")
    try:
        await _apply_reserved_balances(
            balance_snapshot=balance_snapshot,
            resource_snapshots=resource_snapshots,
            logs=logs,
        )
    except Exception as exc:
        return _build_vws_exception_payload(
            exc=exc,
            logs=logs,
            balance_snapshot=balance_snapshot,
            resource_snapshots=resource_snapshots,
            preflight_estimates=preflight_estimates,
            fee_quotes=fee_quotes,
        )

    try:
        simulation = simulate_execution_plan(
            plan=plan,
            balance_snapshot=balance_snapshot,
            execution_state=execution_state,
            context=state.get("artifacts"),
            preflight_estimates=preflight_estimates,
            platform_fee_native_by_node=platform_fee_native_by_node,
        )
    except Exception as exc:
        return _build_vws_exception_payload(
            exc=exc,
            logs=logs,
            balance_snapshot=balance_snapshot,
            resource_snapshots=resource_snapshots,
            preflight_estimates=preflight_estimates,
            fee_quotes=fee_quotes,
        )

    reservation_requirements: Dict[str, List[Dict[str, Any]]] = {}
    for node_id, requirements in simulation.reservation_requirements.items():
        for requirement in requirements:
            resource_snapshot = resource_snapshots.get(
                _balance_key(
                    requirement.sender,
                    requirement.chain,
                    requirement.token_ref,
                )
            )
            _add_reservation_requirement(
                reservation_requirements,
                node_id=node_id,
                resource_snapshot=resource_snapshot,
                required=requirement.required,
                kind=requirement.kind,
            )

    if not simulation.valid and simulation.failure is not None:
        logs.append(
            f"[VWS] Rejected {simulation.failure.node_id}: "
            f"{simulation.failure.category} — {simulation.failure.reason}"
        )
        return {
            "route_decision": "end",
            "reasoning_logs": logs,
            "messages": [
                AIMessage(content=_build_vws_failure_message(simulation.failure))
            ],
            "balance_snapshot": balance_snapshot,
            "resource_snapshots": resource_snapshots,
            "native_requirements": {
                node_id: str(value)
                for node_id, value in simulation.native_requirements.items()
            },
            "reservation_requirements": reservation_requirements,
            "projected_deltas": {
                key: str(value) for key, value in simulation.projected_deltas.items()
            },
            "preflight_estimates": preflight_estimates,
            "vws_simulation": simulation.node_metadata,
            "vws_failure": _serialize_vws_failure(simulation.failure),
            "fee_quotes": fee_quotes,
        }

    enriched_plan = deepcopy(plan)
    for node_id, metadata in simulation.node_metadata.items():
        if node_id in enriched_plan.nodes:
            enriched_plan.nodes[node_id].metadata["vws"] = metadata
    enriched_plan.metadata["vws"] = {
        "valid": True,
        "simulated_nodes": len(simulation.node_metadata),
        "projected_delta_count": len(simulation.projected_deltas),
    }
    enriched_plan.version = max(1, int(getattr(plan, "version", 1))) + 1
    logs.append(
        f"[VWS] Simulated {len(simulation.node_metadata)} node(s) "
        f"with {len(simulation.projected_deltas)} projected delta(s)."
    )
    return {
        "plan_history": [enriched_plan],
        "reasoning_logs": logs,
        "balance_snapshot": balance_snapshot,
        "resource_snapshots": resource_snapshots,
        "native_requirements": {
            node_id: str(value)
            for node_id, value in simulation.native_requirements.items()
        },
        "reservation_requirements": reservation_requirements,
        "projected_deltas": {
            key: str(value) for key, value in simulation.projected_deltas.items()
        },
        "preflight_estimates": preflight_estimates,
        "vws_simulation": simulation.node_metadata,
        "vws_failure": None,
        "fee_quotes": fee_quotes,
    }


async def balance_check_node(state: AgentState) -> Dict[str, Any]:
    """Thin wrapper that forwards VWS validation output downstream."""
    plan_history = state.get("plan_history", [])
    if not plan_history:
        return {"route_decision": "confirm"}

    plan = plan_history[-1]
    all_read_only = all(n.tool == "check_balance" for n in plan.nodes.values())
    if all_read_only:
        # Pure balance queries: skip fee/gas checks entirely and execute directly.
        return {
            "route_decision": "execute",
            # Ensure stale fee quotes from prior plans don't trigger fee collection.
            "fee_quotes": [],
            # Clear any stale reservation metadata from prior plans.
            "balance_snapshot": {},
            "resource_snapshots": {},
            "native_requirements": {},
            "reservation_requirements": {},
            "projected_deltas": {},
            "preflight_estimates": {},
            "vws_simulation": {},
            "vws_failure": None,
            "reasoning_logs": [],
        }

    vws_payload = (
        _copy_vws_payload_from_state(state)
        if _has_vws_payload(state)
        else await run_vws_preflight(state)
    )
    if not vws_payload:
        return {"route_decision": "confirm"}
    if vws_payload.get("route_decision") == "end":
        return vws_payload

    fee_quotes = list(vws_payload.get("fee_quotes") or [])
    if not fee_quotes:
        fee_quotes, _ = _quote_plan_fees(
            (vws_payload.get("plan_history") or [plan])[-1]
        )

    result = {
        "route_decision": "confirm",
        "fee_quotes": fee_quotes,
        "reasoning_logs": list(vws_payload.get("reasoning_logs") or [])
        + ["[BALANCE_CHECK] Using VWS outputs as source of truth."],
        "plan_history": list(vws_payload.get("plan_history") or []),
        "balance_snapshot": dict(vws_payload.get("balance_snapshot") or {}),
        "resource_snapshots": dict(vws_payload.get("resource_snapshots") or {}),
        "native_requirements": dict(vws_payload.get("native_requirements") or {}),
        "reservation_requirements": dict(
            vws_payload.get("reservation_requirements") or {}
        ),
        "projected_deltas": dict(vws_payload.get("projected_deltas") or {}),
        "preflight_estimates": dict(vws_payload.get("preflight_estimates") or {}),
        "vws_simulation": dict(vws_payload.get("vws_simulation") or {}),
        "vws_failure": None,
    }
    _assert_vws_passthrough(result, vws_payload)
    return result
