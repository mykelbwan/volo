from __future__ import annotations

import json
import os
import string
from typing import Optional

from core.utils.http import (
    ExternalServiceError,
    async_raise_for_status,
    async_request_json,
    raise_for_status,
    request_json,
)


_ACROSS_SUCCESS_STATUSES = {"filled"}
_ACROSS_FAILURE_STATUSES = {"expired", "refunded"}


def _across_api_base_url(is_testnet: bool) -> str:
    if is_testnet:
        return "https://testnet.across.to/api"
    return "https://app.across.to/api"


def _debug_enabled() -> bool:
    return os.getenv("BRIDGE_STATUS_WORKER_DEBUG", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _normalize_tx_hash(tx_hash: str) -> str:
    h = (tx_hash or "").strip()
    if not h:
        return h
    if h.startswith("0x"):
        return h
    is_hex = all(c in string.hexdigits for c in h)
    if is_hex and len(h) == 64:
        return f"0x{h}"
    return h


def _across_error_to_status(body: str) -> Optional[str]:
    if not body:
        return None
    if "DepositNotFoundException" in body:
        return "not_found"
    try:
        payload = json.loads(body)
    except Exception:
        return None
    error = str(payload.get("error", "")).strip()
    if error == "DepositNotFoundException":
        return "not_found"
    return None


def fetch_across_status(
    tx_hash: str,
    *,
    is_testnet: bool,
    meta: Optional[dict] = None,
) -> Optional[str]:
    base_url = _across_api_base_url(is_testnet)

    def _request_status(params: dict) -> Optional[str]:
        url = f"{base_url}/deposit/status"
        if _debug_enabled():
            print(f"[bridge-status] across request url={url} params={params}")
        response = request_json("GET", url, params=params, service="across")
        try:
            raise_for_status(response, "across")
        except ExternalServiceError as exc:
            status = _across_error_to_status(getattr(exc, "body", ""))
            if status:
                return status
            raise
        data = response.json()
        status = str(data.get("status", "")).lower()
        if _debug_enabled():
            print(f"[bridge-status] across response status={status or None}")
        return status or None

    tx_hash = _normalize_tx_hash(tx_hash)
    params_list = []

    meta = meta or {}
    deposit_id = meta.get("deposit_id")
    origin_chain_id = meta.get("origin_chain_id")
    if deposit_id is not None and origin_chain_id is not None:
        params_list.append(
            {"originChainId": str(origin_chain_id), "depositId": str(deposit_id)}
        )

    # Docs reference depositTxHash; legacy uses depositTxnRef.
    params_list.append({"depositTxHash": tx_hash})
    params_list.append({"depositTxnRef": tx_hash})

    for params in params_list:
        try:
            status = _request_status(params)
            if status:
                return status
        except Exception as exc:
            if _debug_enabled():
                print(
                    f"[bridge-status] across error params={params} err={exc}"
                )
            continue

    return None


async def fetch_across_status_async(
    tx_hash: str,
    *,
    is_testnet: bool,
    meta: Optional[dict] = None,
) -> Optional[str]:
    """
    Async version of fetch_across_status.
    """
    base_url = _across_api_base_url(is_testnet)

    async def _request_status_async(params: dict) -> Optional[str]:
        url = f"{base_url}/deposit/status"
        if _debug_enabled():
            print(f"[bridge-status] across request url={url} params={params}")
        response = await async_request_json("GET", url, params=params, service="across")
        try:
            await async_raise_for_status(response, "across")
        except ExternalServiceError as exc:
            status = _across_error_to_status(getattr(exc, "body", ""))
            if status:
                return status
            raise
        data = response.json()
        status = str(data.get("status", "")).lower()
        if _debug_enabled():
            print(f"[bridge-status] across response status={status or None}")
        return status or None

    tx_hash = _normalize_tx_hash(tx_hash)
    params_list = []

    meta = meta or {}
    deposit_id = meta.get("deposit_id")
    origin_chain_id = meta.get("origin_chain_id")
    if deposit_id is not None and origin_chain_id is not None:
        params_list.append(
            {"originChainId": str(origin_chain_id), "depositId": str(deposit_id)}
        )

    # Docs reference depositTxHash; legacy uses depositTxnRef.
    params_list.append({"depositTxHash": tx_hash})
    params_list.append({"depositTxnRef": tx_hash})

    for params in params_list:
        try:
            status = await _request_status_async(params)
            if status:
                return status
        except Exception as exc:
            if _debug_enabled():
                print(
                    f"[bridge-status] across error params={params} err={exc}"
                )
            continue

    return None


def interpret_across_status(status: Optional[str]) -> str | None:
    if not status:
        return None
    status = status.lower()
    if status in _ACROSS_SUCCESS_STATUSES:
        return "success"
    if status in _ACROSS_FAILURE_STATUSES:
        return "failed"
    return "pending"
