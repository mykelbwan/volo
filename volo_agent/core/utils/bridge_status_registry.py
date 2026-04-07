from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol

from config.bridge_registry import (
    MAINNET_RELAY_API_BASE_URL,
    RELAY,
    TESTNET_RELAY_API_BASE_URL,
)
from core.utils.bridge_status import (
    fetch_across_status,
    fetch_across_status_async,
    interpret_across_status,
)
from core.utils.http import (
    async_raise_for_status,
    async_request_json,
    raise_for_status,
    request_json,
)


class BridgeStatusProvider(Protocol):
    def fetch_status(
        self, tx_hash: str, *, is_testnet: bool, meta: Optional[dict] = None
    ) -> Optional[str]: ...

    def interpret_status(self, raw_status: Optional[str]) -> Optional[str]: ...


class AsyncBridgeStatusProvider(Protocol):
    async def fetch_status(
        self, tx_hash: str, *, is_testnet: bool, meta: Optional[dict] = None
    ) -> Optional[str]: ...

    def interpret_status(self, raw_status: Optional[str]) -> Optional[str]: ...


@dataclass(frozen=True)
class BridgeStatusResult:
    raw_status: Optional[str]
    normalized_status: Optional[str]


_PROVIDERS: Dict[str, BridgeStatusProvider] = {}
_ASYNC_PROVIDERS: Dict[str, AsyncBridgeStatusProvider] = {}


def register_bridge_status_provider(
    protocol: str, provider: BridgeStatusProvider
) -> None:
    _PROVIDERS[protocol.strip().lower()] = provider


def register_async_bridge_status_provider(
    protocol: str, provider: AsyncBridgeStatusProvider
) -> None:
    _ASYNC_PROVIDERS[protocol.strip().lower()] = provider


def get_bridge_status_provider(protocol: str) -> Optional[BridgeStatusProvider]:
    return _PROVIDERS.get(protocol.strip().lower())


def get_async_bridge_status_provider(
    protocol: str,
) -> Optional[AsyncBridgeStatusProvider]:
    return _ASYNC_PROVIDERS.get(protocol.strip().lower())


def fetch_bridge_status(
    protocol: str,
    tx_hash: str,
    *,
    is_testnet: bool,
    meta: Optional[dict] = None,
) -> BridgeStatusResult:
    provider = get_bridge_status_provider(protocol)
    if provider is None:
        return BridgeStatusResult(raw_status=None, normalized_status=None)
    raw = provider.fetch_status(tx_hash, is_testnet=is_testnet, meta=meta)
    normalized = provider.interpret_status(raw)
    return BridgeStatusResult(raw_status=raw, normalized_status=normalized)


async def fetch_bridge_status_async(
    protocol: str,
    tx_hash: str,
    *,
    is_testnet: bool,
    meta: Optional[dict] = None,
) -> BridgeStatusResult:
    provider = get_async_bridge_status_provider(protocol)
    if provider is None:
        return BridgeStatusResult(raw_status=None, normalized_status=None)
    raw = await provider.fetch_status(tx_hash, is_testnet=is_testnet, meta=meta)
    normalized = provider.interpret_status(raw)
    return BridgeStatusResult(raw_status=raw, normalized_status=normalized)


class AcrossStatusProvider:
    def fetch_status(
        self, tx_hash: str, *, is_testnet: bool, meta: Optional[dict] = None
    ) -> Optional[str]:
        return fetch_across_status(tx_hash, is_testnet=is_testnet, meta=meta)

    def interpret_status(self, raw_status: Optional[str]) -> Optional[str]:
        return interpret_across_status(raw_status)


register_bridge_status_provider("across", AcrossStatusProvider())


class AsyncAcrossStatusProvider:
    def interpret_status(self, raw_status: Optional[str]) -> Optional[str]:
        return interpret_across_status(raw_status)

    async def fetch_status(
        self, tx_hash: str, *, is_testnet: bool, meta: Optional[dict] = None
    ) -> Optional[str]:
        return await fetch_across_status_async(
            tx_hash, is_testnet=is_testnet, meta=meta
        )


register_async_bridge_status_provider("across", AsyncAcrossStatusProvider())


class RelayStatusProvider:
    def fetch_status(
        self, tx_hash: str, *, is_testnet: bool, meta: Optional[dict] = None
    ) -> Optional[str]:
        meta = meta or {}
        request_id = meta.get("request_id") or meta.get("relay_request_id")
        if not request_id:
            return None
        base_url = meta.get("api_base_url")
        if not base_url:
            base_url = (
                TESTNET_RELAY_API_BASE_URL if is_testnet else MAINNET_RELAY_API_BASE_URL
            )
        headers = None
        if RELAY.api_key:
            headers = {"x-api-key": RELAY.api_key}
        try:
            response = request_json(
                "GET",
                f"{base_url}/intents/status/v3",
                params={"requestId": request_id},
                headers=headers,
                service="relay",
            )
            raise_for_status(response, "relay")
            data = response.json()
        except Exception:
            return None
        status = data.get("status") or data.get("data", {}).get("status")
        return str(status).strip().lower() if status else None

    def interpret_status(self, raw_status: Optional[str]) -> Optional[str]:
        if not raw_status:
            return None
        status = raw_status.lower()
        if status in {"success"}:
            return "success"
        if status in {"failure", "refund", "refunded"}:
            return "failed"
        return "pending"


register_bridge_status_provider("relay", RelayStatusProvider())


class AsyncRelayStatusProvider:
    def interpret_status(self, raw_status: Optional[str]) -> Optional[str]:
        return RelayStatusProvider().interpret_status(raw_status)

    async def fetch_status(
        self, tx_hash: str, *, is_testnet: bool, meta: Optional[dict] = None
    ) -> Optional[str]:
        meta = meta or {}
        request_id = meta.get("request_id") or meta.get("relay_request_id")
        if not request_id:
            return None
        base_url = meta.get("api_base_url")
        if not base_url:
            base_url = (
                TESTNET_RELAY_API_BASE_URL if is_testnet else MAINNET_RELAY_API_BASE_URL
            )
        headers = None
        if RELAY.api_key:
            headers = {"x-api-key": RELAY.api_key}
        try:
            response = await async_request_json(
                "GET",
                f"{base_url}/intents/status/v3",
                params={"requestId": request_id},
                headers=headers,
                service="relay",
            )
            await async_raise_for_status(response, "relay")
            data = response.json()
        except Exception:
            return None
        status = data.get("status") or data.get("data", {}).get("status")
        return str(status).strip().lower() if status else None


register_async_bridge_status_provider("relay", AsyncRelayStatusProvider())


class LiFiStatusProvider:
    def fetch_status(
        self, tx_hash: str, *, is_testnet: bool, meta: Optional[dict] = None
    ) -> Optional[str]:
        if not tx_hash:
            return None
        base_url = "https://li.quest/v1"
        params: Dict[str, Any] = {"txHash": tx_hash}
        if isinstance(meta, dict):
            if meta.get("fromChain") is not None:
                params["fromChain"] = meta.get("fromChain")
            if meta.get("toChain") is not None:
                params["toChain"] = meta.get("toChain")
            if meta.get("bridge"):
                params["bridge"] = meta.get("bridge")
        headers = None
        api_key = os.getenv("LIFI_API_KEY", "").strip()
        if api_key:
            headers = {"x-lifi-api-key": api_key}
        try:
            response = request_json(
                "GET",
                f"{base_url}/status",
                params=params,
                headers=headers,
                service="lifi-status",
            )
            raise_for_status(response, "lifi-status")
            data = response.json()
        except Exception:
            return None

        status = data.get("status")
        sub = data.get("substatus")
        if status and sub:
            return f"{status}:{sub}"
        return str(status).strip().lower() if status else None

    def interpret_status(self, raw_status: Optional[str]) -> Optional[str]:
        if not raw_status:
            return None
        parts = str(raw_status).strip().lower().split(":", 1)
        status = parts[0]
        sub = parts[1] if len(parts) > 1 else ""

        if status == "done":
            if sub in {"completed", "partial"}:
                return "success"
            if sub in {"refunded", "failed", "rejected"}:
                return "failed"
            return "success"
        if status in {"failed", "invalid"}:
            return "failed"
        if status in {"pending", "not_found", "started"}:
            return "pending"
        return None


register_bridge_status_provider("lifi", LiFiStatusProvider())


class AsyncLiFiStatusProvider:
    def interpret_status(self, raw_status: Optional[str]) -> Optional[str]:
        return LiFiStatusProvider().interpret_status(raw_status)

    async def fetch_status(
        self, tx_hash: str, *, is_testnet: bool, meta: Optional[dict] = None
    ) -> Optional[str]:
        if not tx_hash:
            return None
        base_url = "https://li.quest/v1"
        params: Dict[str, Any] = {"txHash": tx_hash}
        if isinstance(meta, dict):
            if meta.get("fromChain") is not None:
                params["fromChain"] = meta.get("fromChain")
            if meta.get("toChain") is not None:
                params["toChain"] = meta.get("toChain")
            if meta.get("bridge"):
                params["bridge"] = meta.get("bridge")
        headers = None
        api_key = os.getenv("LIFI_API_KEY", "").strip()
        if api_key:
            headers = {"x-lifi-api-key": api_key}
        try:
            response = await async_request_json(
                "GET",
                f"{base_url}/status",
                params=params,
                headers=headers,
                service="lifi-status",
            )
            await async_raise_for_status(response, "lifi-status")
            data = response.json()
        except Exception:
            return None

        status = data.get("status")
        sub = data.get("substatus")
        if status and sub:
            return f"{status}:{sub}"
        return str(status).strip().lower() if status else None


register_async_bridge_status_provider("lifi", AsyncLiFiStatusProvider())


_MAYAN_EXPLORER_API = "https://explorer-api.mayan.finance/v3"
_MAYAN_SUCCESS_STATUSES = {"completed"}
_MAYAN_FAILURE_STATUSES = {"refunded", "expired"}


class MayanStatusProvider:
    def fetch_status(
        self,
        tx_hash: str,
        *,
        is_testnet: bool = False,
        meta: Optional[dict] = None,
    ) -> Optional[str]:

        if not tx_hash or not tx_hash.strip():
            return None

        url = f"{_MAYAN_EXPLORER_API}/swap/"
        try:
            response = request_json(
                "GET",
                url,
                params={"sourceTxHash": tx_hash.strip()},
                service="mayan-status",
            )
            raise_for_status(response, "mayan-status")
            data = response.json()
        except Exception:
            return None

        # The API may return a list (multiple swaps per tx) or a dict with
        # a "data" or "swaps" key.  Normalise to a list and take the first.
        swaps: list = []
        if isinstance(data, list):
            swaps = data
        elif isinstance(data, dict):
            swaps = (
                data.get("data")
                or data.get("swaps")
                or ([data] if data.get("status") else [])
            )

        if not swaps:
            return None

        raw = str(swaps[0].get("status", "")).strip().upper()
        return raw.lower() if raw else None

    def interpret_status(self, raw_status: Optional[str]) -> Optional[str]:
        if not raw_status:
            return None
        status = raw_status.strip().lower()
        if status in _MAYAN_SUCCESS_STATUSES:
            return "success"
        if status in _MAYAN_FAILURE_STATUSES:
            return "failed"
        return "pending"


register_bridge_status_provider("mayan", MayanStatusProvider())


class AsyncMayanStatusProvider:
    def interpret_status(self, raw_status: Optional[str]) -> Optional[str]:
        return MayanStatusProvider().interpret_status(raw_status)

    async def fetch_status(
        self,
        tx_hash: str,
        *,
        is_testnet: bool = False,
        meta: Optional[dict] = None,
    ) -> Optional[str]:
        if not tx_hash or not tx_hash.strip():
            return None

        url = f"{_MAYAN_EXPLORER_API}/swap/"
        try:
            response = await async_request_json(
                "GET",
                url,
                params={"sourceTxHash": tx_hash.strip()},
                service="mayan-status",
            )
            await async_raise_for_status(response, "mayan-status")
            data = response.json()
        except Exception:
            return None

        swaps: list = []
        if isinstance(data, list):
            swaps = data
        elif isinstance(data, dict):
            swaps = (
                data.get("data")
                or data.get("swaps")
                or ([data] if data.get("status") else [])
            )

        if not swaps:
            return None

        raw = str(swaps[0].get("status", "")).strip().upper()
        return raw.lower() if raw else None


register_async_bridge_status_provider("mayan", AsyncMayanStatusProvider())
