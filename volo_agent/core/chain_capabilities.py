from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict

_LOCK = threading.Lock()
_CACHE: Dict[str, Dict[str, Any]] = {}
_LOADED = False
_LOGGER = logging.getLogger("volo.chain_capabilities")


@dataclass(frozen=True)
class RouterCapabilities:
    supports_native_swaps: bool
    last_checked: str | None = None


def _capabilities_path() -> str:
    return os.getenv("CHAIN_CAPABILITIES_PATH", "chain_capabilities.json")


def _make_key(chain_id: int, protocol: str, router_address: str | None) -> str:
    router = (router_address or "none").lower()
    return f"{chain_id}:{protocol.lower()}:{router}"


def _load_cache() -> None:
    global _LOADED
    if _LOADED:
        return
    path = _capabilities_path()
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f)
            if isinstance(data, dict):
                _CACHE.update(data)
        except Exception as exc:
            _LOGGER.warning(
                "Failed to load chain capabilities cache from %s: %s. "
                "Recovery path: continuing with in-memory defaults.",
                path,
                exc,
            )
    _LOADED = True


def _save_cache() -> None:
    path = _capabilities_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(_CACHE, f, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
    except Exception as exc:
        _LOGGER.warning(
            "Failed to persist chain capabilities cache to %s: %s. "
            "Recovery path: runtime cache remains active; check filesystem permissions.",
            path,
            exc,
        )


def get_router_capabilities(
    chain_id: int,
    protocol: str,
    router_address: str | None,
    default_supports_native_swaps: bool,
) -> RouterCapabilities:
    with _LOCK:
        _load_cache()
        if not default_supports_native_swaps:
            # Hard-disable: chain config explicitly disables native swaps.
            key = _make_key(chain_id, protocol, router_address)
            entry = _CACHE.get(key)
            return RouterCapabilities(
                supports_native_swaps=False,
                last_checked=entry.get("last_checked")
                if isinstance(entry, dict)
                else None,
            )
        key = _make_key(chain_id, protocol, router_address)
        entry = _CACHE.get(key)
        if not isinstance(entry, dict):
            return RouterCapabilities(
                supports_native_swaps=bool(default_supports_native_swaps),
                last_checked=None,
            )
        return RouterCapabilities(
            supports_native_swaps=bool(
                entry.get("supports_native_swaps", default_supports_native_swaps)
            ),
            last_checked=entry.get("last_checked"),
        )


def set_router_capabilities(
    chain_id: int,
    protocol: str,
    router_address: str | None,
    *,
    supports_native_swaps: bool,
) -> None:
    with _LOCK:
        _load_cache()
        key = _make_key(chain_id, protocol, router_address)
        _CACHE[key] = {
            "supports_native_swaps": bool(supports_native_swaps),
            "last_checked": datetime.now(tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        _save_cache()
