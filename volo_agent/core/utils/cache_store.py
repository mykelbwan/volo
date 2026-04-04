from __future__ import annotations

import json
from typing import Any, Optional

from core.utils.upstash_client import get_upstash_client, upstash_configured

_DEFAULT_TTL_SECONDS = 3600


def _upstash_get(key: str) -> Optional[Any]:
    client = get_upstash_client()
    if client is None:
        return None
    try:
        result = client.get(key)
    except Exception:
        return None
    if result is None:
        return None
    try:
        return json.loads(result)
    except Exception:
        return None


def _upstash_set(key: str, value: Any, ttl_seconds: int) -> None:
    client = get_upstash_client()
    if client is None:
        return
    payload = json.dumps(value)
    try:
        client.set(key, payload, ex=ttl_seconds)
        return
    except TypeError:
        pass
    except Exception:
        return
    try:
        client.set(key, payload)
        if ttl_seconds:
            client.expire(key, ttl_seconds)
    except Exception:
        return


def cache_get(key: str) -> Optional[Any]:
    if upstash_configured():
        return _upstash_get(key)
    return None


def cache_set(key: str, value: Any, ttl_seconds: int = _DEFAULT_TTL_SECONDS) -> None:
    if upstash_configured():
        _upstash_set(key, value, ttl_seconds)
        return


def ping() -> bool:
    if upstash_configured():
        client = get_upstash_client()
        if client is None:
            return False
        try:
            result = client.ping()
        except Exception:
            return False
        if isinstance(result, str):
            return result.upper() == "PONG"
        return bool(result)
    return False
