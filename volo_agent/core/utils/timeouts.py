from __future__ import annotations

import os


def _get_float(env_key: str, default: float) -> float:
    raw = os.getenv(env_key)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if value <= 0:
        return default
    return value


# Global timeouts (seconds) for async nodes, blocking tool calls, and HTTP I/O.
NODE_TIMEOUT_SECONDS = _get_float("NODE_TIMEOUT_SECONDS", 120.0)
TOOL_TIMEOUT_SECONDS = _get_float("TOOL_TIMEOUT_SECONDS", 60.0)
EXTERNAL_HTTP_TIMEOUT_SECONDS = _get_float("EXTERNAL_HTTP_TIMEOUT_SECONDS", 60.0)

# Per-tool default timeouts (seconds). Can be overridden via
# TOOL_TIMEOUT_SECONDS_<TOOLNAME> env vars at runtime.
TOOL_DEFAULT_TIMEOUTS = {
    "swap": 300.0,
    "bridge": 300.0,
    "transfer": 45.0,
    "unwrap": 45.0,
    "check_balance": 20.0,
}


def _parse_timeout_value(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    if parsed <= 0:
        return None
    return parsed


def resolve_tool_timeout(tool_name: str | None, explicit: float | None) -> float | None:
    name = (tool_name or "").strip().lower()
    if name:
        env_key = f"TOOL_TIMEOUT_SECONDS_{name.upper()}"
        env_value = _parse_timeout_value(os.getenv(env_key))
        if env_value is not None:
            return env_value
    if explicit is not None:
        return explicit
    if name and name in TOOL_DEFAULT_TIMEOUTS:
        return TOOL_DEFAULT_TIMEOUTS[name]
    return TOOL_TIMEOUT_SECONDS
