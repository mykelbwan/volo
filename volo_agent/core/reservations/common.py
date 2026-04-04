from __future__ import annotations

import hashlib
from typing import Any

NATIVE_MARKER = "0x0000000000000000000000000000000000000000"


def normalize_wallet_scope(*, sender: str | None, sub_org_id: str | None = None) -> str | None:
    sender_value = str(sender or "").strip().lower()
    sub_org_value = str(sub_org_id or "").strip().lower()
    if sub_org_value:
        return f"suborg:{sub_org_value}"
    if sender_value:
        return f"sender:{sender_value}"
    return None


def resource_key(sender: str, chain_name: str, token_ref: str) -> str:
    return (
        f"{str(sender).strip().lower()}|"
        f"{str(chain_name).strip().lower()}|"
        f"{str(token_ref).strip().lower()}"
    )


def split_resource_key(value: str) -> tuple[str, str, str] | None:
    parts = str(value or "").split("|")
    if len(parts) != 3:
        return None
    return parts[0].strip().lower(), parts[1].strip().lower(), parts[2].strip().lower()


def reservation_id_for(*, execution_id: str, node_id: str) -> str:
    raw = f"{str(execution_id)}:{str(node_id)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(str(value))
    except Exception:
        return default
