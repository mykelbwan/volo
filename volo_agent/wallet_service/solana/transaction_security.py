from __future__ import annotations

import hashlib
import secrets
import time
from typing import Any

_MEMO_PROGRAM_ID = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"


def build_replay_protection_memo(idempotency_key: str | None = None) -> str:
    """
    Build a unique memo so identical Solana transfers cannot hash identically.
    """
    stable_component = "anon"
    if idempotency_key and str(idempotency_key).strip():
        stable_component = hashlib.sha256(
            str(idempotency_key).strip().encode("utf-8")
        ).hexdigest()[:16]
    return f"volo:{stable_component}:{time.time_ns()}:{secrets.token_hex(8)}"


def create_replay_protection_memo_instruction(
    signer_pubkey: Any,
    *,
    memo_text: str,
) -> Any:
    from solders.pubkey import Pubkey
    from spl.memo.instructions import MemoParams, create_memo

    return create_memo(
        MemoParams(
            program_id=Pubkey.from_string(_MEMO_PROGRAM_ID),
            signer=signer_pubkey,
            message=str(memo_text).encode("utf-8"),
        )
    )
