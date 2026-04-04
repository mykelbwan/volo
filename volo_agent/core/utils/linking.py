from __future__ import annotations

import re
from typing import Optional

_LINK_TOKEN_RE = re.compile(r"\b([A-F0-9]{8})\b", re.IGNORECASE)
_UNLINK_STRIP_CHARS = " \t\r\n,.;:!?\"'()[]{}"


def extract_link_token(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    raw = text.strip()
    if not raw:
        return None

    match = _LINK_TOKEN_RE.search(raw)
    if not match:
        return None

    token = match.group(1).upper()
    lower = raw.lower()

    if raw.upper() == token:
        return token
    if "link" in lower or "connect" in lower or "code" in lower:
        return token
    return None


def is_link_account_request(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    if not t:
        return False
    if t.startswith("unlink") or " unlink " in f" {t} ":
        return False
    if is_link_status_request(text):
        return False

    if t in {"link", "link account", "link wallet", "link my account", "link my wallet"}:
        return True
    if "link" in t and any(k in t for k in ("account", "wallet", "platform", "identity")):
        return True
    if "connect" in t and any(k in t for k in ("account", "wallet", "platform", "identity")):
        return True
    return False


def is_link_status_request(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    if not t:
        return False

    if t in {"link status", "linked accounts", "linked account", "linked identities"}:
        return True
    if "linked" in t and any(k in t for k in ("account", "accounts", "identity", "identities", "platform")):
        return True
    if "list" in t and any(k in t for k in ("linked accounts", "linked identities", "accounts", "identities")):
        return True
    return False


def is_create_wallet_request(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    if not t:
        return False

    if t in {"create", "create wallet", "create new wallet", "new wallet"}:
        return True
    if "create" in t and any(k in t for k in ("wallet", "new", "account")):
        return True
    if "make" in t and "wallet" in t:
        return True
    if "new" in t and any(k in t for k in ("wallet", "account")):
        return True
    return False


def is_unlink_account_request(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    if not t:
        return False

    if t in {"unlink", "unlink account", "unlink wallet", "disconnect account"}:
        return True
    if t.startswith("unlink ") or t.startswith("disconnect "):
        return True
    if "unlink" in t and any(k in t for k in ("account", "wallet", "identity", "platform")):
        return True
    if "disconnect" in t and any(k in t for k in ("account", "wallet", "identity", "platform")):
        return True
    return False


def extract_unlink_target(text: Optional[str]) -> Optional[str]:
    if not is_unlink_account_request(text):
        return None
    raw = str(text or "").strip().lower()
    if not raw:
        return None

    words = []
    for word in raw.split():
        cleaned = word.strip(_UNLINK_STRIP_CHARS)
        if cleaned:
            words.append(cleaned)

    skip_words = {
        "unlink",
        "disconnect",
        "account",
        "accounts",
        "wallet",
        "wallets",
        "identity",
        "identities",
        "platform",
        "platforms",
        "provider",
        "providers",
        "my",
        "the",
        "this",
        "that",
        "please",
    }
    target_parts = [word for word in words if word not in skip_words]
    if not target_parts:
        return None
    return " ".join(target_parts).strip() or None
