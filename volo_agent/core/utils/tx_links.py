from __future__ import annotations

from typing import Optional
from urllib.parse import urlsplit, urlunsplit


def explorer_tx_url(explorer_base: Optional[str], tx_hash: str) -> Optional[str]:
    if not explorer_base:
        return None
    parsed = urlsplit(explorer_base)
    path = parsed.path.rstrip("/")
    tx_path = f"{path}/tx/{tx_hash}" if path else f"/tx/{tx_hash}"
    return urlunsplit(
        (parsed.scheme, parsed.netloc, tx_path, parsed.query, parsed.fragment)
    )
