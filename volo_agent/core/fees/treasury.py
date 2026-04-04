from __future__ import annotations

import os


def get_fee_treasury(family: str) -> str:
    normalized = str(family or "").strip().lower()
    if not normalized:
        return ""

    family_key = normalized.upper().replace("-", "_")
    family_specific = os.getenv(f"FEE_TREASURY_{family_key}_ADDRESS", "").strip()
    if family_specific:
        return family_specific

    if normalized == "evm":
        return os.getenv("FEE_TREASURY_ADDRESS", "").strip()

    return ""
