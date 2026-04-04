from __future__ import annotations

from typing import Optional

from config.chains import find_chain_by_name
from intent_hub.ontology.trigger import TriggerCondition


def normalize_chain_name(chain: str) -> Optional[str]:
    if not chain:
        return None
    raw = chain.strip()
    if not raw:
        return None
    try:
        cfg = find_chain_by_name(raw.lower())
        return cfg.name.lower()
    except Exception:
        return raw.lower()


def key_for_symbol(symbol: str) -> Optional[str]:
    if not symbol:
        return None
    return symbol.strip().upper()


def key_for_chain_address(chain: str, address: str) -> Optional[str]:
    chain_norm = normalize_chain_name(chain)
    if not chain_norm:
        return None
    addr = (address or "").strip().lower()
    if not addr:
        return None
    return f"{chain_norm}:{addr}"


def key_for_condition(condition: TriggerCondition) -> Optional[str]:
    if condition.token_address and condition.chain:
        key = key_for_chain_address(condition.chain, condition.token_address)
        if key:
            return key
    if condition.asset:
        return key_for_symbol(condition.asset)
    return None
