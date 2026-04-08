from __future__ import annotations

from functools import lru_cache

from .across import AcrossProvider
from .base import BridgeProvider
from .lifi import LiFiProvider
from .mayan import MayanProvider
from .relay import RelayProvider


@lru_cache(maxsize=1)
def get_bridge_providers() -> tuple[BridgeProvider, ...]:
    return (
        AcrossProvider(),
        RelayProvider(),
        MayanProvider(),
        LiFiProvider(),
    )


def get_bridge_provider(name: str) -> BridgeProvider | None:
    target = str(name or "").strip().lower()
    for provider in get_bridge_providers():
        if str(provider.name).strip().lower() == target:
            return provider
    return None
