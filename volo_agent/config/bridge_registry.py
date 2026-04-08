from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from config.chains import find_chain_by_id
from config.solana_chains import get_solana_chain_by_id, is_solana_chain_id


@dataclass(frozen=True)
class BridgeRoute:
    protocol: str
    source_chain_id: int
    dest_chain_id: int
    token_symbol: str
    source_contract: str
    dest_contract: str
    input_token: str
    output_token: str
    is_native_input: bool = False
    is_native_output: bool = False
    enabled: bool = True
    notes: str = ""


@dataclass
class BridgeProtocolConfig:
    name: str
    display_name: str
    api_base_url: str
    api_key_env: Optional[str]
    avg_fill_time_seconds: int
    dynamic_routes: bool = False
    routes: list[BridgeRoute] = field(default_factory=list)

    @property
    def api_key(self) -> Optional[str]:
        if self.api_key_env is None:
            return None
        return os.getenv(self.api_key_env)


_ZERO = "0x0000000000000000000000000000000000000000"


ACROSS = BridgeProtocolConfig(
    name="across",
    display_name="Across Protocol",
    api_base_url="https://app.across.to/api",
    api_key_env=None,  # Across API is public
    avg_fill_time_seconds=120,  # ~2 minutes typical
    routes=[],
)

MAINNET_RELAY_API_BASE_URL = os.getenv(
    "MAINNET_RELAY_API_BASE_URL",
    os.getenv("RELAY_API_BASE_URL", "https://api.relay.link"),
)
TESTNET_RELAY_API_BASE_URL = os.getenv(
    "TESTNET_RELAY_API_BASE_URL",
    "https://api.testnets.relay.link",
)

RELAY = BridgeProtocolConfig(
    name="relay",
    display_name="Relay Protocol",
    api_base_url=MAINNET_RELAY_API_BASE_URL,
    api_key_env="RELAY_API_KEY",
    avg_fill_time_seconds=180,  # ~3 minutes typical (varies by route)
    dynamic_routes=True,
    routes=[],
)


MAYAN = BridgeProtocolConfig(
    name="mayan",
    display_name="Mayan Finance",
    api_base_url="https://price-api.mayan.finance/v3",
    api_key_env=None,  # Public API — no key required
    avg_fill_time_seconds=30,  # Swift mode typical fill time
    dynamic_routes=True,
    routes=[],
)

BRIDGE_PROTOCOLS: dict[str, BridgeProtocolConfig] = {
    "across": ACROSS,
    "relay": RELAY,
    "mayan": MAYAN,
}


def get_dynamic_protocols() -> list[BridgeProtocolConfig]:
    return [
        protocol for protocol in BRIDGE_PROTOCOLS.values() if protocol.dynamic_routes
    ]


def relay_api_base_url(source_chain_id: int, dest_chain_id: int) -> str:
    try:
        if is_solana_chain_id(source_chain_id):
            source = get_solana_chain_by_id(source_chain_id)
        else:
            source = find_chain_by_id(source_chain_id)
        if is_solana_chain_id(dest_chain_id):
            dest = get_solana_chain_by_id(dest_chain_id)
        else:
            dest = find_chain_by_id(dest_chain_id)
    except KeyError:
        return MAINNET_RELAY_API_BASE_URL
    if source.is_testnet or dest.is_testnet:
        return TESTNET_RELAY_API_BASE_URL
    return MAINNET_RELAY_API_BASE_URL
