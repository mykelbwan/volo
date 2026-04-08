from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Mapping, TypeAlias

if TYPE_CHECKING:
    from core.routing.models import BridgeRouteQuote
    from tool_nodes.bridge.simulators.across_simulator import AcrossBridgeQuote
    from tool_nodes.bridge.simulators.relay_simulator import RelayBridgeQuote

BridgeProviderQuote: TypeAlias = "AcrossBridgeQuote | RelayBridgeQuote | BridgeRouteQuote"


@dataclass(frozen=True, slots=True)
class BridgeRequest:
    token_symbol: str
    source_chain_id: int
    dest_chain_id: int
    source_chain_name: str
    dest_chain_name: str
    amount: Decimal
    sub_org_id: str
    sender: str
    recipient: str
    source_is_solana: bool
    dest_is_solana: bool


@dataclass(frozen=True, slots=True)
class BridgeProviderDiagnostics:
    reason: str
    detail: str | None = None
    meta: Mapping[str, Any] = field(default_factory=dict)


class BridgeProvider(ABC):
    name: str = "unknown"

    @abstractmethod
    def supports(self, request: BridgeRequest) -> bool:
        """Return whether this provider supports the requested chain pair."""

    @abstractmethod
    async def quote_dynamic(
        self,
        request: BridgeRequest,
    ) -> BridgeProviderQuote | None:
        """Fetch a fresh provider-specific dynamic quote."""

    @abstractmethod
    def quote_from_route_meta(
        self,
        *,
        request: BridgeRequest,
        route_meta: Mapping[str, Any],
    ) -> BridgeProviderQuote | None:
        """Build a provider-specific quote from planned route metadata."""

    @abstractmethod
    def validate_route_meta(
        self,
        *,
        request: BridgeRequest,
        route_meta: Mapping[str, Any],
    ) -> None:
        """Validate planned route metadata for this provider."""

    @abstractmethod
    async def execute(
        self,
        *,
        request: BridgeRequest,
        quote: BridgeProviderQuote,
        route_meta: Mapping[str, Any] | None = None,
    ) -> Any:
        """Execute the bridge transfer using a provider-specific quote."""
