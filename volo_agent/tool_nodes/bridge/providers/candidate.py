from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .base import BridgeProvider, BridgeProviderDiagnostics, BridgeProviderQuote


class BridgeCandidateOrigin(str, Enum):
    PLANNED = "planned"
    DYNAMIC = "dynamic"


class BridgeCandidateTrust(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(frozen=True, slots=True)
class BridgeCandidate:
    provider: BridgeProvider
    quote: BridgeProviderQuote
    origin: BridgeCandidateOrigin
    trust_level: BridgeCandidateTrust
    diagnostics: tuple[BridgeProviderDiagnostics, ...] = field(default_factory=tuple)

    @property
    def provider_name(self) -> str:
        return str(self.provider.name)
