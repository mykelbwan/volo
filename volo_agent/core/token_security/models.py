from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class SecurityTier(str, Enum):
    WHITELIST = "whitelist"
    GOPLUS_VERIFIED = "goplus_verified"
    FALLBACK_HEURISTIC = "fallback_web3_heuristics"
    UNVERIFIED = "unverified"
    UNSAFE = "unsafe"


class SecurityFlag(str, Enum):
    HONEYPOT = "honeypot"
    """GoPlus or simulation confirms the token cannot be sold."""

    TRANSFER_PAUSED = "transfer_paused"
    """The transfer function is currently disabled / paused."""

    CANNOT_BUY = "cannot_buy"
    """Buys revert — token is permanently or temporarily unacquirable."""

    CANNOT_SELL = "cannot_sell"
    """Sells revert (sell-side honeypot variant)."""

    BLACKLISTED = "blacklisted"
    """GoPlus flagged the contract as a known scam/rug."""

    # ── High-risk flags (token is returned but flagged) ───────────────────
    HIGH_BUY_TAX = "high_buy_tax"
    """Buy tax > 10 %."""

    HIGH_SELL_TAX = "high_sell_tax"
    """Sell tax > 10 %."""

    MINTABLE = "mintable"
    """Owner can mint arbitrary new supply (dilution risk)."""

    PROXY_CONTRACT = "proxy_contract"
    """Implementation can be swapped behind the proxy (upgrade risk)."""

    OWNER_CAN_CHANGE_BALANCE = "owner_can_change_balance"
    """A privileged address can modify holder balances directly."""

    ANTI_WHALE = "anti_whale"
    """Large transfers are capped — may affect institutional-sized trades."""

    # ── Informational flags (low risk, surface to user only) ─────────────
    LOW_LIQUIDITY = "low_liquidity"
    """USD liquidity in the primary DEX pair is below the $10k threshold."""

    UNVERIFIED_SOURCE = "unverified_source"
    """Address was resolved via Web3 heuristics, not a trusted security API."""

    SIMULATION_GAS_HIGH = "simulation_gas_high"
    """``eth_estimateGas`` returned an unusually high value (> 500k gas units)."""

    SIMULATION_REVERTED = "simulation_reverted"
    """``eth_estimateGas`` reverted — contract may restrict transfers."""


#  Critical flags — tokens carrying any of these are rejected 
CRITICAL_FLAGS: frozenset[SecurityFlag] = frozenset(
    {
        SecurityFlag.HONEYPOT,
        SecurityFlag.TRANSFER_PAUSED,
        SecurityFlag.CANNOT_BUY,
        SecurityFlag.CANNOT_SELL,
        SecurityFlag.BLACKLISTED,
    }
)

#  Liquidity thresholds (USD) 
LIQUIDITY_THRESHOLD_GOPLUS = 1_000.0  # $ minimum for GoPlus-verified tokens
LIQUIDITY_THRESHOLD_FALLBACK = 1_000.0  # $ minimum for fallback-scanned tokens

# Tax thresholds
HIGH_TAX_THRESHOLD = 10.0  # percent — above this we raise HIGH_*_TAX flags
CRITICAL_TAX_THRESHOLD = 90.0  # percent — above this we treat as CANNOT_SELL/BUY


# TokenUnsafeError
class TokenUnsafeError(Exception):
    def __init__(
        self,
        address: str,
        flags: list[SecurityFlag],
        symbol: str,
        chain_name: str,
        source: Optional[str] = None,
    ) -> None:
        self.address = address
        self.flags = flags
        self.symbol = symbol
        self.chain_name = chain_name
        self.source = source
        flag_names = ", ".join(f.value for f in flags)
        super().__init__(
            f"Token {symbol} at {address} on {chain_name} failed security "
            f"checks: [{flag_names}]"
        )


class TokenNotFoundError(Exception):
    def __init__(self, symbol: str, chain_name: str) -> None:
        self.symbol = symbol
        self.chain_name = chain_name
        super().__init__(
            f"Token '{symbol}' could not be found on chain '{chain_name}'. "
            "Check the symbol spelling or try a different chain."
        )


#  Core model
class ResolvedToken(BaseModel):
    symbol: str
    name: Optional[str] = None
    chain_name: str
    chain_id: int
    address: str
    decimals: int = 18
    security_tier: SecurityTier
    is_safe: bool
    flags: list[SecurityFlag] = Field(default_factory=list)
    liquidity_usd: Optional[float] = None
    buy_tax: Optional[float] = None
    sell_tax: Optional[float] = None
    total_supply: Optional[str] = None
    is_mintable: bool = False
    is_proxy: bool = False
    source: str
    last_checked: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    #  Convenience properties
    @property
    def is_native(self) -> bool:
        """Return ``True`` if this is the chain's native gas token."""
        return self.address == "0x0000000000000000000000000000000000000000"

    @property
    def has_critical_flags(self) -> bool:
        """Return ``True`` if any critical (rejectable) flag is present."""
        return bool(set(self.flags) & CRITICAL_FLAGS)

    @property
    def human_flags(self) -> list[str]:
        """Return flag values as plain strings (for JSON serialisation)."""
        return [f.value for f in self.flags]

    @property
    def security_summary(self) -> str:
        icon = "✅" if self.is_safe and not self.flags else "⚠️"
        if self.security_tier == SecurityTier.FALLBACK_HEURISTIC:
            icon = "🔬"

        parts = [f"{icon} {self.symbol} on {self.chain_name.title()}"]
        tier_label = {
            SecurityTier.WHITELIST: "whitelisted",
            SecurityTier.GOPLUS_VERIFIED: "GoPlus verified",
            SecurityTier.FALLBACK_HEURISTIC: "Web3 heuristics",
            SecurityTier.UNVERIFIED: "unverified",
            SecurityTier.UNSAFE: "UNSAFE",
        }.get(self.security_tier, self.security_tier.value)
        parts.append(tier_label)

        if self.liquidity_usd is not None:
            parts.append(f"liquidity ${self.liquidity_usd:,.0f}")

        if self.flags:
            parts.append(f"flags: {', '.join(f.value for f in self.flags)}")

        return " — ".join(parts)

    #   Serialisation helpers (MongoDB cache round-trip)
    def to_cache_doc(self) -> dict[str, Any]:
        from datetime import timedelta

        doc = self.model_dump()
        # Enums → plain strings
        doc["security_tier"] = self.security_tier.value
        doc["flags"] = [f.value for f in self.flags]
        # Ensure timezone-aware datetime for BSON Date
        doc["last_checked"] = self.last_checked
        doc["expires_at"] = self.last_checked + timedelta(days=7)
        # Stable compound lookup key
        doc["_cache_key"] = _cache_key(self.symbol, self.chain_id)
        return doc

    @classmethod
    def from_cache_doc(cls, doc: dict[str, Any]) -> "ResolvedToken":
        data = {
            k: v for k, v in doc.items() if not k.startswith("_") and k != "expires_at"
        }

        # Coerce string enums back to enum instances
        if isinstance(data.get("security_tier"), str):
            data["security_tier"] = SecurityTier(data["security_tier"])
        if isinstance(data.get("flags"), list):
            data["flags"] = [
                SecurityFlag(f) if isinstance(f, str) else f for f in data["flags"]
            ]

        # Ensure last_checked is timezone-aware (MongoDB stores UTC)
        lc = data.get("last_checked")
        if isinstance(lc, datetime) and lc.tzinfo is None:
            data["last_checked"] = lc.replace(tzinfo=timezone.utc)

        return cls(**data)


#  Cache key helper
def _cache_key(symbol: str, chain_id: int) -> str:
    return f"{symbol.upper()}:{chain_id}"


#  DexscreenerCandidate
class DexscreenerCandidate(BaseModel):
    symbol: str
    name: Optional[str] = None
    address: str
    chain_id: int
    chain_name: str
    liquidity_usd: float = 0.0
    pair_address: Optional[str] = None
    dex_id: Optional[str] = None
    price_usd: Optional[float] = None
    volume_h24_usd: Optional[float] = None
