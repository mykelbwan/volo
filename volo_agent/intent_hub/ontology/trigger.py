"""
Trigger condition ontology for event-driven execution.

A TriggerCondition is attached to an Intent when the user expresses a
conditional or time-based instruction such as:

  "When ETH drops below $2500, swap 0.5 ETH for USDC"
  "If BTC rises above $100k, bridge 1000 USDC to Base"

The condition is parsed by the semantic parser, stored on the Intent, and
then persisted in the intent_triggers MongoDB collection by the
wait_for_trigger_node so the Observer service can evaluate it independently
of the main LangGraph process.
"""

from __future__ import annotations

from datetime import timedelta
from enum import Enum
from math import isfinite
from typing import Optional

from pydantic import BaseModel, Field


class TriggerType(str, Enum):
    """
    Supported trigger condition types.

    PRICE_BELOW  – fire when asset spot price falls below ``target`` USD.
    PRICE_ABOVE  – fire when asset spot price rises above ``target`` USD.
    TIME_AT      – fire at a specific UTC ISO-8601 timestamp (future).
    """

    PRICE_BELOW = "price_below"
    PRICE_ABOVE = "price_above"
    TIME_AT = "time_at"


class ScheduleUnit(str, Enum):
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"


class TriggerSchedule(BaseModel):
    every: int = Field(default=1, ge=1, description="Repeat interval count.")
    unit: ScheduleUnit = Field(description="Repeat interval unit.")

    def to_timedelta(self) -> timedelta:
        if self.unit == ScheduleUnit.MINUTE:
            return timedelta(minutes=self.every)
        if self.unit == ScheduleUnit.HOUR:
            return timedelta(hours=self.every)
        if self.unit == ScheduleUnit.DAY:
            return timedelta(days=self.every)
        if self.unit == ScheduleUnit.WEEK:
            return timedelta(weeks=self.every)
        raise ValueError(f"Unsupported schedule unit: {self.unit}")


# Maps human-readable asset symbols to CoinGecko coin IDs used by the
# price observer.  Extend this as more assets are monitored.
_COINGECKO_ID_MAP: dict[str, str] = {
    "ETH": "ethereum",
    "BTC": "bitcoin",
    "WBTC": "bitcoin",
    "BNB": "binancecoin",
    "SOL": "solana",
    "MATIC": "matic-network",
    "POL": "matic-network",
    "AVAX": "avalanche-2",
    "ARB": "arbitrum",
    "OP": "optimism",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "AAVE": "aave",
    "PEPE": "pepe",
}


class TriggerCondition(BaseModel):
    """
    A structured condition that must be satisfied before a set of intents
    is executed.

    Attributes
    ----------
    type:
        The kind of trigger (price-based or time-based).
    asset:
        Token symbol to watch (e.g. ``"ETH"``, ``"BTC"``).
        Required for PRICE_BELOW / PRICE_ABOVE triggers.
    target:
        The price threshold in USD.
        Required for PRICE_BELOW / PRICE_ABOVE triggers.
    execute_at:
        UTC ISO-8601 timestamp string for TIME_AT triggers
        (e.g. ``"2025-12-31T00:00:00Z"``).
    """

    type: TriggerType
    asset: Optional[str] = Field(default=None, description="Token symbol, e.g. 'ETH'")
    chain: Optional[str] = Field(
        default=None,
        description="Chain name for address-scoped price triggers (e.g. 'ethereum')",
    )
    token_address: Optional[str] = Field(
        default=None,
        description="Token contract address for address-scoped price triggers",
    )
    target: Optional[float] = Field(default=None, description="Price threshold in USD")
    delay_seconds: Optional[int] = Field(
        default=None,
        description="Delay in seconds for time triggers (e.g. 3600 for 1 hour).",
    )
    schedule: Optional[TriggerSchedule] = Field(
        default=None,
        description="Recurring schedule for time triggers (e.g. every 1 week).",
    )
    execute_at: Optional[str] = Field(
        default=None, description="ISO-8601 UTC timestamp for time triggers"
    )

    # ── Serialisation helpers ─────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Return a plain dict suitable for MongoDB storage."""
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_dict(cls, data: dict) -> "TriggerCondition":
        """Reconstruct from a plain dict (e.g. read from MongoDB)."""
        return cls(**data)

    # ── Human-readable description ────────────────────────────────────────────

    @property
    def description(self) -> str:
        """Return a user-facing description of this condition."""
        if (
            self.type == TriggerType.PRICE_BELOW
            and self.asset
            and self.target is not None
        ):
            return f"{self.asset} price drops below ${self.target:,.2f}"
        if (
            self.type == TriggerType.PRICE_ABOVE
            and self.asset
            and self.target is not None
        ):
            return f"{self.asset} price rises above ${self.target:,.2f}"
        if self.type == TriggerType.TIME_AT:
            if self.schedule and self.execute_at:
                unit = self.schedule.unit.value
                every = self.schedule.every
                unit_label = unit if every == 1 else f"{unit}s"
                return f"every {every} {unit_label} starting {self.execute_at}"
            if self.delay_seconds is not None and self.delay_seconds > 0:
                return f"in {self.delay_seconds} seconds"
            if self.execute_at:
                return f"scheduled time reaches {self.execute_at}"
        return "unknown condition"

    # ── Observer helpers ──────────────────────────────────────────────────────

    @property
    def coingecko_id(self) -> Optional[str]:
        """
        Return the CoinGecko coin ID for the watched asset, or None if
        the asset is not in the supported symbol map.
        """
        if not self.asset:
            return None
        return _COINGECKO_ID_MAP.get(self.asset.upper())

    def is_price_trigger(self) -> bool:
        """Return True for any price-based trigger."""
        return self.type in (TriggerType.PRICE_BELOW, TriggerType.PRICE_ABOVE)

    def is_satisfied_by(self, current_price: float) -> bool:
        """
        Evaluate whether ``current_price`` satisfies this condition.

        Parameters
        ----------
        current_price:
            The current spot price of ``asset`` in USD.

        Returns
        -------
        bool
            True if the condition has been met and the intent should fire.
        """
        if self.target is None:
            return False
        if not isfinite(float(current_price)) or not isfinite(float(self.target)):
            return False
        if self.type == TriggerType.PRICE_BELOW:
            return current_price <= self.target
        if self.type == TriggerType.PRICE_ABOVE:
            return current_price >= self.target
        return False
