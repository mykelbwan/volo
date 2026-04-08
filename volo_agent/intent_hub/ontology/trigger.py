from __future__ import annotations

from datetime import timedelta
from enum import Enum
from math import isfinite
from typing import Optional

from pydantic import BaseModel, Field


class TriggerType(str, Enum):
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

    def to_dict(self) -> dict:
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_dict(cls, data: dict) -> "TriggerCondition":
        return cls(**data)


    @property
    def description(self) -> str:
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

    @property
    def coingecko_id(self) -> Optional[str]:
        if not self.asset:
            return None
        return _COINGECKO_ID_MAP.get(self.asset.upper())

    def is_price_trigger(self) -> bool:
        return self.type in (TriggerType.PRICE_BELOW, TriggerType.PRICE_ABOVE)

    def is_satisfied_by(self, current_price: float) -> bool:
        if self.target is None:
            return False
        if not isfinite(float(current_price)) or not isfinite(float(self.target)):
            return False
        if self.type == TriggerType.PRICE_BELOW:
            return current_price <= self.target
        if self.type == TriggerType.PRICE_ABOVE:
            return current_price >= self.target
        return False
