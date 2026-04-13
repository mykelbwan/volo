from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Tuple


@dataclass
class FeeContext:
    sender: str
    monthly_volume_usd: Decimal = Decimal("0")
    platform_token_balance: Decimal = Decimal("0")
    is_referral: bool = False
    referral_code: str = ""
    total_lifetime_txs: int = 0


class ReductionRule(ABC):
    @abstractmethod
    def apply(self, context: FeeContext) -> Tuple[int, str]: ...


class VolumeDiscount(ReductionRule):
    TIERS: List[Tuple[Decimal, int, str]] = [
        (Decimal("100000"), 10, "$100k+ monthly volume: -10 bps"),
        (Decimal("10000"), 5, "$10k+ monthly volume: -5 bps"),
        (Decimal("1000"), 2, "$1k+ monthly volume: -2 bps"),
    ]

    def apply(self, context: FeeContext) -> Tuple[int, str]:
        for threshold, bps, reason in self.TIERS:
            if context.monthly_volume_usd >= threshold:
                return bps, reason
        return 0, ""


class PlatformTokenDiscount(ReductionRule):
    THRESHOLD: Decimal = Decimal("1000")
    DISCOUNT_BPS: int = 5
    REASON: str = "VOLO holder (≥1 000 VOLO): -5 bps"

    def apply(self, context: FeeContext) -> Tuple[int, str]:
        if context.platform_token_balance >= self.THRESHOLD:
            return self.DISCOUNT_BPS, self.REASON
        return 0, ""


class ReferralDiscount(ReductionRule):
    DISCOUNT_BPS: int = 3
    REASON: str = "Referral bonus: -3 bps"

    def apply(self, context: FeeContext) -> Tuple[int, str]:
        if context.is_referral and context.referral_code:
            return self.DISCOUNT_BPS, self.REASON
        return 0, ""


class LoyaltyDiscount(ReductionRule):
    TIERS: List[Tuple[int, int, str]] = [
        (500, 5, "500+ lifetime txs: -5 bps"),
        (100, 3, "100+ lifetime txs: -3 bps"),
        (25, 1, "25+ lifetime txs: -1 bp"),
    ]

    def apply(self, context: FeeContext) -> Tuple[int, str]:
        for threshold, bps, reason in self.TIERS:
            if context.total_lifetime_txs >= threshold:
                return bps, reason
        return 0, ""


# Hard cap on the total discount regardless of how many rules fire.
# Prevents a user from combining every rule to achieve a 0-fee trade.
MAX_DISCOUNT_BPS: int = 50


class FeeReducer:
    RULES: List[ReductionRule] = [
        VolumeDiscount(),
        PlatformTokenDiscount(),
        ReferralDiscount(),
        LoyaltyDiscount(),
    ]

    def compute_discount(self, context: FeeContext) -> Tuple[int, List[str]]:
        total_bps = 0
        reasons: List[str] = []

        for rule in self.RULES:
            bps, reason = rule.apply(context)
            if bps > 0:
                total_bps += bps
                reasons.append(reason)

        capped = min(total_bps, MAX_DISCOUNT_BPS)

        # If the cap was hit, annotate so the user can see it on the receipt.
        if capped < total_bps:
            reasons.append(f"(discount capped at {MAX_DISCOUNT_BPS} bps)")

        return capped, reasons
