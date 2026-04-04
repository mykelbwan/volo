from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Tuple

# ---------------------------------------------------------------------------
# Fee reduction context
# ---------------------------------------------------------------------------


@dataclass
class FeeContext:
    """
    All data available to discount rules when computing a reduction.

    Populate what you have — every field has a safe default so callers
    only need to supply the values they actually track.

    Attributes:
        sender:                  Ethereum address of the user.
        monthly_volume_usd:      Rolling 30-day volume in USD equivalent.
                                 Used for volume-tier discounts.
        platform_token_balance:  User's VOLO (or equivalent) token balance.
                                 Used for token-holder discounts.
        is_referral:             True when the user signed up via a referral link.
        referral_code:           The referral code used (must be non-empty when
                                 is_referral is True for the discount to apply).
        total_lifetime_txs:      Total number of successful executions by this
                                 user across all tools and chains. Used for
                                 loyalty discounts.
    """

    sender: str
    monthly_volume_usd: Decimal = Decimal("0")
    platform_token_balance: Decimal = Decimal("0")
    is_referral: bool = False
    referral_code: str = ""
    total_lifetime_txs: int = 0


# ---------------------------------------------------------------------------
# Abstract base rule
# ---------------------------------------------------------------------------


class ReductionRule(ABC):
    """
    Base class for every fee reduction rule.

    Each rule is independent and stateless.  Rules are stacked by
    FeeReducer — their discounts are summed and then capped.

    Implement one class per discount concept so adding, removing, or
    adjusting a rule never touches any other rule or the engine.
    """

    @abstractmethod
    def apply(self, context: FeeContext) -> Tuple[int, str]:
        """
        Evaluate the rule against the given context.

        Returns:
            (discount_bps, reason_string)

            discount_bps:   Integer basis-points discount (0 = no discount).
            reason_string:  Non-empty human-readable label shown on the
                            receipt when discount_bps > 0.
                            Return an empty string when discount_bps == 0.
        """
        ...


# ---------------------------------------------------------------------------
# Concrete rules
# ---------------------------------------------------------------------------


class VolumeDiscount(ReductionRule):
    """
    Tiered discount based on rolling 30-day USD volume.

    Tiers (highest threshold wins, only one tier applies):
        ≥ $100 000  →  10 bps
        ≥ $10 000   →   5 bps
        ≥ $1 000    →   2 bps
        < $1 000    →   0 bps  (no discount)

    Designed to be extended: add or adjust tiers here only.
    """

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
    """
    Discount for users holding a minimum balance of the platform token
    (e.g. VOLO).

    Holding ≥ 1 000 VOLO  →  5 bps discount.

    Extend by adding more tiers (similar to VolumeDiscount) once the
    token is live and the economy is tuned.
    """

    THRESHOLD: Decimal = Decimal("1000")
    DISCOUNT_BPS: int = 5
    REASON: str = "VOLO holder (≥1 000 VOLO): -5 bps"

    def apply(self, context: FeeContext) -> Tuple[int, str]:
        if context.platform_token_balance >= self.THRESHOLD:
            return self.DISCOUNT_BPS, self.REASON
        return 0, ""


class ReferralDiscount(ReductionRule):
    """
    One-time discount for users who joined via a referral link.

    Requirements (both must be true):
        - context.is_referral == True
        - context.referral_code is non-empty

    Discount:  3 bps.

    Note: In production this rule should also verify the referral code
    against a registry to prevent abuse.  For V1, the flag is set
    upstream when the sub-org is created.
    """

    DISCOUNT_BPS: int = 3
    REASON: str = "Referral bonus: -3 bps"

    def apply(self, context: FeeContext) -> Tuple[int, str]:
        if context.is_referral and context.referral_code:
            return self.DISCOUNT_BPS, self.REASON
        return 0, ""


class LoyaltyDiscount(ReductionRule):
    """
    Discount for long-term users based on total lifetime transactions.

    Tiers:
        ≥ 500 txs  →  5 bps
        ≥ 100 txs  →  3 bps
        ≥  25 txs  →  1 bp
        <  25 txs  →  0 bps  (new users)

    The tx count is cheap to track in PerformanceLedger and does not
    require any on-chain oracle.
    """

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


# ---------------------------------------------------------------------------
# FeeReducer — stacks all rules
# ---------------------------------------------------------------------------

# Hard cap on the total discount regardless of how many rules fire.
# Prevents a user from combining every rule to achieve a 0-fee trade.
MAX_DISCOUNT_BPS: int = 50


class FeeReducer:
    """
    Applies every registered ReductionRule to a FeeContext and returns
    the combined discount in basis points, capped at MAX_DISCOUNT_BPS.

    Adding a new discount type:
        1. Write a new ReductionRule subclass above.
        2. Add an instance to RULES below.
        That's it — no other file needs to change.
    """

    RULES: List[ReductionRule] = [
        VolumeDiscount(),
        PlatformTokenDiscount(),
        ReferralDiscount(),
        LoyaltyDiscount(),
    ]

    def compute_discount(self, context: FeeContext) -> Tuple[int, List[str]]:
        """
        Stack all rules and return (total_discount_bps, [reason, ...]).

        The returned discount is already capped at MAX_DISCOUNT_BPS.
        Only reasons for rules that actually fired (bps > 0) are included.
        """
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
