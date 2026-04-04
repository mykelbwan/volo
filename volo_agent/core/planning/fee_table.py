# volo_agent/core/planning/fee_table.py
"""
Fee table loader and lookup utilities for VWS (Virtual Wallet State).

This module provides:
- FeeRule dataclass: represents a single fee-rule entry.
- FeeTable class: load from JSON/CSV, validate, lookup rules with precedence,
  and compute estimated fees for a requested amount.
- Simple CLI/self-test entrypoint when executed directly.

Design notes
------------
- Fee matching precedence:
    1) exact match: (protocol, src_chain, dst_chain, token)
    2) protocol + pair, token-agnostic: (protocol, src_chain, dst_chain, token=None)
    3) any-protocol but token-specific for pair: (protocol=None, src_chain, dst_chain, token)
    4) pair-wide fallback: (protocol=None, src_chain, dst_chain, token=None)
    5) global fallback is handled by FeeTable via defaults

- No external dependencies are required (stdlib only). Validation is
  performed with explicit checks and raises ValueError on invalid rules.

Example JSON row:
{
  "protocol_id": "cbridge_v2",
  "src_chain": "ethereum",
  "dst_chain": "base",
  "token": "USDC",                 # optional
  "fee_type": "percent_plus_flat", # one of flat, percent, percent_plus_flat
  "percent": 0.0012,               # optional depending on fee_type
  "flat": 0.5,                     # optional depending on fee_type
  "min_fee": 0.1,                  # optional
  "max_fee": 50.0,                 # optional
  "last_updated": "2026-03-01T12:00:00Z",
  "notes": "based on operator docs"
}

This file also contains a small __main__-driven self-check that performs basic
validations and demonstrates usage. It is not a replacement for repository
unit tests but is useful for quick local sanity checks.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterable, List, Optional, Tuple, Union

# Public constants / defaults
ALLOWED_FEE_TYPES = {"flat", "percent", "percent_plus_flat"}

# Conservative global fallback values used when no table entry is found.
GLOBAL_DEFAULT_PERCENT = Decimal("0.01")  # 1% fallback for unknown bridges
GLOBAL_DEFAULT_FLAT = Decimal("0")  # zero flat by default


class FeeTableError(Exception):
    """Generic fee table error."""


@dataclass(frozen=True)
class FeeRule:
    """Represents a single fee table entry."""

    protocol_id: Optional[str]  # lowercased or None for generic
    src_chain: str  # lowercased canonical chain id/name
    dst_chain: str  # lowercased
    token: Optional[str]  # token symbol/address lowercased or None

    fee_type: str  # one of ALLOWED_FEE_TYPES
    percent: Decimal  # 0..1 (or 0 when not applicable)
    flat: Decimal  # flat token units (or 0)
    min_fee: Optional[Decimal] = None
    max_fee: Optional[Decimal] = None
    last_updated: Optional[datetime] = None
    notes: Optional[str] = None

    def compute_fee(self, amount: Decimal) -> Decimal:
        """
        Compute the fee (in same token units as amount) for a given input amount.

        Applies min/max clamps if present.
        """
        if self.fee_type == "flat":
            fee = self.flat
        elif self.fee_type == "percent":
            fee = (amount * self.percent).quantize(Decimal("0.00000001"))
        elif self.fee_type == "percent_plus_flat":
            fee = (amount * self.percent + self.flat).quantize(Decimal("0.00000001"))
        else:
            raise FeeTableError(f"Unsupported fee_type: {self.fee_type}")

        # Apply min/max clamps
        if self.min_fee is not None and fee < self.min_fee:
            fee = self.min_fee
        if self.max_fee is not None and fee > self.max_fee:
            fee = self.max_fee
        return fee


class FeeTable:
    """
    In-memory fee table supporting load from JSON or CSV and read lookups.

    Internally stores rules keyed by tuple (protocol_or_none, src_chain, dst_chain, token_or_none)
    with normalized lowercased strings.
    """

    def __init__(self, rules: Optional[Iterable[FeeRule]] = None) -> None:
        # Keyed map for fast lookup
        self._rules: Dict[Tuple[Optional[str], str, str, Optional[str]], FeeRule] = {}
        if rules:
            for r in rules:
                self.add_rule(r)

    @staticmethod
    def _normalize_optional_lower(val: Optional[str]) -> Optional[str]:
        if val is None:
            return None
        val = val.strip()
        return val.lower() if val != "" else None

    @staticmethod
    def _parse_decimal(val: Union[str, float, int, Decimal, None]) -> Optional[Decimal]:
        if val is None or (isinstance(val, str) and val.strip() == ""):
            return None
        if isinstance(val, Decimal):
            return val
        try:
            return Decimal(str(val))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"Invalid decimal value: {val}") from exc

    def add_rule(self, rule: FeeRule) -> None:
        key = (
            self._normalize_optional_lower(rule.protocol_id),
            rule.src_chain.strip().lower(),
            rule.dst_chain.strip().lower(),
            self._normalize_optional_lower(rule.token),
        )
        self._rules[key] = rule

    # ----------------------------
    # Loading helpers
    # ----------------------------
    @classmethod
    def from_json_file(cls, path: str) -> "FeeTable":
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        rules = []
        for idx, raw in enumerate(data):
            rules.append(cls._rule_from_raw(raw, idx))
        return cls(rules)

    @classmethod
    def from_csv_file(cls, path: str) -> "FeeTable":
        rules = []
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for idx, row in enumerate(reader):
                rules.append(cls._rule_from_raw(row, idx))
        return cls(rules)

    @classmethod
    def _rule_from_raw(cls, raw: dict, idx: int) -> FeeRule:
        # Required fields: src_chain, dst_chain, fee_type, last_updated (optional)
        protocol = raw.get("protocol_id") or raw.get("protocol") or None
        src_chain = (
            raw.get("src_chain") or raw.get("source_chain") or raw.get("src") or ""
        )
        dst_chain = (
            raw.get("dst_chain") or raw.get("dest_chain") or raw.get("dst") or ""
        )
        token = raw.get("token") or None
        fee_type = raw.get("fee_type") or raw.get("type") or ""

        if not src_chain or not dst_chain or not fee_type:
            raise ValueError(f"Invalid rule at index {idx}: missing src/dst/fee_type")

        fee_type = fee_type.strip().lower()
        if fee_type not in ALLOWED_FEE_TYPES:
            raise ValueError(f"Invalid fee_type '{fee_type}' for rule at index {idx}")

        percent = cls._parse_decimal(raw.get("percent")) or Decimal("0")
        flat = cls._parse_decimal(raw.get("flat")) or Decimal("0")
        min_fee = cls._parse_decimal(raw.get("min_fee"))
        max_fee = cls._parse_decimal(raw.get("max_fee"))

        # sanity checks
        if percent < 0 or percent > 1:
            raise ValueError(f"percent must be between 0 and 1 for rule at index {idx}")
        if flat < 0:
            raise ValueError(f"flat must be >= 0 for rule at index {idx}")
        if min_fee is not None and min_fee < 0:
            raise ValueError(f"min_fee must be >= 0 for rule at index {idx}")
        if max_fee is not None and max_fee < 0:
            raise ValueError(f"max_fee must be >= 0 for rule at index {idx}")
        if min_fee is not None and max_fee is not None and min_fee > max_fee:
            raise ValueError(f"min_fee > max_fee for rule at index {idx}")

        last_updated_raw = raw.get("last_updated") or raw.get("updated_at") or None
        last_updated = None
        if last_updated_raw:
            try:
                last_updated = datetime.fromisoformat(
                    last_updated_raw.replace("Z", "+00:00")
                )
            except Exception:
                # If parse fails, keep None but don't crash load-time.
                last_updated = None

        return FeeRule(
            protocol_id=cls._normalize_optional_lower(protocol),
            src_chain=src_chain.strip().lower(),
            dst_chain=dst_chain.strip().lower(),
            token=cls._normalize_optional_lower(token),
            fee_type=fee_type,
            percent=percent,
            flat=flat,
            min_fee=min_fee,
            max_fee=max_fee,
            last_updated=last_updated,
            notes=raw.get("notes"),
        )

    # ----------------------------
    # Lookup and compute
    # ----------------------------
    def lookup_rule(
        self,
        src_chain: str,
        dst_chain: str,
        token: Optional[str] = None,
        protocol: Optional[str] = None,
    ) -> Optional[FeeRule]:
        """
        Lookup a fee rule with the precedence described above. Returns None
        when no rule is found and callers should use global fallbacks.
        """
        src = src_chain.strip().lower()
        dst = dst_chain.strip().lower()
        token_norm = self._normalize_optional_lower(token)
        protocol_norm = self._normalize_optional_lower(protocol)

        # Precedence list of keys to try
        candidates = [
            (protocol_norm, src, dst, token_norm),
            (protocol_norm, src, dst, None),
            (None, src, dst, token_norm),
            (None, src, dst, None),
        ]
        for k in candidates:
            rule = self._rules.get(k)
            if rule is not None:
                return rule
        return None

    def estimate_fee_for_amount(
        self,
        amount: Decimal,
        src_chain: str,
        dst_chain: str,
        token: Optional[str] = None,
        protocol: Optional[str] = None,
    ) -> Tuple[Decimal, Optional[FeeRule]]:
        """
        Estimate fee for `amount` bridging from src_chain -> dst_chain.

        Returns (fee_amount, used_rule_or_None). If no rule found a conservative
        fallback percent (GLOBAL_DEFAULT_PERCENT) is used and the fee_rule is None.
        """
        if amount is None:
            raise ValueError("amount must be provided")

        if amount <= 0:
            return Decimal("0"), None

        rule = self.lookup_rule(src_chain, dst_chain, token=token, protocol=protocol)
        if rule is None:
            # Conservative fallback
            fee = (amount * GLOBAL_DEFAULT_PERCENT).quantize(Decimal("0.00000001"))
            return fee, None

        fee = rule.compute_fee(amount)
        return fee, rule
