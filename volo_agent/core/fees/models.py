from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import List

# How long a fee quote is valid before it must be re-computed (seconds).
# balance_check_node will refuse to proceed if a quote has expired.
FEE_QUOTE_TTL_SECONDS = 60


@dataclass
class FeeQuote:
    node_id: str
    tool: str
    chain: str
    native_symbol: str
    base_fee_bps: int
    discount_bps: int
    final_fee_bps: int
    fee_amount_native: Decimal
    fee_recipient: str
    chain_family: str = "evm"
    chain_network: str = ""
    discount_reasons: List[str] = field(default_factory=list)
    expires_at: int = field(
        default_factory=lambda: int(time.time()) + FEE_QUOTE_TTL_SECONDS
    )
    is_native_tx: bool = False

    def is_expired(self) -> bool:
        return int(time.time()) > self.expires_at

    def formatted_amount(self) -> str:
        return f"{self.fee_amount_native:.6f} {self.native_symbol}"

    def formatted_rate(self) -> str:
        pct = self.final_fee_bps / 100
        if self.is_native_tx:
            return f"{pct:.2f}% of amount"
        return f"{pct:.2f}% flat (ERC-20 tx)"

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "tool": self.tool,
            "chain": self.chain,
            "chain_family": self.chain_family,
            "chain_network": self.chain_network,
            "native_symbol": self.native_symbol,
            "base_fee_bps": self.base_fee_bps,
            "discount_bps": self.discount_bps,
            "final_fee_bps": self.final_fee_bps,
            "fee_amount_native": str(self.fee_amount_native),
            "fee_recipient": self.fee_recipient,
            "discount_reasons": self.discount_reasons,
            "expires_at": self.expires_at,
            "is_native_tx": self.is_native_tx,
        }

    @classmethod
    def from_dict(cls, d: dict) -> FeeQuote:
        return cls(
            node_id=d["node_id"],
            tool=d["tool"],
            chain=d["chain"],
            chain_family=d.get("chain_family", "evm"),
            chain_network=d.get("chain_network", d.get("chain", "")),
            native_symbol=d["native_symbol"],
            base_fee_bps=d["base_fee_bps"],
            discount_bps=d["discount_bps"],
            final_fee_bps=d["final_fee_bps"],
            fee_amount_native=Decimal(d["fee_amount_native"]),
            fee_recipient=d["fee_recipient"],
            discount_reasons=d.get("discount_reasons", []),
            expires_at=d["expires_at"],
            is_native_tx=d.get("is_native_tx", False),
        )
