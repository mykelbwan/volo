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
    """
    A single fee quote attached to one PlanNode.

    Lifecycle:
        1. Created by FeeEngine.quote_plan() inside balance_check_node.
        2. Serialised into AgentState.fee_quotes (list of dicts).
        3. Displayed on the confirmation receipt.
        4. Re-hydrated in execution_engine_node and collected on-chain
           via FeeCollector after the corresponding main step succeeds.

    Attributes:
        node_id:            The PlanNode.id this quote belongs to.
        tool:               Activity type — "swap" | "bridge".
        chain:              Human-readable chain name (e.g. "Ethereum").
        chain_family:       Collector family, e.g. "evm" or "solana".
        chain_network:      Family-specific network identifier used at collection time.
        native_symbol:      Native token symbol for the chain (e.g. "ETH").
        base_fee_bps:       Gross fee in basis points from FEE_TABLE.
        discount_bps:       Total discount applied (sum of all active rules).
        final_fee_bps:      Effective fee bps after discount (base - discount).
        fee_amount_native:  Actual amount the user will pay in native token.
        fee_recipient:      Treasury address that receives the fee.
        discount_reasons:   Human-readable list of applied discount rules.
        expires_at:         Unix timestamp after which this quote is stale.
        is_native_tx:       True when the input token is the native coin.
                            False for ERC-20 inputs (flat fee is used instead).
    """

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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def is_expired(self) -> bool:
        """Return True if the quote TTL has elapsed."""
        return int(time.time()) > self.expires_at

    def formatted_amount(self) -> str:
        """Human-readable fee amount, e.g. '0.000420 ETH'."""
        return f"{self.fee_amount_native:.6f} {self.native_symbol}"

    def formatted_rate(self) -> str:
        """
        Human-readable rate string shown on the receipt.

        Examples:
            "0.20% of native amount"
            "0.35% flat (ERC-20 tx)"
        """
        pct = self.final_fee_bps / 100
        if self.is_native_tx:
            return f"{pct:.2f}% of amount"
        return f"{pct:.2f}% flat (ERC-20 tx)"

    # ------------------------------------------------------------------
    # Serialisation — state must hold plain dicts (JSON-serialisable)
    # ------------------------------------------------------------------

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
