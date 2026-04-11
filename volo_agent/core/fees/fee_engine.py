from __future__ import annotations

import os
import time
from decimal import Decimal
from typing import Dict, List, Optional

from core.fees.chains import is_native_token, resolve_fee_chain
from core.fees.fee_reducer import FeeContext, FeeReducer
from core.fees.models import FEE_QUOTE_TTL_SECONDS, FeeQuote
from core.fees.treasury import get_fee_treasury
from core.planning.execution_plan import ExecutionPlan, PlanNode

# Zero address used as a placeholder for native tokens in EVM contexts.
_NATIVE = "0x0000000000000000000000000000000000000000"

# ---------------------------------------------------------------------------
# Per-activity fee table (basis points, 1 bps = 0.01%).
#
# Rationale:
#   swap     — straightforward single-chain operation, lowest risk.
#   bridge   — cross-chain, longer settlement, more protocol risk.
#   default  — safe fallback for any future tool not yet in the table.
# ---------------------------------------------------------------------------
FEE_TABLE: Dict[str, int] = {
    "swap": 20,  # 0.20 %
    "bridge": 35,  # 0.35 %
}
DEFAULT_FEE_BPS: int = 20

# ---------------------------------------------------------------------------
# Maximum discount that can ever be applied regardless of how many rules
# stack.  Prevents giving the service away for free.
# ---------------------------------------------------------------------------
MAX_DISCOUNT_BPS: int = 50

# ---------------------------------------------------------------------------
# Flat native-token fee used for ERC-20 input transactions.
#
# When the user swaps or bridges an ERC-20 token we have no price oracle in
# V1, so we cannot compute X% of "1 000 USDC" in ETH terms on the fly.
# Instead we charge a small flat amount per activity type that is calibrated
# to be roughly equivalent in dollar terms at typical gas prices.
#
# These values are intentionally conservative:
#   swap     ≈ $1.50 on Ethereum at $3 000/ETH
#   bridge   ≈ $3.00 on Ethereum
#
# They will be replaced by oracle-derived amounts in a future iteration.
# ---------------------------------------------------------------------------
FLAT_FEE_NATIVE_ERC20: Dict[str, Decimal] = {
    "swap": Decimal("0.0005"),
    "bridge": Decimal("0.001"),
}
DEFAULT_FLAT_FEE: Decimal = Decimal("0.0005")


class FeeEngine:
    """
    Stateless fee-quoting engine.

    Usage (inside balance_check_node):

        engine  = FeeEngine()
        context = FeeContext(sender=sender_address)
        quotes  = engine.quote_plan(plan, context)

    The engine is disabled (returns empty list) when the
    FEE_TREASURY_ADDRESS environment variable is not set, so the system
    works correctly in development without any fee configuration.
    """

    def __init__(self) -> None:
        self._reducer: FeeReducer = FeeReducer()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def is_enabled(self) -> bool:
        """True when at least one fee treasury has been configured."""
        families = ("evm", "solana")
        return any(get_fee_treasury(family) for family in families)

    @staticmethod
    def _activity_key(tool: str) -> str:
        normalized = str(tool or "").strip().lower()
        if normalized == "solana_swap":
            return "swap"
        return normalized or "swap"

    @staticmethod
    def _env_decimal(env_key: str) -> Decimal | None:
        raw = os.getenv(env_key, "").strip()
        if not raw:
            return None
        try:
            value = Decimal(raw)
        except Exception:
            return None
        return value if value > 0 else None

    def _flat_fee_native(self, activity: str, family: str) -> Decimal:
        env_candidates = [
            f"FEE_FLAT_NATIVE_{family.upper()}_{activity.upper()}",
            f"FEE_FLAT_NATIVE_{activity.upper()}",
        ]
        for env_key in env_candidates:
            value = self._env_decimal(env_key)
            if value is not None:
                return value

        default_by_family = {
            "evm": FLAT_FEE_NATIVE_ERC20,
            "solana": {
                "swap": Decimal("0.005"),
                "bridge": Decimal("0.010"),
            },
        }
        family_defaults = default_by_family.get(family, FLAT_FEE_NATIVE_ERC20)
        return family_defaults.get(activity, DEFAULT_FLAT_FEE)

    def quote_node(
        self,
        node: PlanNode,
        context: FeeContext,
    ) -> Optional[FeeQuote]:
        """
        Produce a :class:`FeeQuote` for a single *node*.

        Returns ``None`` if:
          - the fee engine is disabled (no treasury address).
          - the node's chain cannot be resolved.
          - the node has no chain arg (e.g. a planning-only node).

        The quote is valid for :data:`FEE_QUOTE_TTL_SECONDS` seconds.
        balance_check_node will reject quotes that have expired.
        """
        if not self.is_enabled:
            return None
        if node.tool in ("check_balance", "transfer", "unwrap"):
            return None

        args = node.args
        activity = self._activity_key(node.tool)

        # ── Resolve chain ────────────────────────────────────────────────
        chain = resolve_fee_chain(args, tool=node.tool)
        if chain is None:
            return None
        treasury = get_fee_treasury(chain.family)
        if not treasury:
            return None

        # ── Base fee from activity table ─────────────────────────────────
        base_bps: int = FEE_TABLE.get(activity, DEFAULT_FEE_BPS)

        # ── Apply reduction rules ────────────────────────────────────────
        discount_bps, discount_reasons = self._reducer.compute_discount(context)
        # Hard cap: never discount beyond MAX_DISCOUNT_BPS
        discount_bps = min(discount_bps, MAX_DISCOUNT_BPS)
        final_bps: int = max(base_bps - discount_bps, 0)

        # ── Determine whether the input token is native ──────────────────
        # swap   → token_in_address
        # bridge → source_address
        token_address: Optional[str] = (
            args.get("token_in_address")
            or args.get("source_address")
            or args.get("token_address")
            or args.get("token_in_mint")
            or args.get("token_mint")
        )
        is_native_tx: bool = is_native_token(token_address, chain)

        # ── Compute fee amount in native token ───────────────────────────
        fee_amount_native: Decimal

        if is_native_tx:
            # Percentage-based: final_bps% of the input amount.
            raw_amount = args.get("amount_in") or args.get("amount")
            try:
                amount = Decimal(str(raw_amount))
                fee_amount_native = (Decimal(final_bps) / Decimal("10000")) * amount
            except Exception:
                # Fallback to flat fee if amount is unparseable (e.g. a marker
                # that hasn't been resolved yet — executor handles those later).
                fee_amount_native = self._flat_fee_native(activity, chain.family)
        else:
            # ERC-20 input: flat native fee with discount applied proportionally.
            base_flat: Decimal = self._flat_fee_native(activity, chain.family)
            if base_bps > 0:
                discount_ratio = Decimal(final_bps) / Decimal(base_bps)
            else:
                discount_ratio = Decimal("1")
            fee_amount_native = (base_flat * discount_ratio).quantize(
                Decimal("0.000001")
            )

        fee_amount_native = fee_amount_native.quantize(Decimal("0.000001"))

        return FeeQuote(
            node_id=node.id,
            tool=node.tool,
            chain=chain.name,
            chain_family=chain.family,
            chain_network=chain.network,
            native_symbol=chain.native_symbol,
            base_fee_bps=base_bps,
            discount_bps=discount_bps,
            final_fee_bps=final_bps,
            fee_amount_native=fee_amount_native,
            fee_recipient=treasury,
            discount_reasons=discount_reasons,
            expires_at=int(time.time()) + FEE_QUOTE_TTL_SECONDS,
            is_native_tx=is_native_tx,
        )

    def quote_plan(
        self,
        plan: ExecutionPlan,
        context: FeeContext,
    ) -> List[FeeQuote]:
        """
        Produce one :class:`FeeQuote` for every node in *plan* that can be
        quoted.  Nodes that return ``None`` from :meth:`quote_node` (e.g.
        planning-only stubs without a chain) are silently skipped.

        Returns an empty list when the engine is disabled.
        """
        if not self.is_enabled:
            return []

        quotes: List[FeeQuote] = []
        for node in plan.nodes.values():
            quote = self.quote_node(node, context)
            if quote is not None:
                quotes.append(quote)

        return quotes
