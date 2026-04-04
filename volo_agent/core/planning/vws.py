"""
core/planning/vws.py
--------------------
Virtual Wallet State (VWS) — pure, side-effect-free simulation of wallet
balance and gas changes across a sequence of planned steps.

This file is a refactor focused on removing duplication (DRY) across the
step simulators by extracting common gas + token check/deduction logic into
small helper methods while preserving the original public behaviour.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from core.planning.fee_table import FeeTable

_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# EVM zero-address — canonical identifier for native tokens (ETH, MATIC, …).
NATIVE_ADDRESS: str = "0x0000000000000000000000000000000000000000"

# Bridge fee table: protocol → conservative fee rate (as a decimal fraction).
# These are pessimistic upper bounds, not averages.  Real fees are usually
# lower; the gap ensures VWS never approves a plan that would arrive short.
BRIDGE_FEE_TABLE: Dict[str, Decimal] = {
    "across": Decimal("0.002"),  # 0.1–0.2 % typical → use 0.2 %
    "relay": Decimal("0.003"),  # 0.2–0.4 % typical
    "mayan": Decimal("0.005"),  # 0.3–0.5 % incl. Swift relayer fee
    "lifi": Decimal("0.004"),  # aggregated, varies by underlying bridge
    "socket": Decimal("0.004"),  # aggregated, varies
}

# Applied on top of the protocol fee as an additional safety margin.
# Absorbs minor slippage variance so VWS never approves a borderline plan.
ARRIVAL_BUFFER: Decimal = Decimal("0.995")  # 0.5 % extra margin

# Default fee used for any bridge protocol not in the table above.
DEFAULT_BRIDGE_FEE: Decimal = Decimal("0.005")  # 0.5 % conservative

# Gas unit estimates — pessimistic upper bounds per tool type.
# Units are EVM gas units.  For Solana tools the value is 0 because
# Solana transaction fees are negligible (< $0.001).
GAS_UNITS: Dict[str, int] = {
    "swap": 200_000,  # Uniswap V3 multi-hop, conservative
    "bridge": 300_000,  # depositV3 / Mayan forwarder, conservative
    "transfer": 65_000,  # ERC-20 transfer
    "unwrap": 100_000,  # WETH.withdraw() with conservative headroom
    "evm_native_transfer": 21_000,  # intrinsic native transfer gas
    "evm_token_transfer": 65_000,  # ERC-20 transfer
    "solana_native_transfer": 0,
    "solana_token_transfer": 0,
    "solana_swap": 0,  # Solana fees treated as zero for VWS purposes
}

# Fallback gas price used only when gas_price_cache has no entry for a chain
# (e.g. cold start or unknown chain).  30 gwei is a safe Ethereum L1 estimate;
# L2s are far cheaper so this is always conservative.
FALLBACK_GAS_PRICE_WEI: int = 30 * 10**9  # 30 gwei


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StepResult:
    """
    Outcome of simulating a single plan step against a VirtualWalletState.

    Attributes
    ----------
    success:
        ``True`` when all balance and gas checks passed.
    rejection_reason:
        Human-readable explanation of why the step was rejected.
        Empty string on success.
    gas_cost_native:
        Estimated gas cost in native token units for this step.
        Always populated, even on failure.
    """

    success: bool
    rejection_reason: str = ""
    gas_cost_native: Decimal = Decimal("0")


@dataclass
class SimulationResult:
    """
    Outcome of simulating an entire candidate plan.

    Attributes
    ----------
    valid:
        ``True`` when every step passed all VWS checks.
    rejection_reasons:
        Ordered list of reasons each failed step was rejected.
        Empty when ``valid`` is ``True``.
    total_gas_cost_native:
        Sum of estimated gas costs across all steps, keyed by chain name.
        Used by the plan scorer to compare total cost across candidates.
    total_estimated_fees:
        Sum of estimated bridge / protocol fees across all steps.
        Expressed in the same token units as the bridged amounts.
    """

    valid: bool
    rejection_reasons: List[str] = field(default_factory=list)
    total_gas_cost_native: Dict[str, Decimal] = field(default_factory=dict)
    total_estimated_fees: Decimal = Decimal("0")


# ---------------------------------------------------------------------------
# VirtualWalletState
# ---------------------------------------------------------------------------


class VirtualWalletState:
    """
    Simulates wallet balance changes for a sequence of planned steps without
    making any network calls or touching on-chain state.

    Internal representation
    -----------------------
    Balances are stored in a flat dict keyed by ``(chain, token_address)``
    pairs (both lowercased).  These generic names remain in VWS because the
    store is shared across swaps, bridges, reservations, and historical
    snapshots; the neutral transfer contract is normalized before entering VWS.
    Native tokens are represented by :data:`NATIVE_ADDRESS` (the EVM
    zero-address) on their respective chain.

    All amounts are ``Decimal`` in human-readable units (e.g. ``Decimal("1.5")``
    for 1.5 ETH), consistent with the rest of the Volo codebase.
    """

    def __init__(
        self,
        balances: Dict[Tuple[str, str], Decimal],
        fee_table: Optional[FeeTable] = None,
    ) -> None:
        # Mutable internal state — never shared across candidates.
        # Keyed as (chain_lower, token_address_lower).
        self._balances: Dict[Tuple[str, str], Decimal] = dict(balances)
        # Optional FeeTable instance used to compute protocol-specific fees.
        # When provided, VWS will use it for more accurate bridge fee estimates.
        self._fee_table: Optional[FeeTable] = fee_table

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_balance_snapshot(
        cls,
        snapshot: Dict[str, str],
        sender: str,
        fee_table: Optional["FeeTable"] = None,
    ) -> "VirtualWalletState":
        """
        Build a ``VirtualWalletState`` from the ``balance_snapshot`` dict
        stored in ``AgentState``.

        The snapshot is keyed as ``"sender|chain|token_address"`` with values
        that are string-encoded ``Decimal`` amounts in human-readable units
        (not wei).  Keys belonging to a different sender are ignored so the
        VWS only tracks the balances of the wallet that will sign the
        transactions.

        Parameters
        ----------
        snapshot:
            ``AgentState["balance_snapshot"]`` — may be ``None`` or empty,
            in which case an empty VWS is returned.
        sender:
            Ethereum address of the active wallet (case-insensitive).

        Returns
        -------
        VirtualWalletState
            Initialised with the wallet's known balances.  Missing balances
            default to ``Decimal("0")`` when queried.
        """
        balances: Dict[Tuple[str, str], Decimal] = {}
        if not snapshot:
            return cls(balances, fee_table=fee_table)

        sender_lower = sender.strip().lower()

        for key, value in snapshot.items():
            parts = key.split("|")
            if len(parts) != 3:
                continue

            snap_sender, chain, token_addr = parts

            if snap_sender.strip() != sender_lower:
                # Balance belongs to a different wallet — skip.
                continue

            try:
                balance = Decimal(str(value))
            except Exception:
                continue

            if balance < 0:
                continue

            balances[(chain.strip().lower(), token_addr.strip().lower())] = balance

        _LOGGER.debug(
            "[VWS] Loaded %d balance entries for sender %s",
            len(balances),
            sender[:10],
        )
        return cls(balances, fee_table=fee_table)

    def clone(self) -> "VirtualWalletState":
        """
        Return a deep copy of this state so each candidate plan can be
        simulated independently without mutating the original.
        """
        return VirtualWalletState(dict(self._balances))

    # ------------------------------------------------------------------
    # Balance accessors
    # ------------------------------------------------------------------

    def get_balance(self, chain: str, token_address: str) -> Decimal:
        """
        Return the current simulated balance for *(chain, token_address)*.
        Returns ``Decimal("0")`` when the token has never been seen.
        """
        key = (chain.strip().lower(), token_address.strip().lower())
        return self._balances.get(key, Decimal("0"))

    def _deduct(
        self,
        chain: str,
        token_address: str,
        amount: Decimal,
    ) -> bool:
        """
        Subtract *amount* from *(chain, token_address)* balance.

        Returns ``False`` without modifying state when the balance is
        insufficient — the caller should treat this as a plan rejection.
        """
        key = (chain.strip().lower(), token_address.strip().lower())
        balance = self._balances.get(key, Decimal("0"))
        if balance < amount:
            return False
        self._balances[key] = balance - amount
        return True

    def _add(
        self,
        chain: str,
        token_address: str,
        amount: Decimal,
    ) -> None:
        """
        Add *amount* to *(chain, token_address)* balance.

        Used to model tokens arriving on the destination chain after a bridge.
        """
        key = (chain.strip().lower(), token_address.strip().lower())
        self._balances[key] = self._balances.get(key, Decimal("0")) + amount

    # ------------------------------------------------------------------
    # Static estimators  (no I/O)
    # ------------------------------------------------------------------

    @staticmethod
    def _gas_price_wei(chain_id: Optional[int]) -> int:
        """
        Return the cached gas price in Wei for *chain_id*.

        Uses the module-level ``gas_price_cache`` singleton which is kept
        warm by the routing layer — this call does **not** make an RPC
        request.  Falls back to :data:`FALLBACK_GAS_PRICE_WEI` (30 gwei)
        on any error or cache miss.
        """
        if chain_id is None:
            return FALLBACK_GAS_PRICE_WEI
        try:
            from wallet_service.evm.gas_price import gas_price_cache  # noqa: PLC0415

            entry = gas_price_cache._entries.get(chain_id)  # type: ignore[attr-defined]
            if entry is not None:
                return entry.gas_price_wei
        except Exception:
            pass
        return FALLBACK_GAS_PRICE_WEI

    @staticmethod
    def _estimate_gas_cost(tool: str, chain_id: Optional[int]) -> Decimal:
        """
        Return the estimated gas cost in native token units for one step.

        Gas cost = gas_units × gas_price_wei / 1e18

        Returns ``Decimal("0")`` for Solana tools or when the chain is
        unknown (Solana gas is negligible and handled separately).
        """
        units = GAS_UNITS.get(tool, 200_000)
        if units == 0 or chain_id is None:
            return Decimal("0")

        price_wei = VirtualWalletState._gas_price_wei(chain_id)
        return Decimal(units * price_wei) / Decimal(10**18)

    @staticmethod
    def estimate_bridge_arrival(
        amount: Decimal,
        protocol: str,
    ) -> Decimal:
        """
        Estimate how many tokens arrive on the destination chain after a
        bridge step using the conservative fee table plus the safety buffer.

        Formula::

            arrival = amount × (1 − fee_rate) × ARRIVAL_BUFFER

        This is a public helper so the plan generator and scorer can use
        the same formula as the simulation layer.

        Parameters
        ----------
        amount:
            Input amount being bridged (human-readable).
        protocol:
            Bridge protocol name, e.g. ``"across"``, ``"mayan"``.
        """
        fee_rate = BRIDGE_FEE_TABLE.get(protocol.strip().lower(), DEFAULT_BRIDGE_FEE)
        return amount * (Decimal("1") - fee_rate) * ARRIVAL_BUFFER

    def _estimate_bridge_arrival(
        self,
        amount: Decimal,
        protocol: str,
        src_chain: Optional[str] = None,
        dst_chain: Optional[str] = None,
        token: Optional[str] = None,
    ) -> Decimal:
        """
        Instance-level arrival estimator.

        If a FeeTable was passed to the VirtualWalletState constructor it is
        used to compute a protocol/token-specific fee (in token units).
        Otherwise this method falls back to the original conservative
        BRIDGE_FEE_TABLE percentage-based estimate.

        The returned value is the conservative arrival amount (token units)
        after subtracting estimated protocol fee and applying ARRIVAL_BUFFER.
        """
        # Prefer FeeTable-based fee estimation when available.
        try:
            if self._fee_table is not None:
                # FeeTable.estimate_fee_for_amount returns (fee_amount, used_rule_or_None)
                fee_amount, _ = self._fee_table.estimate_fee_for_amount(
                    amount,
                    src_chain or "",
                    dst_chain or "",
                    token=token,
                    protocol=protocol,
                )
                arrival = (amount - fee_amount) * ARRIVAL_BUFFER
                # Prevent negative arrival in pathological cases
                if arrival < Decimal("0"):
                    return Decimal("0")
                return arrival
        except Exception:
            # Any error in FeeTable usage should degrade gracefully to the
            # legacy behaviour rather than crash the simulator.
            _LOGGER.exception(
                "[VWS] FeeTable estimation failed; falling back to defaults"
            )

        # Legacy fallback: percentage-based estimate
        fee_rate = BRIDGE_FEE_TABLE.get(protocol.strip().lower(), DEFAULT_BRIDGE_FEE)
        return amount * (Decimal("1") - fee_rate) * ARRIVAL_BUFFER

    # ------------------------------------------------------------------
    # Helper: consolidated checks (DRY)
    # ------------------------------------------------------------------

    def _perform_gas_check_and_deduct(
        self,
        tool: str,
        chain: str,
        chain_id: Optional[int],
        native_address: str,
        gas_profile: Optional[str] = None,
        gas_cost_native_override: Optional[Decimal] = None,
    ) -> Tuple[bool, Decimal, Optional[str]]:
        """
        Estimate gas for *tool* on *chain* and deduct it from native_address.

        Returns:
            (success, gas_cost, rejection_reason_or_None)
        """
        gas_cost = (
            gas_cost_native_override
            if gas_cost_native_override is not None
            else self._estimate_gas_cost(gas_profile or tool, chain_id)
        )
        if gas_cost > 0:
            if not self._deduct(chain, native_address, gas_cost):
                have = self.get_balance(chain, native_address)
                reason = (
                    f"not enough gas on {chain} to pay for the {tool} "
                    f"(need ~{gas_cost:.6f}, have {have:.6f} native token)"
                )
                return False, gas_cost, reason
        return True, gas_cost, None

    def _perform_token_check_and_deduct(
        self,
        chain: str,
        token_address: str,
        amount: Decimal,
        token_label: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Ensure *chain/token_address* has at least *amount* and deduct it.

        Returns:
            (success, rejection_reason_or_None)
        """
        if not self._deduct(chain, token_address, amount):
            have = self.get_balance(chain, token_address)
            label = token_label or (token_address[:10] + "…")
            reason = f"not enough {label} on {chain} (need {amount}, have {have:.6f})"
            return False, reason
        return True, None

    def reserve_balance(
        self,
        *,
        chain: str,
        token_address: str,
        amount: Decimal,
        label: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Reserve *amount* from an already-tracked balance for non-step spends.

        This is used by higher-level VWS orchestration to model additional
        native requirements, such as platform fees, without re-implementing
        balance deduction logic outside the simulator.
        """
        if amount <= 0:
            return True, None
        if not self._deduct(chain, token_address, amount):
            have = self.get_balance(chain, token_address)
            reason = (
                f"not enough native token on {chain} to reserve {label} "
                f"(need {amount:.6f}, have {have:.6f})"
            )
            return False, reason
        return True, None

    # ------------------------------------------------------------------
    # Step simulators (use helpers)
    # ------------------------------------------------------------------

    def simulate_swap(
        self,
        *,
        chain: str,
        chain_id: Optional[int],
        token_in_address: str,
        amount_in: Decimal,
        native_address: str = NATIVE_ADDRESS,
        token_out_address: Optional[str] = None,
        amount_out: Optional[Decimal] = None,
        gas_cost_native_override: Optional[Decimal] = None,
        tool_name: str = "swap",
    ) -> StepResult:
        """
        Simulate a swap step (EVM ``swap`` or Solana ``solana_swap`` tool).

        Checks performed
        ----------------
        1. Sufficient native token balance for gas on *chain*.
        2. Sufficient *token_in* balance for *amount_in*.

        The output amount is not tracked because any positive output can
        feed the next step — the route planner will compute the exact amount.
        """
        # Gas check + deduct
        ok, gas_cost, reason = self._perform_gas_check_and_deduct(
            tool=tool_name,
            chain=chain,
            chain_id=chain_id,
            native_address=native_address,
            gas_cost_native_override=gas_cost_native_override,
        )
        if not ok:
            return StepResult(
                success=False, rejection_reason=reason or "", gas_cost_native=gas_cost
            )

        # Token input check + deduct
        ok, reason = self._perform_token_check_and_deduct(
            chain=chain,
            token_address=token_in_address,
            amount=amount_in,
            token_label=None,
        )
        if not ok:
            return StepResult(
                success=False, rejection_reason=reason or "", gas_cost_native=gas_cost
            )

        if (
            amount_out is not None
            and amount_out > 0
            and token_out_address
            and token_out_address.strip()
        ):
            self._add(chain, token_out_address, amount_out)

        return StepResult(success=True, gas_cost_native=gas_cost)

    def simulate_bridge(
        self,
        *,
        source_chain: str,
        source_chain_id: Optional[int],
        dest_chain: str,
        token_address: str,
        dest_token_address: str,
        amount: Decimal,
        protocol: str = "across",
        native_address: str = NATIVE_ADDRESS,
        arrival_amount: Optional[Decimal] = None,
        gas_cost_native_override: Optional[Decimal] = None,
    ) -> StepResult:
        """
        Simulate a bridge step.

        Checks performed
        ----------------
        1. Sufficient native token on the source chain for gas.
        2. Sufficient *token_address* balance on source chain for *amount*.

        Side effects on success
        -----------------------
        * Deducts *amount* from the source-chain token balance.
        * Credits the estimated arrival amount to the destination-chain
          token balance so subsequent steps on the destination chain can
          see the funds.
        """
        # Gas check + deduct
        ok, gas_cost, reason = self._perform_gas_check_and_deduct(
            tool="bridge",
            chain=source_chain,
            chain_id=source_chain_id,
            native_address=native_address,
            gas_cost_native_override=gas_cost_native_override,
        )
        if not ok:
            return StepResult(
                success=False, rejection_reason=reason or "", gas_cost_native=gas_cost
            )

        # Source token check + deduct
        ok, reason = self._perform_token_check_and_deduct(
            chain=source_chain,
            token_address=token_address,
            amount=amount,
            token_label=None,
        )
        if not ok:
            return StepResult(
                success=False, rejection_reason=reason or "", gas_cost_native=gas_cost
            )

        # Credit destination balance using conservative arrival estimate
        arrival = arrival_amount
        if arrival is None:
            arrival = self._estimate_bridge_arrival(
                amount,
                protocol,
                src_chain=source_chain,
                dst_chain=dest_chain,
                token=None,  # token symbol not easily available in bridge tool args here
            )
        self._add(dest_chain, dest_token_address, arrival)

        return StepResult(success=True, gas_cost_native=gas_cost)

    def simulate_transfer(
        self,
        *,
        network: str,
        chain_id: Optional[int],
        asset_ref: str,
        amount: Decimal,
        native_asset_ref: str = NATIVE_ADDRESS,
        gas_profile: str = "transfer",
        gas_cost_native_override: Optional[Decimal] = None,
    ) -> StepResult:
        """
        Simulate a transfer step.

        Checks: sufficient gas + sufficient token balance.  The recipient
        balance is not tracked (VWS only models the sender's wallet).
        """
        # Gas check + deduct
        ok, gas_cost, reason = self._perform_gas_check_and_deduct(
            tool="transfer",
            chain=network,
            chain_id=chain_id,
            native_address=native_asset_ref,
            gas_profile=gas_profile,
            gas_cost_native_override=gas_cost_native_override,
        )
        if not ok:
            return StepResult(
                success=False, rejection_reason=reason or "", gas_cost_native=gas_cost
            )

        # Token deduction
        ok, reason = self._perform_token_check_and_deduct(
            chain=network,
            token_address=asset_ref,
            amount=amount,
            token_label=None,
        )
        if not ok:
            return StepResult(
                success=False, rejection_reason=reason or "", gas_cost_native=gas_cost
            )

        return StepResult(success=True, gas_cost_native=gas_cost)

    def simulate_unwrap(
        self,
        *,
        chain: str,
        chain_id: Optional[int],
        wrapped_token_address: str,
        amount_wrapped: Decimal,
        native_address: str = NATIVE_ADDRESS,
        gas_cost_native_override: Optional[Decimal] = None,
    ) -> StepResult:
        """
        Simulate unwrapping wrapped native token (e.g. WETH -> ETH).

        Checks: sufficient native gas + sufficient wrapped token balance.
        Side effects on success: deduct wrapped token amount and credit the
        same amount as native balance on the same chain.
        """
        ok, gas_cost, reason = self._perform_gas_check_and_deduct(
            tool="unwrap",
            chain=chain,
            chain_id=chain_id,
            native_address=native_address,
            gas_cost_native_override=gas_cost_native_override,
        )
        if not ok:
            return StepResult(
                success=False,
                rejection_reason=reason or "",
                gas_cost_native=gas_cost,
            )

        ok, reason = self._perform_token_check_and_deduct(
            chain=chain,
            token_address=wrapped_token_address,
            amount=amount_wrapped,
            token_label="wrapped token",
        )
        if not ok:
            return StepResult(
                success=False,
                rejection_reason=reason or "",
                gas_cost_native=gas_cost,
            )

        self._add(chain, native_address, amount_wrapped)
        return StepResult(success=True, gas_cost_native=gas_cost)

    # ------------------------------------------------------------------
    # Debug helpers
    # ------------------------------------------------------------------

    def snapshot(self) -> Dict[Tuple[str, str], Decimal]:
        """Return a read-only copy of the current balance map."""
        return dict(self._balances)

    def __repr__(self) -> str:
        entries = ", ".join(
            f"{chain}/{addr[:8]}…={bal:.4f}"
            for (chain, addr), bal in self._balances.items()
            if bal > 0
        )
        return f"VirtualWalletState({entries or 'empty'})"
