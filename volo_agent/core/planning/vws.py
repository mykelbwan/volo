from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from core.planning.fee_table import FeeTable

_LOGGER = logging.getLogger(__name__)
NATIVE_ADDRESS: str = "0x0000000000000000000000000000000000000000"
BRIDGE_FEE_TABLE: Dict[str, Decimal] = {
    "across": Decimal("0.002"),  # 0.1–0.2 % typical → use 0.2 %
    "relay": Decimal("0.003"),  # 0.2–0.4 % typical
    "mayan": Decimal("0.005"),  # 0.3–0.5 % incl. Swift relayer fee
    "lifi": Decimal("0.004"),  # aggregated, varies by underlying bridge
}
ARRIVAL_BUFFER: Decimal = Decimal("0.995")  # 0.5 % extra margin
DEFAULT_BRIDGE_FEE: Decimal = Decimal("0.005")  # 0.5 % conservative

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

FALLBACK_GAS_PRICE_WEI: int = 30 * 10**9  # 30 gwei

@dataclass(frozen=True)
class StepResult:
    success: bool
    rejection_reason: str = ""
    gas_cost_native: Decimal = Decimal("0")


@dataclass
class SimulationResult:
    valid: bool
    rejection_reasons: List[str] = field(default_factory=list)
    total_gas_cost_native: Dict[str, Decimal] = field(default_factory=dict)
    total_estimated_fees: Decimal = Decimal("0")

class VirtualWalletState:
    def __init__(
        self,
        balances: Dict[Tuple[str, str], Decimal],
        fee_table: Optional[FeeTable] = None,
    ) -> None:
        self._balances: Dict[Tuple[str, str], Decimal] = dict(balances)
        self._fee_table: Optional[FeeTable] = fee_table

    @classmethod
    def from_balance_snapshot(
        cls,
        snapshot: Dict[str, str],
        sender: str,
        fee_table: Optional["FeeTable"] = None,
    ) -> "VirtualWalletState":
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
        return VirtualWalletState(dict(self._balances))

    def get_balance(self, chain: str, token_address: str) -> Decimal:
        key = (chain.strip().lower(), token_address.strip().lower())
        return self._balances.get(key, Decimal("0"))

    def _deduct(
        self,
        chain: str,
        token_address: str,
        amount: Decimal,
    ) -> bool:
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
        key = (chain.strip().lower(), token_address.strip().lower())
        self._balances[key] = self._balances.get(key, Decimal("0")) + amount

    @staticmethod
    def _gas_price_wei(chain_id: Optional[int]) -> int:
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

    def _perform_gas_check_and_deduct(
        self,
        tool: str,
        chain: str,
        chain_id: Optional[int],
        native_address: str,
        gas_profile: Optional[str] = None,
        gas_cost_native_override: Optional[Decimal] = None,
    ) -> Tuple[bool, Decimal, Optional[str]]:
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

    def snapshot(self) -> Dict[Tuple[str, str], Decimal]:
        return dict(self._balances)

    def __repr__(self) -> str:
        entries = ", ".join(
            f"{chain}/{addr[:8]}…={bal:.4f}"
            for (chain, addr), bal in self._balances.items()
            if bal > 0
        )
        return f"VirtualWalletState({entries or 'empty'})"
