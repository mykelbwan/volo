from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, cast

from config.solana_chains import SOL_DECIMALS, _KNOWN_DECIMALS, is_solana_chain_id
from web3 import Web3
from web3.exceptions import ContractLogicError
from web3.types import Wei

from config.abi import ERC20_ABI
from core.token_security.models import (
    LIQUIDITY_THRESHOLD_FALLBACK,
    DexscreenerCandidate,
    SecurityFlag,
    SecurityTier,
)
from core.token_security.registry_lookup import get_registry_decimals_by_address

logger = logging.getLogger(__name__)

_KNOWN_SOLANA_DECIMALS_LOWER = {k.lower(): v for k, v in _KNOWN_DECIMALS.items()}
_DEAD_ADDRESS = "0x000000000000000000000000000000000000dEaD"

# Gas limit above which we consider the transfer hook suspiciously complex.
_HONEYPOT_GAS_THRESHOLD = 500_000

# totalSupply values outside this range indicate a broken or malicious token.
_MIN_SUPPLY = 1
_MAX_SUPPLY = 10**30  # raw units (before decimal scaling)


@dataclass
class SimulationReport:
    address: str
    chain_id: int
    rpc_url: str
    symbol: str = ""
    decimals: int = 18
    total_supply_raw: Optional[int] = None
    transfer_gas_estimate: Optional[int] = None
    transfer_reverted: bool = False
    liquidity_usd: float = 0.0
    flags: list[SecurityFlag] = field(default_factory=list)
    security_tier: SecurityTier = SecurityTier.FALLBACK_HEURISTIC

    @property
    def is_safe(self) -> bool:
        """Return ``True`` if the simulation found no critical issues."""
        return self.security_tier != SecurityTier.UNVERIFIED

    def short_summary(self) -> str:
        """Return a one-line summary suitable for logging."""
        parts = [
            f"{self.symbol or self.address[:10]}@chain{self.chain_id}",
            "SAFE" if self.is_safe else "UNVERIFIED",
            f"decimals={self.decimals}",
        ]
        if self.transfer_gas_estimate is not None:
            parts.append(f"gas={self.transfer_gas_estimate:,}")
        if self.transfer_reverted:
            parts.append("REVERTED")
        if self.flags:
            parts.append(f"flags=[{', '.join(f.value for f in self.flags)}]")
        return " ".join(parts)


class SimulationFallback:
    def scan(
        self,
        candidate: DexscreenerCandidate,
        rpc_url: str,
    ) -> SimulationReport:
        address_raw = candidate.address.strip()
        chain_id = candidate.chain_id
        liquidity_usd = candidate.liquidity_usd

        if is_solana_chain_id(chain_id):
            return self._scan_solana_candidate(
                candidate=candidate,
                rpc_url=rpc_url,
            )

        # Normalise address to Web3 checksum format
        try:
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            checksum_address = w3.to_checksum_address(address_raw)
        except Exception as exc:
            logger.warning(
                "SimulationFallback: invalid address %r for chain %d: %s",
                address_raw,
                chain_id,
                exc,
            )
            return SimulationReport(
                address=address_raw,
                chain_id=chain_id,
                rpc_url=rpc_url,
                symbol=candidate.symbol,
                liquidity_usd=liquidity_usd,
                flags=[SecurityFlag.SIMULATION_REVERTED],
                security_tier=SecurityTier.UNVERIFIED,
            )

        report = SimulationReport(
            address=checksum_address,
            chain_id=chain_id,
            rpc_url=rpc_url,
            symbol=candidate.symbol,
            liquidity_usd=liquidity_usd,
        )

        # ERC-20 conformance reads
        conformance_ok = self._check_erc20_conformance(w3, checksum_address, report)
        if not conformance_ok:
            # Contract failed basic ERC-20 reads — not tradeable.
            report.flags.append(SecurityFlag.SIMULATION_REVERTED)
            report.security_tier = SecurityTier.UNVERIFIED
            logger.info("SimulationFallback: %s", report.short_summary())
            return report

        # Transfer gas simulation (honeypot check)
        self._simulate_transfer(w3, checksum_address, report)
        # Supply sanity check
        self._check_supply(report)
        # Liquidity check
        self._check_liquidity(report)
        # Determine final tier
        self._assign_tier(report)

        logger.info("SimulationFallback: %s", report.short_summary())
        return report

    def _scan_solana_candidate(
        self,
        candidate: DexscreenerCandidate,
        rpc_url: str,
    ) -> SimulationReport:
        address_raw = candidate.address.strip()
        chain_id = candidate.chain_id
        liquidity_usd = candidate.liquidity_usd

        if not self._is_valid_solana_address(address_raw):
            logger.warning(
                "SimulationFallback: invalid Solana mint %r for chain %d",
                address_raw,
                chain_id,
            )
            return SimulationReport(
                address=address_raw,
                chain_id=chain_id,
                rpc_url=rpc_url,
                symbol=candidate.symbol,
                liquidity_usd=liquidity_usd,
                flags=[SecurityFlag.SIMULATION_REVERTED],
                security_tier=SecurityTier.UNVERIFIED,
            )

        report = SimulationReport(
            address=address_raw,
            chain_id=chain_id,
            rpc_url=rpc_url,
            symbol=candidate.symbol,
            liquidity_usd=liquidity_usd,
        )
        report.decimals = self._resolve_solana_decimals(address_raw, chain_id)
        self._check_liquidity(report)
        report.security_tier = SecurityTier.FALLBACK_HEURISTIC
        if SecurityFlag.UNVERIFIED_SOURCE not in report.flags:
            report.flags.append(SecurityFlag.UNVERIFIED_SOURCE)
        logger.info("SimulationFallback: %s", report.short_summary())
        return report

    def _resolve_solana_decimals(self, mint: str, chain_id: int) -> int:
        try:
            decimals = get_registry_decimals_by_address(mint, chain_id)
            if decimals is not None:
                return int(decimals)
        except Exception as exc:
            logger.debug(
                "SimulationFallback: sync decimals lookup failed for Solana mint %s: %s",
                mint[:10],
                exc,
            )

        known = _KNOWN_SOLANA_DECIMALS_LOWER.get(mint.lower())
        if known is not None:
            return int(known)

        return SOL_DECIMALS

    def _is_valid_solana_address(self, address: str) -> bool:
        try:
            from solders.pubkey import Pubkey  # noqa: PLC0415

            Pubkey.from_string(address)
            return True
        except Exception:
            return False

    def scan_address(
        self,
        address: str,
        symbol: str,
        chain_id: int,
        rpc_url: str,
        liquidity_usd: float = 0.0,
    ) -> SimulationReport:
        candidate = DexscreenerCandidate(
            symbol=symbol,
            address=address,
            chain_id=chain_id,
            chain_name="",
            liquidity_usd=liquidity_usd,
        )
        return self.scan(candidate, rpc_url=rpc_url)

    def _check_erc20_conformance(
        self,
        w3: Web3,
        address: str,
        report: SimulationReport,
    ) -> bool:
        try:
            checksum_addr = w3.to_checksum_address(address)
            contract = w3.eth.contract(address=checksum_addr, abi=ERC20_ABI)
        except Exception as exc:
            logger.warning(
                "SimulationFallback: could not instantiate contract at %s: %s",
                address,
                exc,
            )
            return False

        # decimals()
        try:
            report.decimals = int(contract.functions.decimals().call())
        except (ContractLogicError, Exception) as exc:
            logger.info(
                "SimulationFallback: decimals() reverted for %s: %s",
                address[:10],
                exc,
            )
            return False

        # symbol() — non-fatal; fall back to the Dexscreener symbol
        try:
            on_chain_symbol: str = contract.functions.symbol().call()
            if on_chain_symbol:
                report.symbol = on_chain_symbol
        except Exception as exc:
            logger.debug(
                "SimulationFallback: symbol() failed for %s (non-fatal): %s",
                address[:10],
                exc,
            )

        # totalSupply()
        try:
            report.total_supply_raw = int(contract.functions.totalSupply().call())
        except (ContractLogicError, Exception) as exc:
            logger.info(
                "SimulationFallback: totalSupply() reverted for %s: %s",
                address[:10],
                exc,
            )
            return False

        return True

    def _simulate_transfer(
        self,
        w3: Web3,
        address: str,
        report: SimulationReport,
    ) -> None:
        try:
            checksum_addr = w3.to_checksum_address(address)
            contract = w3.eth.contract(address=checksum_addr, abi=ERC20_ABI)
            dead_checksum = w3.to_checksum_address(_DEAD_ADDRESS)

            # Build the transaction dict for estimate_gas.
            # Cast to TxParams: web3.py's build_transaction accepts a plain
            # dict at runtime but the stub expects TxParams, so we cast to
            from web3.types import TxParams  # noqa: PLC0415

            tx = contract.functions.transfer(dead_checksum, 1).build_transaction(
                cast(
                    TxParams,
                    {
                        "from": dead_checksum,
                        "gas": 1_000_000,  # upper cap for the estimate call
                        "maxFeePerGas": Wei(0),
                        "maxPriorityFeePerGas": Wei(0),
                        "type": "0x2",
                    },
                )
            )

            gas = w3.eth.estimate_gas(tx)
            report.transfer_gas_estimate = gas

            logger.debug(
                "SimulationFallback: transfer gas estimate for %s = %d",
                address[:10],
                gas,
            )

        except ContractLogicError as exc:
            report.transfer_reverted = True
            logger.info(
                "SimulationFallback: transfer() simulation REVERTED for %s: %s",
                address[:10],
                exc,
            )
        except Exception as exc:
            # Other RPC-level errors (timeout, connection refused, etc.) —
            # treat as a revert to be conservative.
            report.transfer_reverted = True
            logger.warning(
                "SimulationFallback: eth_estimateGas failed for %s: %s",
                address[:10],
                exc,
            )

    def _check_supply(self, report: SimulationReport) -> None:
        supply = report.total_supply_raw
        if supply is None:
            return  # already handled by conformance check

        if supply < _MIN_SUPPLY:
            logger.info(
                "SimulationFallback: zero totalSupply for %s — flagging.",
                report.address[:10],
            )
            if SecurityFlag.SIMULATION_REVERTED not in report.flags:
                report.flags.append(SecurityFlag.SIMULATION_REVERTED)

        elif supply > _MAX_SUPPLY:
            logger.info(
                "SimulationFallback: absurdly large totalSupply (%d) for %s.",
                supply,
                report.address[:10],
            )
            # Not necessarily critical — some tokens have large supplies by
            # design (e.g. SHIB).  We flag it but don't mark as UNVERIFIED
            # unless combined with other signals.
            if SecurityFlag.SIMULATION_REVERTED not in report.flags:
                report.flags.append(SecurityFlag.UNVERIFIED_SOURCE)

    def _check_liquidity(self, report: SimulationReport) -> None:
        if report.liquidity_usd < LIQUIDITY_THRESHOLD_FALLBACK:
            if SecurityFlag.LOW_LIQUIDITY not in report.flags:
                report.flags.append(SecurityFlag.LOW_LIQUIDITY)

    def _assign_tier(self, report: SimulationReport) -> None:
        # Populate flags from raw measurement values before deciding tier
        if report.transfer_reverted:
            if SecurityFlag.SIMULATION_REVERTED not in report.flags:
                report.flags.append(SecurityFlag.SIMULATION_REVERTED)

        if (
            report.transfer_gas_estimate is not None
            and report.transfer_gas_estimate > _HONEYPOT_GAS_THRESHOLD
        ):
            if SecurityFlag.SIMULATION_GAS_HIGH not in report.flags:
                report.flags.append(SecurityFlag.SIMULATION_GAS_HIGH)

        # Mark as UNVERIFIED if any critical simulation signal is present.
        # SIMULATION_REVERTED is critical (transfer is blocked).
        # SIMULATION_GAS_HIGH alone is not critical — it's informational.
        has_critical = SecurityFlag.SIMULATION_REVERTED in report.flags

        if has_critical:
            report.security_tier = SecurityTier.UNVERIFIED
        else:
            report.security_tier = SecurityTier.FALLBACK_HEURISTIC
            # Always add UNVERIFIED_SOURCE to inform the LLM that this token
            # was NOT verified by GoPlus, even if all heuristics passed.
            if SecurityFlag.UNVERIFIED_SOURCE not in report.flags:
                report.flags.append(SecurityFlag.UNVERIFIED_SOURCE)
