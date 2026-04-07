from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Optional

from requests import HTTPError, RequestException

from config.chains import CHAINS
from config.solana_chains import SOLANA_CHAINS
from core.token_security.models import (
    CRITICAL_TAX_THRESHOLD,
    HIGH_TAX_THRESHOLD,
    LIQUIDITY_THRESHOLD_GOPLUS,
    SecurityFlag,
    SecurityTier,
)
from core.utils.http import request_json

logger = logging.getLogger(__name__)
_BASE_URL = "https://api.gopluslabs.io"
_TOKEN_SECURITY_ENDPOINT = "/api/v1/token_security/{chain_id}"

_DEFAULT_TIMEOUT_SECONDS: float = 12.0

# Maximum number of addresses to include in a single API call.
# GoPlus supports up to 100; we cap at 10 to stay well within any limits.
_MAX_BATCH_SIZE: int = 10
_API_KEY: str = os.getenv("GOPLUS_API_KEY", "")

def _build_goplus_supported_chain_ids() -> frozenset[int]:
    chain_ids = {
        chain.chain_id for chain in CHAINS.values() if not chain.is_testnet
    }
    chain_ids.update(
        chain.chain_id for chain in SOLANA_CHAINS.values() if not chain.is_testnet
    )
    return frozenset(chain_ids)


# Unsupported/testnet chains automatically fall back to simulation.
GOPLUS_SUPPORTED_CHAIN_IDS: frozenset[int] = _build_goplus_supported_chain_ids()


@dataclass
class GoplusTokenReport:
    address: str
    chain_id: int
    token_name: Optional[str] = None
    token_symbol: Optional[str] = None
    decimals: int = 18
    total_supply: Optional[str] = None
    holder_count: Optional[int] = None
    raw_buy_tax: float = 0.0  # fraction: 0.05 = 5 %
    raw_sell_tax: float = 0.0
    is_honeypot: bool = False
    cannot_buy: bool = False
    cannot_sell_all: bool = False
    transfer_pausable: bool = False
    is_blacklisted: bool = False
    honeypot_with_same_creator: bool = False
    is_mintable: bool = False
    is_proxy: bool = False
    owner_change_balance: bool = False
    is_anti_whale: bool = False
    flags: list[SecurityFlag] = field(default_factory=list)
    security_tier: SecurityTier = SecurityTier.GOPLUS_VERIFIED

    @property
    def buy_tax_pct(self) -> float:
        return self.raw_buy_tax * 100.0

    @property
    def sell_tax_pct(self) -> float:
        return self.raw_sell_tax * 100.0

    @property
    def is_safe(self) -> bool:
        return self.security_tier != SecurityTier.UNSAFE

    def short_summary(self) -> str:
        parts = [f"{self.token_symbol or self.address[:10]}@{self.chain_id}"]
        parts.append("SAFE" if self.is_safe else "UNSAFE")
        if self.flags:
            parts.append(f"flags=[{', '.join(f.value for f in self.flags)}]")
        if self.buy_tax_pct > 0 or self.sell_tax_pct > 0:
            parts.append(f"tax=buy{self.buy_tax_pct:.1f}%/sell{self.sell_tax_pct:.1f}%")
        return " ".join(parts)


class GoplusError(Exception):
    """Base class for GoPlus client errors."""


class GoplusRateLimitError(GoplusError):
    """Raised after all retries are exhausted due to rate limiting."""


class GoplusUnavailableError(GoplusError):
    """Raised after all retries are exhausted due to server errors."""


class GoplusChainNotSupportedError(GoplusError):
    """Raised when a chain_id is not in ``GOPLUS_SUPPORTED_CHAIN_IDS``."""


def _get_with_retry(
    url: str,
    params: dict,
    headers: dict,
    timeout: float,
) -> dict:
    try:
        resp = request_json(
            "GET",
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            service="goplus",
        )
    except RequestException as exc:
        raise GoplusError(f"GoPlus request failed due to network error: {exc}") from exc

    if resp.status_code == 200:
        try:
            return resp.json()
        except ValueError as exc:
            raise GoplusError(f"GoPlus returned non-JSON response: {resp.text[:200]}") from exc

    # request_json already retries transient 429/5xx statuses.
    if resp.status_code == 429:
        raise GoplusRateLimitError(
            "GoPlus rate limit hit after shared retry attempts."
        )
    if resp.status_code >= 500:
        raise GoplusUnavailableError(
            f"GoPlus unavailable (HTTP {resp.status_code}) after shared retry attempts."
        )

    try:
        resp.raise_for_status()
    except HTTPError as exc:
        raise GoplusError(
            f"GoPlus non-retryable HTTP {resp.status_code}: {resp.text[:200]}"
        ) from exc

    raise GoplusError("GoPlus request failed unexpectedly.")


def _flag_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip() == "1"


def _flag_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return default


def _flag_int(value: object) -> Optional[int]:
    """Safely parse a GoPlus numeric string to int, returning None on error."""
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None


def _derive_flags_and_tier(report: GoplusTokenReport) -> None:
    flags: list[SecurityFlag] = []
    is_critical = False

    if report.is_honeypot or report.honeypot_with_same_creator:
        flags.append(SecurityFlag.HONEYPOT)
        is_critical = True

    if report.cannot_sell_all or report.sell_tax_pct > CRITICAL_TAX_THRESHOLD:
        flags.append(SecurityFlag.CANNOT_SELL)
        is_critical = True

    if report.transfer_pausable:
        flags.append(SecurityFlag.TRANSFER_PAUSED)
        is_critical = True

    if report.cannot_buy:
        flags.append(SecurityFlag.CANNOT_BUY)
        is_critical = True

    if report.buy_tax_pct > HIGH_TAX_THRESHOLD:
        flags.append(SecurityFlag.HIGH_BUY_TAX)

    if report.sell_tax_pct > HIGH_TAX_THRESHOLD:
        flags.append(SecurityFlag.HIGH_SELL_TAX)

    if report.is_mintable:
        flags.append(SecurityFlag.MINTABLE)

    if report.is_proxy:
        flags.append(SecurityFlag.PROXY_CONTRACT)

    if report.owner_change_balance:
        flags.append(SecurityFlag.OWNER_CAN_CHANGE_BALANCE)

    if report.is_anti_whale:
        flags.append(SecurityFlag.ANTI_WHALE)

    if report.is_blacklisted:
        flags.append(SecurityFlag.BLACKLISTED)

    report.flags = flags
    report.security_tier = (
        SecurityTier.UNSAFE if is_critical else SecurityTier.GOPLUS_VERIFIED
    )


def _parse_token_result(
    address: str,
    data: dict,
    chain_id: int,
) -> GoplusTokenReport:
    report = GoplusTokenReport(
        address=address,
        chain_id=chain_id,
        token_name=data.get("token_name"),
        token_symbol=data.get("token_symbol"),
        decimals=_flag_int(data.get("decimals")) or 18,
        total_supply=str(data.get("total_supply", "")) or None,
        holder_count=_flag_int(data.get("holder_count")),
        raw_buy_tax=_flag_float(data.get("buy_tax")),
        raw_sell_tax=_flag_float(data.get("sell_tax")),
        is_honeypot=_flag_bool(data.get("is_honeypot")),
        cannot_buy=_flag_bool(data.get("cannot_buy")),
        cannot_sell_all=_flag_bool(data.get("cannot_sell_all")),
        transfer_pausable=_flag_bool(data.get("transfer_pausable")),
        is_blacklisted=_flag_bool(data.get("is_blacklisted")),
        honeypot_with_same_creator=_flag_bool(data.get("honeypot_with_same_creator")),
        is_mintable=_flag_bool(data.get("is_mintable")),
        is_proxy=_flag_bool(data.get("is_proxy")),
        owner_change_balance=_flag_bool(data.get("owner_change_balance")),
        is_anti_whale=_flag_bool(data.get("is_anti_whale")),
    )
    _derive_flags_and_tier(report)
    return report


class GoplusScanner:
    def __init__(
        self,
        api_key: str = _API_KEY,
        timeout: float = _DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._api_key = api_key
        self._timeout = timeout
        self._headers: dict = {}
        if self._api_key:
            self._headers["Authorization"] = self._api_key

    def scan_batch(
        self,
        addresses: list[str],
        chain_id: int,
        liquidity_map: Optional[dict[str, float]] = None,
    ) -> dict[str, GoplusTokenReport]:
        if chain_id not in GOPLUS_SUPPORTED_CHAIN_IDS:
            raise GoplusChainNotSupportedError(
                f"GoPlus does not support chain_id={chain_id}. "
                f"Supported IDs: {sorted(GOPLUS_SUPPORTED_CHAIN_IDS)}"
            )

        if not addresses:
            return {}

        # Normalise and truncate
        normalised = [
            a.strip().lower() for a in addresses[:_MAX_BATCH_SIZE] if a and a.strip()
        ]
        unique_addresses = list(dict.fromkeys(normalised))  # preserve order, dedup
        if not unique_addresses:
            return {}

        url = f"{_BASE_URL}{_TOKEN_SECURITY_ENDPOINT.format(chain_id=chain_id)}"
        params = {"contract_addresses": ",".join(unique_addresses)}

        logger.info(
            "GoPlus: scanning %d address(es) on chain_id=%d: %s",
            len(unique_addresses),
            chain_id,
            [a[:10] + "…" for a in unique_addresses],
        )

        raw = _get_with_retry(
            url=url,
            params=params,
            headers=self._headers,
            timeout=self._timeout,
        )

        return self._parse_response(
            raw=raw,
            chain_id=chain_id,
            requested_addresses=unique_addresses,
            liquidity_map=liquidity_map or {},
        )

    def scan_single(
        self,
        address: str,
        chain_id: int,
        liquidity_usd: Optional[float] = None,
    ) -> Optional[GoplusTokenReport]:
        normalized = address.strip().lower()
        liq_map = {normalized: liquidity_usd} if liquidity_usd is not None else {}
        results = self.scan_batch([address], chain_id=chain_id, liquidity_map=liq_map)
        return results.get(normalized)

    def is_chain_supported(self, chain_id: int) -> bool:
        """Return ``True`` if ``chain_id`` is in ``GOPLUS_SUPPORTED_CHAIN_IDS``."""
        return chain_id in GOPLUS_SUPPORTED_CHAIN_IDS

    def _parse_response(
        self,
        raw: dict,
        chain_id: int,
        requested_addresses: list[str],
        liquidity_map: dict[str, float],
    ) -> dict[str, GoplusTokenReport]:
        code = raw.get("code")
        message = raw.get("message", "")

        # GoPlus signals API-level errors via ``code != 1``
        if str(code) != "1":
            raise GoplusError(
                f"GoPlus API returned error code={code!r} message={message!r}. "
                f"Chain {chain_id}, addresses: {requested_addresses[:3]}"
            )

        result_data: dict = raw.get("result") or {}

        if not result_data:
            logger.info(
                "GoPlus: API returned empty result for chain_id=%d. "
                "Token(s) may not be indexed yet.",
                chain_id,
            )
            return {}

        reports: dict[str, GoplusTokenReport] = {}

        for addr_lower, token_data in result_data.items():
            if not isinstance(token_data, dict):
                logger.warning(
                    "GoPlus: unexpected non-dict result for address %s — skipping.",
                    addr_lower,
                )
                continue

            try:
                report = _parse_token_result(
                    address=addr_lower,
                    data=token_data,
                    chain_id=chain_id,
                )

                # Apply liquidity flag if we have Dexscreener data
                liquidity = liquidity_map.get(addr_lower)
                if liquidity is not None and liquidity < LIQUIDITY_THRESHOLD_GOPLUS:
                    if SecurityFlag.LOW_LIQUIDITY not in report.flags:
                        report.flags.append(SecurityFlag.LOW_LIQUIDITY)

                reports[addr_lower] = report

                logger.info(
                    "GoPlus: %s",
                    report.short_summary(),
                )

            except Exception as exc:
                logger.warning(
                    "GoPlus: failed to parse result for address %s: %s — skipping.",
                    addr_lower,
                    exc,
                )

        missing = set(requested_addresses) - set(reports.keys())
        if missing:
            logger.debug(
                "GoPlus: %d requested address(es) absent from response "
                "(may be unindexed): %s",
                len(missing),
                [a[:10] + "…" for a in sorted(missing)],
            )

        return reports
