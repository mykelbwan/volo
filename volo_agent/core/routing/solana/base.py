from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional

from core.routing.models import SolanaSwapRouteQuote

_LOGGER = logging.getLogger("volo.routing.solana")


class SolanaSwapAggregator(ABC):
    name: str = "unknown"
    TIMEOUT_SECONDS: float = 60.0

    @abstractmethod
    async def get_quote(
        self,
        *,
        network: str,
        rpc_url: str,
        input_mint: str,
        output_mint: str,
        amount_in: Decimal,
        input_decimals: int,
        output_decimals: int,
        slippage_pct: float,
        sender: str,
    ) -> Optional[SolanaSwapRouteQuote]:
        raise NotImplementedError

    def _log_failure(self, reason: str, exc: Optional[Exception] = None) -> None:
        if exc is not None:
            _LOGGER.warning(
                "[solana:%s] %s — %s: %s",
                self.name,
                reason,
                type(exc).__name__,
                exc,
            )
        else:
            _LOGGER.warning("[solana:%s] %s", self.name, reason)

    def _log_debug(self, msg: str) -> None:
        _LOGGER.debug("[solana:%s] %s", self.name, msg)
