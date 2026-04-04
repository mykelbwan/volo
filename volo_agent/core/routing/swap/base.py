from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional

from core.routing.models import SwapRouteQuote

_LOGGER = logging.getLogger("volo.routing.swap")


class SwapAggregator(ABC):
    #: Machine-readable identifier.  Must be unique across all adapters and
    #: match the key used in ``PerformanceLedger`` (e.g. ``"1inch"``).
    name: str = "unknown"

    #: Per-request timeout in seconds.  The router enforces this via
    #: ``asyncio.wait_for`` — adapters do not need their own timer.
    TIMEOUT_SECONDS: float = 5.0

    @abstractmethod
    async def get_quote(
        self,
        *,
        chain_id: int,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_pct: float,
        sender: str,
    ) -> Optional[SwapRouteQuote]:
        ...

    def _log_failure(self, reason: str, exc: Optional[Exception] = None) -> None:
        if exc is not None:
            _LOGGER.warning(
                "[swap:%s] %s — %s: %s", self.name, reason, type(exc).__name__, exc
            )
        else:
            _LOGGER.warning("[swap:%s] %s", self.name, reason)

    def _log_debug(self, msg: str) -> None:
        _LOGGER.debug("[swap:%s] %s", self.name, msg)
