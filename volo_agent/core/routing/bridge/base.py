from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional

from core.routing.models import BridgeRouteQuote

_LOGGER = logging.getLogger("volo.routing.bridge")


class BridgeAggregator(ABC):
    name: str = "unknown"
    TIMEOUT_SECONDS: float = 60.0

    @abstractmethod
    async def get_quote(
        self,
        *,
        token_symbol: str,
        source_chain_id: int,
        dest_chain_id: int,
        source_chain_name: str,
        dest_chain_name: str,
        amount: Decimal,
        sender: str,
        recipient: str,
    ) -> Optional[BridgeRouteQuote]:
        raise NotImplementedError

    def _log_failure(self, reason: str, exc: Optional[Exception] = None) -> None:
        if exc is not None:
            _LOGGER.warning(
                "[bridge:%s] %s — %s: %s",
                self.name,
                reason,
                type(exc).__name__,
                exc,
            )
        else:
            _LOGGER.warning("[bridge:%s] %s", self.name, reason)

    def _log_debug(self, msg: str) -> None:
        _LOGGER.debug("[bridge:%s] %s", self.name, msg)
