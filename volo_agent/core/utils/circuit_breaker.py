from datetime import datetime, timedelta
from typing import List, Optional

from core.memory.ledger import PerformanceLedger, get_ledger


class CircuitBreaker:
    """
    Deterministic service to identify tools that are currently unreliable.
    Uses PerformanceLedger data to identify 'open' circuits.
    """

    # Thresholds for disabling a tool
    CONSECUTIVE_FAILURE_LIMIT = 3
    COOLDOWN_MINUTES = 10

    def __init__(self, ledger: Optional[PerformanceLedger] = None):
        self.ledger = ledger or get_ledger()

    def get_disabled_tools(self) -> List[str]:
        """
        Returns a list of 'tool:chain' keys that are currently disabled.
        A tool is disabled if it has exceeded consecutive failures AND
        is within the cooldown period.
        """
        disabled = []
        now = datetime.now()

        for key, stats in self.ledger.data.items():
            consecutive = stats.get("consecutive_failures", 0)
            if consecutive >= self.CONSECUTIVE_FAILURE_LIMIT:
                last_run_str = stats.get("last_run", "")
                if not last_run_str:
                    continue

                try:
                    last_run = datetime.fromisoformat(last_run_str)
                    if now - last_run < timedelta(minutes=self.COOLDOWN_MINUTES):
                        disabled.append(key)
                except ValueError:
                    continue

        return disabled

    def is_tool_disabled(self, tool: str, chain: str) -> bool:
        """Helper to check a specific tool:chain pair."""
        disabled_list = self.get_disabled_tools()
        return f"{tool}:{chain.lower()}" in disabled_list
