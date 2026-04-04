from datetime import datetime, timedelta

from core.utils.circuit_breaker import CircuitBreaker


class _Ledger:
    def __init__(self, data):
        self.data = data


def test_circuit_breaker_disables_recent_failures():
    ledger = _Ledger(
        {
            "swap:ethereum": {
                "consecutive_failures": 3,
                "last_run": datetime.now().isoformat(),
            }
        }
    )

    breaker = CircuitBreaker(ledger)
    disabled = breaker.get_disabled_tools()
    assert "swap:ethereum" in disabled


def test_circuit_breaker_respects_cooldown():
    ledger = _Ledger(
        {
            "swap:ethereum": {
                "consecutive_failures": 3,
                "last_run": (datetime.now() - timedelta(minutes=60)).isoformat(),
            }
        }
    )

    breaker = CircuitBreaker(ledger)
    disabled = breaker.get_disabled_tools()
    assert "swap:ethereum" not in disabled
