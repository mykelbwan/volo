import asyncio
from unittest.mock import AsyncMock

from core.observer.trigger_matcher import MatchResult, TriggerMatcher


class _DummyCache:
    def __init__(self, prices=None, stale=False, age=0.0):
        self._prices = prices or {}
        self._stale = stale
        self._age = age

    def get_sync(self, asset: str):
        return self._prices.get(asset)

    def is_stale(self, asset: str, max_age_seconds: float):
        return self._stale

    def age_seconds(self, asset: str):
        return self._age


def test_evaluate_matches_price_trigger():
    registry = AsyncMock()
    registry.expire_old_triggers.return_value = 0
    registry.get_pending_price_triggers.return_value = [
        {
            "trigger_id": "t-1",
            "thread_id": "thread-1",
            "user_id": "user-1",
            "trigger_condition": {
                "type": "price_below",
                "asset": "ETH",
                "target": 1000.0,
            },
        }
    ]

    cache = _DummyCache(prices={"ETH": 900.0})
    matcher = TriggerMatcher(registry=registry, cache=cache)

    report = asyncio.run(matcher.evaluate({"ETH": 900.0}))

    assert len(report.matches) == 1
    match = report.matches[0]
    assert isinstance(match, MatchResult)
    assert match.trigger_id == "t-1"
    assert match.resume_payload["matched_price"] == 900.0
    assert match.resume_payload["trigger_type"] == "price_below"
    assert match.resume_payload["condition_met"] is True


def test_evaluate_skips_stale_price():
    registry = AsyncMock()
    registry.expire_old_triggers.return_value = 0
    registry.get_pending_price_triggers.return_value = [
        {
            "trigger_id": "t-2",
            "thread_id": "thread-2",
            "user_id": "user-2",
            "trigger_condition": {
                "type": "price_above",
                "asset": "ETH",
                "target": 2000.0,
            },
        }
    ]

    cache = _DummyCache(prices={"ETH": 2100.0}, stale=True, age=999.0)
    matcher = TriggerMatcher(registry=registry, cache=cache)

    report = asyncio.run(matcher.evaluate())

    assert len(report.matches) == 0
    assert len(report.skipped) == 1
    assert report.skipped[0].reason == "stale_price"


def test_evaluate_time_triggers_builds_resume_payload():
    registry = AsyncMock()
    registry.get_pending_time_triggers.return_value = [
        {
            "trigger_id": "t-3",
            "thread_id": "thread-3",
            "user_id": "user-3",
            "trigger_condition": {"type": "time_at", "execute_at": "2030-01-01T00:00:00Z"},
        }
    ]

    cache = _DummyCache()
    matcher = TriggerMatcher(registry=registry, cache=cache)

    report = asyncio.run(matcher.evaluate_time_triggers())

    assert len(report.matches) == 1
    assert report.matches[0].resume_payload["trigger_type"] == "time_at"


def test_time_trigger_with_schedule_sets_next_execute_at():
    registry = AsyncMock()
    registry.get_pending_time_triggers.return_value = [
        {
            "trigger_id": "t-4",
            "thread_id": "thread-4",
            "user_id": "user-4",
            "trigger_condition": {
                "type": "time_at",
                "execute_at": "2030-01-01T00:00:00Z",
                "schedule": {"every": 1, "unit": "hour"},
            },
        }
    ]

    cache = _DummyCache()
    matcher = TriggerMatcher(registry=registry, cache=cache)

    report = asyncio.run(matcher.evaluate_time_triggers())

    assert len(report.matches) == 1
    match = report.matches[0]
    assert match.next_execute_at == "2030-01-01T01:00:00+00:00"


def test_evaluate_matches_chain_address_price_key():
    registry = AsyncMock()
    registry.expire_old_triggers.return_value = 0
    registry.get_pending_price_triggers.return_value = [
        {
            "trigger_id": "t-addr",
            "thread_id": "thread-addr",
            "user_id": "user-addr",
            "trigger_condition": {
                "type": "price_above",
                "asset": "PEPE",
                "chain": "ethereum",
                "token_address": "0xABCDEFabcdef0000000000000000000000000000",
                "target": 0.000001,
            },
        }
    ]

    cache = _DummyCache(prices={"ethereum:0xabcdefabcdef0000000000000000000000000000": 0.000002})
    matcher = TriggerMatcher(registry=registry, cache=cache)

    report = asyncio.run(
        matcher.evaluate(
            {"ethereum:0xabcdefabcdef0000000000000000000000000000": 0.000002}
        )
    )

    assert len(report.matches) == 1
    match = report.matches[0]
    assert match.trigger_id == "t-addr"
    assert match.resume_payload["matched_price"] == 0.000002
    assert match.resume_payload["asset"] == "PEPE"
