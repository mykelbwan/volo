from __future__ import annotations

import asyncio
from decimal import Decimal

from core.volume import tracker


def test_track_execution_volume_for_bridge_uses_source_chain_and_amount(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_track_volume(*, exec_type, chain, token_symbol, normalized_amount, dt=None):  # noqa: ANN001
        captured.update(
            {
                "exec_type": exec_type,
                "chain": chain,
                "token_symbol": token_symbol,
                "normalized_amount": normalized_amount,
                "dt": dt,
            }
        )

    monkeypatch.setattr(tracker, "track_volume", _fake_track_volume)

    tracker.track_execution_volume(
        "bridge",
        {"source_chain": "base", "token_symbol": "USDC", "amount": "1.25"},
        {"input_amount": "2.5"},
    )

    assert captured["exec_type"] == "bridge"
    assert captured["chain"] == "base"
    assert captured["token_symbol"] == "USDC"
    assert captured["normalized_amount"] == Decimal("2.5")


def test_track_volume_no_running_loop_discards_without_creating_pending_task():
    tracker.track_volume(
        exec_type="swap",
        chain="ethereum",
        token_symbol="ETH",
        normalized_amount=Decimal("1"),
    )


def test_track_volume_schedules_background_increment(monkeypatch):
    created = {}

    class _Loop:
        def create_task(self, coro, name=None):  # noqa: ANN001
            created["coro"] = coro
            created["name"] = name
            coro.close()
            return None

    monkeypatch.setattr(tracker.asyncio, "get_running_loop", lambda: _Loop())

    tracker.track_volume(
        exec_type="swap",
        chain="ethereum",
        token_symbol="ETH",
        normalized_amount=Decimal("3"),
    )

    assert created["name"] == "vol_incr:swap:ethereum:ETH"
    assert asyncio.iscoroutine(created["coro"])
