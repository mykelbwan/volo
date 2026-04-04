from __future__ import annotations

import os
import threading

import pytest

from wallet_service.common.cdp_helpers import get_cdp_client_config


def _clear_cdp_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "CDP_API_KEY_ID",
        "CDP_API_KEY_SECRET",
        "CDP_WALLET_SECRET",
        "COINBASE_API_KEY_ID",
        "COINBASE_SECRET_KEY",
        "COINBASE_SERVER_WALLET",
        "DISABLE_CDP_USAGE_TRACKING",
        "DISABLE_CDP_ERROR_REPORTING",
    ):
        monkeypatch.delenv(key, raising=False)


def test_get_cdp_client_config_concurrent_threads_converges_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catch concurrent config races by hammering the loader from many threads
    # and asserting every caller sees the same immutable credential snapshot.
    _clear_cdp_env(monkeypatch)
    monkeypatch.setenv("COINBASE_API_KEY_ID", "coinbase-key")
    monkeypatch.setenv("COINBASE_SECRET_KEY", "coinbase-secret")
    monkeypatch.setenv("COINBASE_SERVER_WALLET", "coinbase-wallet")

    barrier = threading.Barrier(40)
    errors: list[BaseException] = []
    snapshots: list[tuple[str, str, str | None, bool, bool]] = []

    def _worker() -> None:
        try:
            barrier.wait(timeout=3.0)
            for _ in range(50):
                config = get_cdp_client_config()
            snapshots.append(
                (
                    config.api_key_id,
                    config.api_key_secret,
                    config.wallet_secret,
                    config.disable_usage_tracking,
                    config.disable_error_reporting,
                )
            )
        except BaseException as exc:  # pragma: no cover - failure path only
            errors.append(exc)

    threads = [threading.Thread(target=_worker, daemon=True) for _ in range(40)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert snapshots
    assert set(snapshots) == {
        (
            "coinbase-key",
            "coinbase-secret",
            "coinbase-wallet",
            True,
            True,
        )
    }
    assert os.environ.get("CDP_API_KEY_ID") is None
    assert os.environ.get("CDP_API_KEY_SECRET") is None
    assert os.environ.get("CDP_WALLET_SECRET") is None


def test_get_cdp_client_config_prefers_explicit_cdp_values_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Explicit CDP values must win over Coinbase-compatible fallbacks without
    # rewriting global env in place.
    _clear_cdp_env(monkeypatch)
    monkeypatch.setenv("CDP_API_KEY_ID", "already-set-id")
    monkeypatch.setenv("CDP_API_KEY_SECRET", "already-set-secret")
    monkeypatch.setenv("CDP_WALLET_SECRET", "already-set-wallet")
    monkeypatch.setenv("COINBASE_API_KEY_ID", "wrong-id")
    monkeypatch.setenv("COINBASE_SECRET_KEY", "wrong-secret")
    monkeypatch.setenv("COINBASE_SERVER_WALLET", "wrong-wallet")

    barrier = threading.Barrier(24)
    errors: list[BaseException] = []

    def _worker() -> None:
        try:
            barrier.wait(timeout=3.0)
            config = get_cdp_client_config()
            assert config.api_key_id == "already-set-id"
            assert config.api_key_secret == "already-set-secret"
            assert config.wallet_secret == "already-set-wallet"
        except BaseException as exc:  # pragma: no cover - failure path only
            errors.append(exc)

    threads = [threading.Thread(target=_worker, daemon=True) for _ in range(24)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert os.environ["CDP_API_KEY_ID"] == "already-set-id"
    assert os.environ["CDP_API_KEY_SECRET"] == "already-set-secret"
    assert os.environ["CDP_WALLET_SECRET"] == "already-set-wallet"
    assert os.environ.get("DISABLE_CDP_USAGE_TRACKING") is None
    assert os.environ.get("DISABLE_CDP_ERROR_REPORTING") is None
