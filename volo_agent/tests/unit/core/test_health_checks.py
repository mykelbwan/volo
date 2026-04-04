import asyncio

import pytest

import core.health as health


def test_run_startup_checks_success(monkeypatch):
    monkeypatch.delenv("SKIP_MONGODB_HEALTHCHECK", raising=False)
    monkeypatch.setenv("SKIP_REDIS_HEALTHCHECK", "1")
    monkeypatch.setattr(health.env_config, "CDP_API_KEY_ID", "id")
    monkeypatch.setattr(health.env_config, "CDP_API_KEY_SECRET", "secret")
    monkeypatch.setattr(health.env_config, "CDP_WALLET_SECRET", "wallet")
    monkeypatch.setattr(health.MongoDB, "ping", lambda: True)

    result = health.run_startup_checks()

    assert result.ok is True
    assert result.checks["cdp_credentials"] == "ok"
    assert result.checks["mongodb"] == "ok"


def test_run_startup_checks_async_success(monkeypatch):
    monkeypatch.delenv("SKIP_MONGODB_HEALTHCHECK", raising=False)
    monkeypatch.setenv("SKIP_REDIS_HEALTHCHECK", "1")
    monkeypatch.setattr(health.env_config, "CDP_API_KEY_ID", "id")
    monkeypatch.setattr(health.env_config, "CDP_API_KEY_SECRET", "secret")
    monkeypatch.setattr(health.env_config, "CDP_WALLET_SECRET", "wallet")
    monkeypatch.setattr(health.AsyncMongoDB, "ping", lambda: asyncio.sleep(0, result=True))

    result = asyncio.run(health.run_startup_checks_async())

    assert result.ok is True


def test_run_startup_checks_skip_mongodb(monkeypatch):
    monkeypatch.setenv("SKIP_MONGODB_HEALTHCHECK", "1")
    monkeypatch.setenv("SKIP_REDIS_HEALTHCHECK", "1")
    monkeypatch.setattr(health.env_config, "CDP_API_KEY_ID", "id")
    monkeypatch.setattr(health.env_config, "CDP_API_KEY_SECRET", "secret")
    monkeypatch.setattr(health.env_config, "CDP_WALLET_SECRET", "wallet")
    monkeypatch.setattr(health.MongoDB, "ping", lambda: False)

    result = health.run_startup_checks()

    assert result.ok is True
    assert result.checks["mongodb"] == "skipped"
