from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Dict, List

from config import env as env_config
from core.database.mongodb import MongoDB
from core.database.mongodb_async import AsyncMongoDB
from core.utils.cache_store import ping as cache_ping
from core.utils.upstash_client import upstash_configured

logger = logging.getLogger("volo.health")


@dataclass(frozen=True)
class HealthCheckResult:
    ok: bool
    checks: Dict[str, str]


class HealthCheckError(RuntimeError):
    pass


def _skip_mongodb_check() -> bool:
    value = os.getenv("SKIP_MONGODB_HEALTHCHECK", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _skip_upstash_check() -> bool:
    if not upstash_configured():
        return True
    value = os.getenv("SKIP_REDIS_HEALTHCHECK", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _check_cdp_credentials() -> str:
    missing = []
    if not env_config.CDP_API_KEY_ID:
        missing.append("CDP_API_KEY_ID")
    if not env_config.CDP_API_KEY_SECRET:
        missing.append("CDP_API_KEY_SECRET")
    if not env_config.CDP_WALLET_SECRET:
        missing.append("CDP_WALLET_SECRET")
    if missing:
        raise HealthCheckError("CDP credentials are missing: " + ", ".join(missing))
    return "ok"


def _check_mongodb_sync() -> str:
    if not MongoDB.ping():
        raise HealthCheckError("MongoDB ping failed.")
    return "ok"


async def _check_mongodb_async() -> str:
    if not await AsyncMongoDB.ping():
        raise HealthCheckError("MongoDB ping failed.")
    return "ok"


def _check_upstash() -> str:
    if not cache_ping():
        raise HealthCheckError("Upstash ping failed.")
    return "ok"


def run_startup_checks(raise_on_failure: bool = True) -> HealthCheckResult:
    checks: Dict[str, str] = {}
    errors: List[str] = []

    checks_to_run = [
        ("cdp_credentials", _check_cdp_credentials),
    ]
    if _skip_mongodb_check():
        checks["mongodb"] = "skipped"
    else:
        checks_to_run.append(("mongodb", _check_mongodb_sync))
    if _skip_upstash_check():
        checks["upstash"] = "skipped"
    else:
        checks_to_run.append(("upstash", _check_upstash))

    for name, fn in checks_to_run:
        try:
            checks[name] = fn()
        except Exception as exc:
            checks[name] = f"error: {exc}"
            errors.append(f"{name}: {exc}")

    result = HealthCheckResult(ok=not errors, checks=checks)
    if result.ok:
        logger.info("Startup health checks passed.")
    else:
        logger.warning("Startup health checks failed: %s", "; ".join(errors))
        if raise_on_failure:
            raise HealthCheckError("Startup health checks failed: " + "; ".join(errors))

    return result


async def run_startup_checks_async(
    raise_on_failure: bool = True,
) -> HealthCheckResult:
    checks: Dict[str, str] = {}
    errors: List[str] = []

    for name, fn in (("cdp_credentials", _check_cdp_credentials),):
        try:
            checks[name] = fn()
        except Exception as exc:
            checks[name] = f"error: {exc}"
            errors.append(f"{name}: {exc}")

    if _skip_mongodb_check():
        checks["mongodb"] = "skipped"
    else:
        try:
            checks["mongodb"] = await _check_mongodb_async()
        except Exception as exc:
            checks["mongodb"] = f"error: {exc}"
            errors.append(f"mongodb: {exc}")

    if _skip_upstash_check():
        checks["upstash"] = "skipped"
    else:
        try:
            checks["upstash"] = _check_upstash()
        except Exception as exc:
            checks["upstash"] = f"error: {exc}"
            errors.append(f"upstash: {exc}")

    result = HealthCheckResult(ok=not errors, checks=checks)
    if result.ok:
        logger.info("Startup health checks passed.")
    else:
        logger.warning("Startup health checks failed: %s", "; ".join(errors))
        if raise_on_failure:
            raise HealthCheckError("Startup health checks failed: " + "; ".join(errors))

    return result
