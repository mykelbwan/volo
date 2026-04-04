from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

# Primary normalised-amount bucket: volume:{type}:{chain}:{token}:{YYYYMMDDHH}
_VOL_PREFIX = "volume"

# USD-value sibling:  vol_usd:{type}:{chain}:{token}:{YYYYMMDDHH}
_USD_PREFIX = "vol_usd"

# Execution-count sibling: vol_cnt:{type}:{chain}:{token}:{YYYYMMDDHH}
_CNT_PREFIX = "vol_cnt"

# Keys expire after 3 days.  The background flusher normally writes to MongoDB
# and deletes keys every few minutes, so this TTL is only a safety net against
# orphaned keys when the flusher is down for an extended period.
VOLUME_KEY_TTL_SECONDS: int = 60 * 60 * 24 * 3  # 72 hours


def _sanitize(part: str) -> str:
    return part.strip().lower().replace(" ", "_").replace(":", "_")


def hour_bucket(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.strftime("%Y%m%d%H")


def volume_key(exec_type: str, chain: str, token: str, bucket: str) -> str:
    return (
        f"{_VOL_PREFIX}:{_sanitize(exec_type)}"
        f":{_sanitize(chain)}"
        f":{_sanitize(token)}"
        f":{bucket}"
    )


def volume_count_key(exec_type: str, chain: str, token: str, bucket: str) -> str:
    return (
        f"{_CNT_PREFIX}:{_sanitize(exec_type)}"
        f":{_sanitize(chain)}"
        f":{_sanitize(token)}"
        f":{bucket}"
    )


def derive_sibling_keys(primary_key: str) -> tuple[str, str]:
    expected_prefix = _VOL_PREFIX + ":"
    if not primary_key.startswith(expected_prefix):
        raise ValueError(
            f"Not a primary volume key (expected prefix {expected_prefix!r}): "
            f"{primary_key!r}"
        )
    suffix = primary_key[len(expected_prefix) :]
    return f"{_USD_PREFIX}:{suffix}", f"{_CNT_PREFIX}:{suffix}"


def parse_volume_key(key: str) -> Optional[dict[str, str]]:
    if not key.startswith(_VOL_PREFIX + ":"):
        return None

    parts = key.split(":")
    # Expected structure: ["volume", exec_type, chain, token, hour_bucket]
    if len(parts) != 5:
        return None

    _, exec_type, chain, token, bucket = parts

    # Validate bucket: must be exactly 10 decimal digits (YYYYMMDDHH).
    if len(bucket) != 10 or not bucket.isdigit():
        return None

    return {
        "exec_type": exec_type,
        "chain": chain,
        "token": token,
        "hour_bucket": bucket,
    }


def bucket_to_datetime(bucket: str) -> datetime:
    return datetime.strptime(bucket, "%Y%m%d%H").replace(tzinfo=timezone.utc)


# Pattern passed to Redis SCAN to enumerate only primary volume keys.
# Sibling keys (vol_usd:*, vol_cnt:*) are excluded because the flusher
# derives them from the primary keys via derive_sibling_keys().
VOLUME_SCAN_PATTERN = f"{_VOL_PREFIX}:*"
