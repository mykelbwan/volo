from datetime import datetime, timedelta, timezone

from core.idempotency.store import IdempotencyRecord, IdempotencyStore


def test_idempotency_record_fields():
    record = IdempotencyRecord(
        key="k",
        status="pending",
        created_at=datetime.now(tz=timezone.utc),
        expires_at=datetime.now(tz=timezone.utc),
        tx_hash="0xabc",
    )
    assert record.tx_hash == "0xabc"


def test_record_from_doc_recovers_required_fields():
    now = datetime.now(tz=timezone.utc)
    fallback_expires = now + timedelta(minutes=5)
    record = IdempotencyStore._record_from_doc(
        {
            "key": None,
            "status": None,
            "created_at": None,
            "expires_at": None,
            "result": "not-a-dict",
            "metadata": ["not", "dict"],
            "tx_hash": 123,
            "error": {"not": "str"},
        },
        fallback_key="k-fallback",
        now=now,
        fallback_expires_at=fallback_expires,
    )
    assert record.key == "k-fallback"
    assert record.status == "pending"
    assert record.created_at == now
    assert record.expires_at == fallback_expires
    assert record.result is None
    assert record.metadata is None
    assert record.tx_hash is None
    assert record.error is None


def test_record_from_doc_parses_status_and_datetime_strings():
    now = datetime.now(tz=timezone.utc)
    fallback_expires = now + timedelta(minutes=5)
    record = IdempotencyStore._record_from_doc(
        {
            "key": "k-1",
            "status": "SUCCESS",
            "created_at": "2026-01-02T03:04:05Z",
            "expires_at": "2026-01-02T03:09:05+00:00",
        },
        fallback_key="k-fallback",
        now=now,
        fallback_expires_at=fallback_expires,
    )
    assert record.key == "k-1"
    assert record.status == "success"
    assert record.created_at.tzinfo is not None
    assert record.expires_at.tzinfo is not None
