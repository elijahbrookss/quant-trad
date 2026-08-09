from __future__ import annotations

from datetime import UTC, datetime
from threading import Event

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.capacity_observability import (
    DatabaseCapacitySampler,
    capacity_sample_bucket,
)


def test_capacity_sample_bucket_aligns_to_interval_in_utc() -> None:
    assert capacity_sample_bucket(
        datetime(2026, 8, 4, 12, 7, 43, tzinfo=UTC),
        interval_seconds=300,
    ) == datetime(2026, 8, 4, 12, 5)

    assert capacity_sample_bucket(
        datetime(2026, 8, 4, 12, 7, 43),
        interval_seconds=300,
    ) == datetime(2026, 8, 4, 12, 5)


def test_capacity_sampler_passes_aligned_identity_and_retention() -> None:
    captured: list[dict] = []

    def sample_fn(**kwargs):
        captured.append(dict(kwargs))
        return {
            "sampled": True,
            "database_size_bytes": 1024,
            "relation_count": 3,
            "relation_rows_inserted": 3,
            "retention_rows_deleted": 0,
            "sample_query_ms": 1.5,
        }

    sampler = DatabaseCapacitySampler(
        sample_fn=sample_fn,
        enabled=True,
        interval_seconds=300,
        retention_days=30,
    )

    result = sampler.sample_once(now=datetime(2026, 8, 4, 12, 7, 43, tzinfo=UTC))

    assert result["sampled"] is True
    assert captured == [
        {
            "sampled_at": datetime(2026, 8, 4, 12, 5),
            "retention_days": 30,
        }
    ]


def test_capacity_sampler_starts_immediately_and_stops_cleanly() -> None:
    sampled = Event()

    def sample_fn(**_kwargs):
        sampled.set()
        return {"sampled": False, "reason": "test"}

    sampler = DatabaseCapacitySampler(
        sample_fn=sample_fn,
        enabled=True,
        interval_seconds=60,
        retention_days=1,
    )

    sampler.start()
    assert sampled.wait(timeout=2.0)
    sampler.stop(timeout_s=2.0)
