from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.async_jobs import repository
from portal.backend.service.async_jobs.repository import _partition_hash, _partition_slot


def test_partition_hash_is_signed_32bit_and_stable() -> None:
    key = "7c574585-a8a4-4166-8b2b-71199e768192|3600"
    value1 = _partition_hash(key)
    value2 = _partition_hash(key)

    assert value1 == value2
    assert -(2**31) <= value1 <= (2**31 - 1)


def test_partition_hash_empty_key_is_zero() -> None:
    assert _partition_hash(None) == 0
    assert _partition_hash("") == 0


def test_partition_slot_normalizes_negative_hashes() -> None:
    # Mirrors Postgres modulo behavior fix in claim_next_job: slots must be 0..N-1.
    assert _partition_slot(-1920146491, 3) == 2
    assert _partition_slot(-1, 3) == 2
    assert _partition_slot(-2, 3) == 1


def test_partition_slot_matches_positive_hashes() -> None:
    assert _partition_slot(1229646920, 3) == 2
    assert _partition_slot(0, 3) == 0
    assert _partition_slot(5, 3) == 2


def test_stale_running_reclaim_is_throttled_by_job_type() -> None:
    repository._RECLAIM_LAST_MONOTONIC_BY_JOB_TYPES.clear()

    assert repository._should_reclaim_stale_running_jobs(["signals", "overlays"], now_monotonic=100.0) is True
    assert repository._should_reclaim_stale_running_jobs(["overlays", "signals"], now_monotonic=101.0) is False
    assert repository._should_reclaim_stale_running_jobs(["signals", "overlays"], now_monotonic=131.0) is True
