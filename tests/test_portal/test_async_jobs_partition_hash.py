from __future__ import annotations

import threading

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.async_jobs import repository
from portal.backend.service.async_jobs.repository import (
    AsyncJobOwnershipError,
    ClaimedJob,
    _partition_hash,
    _partition_slot,
)


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


@pytest.mark.parametrize(
    ("worker_id", "partition_index", "partition_total", "message"),
    [
        ("", 0, 1, "worker_id is required"),
        ("worker-1", 0, 0, "partition_total must be positive"),
        ("worker-1", -1, 2, "partition_index must be within"),
        ("worker-1", 2, 2, "partition_index must be within"),
    ],
)
def test_claim_rejects_invalid_owner_or_partition_before_database_access(
    worker_id: str,
    partition_index: int,
    partition_total: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        repository.claim_next_job(
            worker_id=worker_id,
            job_types=["signals"],
            partition_index=partition_index,
            partition_total=partition_total,
        )


def test_stale_running_reclaim_is_throttled_by_job_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reclaim_history: dict[tuple[str, ...], float] = {}
    monkeypatch.setattr(
        repository,
        "_RECLAIM_LAST_MONOTONIC_BY_JOB_TYPES",
        reclaim_history,
    )
    monkeypatch.setattr(repository, "_reclaim_interval_seconds", lambda: 30.0)

    assert repository._should_reclaim_stale_running_jobs(["signals", "overlays"], now_monotonic=100.0) is True
    assert repository._should_reclaim_stale_running_jobs(["overlays", "signals"], now_monotonic=101.0) is False
    assert repository._should_reclaim_stale_running_jobs(["signals", "overlays"], now_monotonic=131.0) is True
    assert reclaim_history == {("overlays", "signals"): 131.0}


def _claimed_job() -> ClaimedJob:
    return ClaimedJob(
        id="job-1",
        job_type="research_check",
        payload={},
        attempts=1,
        max_attempts=2,
        partition_key=None,
        partition_hash=0,
        lock_owner="worker-1",
        claim_token="claim-token",
        claim_generation=1,
    )


def test_claim_heartbeat_renews_at_a_bounded_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = threading.Event()
    calls: list[str] = []

    def fake_heartbeat(job: ClaimedJob) -> None:
        calls.append(job.id)
        called.set()

    monkeypatch.setattr(repository, "heartbeat_job", fake_heartbeat)

    with repository.maintain_job_heartbeat(
        _claimed_job(),
        interval_seconds=0.01,
    ):
        assert called.wait(timeout=0.5)

    assert calls
    assert set(calls) == {"job-1"}


def test_claim_heartbeat_surfaces_ownership_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted = threading.Event()

    def reject_heartbeat(job: ClaimedJob) -> None:
        attempted.set()
        raise AsyncJobOwnershipError(f"stale: {job.id}")

    monkeypatch.setattr(repository, "heartbeat_job", reject_heartbeat)

    with pytest.raises(AsyncJobOwnershipError, match="stale: job-1"):
        with repository.maintain_job_heartbeat(
            _claimed_job(),
            interval_seconds=0.01,
        ):
            assert attempted.wait(timeout=0.5)


def test_claim_heartbeat_preserves_handler_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted = threading.Event()

    def reject_heartbeat(job: ClaimedJob) -> None:
        attempted.set()
        raise AsyncJobOwnershipError(f"stale: {job.id}")

    monkeypatch.setattr(repository, "heartbeat_job", reject_heartbeat)

    with pytest.raises(ValueError, match="malformed request"):
        with repository.maintain_job_heartbeat(
            _claimed_job(),
            interval_seconds=0.01,
        ):
            assert attempted.wait(timeout=0.5)
            raise ValueError("malformed request")
