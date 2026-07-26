from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import threading
import time
import uuid

import pytest
from sqlalchemy import delete, func, select

pytestmark = pytest.mark.db

from portal.backend.db import AsyncJobRecord, db
from portal.backend.db.models import ResearchItemRecord, ResearchLinkRecord
from portal.backend.service.async_jobs import (
    AsyncJobOwnershipError,
    claim_next_job,
    complete_job,
    complete_job_with_owned_effect,
    enqueue_job,
    enqueue_or_reuse_job,
    fail_job,
    get_job,
    heartbeat_job,
)
from portal.backend.service.async_jobs import repository
from portal.backend.service.research import repository as research_repository
from portal.backend.service.research import service as research_service


def _job_type(label: str) -> str:
    return f"test_{label}_{uuid.uuid4().hex[:12]}"


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _delete_jobs(*job_ids: str) -> None:
    normalized = [str(job_id) for job_id in job_ids if str(job_id)]
    if not normalized:
        return
    with db.session() as session:
        session.execute(
            delete(AsyncJobRecord).where(AsyncJobRecord.id.in_(normalized))
        )


def test_reclaim_fences_stale_owner_and_commits_one_owned_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_type = _job_type("reclaim")
    job_id = enqueue_job(
        job_type=job_type,
        payload={"request_fingerprint": uuid.uuid4().hex * 2},
        max_attempts=3,
    )
    side_effect_job_id = str(uuid.uuid4())
    rolled_back_job_id = str(uuid.uuid4())
    try:
        first = claim_next_job(worker_id="worker-1", job_types=[job_type])
        assert first is not None
        public_claim = get_job(job_id)
        assert public_claim is not None
        assert public_claim["status"] == "running"
        assert public_claim["claim_generation"] == first.claim_generation
        assert public_claim["heartbeat_at"]
        assert "claim_token" not in public_claim
        assert "claim_token_hash" not in public_claim

        with db.session() as session:
            record = session.get(AsyncJobRecord, job_id)
            assert record is not None
            record.locked_at = _utcnow() - timedelta(hours=1)
            heartbeat_before = record.heartbeat_at

        heartbeat_job(first)
        with db.session() as session:
            record = session.get(AsyncJobRecord, job_id)
            assert record is not None
            assert record.locked_at < _utcnow() - timedelta(minutes=30)
            assert record.heartbeat_at is not None
            assert heartbeat_before is not None
            assert record.heartbeat_at >= heartbeat_before

        monkeypatch.setattr(repository, "_running_timeout_seconds", lambda: 1.0)
        monkeypatch.setattr(
            repository,
            "_should_reclaim_stale_running_jobs",
            lambda *_args, **_kwargs: True,
        )
        assert (
            claim_next_job(worker_id="worker-2", job_types=[job_type]) is None
        )

        with db.session() as session:
            record = session.get(AsyncJobRecord, job_id)
            assert record is not None
            record.heartbeat_at = _utcnow() - timedelta(minutes=5)

        second = claim_next_job(worker_id="worker-2", job_types=[job_type])
        assert second is not None
        assert second.claim_token != first.claim_token
        assert second.claim_generation == first.claim_generation + 2
        reclaimed = get_job(job_id)
        assert reclaimed is not None
        assert str(reclaimed["error"]).startswith(
            "async_job_reclaimed_timeout"
        )

        stale_effect_called = False

        def stale_effect(_session):
            nonlocal stale_effect_called
            stale_effect_called = True
            return {"owner": "stale"}

        with pytest.raises(
            AsyncJobOwnershipError,
            match="async_job_claim_not_current",
        ):
            complete_job_with_owned_effect(first, stale_effect)
        assert stale_effect_called is False
        with pytest.raises(AsyncJobOwnershipError):
            heartbeat_job(first)
        with pytest.raises(AsyncJobOwnershipError):
            fail_job(first, error="stale failure")

        def failing_owned_effect(session):
            now = _utcnow()
            session.add(
                AsyncJobRecord(
                    id=rolled_back_job_id,
                    job_type=_job_type("rolled_back"),
                    status="queued",
                    payload={},
                    partition_hash=0,
                    attempts=0,
                    max_attempts=1,
                    available_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            session.flush()
            raise RuntimeError("owned effect failed")

        with pytest.raises(RuntimeError, match="owned effect failed"):
            complete_job_with_owned_effect(second, failing_owned_effect)
        assert get_job(job_id)["status"] == "running"
        with db.session() as session:
            assert session.get(AsyncJobRecord, rolled_back_job_id) is None

        def owned_effect(session):
            now = _utcnow()
            session.add(
                AsyncJobRecord(
                    id=side_effect_job_id,
                    job_type=_job_type("side_effect"),
                    status="queued",
                    payload={},
                    partition_hash=0,
                    attempts=0,
                    max_attempts=1,
                    available_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            return {"owner": "worker-2"}

        result = complete_job_with_owned_effect(second, owned_effect)
        assert result == {"owner": "worker-2"}
        completed = get_job(job_id)
        assert completed is not None
        assert completed["status"] == "succeeded"
        assert completed["result"] == {"owner": "worker-2"}
        assert completed["lock_owner"] is None
        assert completed["heartbeat_at"] is None
        with db.session() as session:
            assert session.get(AsyncJobRecord, side_effect_job_id) is not None

        duplicate_effect_called = False

        def duplicate_effect(_session):
            nonlocal duplicate_effect_called
            duplicate_effect_called = True
            return {"owner": "duplicate"}

        with pytest.raises(AsyncJobOwnershipError):
            complete_job_with_owned_effect(second, duplicate_effect)
        assert duplicate_effect_called is False
        assert get_job(job_id)["result"] == {"owner": "worker-2"}
    finally:
        _delete_jobs(job_id, side_effect_job_id, rolled_back_job_id)


def test_retry_claim_advances_generation_and_exhausts_once() -> None:
    job_type = _job_type("retry")
    job_id = enqueue_job(
        job_type=job_type,
        payload={},
        max_attempts=2,
    )
    try:
        first = claim_next_job(worker_id="worker-1", job_types=[job_type])
        assert first is not None
        fail_job(first, error="retry me")
        retried = get_job(job_id)
        assert retried is not None
        assert retried["status"] == "retry"
        assert retried["lock_owner"] is None
        assert retried["heartbeat_at"] is None

        second = claim_next_job(worker_id="worker-2", job_types=[job_type])
        assert second is not None
        assert second.claim_generation == first.claim_generation + 1
        assert second.attempts == 2
        assert get_job(job_id)["error"] == "retry me"

        with pytest.raises(AsyncJobOwnershipError):
            complete_job(first, {"stale": True})

        fail_job(second, error="terminal failure")
        failed = get_job(job_id)
        assert failed is not None
        assert failed["status"] == "failed"
        assert failed["attempts"] == 2
        assert failed["error"] == "terminal failure"
    finally:
        _delete_jobs(job_id)


def test_stale_claim_exhaustion_respects_max_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_type = _job_type("stale_exhausted")
    job_id = enqueue_job(job_type=job_type, payload={}, max_attempts=1)
    try:
        claim = claim_next_job(worker_id="worker-1", job_types=[job_type])
        assert claim is not None
        with db.session() as session:
            record = session.get(AsyncJobRecord, job_id)
            assert record is not None
            record.heartbeat_at = repository._database_now(
                session
            ) - timedelta(minutes=5)

        monkeypatch.setattr(repository, "_running_timeout_seconds", lambda: 1.0)
        monkeypatch.setattr(
            repository,
            "_should_reclaim_stale_running_jobs",
            lambda *_args, **_kwargs: True,
        )

        assert (
            claim_next_job(worker_id="worker-2", job_types=[job_type]) is None
        )
        failed = get_job(job_id)
        assert failed is not None
        assert failed["status"] == "failed"
        assert failed["attempts"] == 1
        assert failed["claim_generation"] == claim.claim_generation + 1
        assert str(failed["error"]).startswith(
            "async_job_reclaimed_exhausted"
        )
    finally:
        _delete_jobs(job_id)


def test_current_heartbeat_blocks_and_defeats_concurrent_reclaim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_type = _job_type("heartbeat_reclaim")
    job_id = enqueue_job(job_type=job_type, payload={}, max_attempts=2)
    claim = claim_next_job(worker_id="worker-1", job_types=[job_type])
    assert claim is not None
    heartbeat_locked = threading.Event()
    release_heartbeat = threading.Event()

    def renew_while_holding_claim_lock() -> None:
        with db.session() as session:
            record = repository._require_current_claim(
                session=session,
                job=claim,
            )
            now = repository._database_now(session)
            record.heartbeat_at = now
            record.updated_at = now
            heartbeat_locked.set()
            assert release_heartbeat.wait(timeout=5.0)

    def reclaim_stale() -> int:
        with db.session() as session:
            return repository._reclaim_stale_running_jobs(
                session=session,
                job_types=[job_type],
                now=repository._database_now(session),
            )

    try:
        with db.session() as session:
            record = session.get(AsyncJobRecord, job_id)
            assert record is not None
            record.heartbeat_at = repository._database_now(
                session
            ) - timedelta(minutes=5)

        monkeypatch.setattr(repository, "_running_timeout_seconds", lambda: 1.0)
        with ThreadPoolExecutor(max_workers=2) as executor:
            heartbeat_future = executor.submit(
                renew_while_holding_claim_lock
            )
            assert heartbeat_locked.wait(timeout=5.0)
            reclaim_future = executor.submit(reclaim_stale)
            time.sleep(0.1)
            assert not reclaim_future.done()
            release_heartbeat.set()
            heartbeat_future.result(timeout=5.0)
            assert reclaim_future.result(timeout=5.0) == 0

        running = get_job(job_id)
        assert running is not None
        assert running["status"] == "running"
        assert running["claim_generation"] == claim.claim_generation
        complete_job(claim, {"heartbeat_won": True})
    finally:
        release_heartbeat.set()
        _delete_jobs(job_id)


def test_concurrent_idempotent_enqueue_creates_one_inflight_job() -> None:
    job_type = _job_type("idempotent")
    partition_key = f"partition-{uuid.uuid4()}"
    request_fingerprint = uuid.uuid4().hex * 2
    ready = threading.Barrier(2)

    def enqueue():
        ready.wait(timeout=5.0)
        return enqueue_or_reuse_job(
            job_type=job_type,
            payload={"request_fingerprint": request_fingerprint},
            partition_key=partition_key,
            request_fingerprint=request_fingerprint,
        )

    job_ids: set[str] = set()
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _index: enqueue(), range(2)))
        job_ids = {result.id for result in results}
        assert len(job_ids) == 1
        assert sorted(result.reused for result in results) == [False, True]
        with db.session() as session:
            count = session.execute(
                select(func.count())
                .select_from(AsyncJobRecord)
                .where(AsyncJobRecord.job_type == job_type)
                .where(AsyncJobRecord.partition_key == partition_key)
                .where(
                    AsyncJobRecord.request_fingerprint
                    == request_fingerprint
                )
            ).scalar_one()
        assert count == 1
    finally:
        _delete_jobs(*job_ids)


def test_idempotent_enqueue_retries_when_conflicting_job_turns_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_type = _job_type("idempotent_terminal_race")
    partition_key = f"partition-{uuid.uuid4()}"
    request_fingerprint = uuid.uuid4().hex * 2
    first = enqueue_or_reuse_job(
        job_type=job_type,
        payload={"request_fingerprint": request_fingerprint},
        partition_key=partition_key,
        request_fingerprint=request_fingerprint,
    )
    claim = claim_next_job(worker_id="worker-1", job_types=[job_type])
    assert claim is not None
    original_find = repository._find_inflight_idempotency_job
    transitioned = False

    def find_after_terminal_transition(**kwargs):
        nonlocal transitioned
        if not transitioned:
            transitioned = True
            complete_job(claim, {"completed_during_dispatch": True})
        return original_find(**kwargs)

    monkeypatch.setattr(
        repository,
        "_find_inflight_idempotency_job",
        find_after_terminal_transition,
    )
    second_id = ""
    try:
        second = enqueue_or_reuse_job(
            job_type=job_type,
            payload={"request_fingerprint": request_fingerprint},
            partition_key=partition_key,
            request_fingerprint=request_fingerprint,
        )
        second_id = second.id
        assert transitioned is True
        assert second.reused is False
        assert second.id != first.id
        assert get_job(first.id)["status"] == "succeeded"
        assert get_job(second.id)["status"] == "queued"
    finally:
        _delete_jobs(first.id, second_id)


def test_research_artifacts_rollback_with_owned_job_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_type = _job_type("research_rollback")
    job_id = enqueue_job(job_type=job_type, payload={}, max_attempts=1)
    claim = claim_next_job(worker_id="worker-1", job_types=[job_type])
    assert claim is not None
    title = f"Transactional research check {uuid.uuid4()}"
    request = {
        "title": title,
        "check_family": "indicator_forward_outcome",
    }
    evaluation = {
        "schema_version": "research_check_evaluation.v1",
        "status": "completed",
        "check_family": "indicator_forward_outcome",
        "scope": {
            "instrument_id": "instrument-test",
            "symbol": "TEST/USD",
            "timeframe": "1h",
            "datasource": "test",
            "exchange": "test",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
        },
        "detector": {
            "type": "record_match",
            "output_name": "entry",
        },
        "outcomes": {},
        "result": {
            "schema_version": "research_check_result.v1",
            "check_family": "indicator_forward_outcome",
            "status": "completed",
            "sample_count": 1,
        },
    }
    with db.session() as session:
        research_counts_before = (
            session.execute(
                select(func.count()).select_from(ResearchItemRecord)
            ).scalar_one(),
            session.execute(
                select(func.count()).select_from(ResearchLinkRecord)
            ).scalar_one(),
        )

    def reject_link(**_kwargs):
        raise RuntimeError("forced research link failure")

    monkeypatch.setattr(research_repository, "create_link", reject_link)
    try:
        with pytest.raises(
            RuntimeError,
            match="forced research link failure",
        ):
            complete_job_with_owned_effect(
                claim,
                lambda session: research_service.persist_research_check(
                    request,
                    evaluation=evaluation,
                    session=session,
                ),
            )

        assert get_job(job_id)["status"] == "running"
        with db.session() as session:
            research_counts_after = (
                session.execute(
                    select(func.count()).select_from(ResearchItemRecord)
                ).scalar_one(),
                session.execute(
                    select(func.count()).select_from(ResearchLinkRecord)
                ).scalar_one(),
            )
        assert research_counts_after == research_counts_before
        fail_job(claim, error="expected rollback test cleanup")
    finally:
        _delete_jobs(job_id)
