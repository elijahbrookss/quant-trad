from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import threading
import uuid

import pytest
from sqlalchemy import text

from core.execution_control import ExecutionCancelledError
from portal.backend.db import AsyncJobRecord, db
from portal.backend.service.async_jobs import repository as jobs
from portal.backend.service.research import service
from portal.backend.workers.research_worker import execute_claimed_research_job
from portal.backend.workers import research_worker

pytestmark = pytest.mark.db


def _enqueued(*, job_type=None):
    kind = job_type or f"test_cancel_{uuid.uuid4().hex[:12]}"
    job_id = jobs.enqueue_job(job_type=kind, payload={"request": {}}, max_attempts=2)
    return kind, job_id


def test_cancel_queued_and_completed_jobs_are_idempotent():
    kind, job_id = _enqueued()
    cancelled = jobs.request_job_cancellation(job_id, job_types=[kind])
    assert cancelled["status"] == "cancelled"
    assert cancelled["result"]["execution_stopped"] is True
    assert jobs.request_job_cancellation(job_id, job_types=[kind]) == cancelled
    assert jobs.claim_next_job(worker_id="unused", job_types=[kind]) is None
    with pytest.raises(KeyError):
        jobs.request_job_cancellation(job_id, job_types=["another_type"])

    kind, job_id = _enqueued()
    claim = jobs.claim_next_job(worker_id="winner", job_types=[kind])
    jobs.complete_job(claim, {"complete": True})
    assert jobs.request_job_cancellation(job_id, job_types=[kind])["result"] == {"complete": True}


def test_running_cancellation_keeps_claim_and_refuses_publication_or_reclaim(monkeypatch):
    kind = f"test_cancel_{uuid.uuid4().hex[:12]}"
    args = dict(job_type=kind, payload={"request": {}}, partition_key="scope",
                request_fingerprint="a"*64, max_attempts=3)
    queued = jobs.enqueue_or_reuse_job(**args)
    claim = jobs.claim_next_job(worker_id="owner", job_types=[kind])
    requested = jobs.request_job_cancellation(queued.id, job_types=[kind])
    assert requested["status"] == "running"
    assert requested["result"]["execution_stopped"] is False
    assert requested["payload"] == claim.payload
    assert jobs.enqueue_or_reuse_job(**args).id == queued.id
    assert jobs.heartbeat_job(claim) is True
    published = []
    with pytest.raises(ExecutionCancelledError):
        jobs.complete_job_with_owned_effect(claim, lambda session: published.append(True) or {})
    assert not published
    with db.session() as session:
        session.get(AsyncJobRecord, queued.id).heartbeat_at = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=1)
    monkeypatch.setattr(jobs, "_should_reclaim_stale_running_jobs", lambda *a, **kw: True)
    assert jobs.claim_next_job(worker_id="replacement", job_types=[kind]) is None
    assert jobs.get_job(queued.id)["status"] == "running"
    jobs.acknowledge_job_cancellation(claim)
    assert jobs.get_job(queued.id)["status"] == "cancelled"
    assert jobs.enqueue_or_reuse_job(**args).id != queued.id
    with pytest.raises(jobs.AsyncJobOwnershipError):
        jobs.complete_job(claim, {})


def test_completion_lock_wins_concurrent_cancellation():
    kind, job_id = _enqueued()
    claim = jobs.claim_next_job(worker_id="owner", job_types=[kind])
    inside_effect, release_effect = threading.Event(), threading.Event()

    def effect(session):
        inside_effect.set()
        assert release_effect.wait(10)
        return {"atomic": True}

    with ThreadPoolExecutor(max_workers=2) as pool:
        completion = pool.submit(jobs.complete_job_with_owned_effect, claim, effect)
        assert inside_effect.wait(10)
        cancellation = pool.submit(jobs.request_job_cancellation, job_id, job_types=[kind])
        release_effect.set()
        assert completion.result(timeout=10) == {"atomic": True}
        assert cancellation.result(timeout=10)["status"] == "succeeded"


def test_running_sql_is_interrupted_before_acknowledgement(monkeypatch):
    kind, job_id = _enqueued()
    monkeypatch.setattr(research_worker, "JOB_TYPE_RESEARCH_CHECK_RUN", kind)
    claim = jobs.claim_next_job(worker_id="cancel-sql-test", job_types=[kind])
    assert claim.id == job_id
    entered = threading.Event()

    def build(request):
        with db.session() as session:
            entered.set()
            session.execute(text("SELECT pg_sleep(30)"))
        pytest.fail("cancelled SQL returned successfully")

    monkeypatch.setattr(service, "build_research_check_evidence", build)
    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(execute_claimed_research_job, claim)
        assert entered.wait(10)
        jobs.request_job_cancellation(job_id, job_types=[kind])
        with pytest.raises(ExecutionCancelledError):
            running.result(timeout=8)
    assert jobs.get_job(job_id)["status"] == "running"
    jobs.acknowledge_job_cancellation(claim)
    assert jobs.get_job(job_id)["result"]["execution_stopped"] is True
    with db.session() as session:
        assert session.execute(text("SELECT 42")).scalar_one() == 42
