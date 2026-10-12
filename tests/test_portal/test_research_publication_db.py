from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
from threading import Event, Lock
from time import monotonic
import uuid

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from portal.backend.db.models import ResearchItemRecord
from portal.backend.db.session import db
from portal.backend.service.research import publication, repository

pytestmark = pytest.mark.db


def test_disposable_publication_contention_rollback_and_fresh_process(tmp_path, monkeypatch):
    token = uuid.uuid4().hex
    question = repository.create_item(kind="study", title=f"Question {token}", payload={"legacy": "retained"})
    claim = repository.create_item(kind="hypothesis", title=f"Claim {token}", body="Initial claim")
    finding = repository.create_item(kind="observation", title=f"Finding {token}", body="Inconclusive manual note")
    repository.create_link(source_item_id=question["id"], target_type="research_item", target_id=claim["id"], relation="tests")
    publication.adopt(question["id"], {"question": "Is the claim supported?", "scope": "Synthetic disposable evidence"})
    request = {"request_id": token, "expected_previous_hash": None, "conclusion": "No conclusion supported.",
               "limitations": "Manual synthetic finding; no statistical inference or actual replay.", "scope": "Disposable",
               "references": [{"item_id": item["id"], "kind": item["kind"], "role": "context",
                               "content_hash": publication.content_identity(item)} for item in (claim, finding)]}
    local = tmp_path / "campaign.json"
    local.write_text(json.dumps(request))
    request = json.loads(local.read_text())

    # Hold only the first writer AFTER its row lock. The second must reach a
    # PostgreSQL lock wait before release; a barrier after both locks deadlocks.
    original_locked = publication._locked
    first_locked, second_started, release = Event(), Event(), Event()
    order_lock = Lock()
    attempts, second_pid = [], []

    def contended_locked(item_id, session):
        with order_lock:
            attempts.append(item_id)
            number = len(attempts)
        session.execute(text("SET LOCAL lock_timeout = '20s'"))
        if number == 2:
            second_pid.append(session.scalar(text("SELECT pg_backend_pid()")))
            second_started.set()
        record = original_locked(item_id, session)
        if number == 1:
            first_locked.set()
            assert release.wait(20), "first writer was not released"
        return record

    with monkeypatch.context() as patch, ThreadPoolExecutor(max_workers=2) as pool:
        patch.setattr(publication, "_locked", contended_locked)
        first_future = pool.submit(publication.publish, question["id"], deepcopy(request))
        try:
            assert first_locked.wait(10), "first writer did not acquire the row lock"
            second_future = pool.submit(publication.publish, question["id"], deepcopy(request))
            assert second_started.wait(10), "second writer did not start its lock query"
            deadline = monotonic() + 10
            waiting = False
            while monotonic() < deadline:
                with db.session() as session:
                    waiting = session.scalar(text(
                        "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = :pid"
                    ), {"pid": second_pid[0]})
                if waiting:
                    break
                release.wait(0.02)
            assert waiting, "second writer never overlapped the held PostgreSQL row lock"
        finally:
            release.set()
        results = [first_future.result(timeout=10), second_future.result(timeout=10)]
    assert results[0] == results[1]
    first = results[0]

    # Cause an actual server SQL error AFTER the publication UPDATE has flushed.
    # A separate committed read must prove rollback, then the same request retries.
    retry = request | {"request_id": token + "retry", "expected_previous_hash": first["publication_hash"]}
    with monkeypatch.context() as patch:
        patch.setattr(publication, "MAX_HISTORY_BYTES", 1)
        with pytest.raises(ValueError, match="retain this question"):
            publication.publish(question["id"], deepcopy(retry))
    assert publication.history(question["id"])["publications"] == [first]
    original_session = db.session
    flushed_requests = []

    def fail_after_flush(session, _context):
        flushed_requests.append(session.scalar(text(
            "SELECT payload->'question_contract'->'publications'->-1->>'request_id' "
            "FROM portal_research_items WHERE id = :id"
        ), {"id": question["id"]}))
        session.execute(text("SELECT 1 / 0"))

    @contextmanager
    def failing_session():
        with original_session() as session:
            event.listen(session, "after_flush", fail_after_flush)
            yield session

    with monkeypatch.context() as patch:
        patch.setattr(db, "session", failing_session)
        with pytest.raises(DBAPIError, match="division by zero"):
            publication.publish(question["id"], deepcopy(retry))
    assert flushed_requests == [retry["request_id"]]
    assert publication.history(question["id"])["publications"] == [first]
    second = publication.publish(question["id"], deepcopy(retry))
    assert second["revision"] == 2
    assert publication.publish(question["id"], deepcopy(retry)) == second

    local.unlink()
    with db.session() as session:
        record = session.get(ResearchItemRecord, claim["id"])
        record.body = "Later corrected claim"
    repository.create_link(source_item_id=question["id"], target_type="research_item", target_id=claim["id"], relation="tests", metadata={"later": True})
    with pytest.raises(ValueError, match="revision_conflict"):
        publication.publish(question["id"], request | {"request_id": token + "stale"})

    # New interpreter, empty working directory, no campaign path or inherited
    # Python objects. Only the question ID and existing isolated server connection.
    reader = """
import json, sys
from portal.backend.service.research import publication
print('PUBLICATION_HISTORY=' + json.dumps(publication.history(sys.argv[1])))
"""
    child = subprocess.run(
        [sys.executable, "-c", reader, question["id"]], cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2])},
        capture_output=True, text=True, timeout=60,
    )
    # Avoid exposing connection/environment details if the subprocess fails.
    assert child.returncode == 0, "fresh-process history retrieval failed"
    output = [line.removeprefix("PUBLICATION_HISTORY=") for line in child.stdout.splitlines()
              if line.startswith("PUBLICATION_HISTORY=")]
    assert len(output) == 1
    history = json.loads(output[0])
    assert history["publications"] == [first, second]
    assert first["references"][0]["snapshot"]["body"] == "Initial claim"
    assert first["relationships"][0]["metadata"] == {}
    with db.session() as session:
        record = session.get(ResearchItemRecord, question["id"])
        assert record.payload["legacy"] == "retained"
        assert record.payload[publication.KEY]["publications"] == [first, second]
