"""Real PostgreSQL qualification for bounded portal reads; disposable inputs only."""
from io import BytesIO
import json
import logging
import socket
from threading import Event, Thread
from time import monotonic, sleep
import uuid
from zipfile import ZipFile

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
import uvicorn

from portal.backend.controller import research
from portal.backend.db.models import ResearchItemRecord, ResearchProtocolRecord
from portal.backend.db.session import db
from portal.backend.service.research import browse, evidence_export, publication, repository

pytestmark = pytest.mark.db


def _app():
    app = FastAPI()
    app.include_router(research.router, prefix="/api/research")
    return app


def _idle_pool():
    deadline = monotonic() + 3
    while db._engine.pool.checkedout() and monotonic() < deadline:
        sleep(0.01)
    assert db._engine.pool.checkedout() == 0
    with db.session() as session:
        assert session.scalar(text("SELECT 1")) == 1
    assert db._engine.pool.checkedout() == 0


def test_disposable_saved_portal_read_export_timings_and_publication_integrity():
    token = uuid.uuid4().hex
    claim = repository.create_item(kind="hypothesis", title=token + " claim", payload={})
    root = repository.create_item(kind="study", title=token, payload={"encoded_report": "x" * (2 * 1024 * 1024)})
    ids = [root["id"], claim["id"]]
    samples = {}
    try:
        publication.adopt(root["id"], {"question": "Synthetic qualification?", "scope": "Disposable only"})
        published = publication.publish(root["id"], {"request_id": token, "expected_previous_hash": None,
            "conclusion": "No scientific result", "limitations": "Synthetic fixture only", "scope": "Disposable only",
            "references": [{"item_id": claim["id"], "kind": "hypothesis", "role": "context", "content_hash": publication.content_identity(claim)}]})
        suffix = "?revision=1&publication_hash=" + published["publication_hash"]
        paths = {"inventory": "/browse?q=" + token, "overview": "/overview", "summary": f"/items/{root['id']}/summary",
                 "relationships": f"/items/{root['id']}/relationships", "activity": "/activity?days=1",
                 "images": f"/items/{root['id']}/images", "record": f"/items/{root['id']}/record",
                 "preflight": f"/items/{root['id']}/export/preflight" + suffix,
                 "bundle": f"/items/{root['id']}/export/content" + suffix}
        with TestClient(_app()) as client:
            for name, path in paths.items():
                durations = []
                for _ in range(5):
                    started = monotonic()
                    response = client.get("/api/research" + path)
                    durations.append(monotonic() - started)
                    assert response.status_code == 200, response.text[:500]
                    _idle_pool()
                ordered = sorted(durations)
                samples[name] = {"samples": 5, "p50_seconds": ordered[2], "p95_nearest_rank_seconds": ordered[4], "response_bytes": len(response.content)}
                if name in {"inventory", "overview", "summary"}:
                    assert len(response.content) < 250_000
                if name == "bundle":
                    with ZipFile(BytesIO(response.content)) as archive:
                        original = json.loads(archive.read("records/root-current.json"))
                        assert original["payload"]["encoded_report"] == "x" * (2 * 1024 * 1024)
                        assert json.loads(archive.read("records/selected-publication.json"))["publication_hash"] == published["publication_hash"]
                        assert b"no untyped payload is extracted" in archive.read("README.txt")
                        assert not any("report" in name for name in archive.namelist())
            stale = client.get(f"/api/research/items/{root['id']}/export/content?revision=1&publication_hash=changed")
            assert stale.status_code == 409
            _idle_pool()
        logging.getLogger(__name__).info("portal_db_timing %s", json.dumps({"fixture": "synthetic_2MiB_embedded_report_one_publication", "measurements": samples}, sort_keys=True))
    finally:
        with db.session() as session:
            session.query(ResearchItemRecord).filter(ResearchItemRecord.id.in_(ids)).delete(synchronize_session=False)


def test_disposable_compact_session_is_readonly_repeatable_and_rolls_back():
    assert db.ensure_schema()
    with browse.read_session() as session:
        assert session.scalar(text("SHOW transaction_read_only")) == "on"
        assert session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
        assert session.scalar(text("SHOW statement_timeout")) == "5s"
    with pytest.raises(DBAPIError):
        with browse.read_session() as session:
            session.execute(text("CREATE TABLE forbidden_portal_read_write(id int)"))
    _idle_pool()


@pytest.mark.parametrize("compact,expected_seconds", [(True, 5), (False, 20)])
def test_disposable_disconnect_query_stop_and_pool_release(monkeypatch, compact, expected_seconds):
    """Actual owned pg_sleep: disconnect does not cancel; configured deadline does."""
    assert db.ensure_schema()
    _idle_pool()
    entered, finished = Event(), Event()
    observation = {}
    def slow_read():
        try:
            with (browse.read_session() if compact else db.session()) as session:
                observation["pid"] = session.scalar(text("SELECT pg_backend_pid()"))
                observation["query_started"] = monotonic()
                entered.set()
                session.execute(text("SELECT pg_sleep(30)"))
                pytest.fail("Configured SQL/read deadline did not stop the query")
        except Exception as exc:
            observation["error"] = type(exc).__name__
            raise
        finally:
            observation["worker_unwound"] = monotonic()
            finished.set()
    monkeypatch.setattr(research.research_browse, "overview", slow_read)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    server = uvicorn.Server(uvicorn.Config(_app(), log_level="error", lifespan="off"))
    thread = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    client = socket.create_connection(listener.getsockname(), timeout=3)
    try:
        client.sendall(b"GET /api/research/overview HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        assert entered.wait(5)
        # Observe the actual owned sleep on PostgreSQL before closing transport.
        with db._engine.connect() as monitor:
            for _ in range(100):
                active = monitor.execute(text("SELECT state, query FROM pg_stat_activity WHERE pid=:pid"), {"pid": observation["pid"]}).mappings().one()
                if active["state"] == "active" and "pg_sleep(30)" in active["query"]:
                    break
                monitor.rollback()
                sleep(0.01)
            else:
                pytest.fail("Owned SQL was not observed active")
        observation["disconnected"] = monotonic()
        client.shutdown(socket.SHUT_RDWR)
        client.close()
        sleep(0.05)
        assert not finished.is_set(), "Disconnect must not be advertised as synchronous SQL cancellation"
        assert finished.wait(expected_seconds + 5)
        _idle_pool()
        observation["pool_released"] = monotonic()
        with db._engine.connect() as monitor:
            active = monitor.execute(text("SELECT state, query FROM pg_stat_activity WHERE pid=:pid"), {"pid": observation["pid"]}).mappings().one_or_none()
            assert active is None or active["state"] != "active" or "pg_sleep(30)" not in active["query"]
        elapsed = observation["worker_unwound"] - observation["query_started"]
        assert expected_seconds - 1 <= elapsed < expected_seconds + 5
        assert observation["error"] in {"OperationalError", "ExecutionBudgetExceededError"}
        logging.getLogger(__name__).info("portal_db_disconnect_timing %s", json.dumps({"compact_session": compact, "configured_seconds": expected_seconds,
              "query_to_unwind_seconds": elapsed, "disconnect_to_unwind_seconds": observation["worker_unwound"] - observation["disconnected"],
              "disconnect_to_pool_release_seconds": observation["pool_released"] - observation["disconnected"], "error_type": observation["error"], "pool_checked_out": db._engine.pool.checkedout()}, sort_keys=True))
    finally:
        client.close()
        server.should_exit = True
        thread.join(timeout=5)
        listener.close()
    assert not thread.is_alive()
    _idle_pool()


def test_disposable_nested_custody_dependency_cap_and_oversize_refuse():
    token = uuid.uuid4().hex
    sealed = repository.create_item(kind="observation", title="sealed fixture", payload={"nested": {"dataset_id": token}})
    excessive = repository.create_item(kind="study", title="dependency cap fixture", payload={"nested": [{"dataset_id": token + str(i)} for i in range(513)]})
    oversized = repository.create_item(kind="study", title="oversize fixture", payload={"encoded_report": "x" * (9 * 1024 * 1024)})
    ids = [sealed["id"], excessive["id"], oversized["id"]]
    try:
        with db.session() as session:
            session.add(ResearchProtocolRecord(id=token, schema_version="synthetic", protocol_hash=token,
                blindness_class="synthetic", private_manifest={"dataset_id": token}, public_manifest={}, created_by="qualification"))
        with TestClient(_app()) as client:
            for endpoint in ("summary", "relationships", "record", "export/preflight"):
                response = client.get(f"/api/research/items/{sealed['id']}/{endpoint}")
                assert response.status_code == 403, response.text
                assert "inaccessible" in response.json()["detail"]
                _idle_pool()
            response = client.get(f"/api/research/items/{excessive['id']}/summary")
            assert response.status_code == 409 and "too many dependency" in response.json()["detail"]
            response = client.get(f"/api/research/items/{oversized['id']}/record")
            assert response.status_code == 409 and "8 MiB" in response.json()["detail"]
            _idle_pool()
    finally:
        with db.session() as session:
            session.query(ResearchItemRecord).filter(ResearchItemRecord.id.in_(ids)).delete(synchronize_session=False)
            session.query(ResearchProtocolRecord).filter(ResearchProtocolRecord.id == token).delete(synchronize_session=False)
