"""Disposable PG query baselines and real HTTP disconnect/connection ownership.

These synthetic fixtures measure the existing 5 s SQL limit. They do not promise
production latency or turn a browser disconnect into scientific-job cancellation.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta
import http.client
import json
import logging
import socket
from threading import Thread
from time import monotonic, sleep
from urllib.parse import urlencode
from uuid import uuid4

import pytest
from sqlalchemy import delete, event, insert, text

from portal.backend.db.models import ResearchItemRecord as Item
from portal.backend.db.session import db
from portal.backend.service.research import activity_range, browse, scope

pytestmark = pytest.mark.db
logger = logging.getLogger(__name__)


def test_disposable_representative_activity_and_scope_timings():
    from portal.backend.controller.research import _read_call

    prefix = "read-measure-" + uuid4().hex + "-"
    checks = [prefix + "check-" + str(i) for i in range(64)]
    studies = [prefix + "study-" + str(i) for i in range(32)]
    start = datetime(2001, 1, 1)
    bounds = ("2001-01-01T00:00Z", "2001-01-15T00:00Z")
    baseline = _read_call(activity_range.activity, "hypotheses_created", *bounds)
    records = [{"id": prefix + "activity-" + str(i), "kind": "hypothesis",
                "status": "draft", "title": "Synthetic interval baseline", "tags": [],
                "payload": {}, "created_at": start + timedelta(minutes=i)} for i in range(10_000)]
    records.extend({"id": identity, "kind": "research_check", "status": "tested",
                    "title": "Synthetic stored-identity scope", "symbol": "FIXTURE", "tags": [],
                    "payload": {"result": {"result_hash": f"{i:064x}"},
                                "evidence": {"evidence_hash": "e" * 64}}}
                   for i, identity in enumerate(checks))
    for i, identity in enumerate(studies):
        references = [{"kind": "research_check", "item_id": checks[(i * 2 + j) % 64],
                       "result_hash": f"{(i * 2 + j) % 64:064x}", "evidence_hash": "e" * 64}
                      for j in range(32)]
        records.append({"id": identity, "kind": "study", "status": "active", "tags": [],
                        "title": "Synthetic bounded-scope baseline", "payload": {
                            "question_contract": {"publications": [{"references": references}]}}})
    try:
        with db.session() as session:
            for offset in range(0, len(records), 1000):
                session.execute(insert(Item), records[offset:offset + 1000])
        began = monotonic()
        result = _read_call(activity_range.activity, "hypotheses_created", *bounds)
        activity_ms = (monotonic() - began) * 1000
        assert result["total"] == baseline["total"] + 10_000
        assert len(result["buckets"]) == 14

        def projection(identities):
            items = [{"id": identity, "kind": "study"} for identity in identities]
            with browse.read_session() as session:
                scope.inventory(items, session)
            return items

        began = monotonic()
        ordinary = _read_call(projection, studies[:31])
        scope_ms = (monotonic() - began) * 1000
        assert all(item["scope"]["complete"] and len(item["scope"]["items"]) == 32 for item in ordinary)
        began = monotonic()
        overflow = _read_call(projection, studies)
        overflow_ms = (monotonic() - began) * 1000
        assert all("scope" not in item and "scope_status" in item for item in overflow)
        logger.info("readability_query_measurement | %s", json.dumps({
            "fixture": "synthetic disposable PostgreSQL", "activity_rows_added": 10_000,
            "activity_ms": activity_ms, "buckets": 14, "scope_studies": 31,
            "scope_references": 992, "distinct_checks": 64, "scope_ms": scope_ms,
            "overflow_references": 1024, "overflow_ms": overflow_ms,
            "meaning": "observed baseline, not a production latency guarantee"}))
    finally:
        with db.session() as session:
            identities = [record["id"] for record in records]
            for offset in range(0, len(identities), 1000):
                session.execute(delete(Item).where(Item.id.in_(identities[offset:offset + 1000])))


def test_disposable_http_disconnect_releases_connection_after_sql_timeout(monkeypatch):
    from fastapi import FastAPI
    import uvicorn
    from portal.backend.controller.research import router

    with db.session() as session:
        engine = session.get_bind()
    tag = "qt-read-disconnect-" + uuid4().hex
    target_pids, returned = [], []
    original = browse.read_session

    @contextmanager
    def observed_read():
        with original() as session:
            row = session.execute(text(
                "SELECT set_config('application_name', :tag, true), pg_backend_pid()"
            ), {"tag": tag}).one()
            target_pids.append(row[1])
            yield session

    monkeypatch.setattr(browse, "read_session", observed_read)

    def checked_in(connection, _record):
        if connection is not None and target_pids and connection.get_backend_pid() == target_pids[-1]:
            returned.append(monotonic())

    event.listen(engine, "checkin", checked_in)
    app = FastAPI()
    app.include_router(router, prefix="/api/research")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", access_log=False,
        log_level="warning", timeout_graceful_shutdown=2))
    thread = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    client = None
    try:
        deadline = monotonic() + 10
        while not server.started and thread.is_alive() and monotonic() < deadline:
            sleep(0.02)
        assert server.started
        path = "/api/research/activity/range?" + urlencode({
            "type": "hypotheses_created", "from": "2001-01-01T00:00Z", "to": "2001-01-01T01:00Z"})
        table = engine.dialect.identifier_preparer.format_table(Item.__table__)
        with engine.connect() as locker, engine.connect().execution_options(isolation_level="AUTOCOMMIT") as monitor:
            locker.execute(text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
            try:
                connected = http.client.HTTPConnection("127.0.0.1", port, timeout=12)
                began = monotonic()
                try:
                    connected.request("GET", path)
                    response = connected.getresponse()
                    response.read()
                    assert response.status == 503
                finally:
                    connected.close()
                locked_response_ms = (monotonic() - began) * 1000
                assert returned
                target_pids.clear()
                returned.clear()
                client = socket.create_connection(("127.0.0.1", port), timeout=2)
                client.sendall((f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n").encode())
                deadline = monotonic() + 3
                blocked = False
                while monotonic() < deadline:
                    monitor.execute(text("SELECT pg_stat_clear_snapshot()"))
                    if target_pids:
                        blocked = bool(monitor.execute(text(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=:pid AND wait_event_type='Lock')"
                        ), {"pid": target_pids[-1]}).scalar_one())
                    if blocked:
                        break
                    sleep(0.02)
                assert blocked, "request must reach the actual locked SQL statement before disconnect"
                disconnected_at = monotonic()
                client.shutdown(socket.SHUT_RDWR)
                client.close()
                client = None
                deadline = disconnected_at + 12
                while not returned and monotonic() < deadline:
                    sleep(0.02)
                assert returned, "owned request connection did not return after the unchanged 5 s SQL timeout"
                released_ms = (returned[0] - disconnected_at) * 1000
                state = monitor.execute(text(
                    "SELECT state,xact_start FROM pg_stat_activity WHERE pid=:pid"
                ), {"pid": target_pids[-1]}).one_or_none()
                assert state is None or (state.state == "idle" and state.xact_start is None)
                logger.info("readability_disconnect_measurement | %s", json.dumps({
                    "fixture": "real loopback HTTP and disposable PostgreSQL table lock",
                    "configured_sql_timeout_ms": 5000, "connected_timeout_response_ms": locked_response_ms,
                    "http_status": 503, "disconnect_to_pool_return_ms": released_ms,
                    "meaning": "socket close does not cancel synchronous SQL; deadline cleanup returns its connection"}))
            finally:
                locker.rollback()
    finally:
        if client is not None:
            client.close()
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
        event.remove(engine, "checkin", checked_in)
        assert not thread.is_alive(), "owned disposable HTTP server did not stop"
