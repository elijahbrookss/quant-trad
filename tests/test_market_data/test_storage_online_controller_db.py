"""Persistent controller against owned SSD/HDD QT capture and actual leases."""
from datetime import timedelta
import json
import os
import socket
import threading

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from scripts.automation.storage_online_controller import OnlineController, serve
from scripts.db import archive_root_v2_copy as archives
from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_handoff as handoff
from scripts.db import fact_header_v2_online_proof as protection
from tests.test_market_data.test_archive_online_copy_db import _prepare, _synthetic_descriptor, online
from tests.test_market_data.test_archive_root_copy_db import _hashes
from tests.test_market_data.test_fact_header_copy_db import _frozen_records
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import BASE, storage

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                reason="requires owned SSD/HDD storage-demo topology")]


def _setup(storage, tmp_path, monkeypatch):
    engine, options, source, _ = _prepare(storage, tmp_path, monkeypatch)
    with engine.begin() as conn:
        protection.prepare(conn)
        started = capture.inspect_capture(conn)["started_at"]
    # Reuse qualified finite fixture setup; it does not stop any host clients.
    handoff.stage_handoff(engine, placement=storage.copy_plan,
                          max_duration_seconds=120, **options)
    settings = dict(placement=storage.copy_plan, expected_started_at=started,
                    max_objects=128, max_bytes=64*1024**2, **options)
    return engine, settings, source


def _command(worker, operation):
    return worker.command(dict(controller_id=worker.controller_id,
                               sequence=worker._sequence+1, operation=operation))


def _drain(worker):
    for _ in range(30):
        _command(worker, "archive_copy")
        with worker.engine.begin() as conn:
            if (not conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {online.QUEUE})"))
                    and not conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {online.PROGRESS} "
                                              "WHERE NOT baseline_complete)"))):
                return
    pytest.fail("tiny controller archive fixture did not converge")


def _reprove(worker):
    for _ in range(30):
        _command(worker, "reprove")
        if len(worker.status()["reproved_families_at_observation"]) == len(archives.FAMILIES):
            return
    pytest.fail("tiny controller file reproof did not converge")


def test_controller_reentry_rehashes_and_retains_proof_through_lost_commit(
        storage, tmp_path, monkeypatch):
    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    with engine.begin() as conn:
        frozen = _frozen_records(conn)
    with OnlineController(engine, **settings) as first:
        first_id = first.controller_id
        _drain(first)
        assert first.proof.hashed_bytes > 0
        request = dict(controller_id=first_id, sequence=first._sequence+1, operation="archive_copy")
        reply = first.command(request)
        assert first.command(request) == reply
        assert not reply["final_switch_authorized"]
        with pytest.raises(ValueError, match="command_invalid"):
            first.command(request | {"operation": "switch", "sequence": request["sequence"]+1})
    with OnlineController(engine, **settings) as worker:
        assert worker.controller_id != first_id and worker.proof.hashed_bytes == 0
        with pytest.raises(ValueError, match="command_invalid"):
            worker.command(request)
        # Durable cursors/empty tails do not recreate process-local file proof.
        _drain(worker)
        with pytest.raises(RuntimeError, match="object_not_verified"):
            worker.commit_database()
        assert worker.state == "commit_unknown"
        assert not worker.reconcile_database()["database_handoff_committed"]
        assert worker.state == "rolled_back"
    with OnlineController(engine, **settings) as worker:
        _reprove(worker)
        # A real QT publisher keeps writing while this same controller owns leases.
        _raw_book_fixture(storage, source, monkeypatch,
                          definition_id="persistent-online-publication",
                          provider_product_id="BTC-USD-CONTROLLER",
                          event_start=BASE+timedelta(hours=2))
        _drain(worker)
        for _ in range(10):
            report = _command(worker, "sql_copy")
            if report["result"]["outcome"] == "both_tails_observed_empty":
                break
        else:
            pytest.fail("tiny SQL tail did not converge")
        hashed = worker.proof.hashed_bytes
        def no_full_scan(*args, **kwargs):
            pytest.fail("persistent controller used final full data verifier")
        monkeypatch.setattr(archives, "_sha256_file", no_full_scan)
        monkeypatch.setattr(handoff.headers, "_verified_pages", no_full_scan)
        original_commit = Connection._commit_impl
        switching = set()
        def observe(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("ALTER TABLE market.fact_versions SET SCHEMA"):
                switching.add(id(conn))
        def lost_reply(conn):
            if id(conn) in switching:
                worker.proof.verify_all()
                original_commit(conn)
                worker.proof.verify_all()
                raise RuntimeError("controller actual commit reply lost")
            original_commit(conn)
        event.listen(engine, "after_cursor_execute", observe)
        try:
            with monkeypatch.context() as lost:
                lost.setattr(Connection, "_commit_impl", lost_reply)
                with pytest.raises(RuntimeError, match="actual commit reply lost"):
                    worker.commit_database()
        finally:
            event.remove(engine, "after_cursor_execute", observe)
        assert worker.state == "commit_unknown" and switching
        assert worker.reconcile_database()["database_handoff_committed"]
        assert worker.state == "committed" and worker.proof.hashed_bytes == hashed
        worker.proof.verify_all()
        with engine.begin() as conn:
            assert _frozen_records(conn) == frozen


def test_controller_pipe_sequences_eof_and_terminal_cancel(storage, tmp_path, monkeypatch):
    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    failures = []
    client, server = socket.socketpair()
    client.settimeout(10)
    def host():
        try:
            with client, client.makefile("rwb", buffering=0) as channel:
                greeting = json.loads(channel.readline(16385))
                for seq, operation in enumerate(("archive_copy", "status", "cancel"), 1):
                    request = dict(controller_id=greeting["controller_id"],
                                   sequence=seq, operation=operation)
                    channel.write(json.dumps(request).encode()+b"\n")
                    reply = json.loads(channel.readline(16385))
                    assert reply["last_sequence"] == seq
                    assert not reply["collection_resume_authorized"]
                assert reply["state"] == "cancelled"
        except BaseException as exc:
            failures.append(exc)
    thread = threading.Thread(target=host)
    thread.start()
    try:
        with server, OnlineController(engine, **settings) as worker:
            serve(worker, input_fd=server.fileno(), output_fd=server.fileno())
            assert worker.state == "cancelled"
    finally:
        thread.join(timeout=15)
    assert not thread.is_alive() and not failures
    with engine.begin() as conn:
        queued = conn.scalar(text(f"SELECT count(*) FROM {online.QUEUE}"))
        _synthetic_descriptor(conn, source, "!after-controller-cancel")
        assert conn.scalar(text(f"SELECT count(*) FROM {online.QUEUE}")) == queued
        assert conn.scalar(text(f"SELECT receipt FROM {capture.CANCELLED}"))
    with pytest.raises(RuntimeError, match="attempt_cancelled"):
        with OnlineController(engine, **settings):
            pytest.fail("canceled controller reentered")


def test_controller_busy_lost_owner_and_changed_original_attempt(storage, tmp_path, monkeypatch):
    engine, settings, _ = _setup(storage, tmp_path, monkeypatch)
    with OnlineController(engine, **settings) as worker:
        with pytest.raises(RuntimeError, match="controller_busy"):
            with OnlineController(engine, **settings):
                pytest.fail("second controller admitted")
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_terminate_backend(:pid,5000)"), {"pid": worker._pid})
        with pytest.raises((RuntimeError, DBAPIError)):
            _command(worker, "archive_copy")
        assert worker.state == "failed"
    # Lost ownership cannot transparently reconnect, but a fresh controller may
    # re-admit the unchanged attempt and start with zero live proof.
    with OnlineController(engine, **settings) as worker:
        assert worker.proof.hashed_bytes == 0
        with engine.begin() as conn:
            saved = conn.scalar(text(f"SELECT attempt_seconds FROM {capture.STATE}"))
            conn.execute(text(f"UPDATE {capture.STATE} SET attempt_seconds=:seconds"),
                         {"seconds": saved-1})
        with pytest.raises(RuntimeError, match="attempt_binding_changed"):
            _command(worker, "sql_copy")
        assert worker.state == "failed"
