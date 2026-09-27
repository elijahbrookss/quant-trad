"""Persistent controller against owned SSD/HDD QT capture and actual leases."""
from datetime import timedelta
import json
import os
import socket
import threading
import time

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


def _setup(storage, tmp_path, monkeypatch, *, stage=True):
    engine, options, source, _ = _prepare(storage, tmp_path, monkeypatch)
    with engine.begin() as conn:
        protection.prepare(conn)
        started = capture.inspect_capture(conn)["started_at"]
    # Reuse qualified finite fixture setup; it does not stop any host clients.
    if stage:
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
            worker.commit_database(deadline=time.monotonic()+30)
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
                    worker.commit_database(deadline=time.monotonic()+30)
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

def test_controller_final_deadline_is_separate_bounded_and_not_renewed(
        storage, tmp_path, monkeypatch):
    engine, settings, _ = _setup(storage, tmp_path, monkeypatch)
    settings["command_seconds"] = 2
    with engine.begin() as conn:
        original = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
        frozen = _frozen_records(conn)
    with OnlineController(engine, **settings) as worker:
        _drain(worker)
        _reprove(worker)
        for invalid in (True, float("inf"), time.monotonic()-1,
                        time.monotonic()+settings["resource_limits"]["movement_timeout_seconds"]+10):
            with pytest.raises(ValueError, match="final_deadline"):
                worker.commit_database(deadline=invalid)
            assert worker.state == "background"
        deadline = time.monotonic()+0.05
        time.sleep(0.06)
        with pytest.raises(ValueError, match="final_deadline_not_admitted"):
            worker.commit_database(deadline=deadline)
        assert worker.state == "background"
        stalled = []
        expiry_deadline = None
        def stall(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("ALTER TABLE market.fact_versions SET SCHEMA"):
                stalled.append(time.monotonic())
                # CI admission can take longer than a two-second page. Reach
                # the actual rename, then spend only the original final window;
                # do not reset a clock or depend on the runner's setup speed.
                delay = (max(0.25, expiry_deadline-time.monotonic()+0.25)
                         if expiry_deadline is not None else 2.25)
                conn.execute(text("SELECT pg_sleep(:seconds)"), {"seconds": delay})
        event.listen(engine, "after_cursor_execute", stall)
        try:
            expiry_deadline = time.monotonic()+30
            with pytest.raises((RuntimeError, DBAPIError)):
                worker.commit_database(deadline=expiry_deadline)
        finally:
            event.remove(engine, "after_cursor_execute", stall)
        assert worker.state == "commit_unknown" and stalled
        assert not worker.reconcile_database()["database_handoff_committed"]
        with engine.begin() as conn:
            assert dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one()) == original
            assert _frozen_records(conn) == frozen
    with OnlineController(engine, **settings) as worker:
        _reprove(worker)
        expiry_deadline = None
        event.listen(engine, "after_cursor_execute", stall)
        try:
            start = time.monotonic()
            report = worker.commit_database(deadline=start+15)
            assert time.monotonic()-start > 2
        finally:
            event.remove(engine, "after_cursor_execute", stall)
        assert report["database_handoff_committed"] and worker.state == "committed"
        assert worker.limits["movement_timeout_seconds"] == 2
        with engine.begin() as conn:
            assert _frozen_records(conn) == frozen

def test_controller_explicit_preparation_preserves_pages_and_original_attempt(
        storage, tmp_path, monkeypatch):
    from scripts.db import fact_header_v2_references as references
    from scripts.db import archive_reference_v2_placement as catalogs
    engine, options, source, _ = _prepare(storage, tmp_path, monkeypatch)
    with engine.begin() as conn:
        protection.prepare(conn)
        original = capture.inspect_capture(conn)
        frozen = _frozen_records(conn)
        retained = conn.scalar(text("SELECT to_regclass(:name)"),
                               {"name": catalogs.RETAINED_LEGACY}) is not None
    if retained:
        catalogs.move_reference_catalog(engine, relation=catalogs.RETAINED_LEGACY,
            policy=options["policy"], resource_limits=options["resource_limits"])
    settings = dict(placement=storage.copy_plan, expected_started_at=original["started_at"],
                    max_objects=128, max_bytes=64*1024**2, command_seconds=10, **options)

    def phase(worker, step, relation=None):
        request = dict(controller_id=worker.controller_id, sequence=worker._sequence+1,
                       operation="prepare_step", step=step, relation=relation,
                       max_duration_seconds=30)
        result = worker.command(request)
        assert worker.command(request) == result
        assert result["result"]["committed"] and not result["final_switch_authorized"]
        assert worker.limits["movement_timeout_seconds"] == 10
        return result

    with OnlineController(engine, **settings) as first:
        invalid = dict(controller_id=first.controller_id, sequence=1,
                       operation="prepare_step", step="identity_history", relation=None,
                       max_duration_seconds=first._admitted_limits["movement_timeout_seconds"]+1)
        with pytest.raises(ValueError, match="preparation_command_invalid"):
            first.command(invalid)
        assert first.state == "background" and first._sequence == 0
        for _ in range(32):
            result = _command(first, "sql_copy")["result"]
            if result["outcome"] == "identity_relocation_required":
                phase(first, "identity_history")
                break
        else:
            pytest.fail("finite header baseline did not converge")
        first_id = first.controller_id

    # An exited controller loses file proof, not the committed relocation/pages.
    with OnlineController(engine, **settings) as worker:
        assert worker.controller_id != first_id and worker.proof.hashed_bytes == 0
        _raw_book_fixture(storage, source, monkeypatch,
            definition_id="worker-preparation-publication",
            provider_product_id="BTC-USD-WORKER-PHASE",
            event_start=BASE+timedelta(hours=2))
        for _ in range(64):
            result = _command(worker, "sql_copy")["result"]
            if result["outcome"] == "raw_relocation_required":
                phase(worker, "raw_history")
            elif result["outcome"] == "both_tails_observed_empty":
                break
        else:
            pytest.fail("finite raw/tail fixture did not converge")
        phase(worker, "identity_capture")
        with engine.begin() as conn:
            slots = references.inspect_references(conn)["references"]
        for slot in slots:
            if slot["relation"] != references.PARENT:
                phase(worker, "reference_prepare", slot["relation"])
                phase(worker, "reference_validate", slot["relation"])
        phase(worker, "reference_adopt")
        _drain(worker)
        assert worker.proof.hashed_bytes > 0
        with engine.begin() as conn:
            assert capture.inspect_capture(conn)["started_at"] == original["started_at"]
            assert _frozen_records(conn) == frozen
            assert references.inspect_references(conn)["references_complete"]
        assert not worker.status()["migration_ready"]


def test_controller_rollback_fence_retains_ownership_during_source_publication(
        storage, tmp_path, monkeypatch):
    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    with engine.begin() as conn:
        original = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
        frozen = _frozen_records(conn)
    with OnlineController(engine, **settings) as worker:
        # Interrupt after the actual SQL rename. A saved negative outcome
        # alone must not authorize host restart; the live fence is separate.
        _drain(worker)
        renamed = []
        def fail_after_rename(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("ALTER TABLE market.fact_versions SET SCHEMA"):
                renamed.append(True)
                raise RuntimeError("rollback fence deliberate switch interruption")
        event.listen(engine, "after_cursor_execute", fail_after_rename)
        try:
            with pytest.raises(RuntimeError, match="deliberate switch interruption"):
                worker.commit_database(deadline=time.monotonic()+30)
        finally:
            event.remove(engine, "after_cursor_execute", fail_after_rename)
        assert renamed
        assert not worker.reconcile_database()["database_handoff_committed"]
        with worker.rollback_source_fence(deadline=time.monotonic()+30) as check:
            assert check() == {"database_handoff_committed": False,
                               "database_resume_fence_held": True,
                               "collection_resume_authorized": False}
            with engine.begin() as competing:
                with pytest.raises(RuntimeError, match="migration_busy"):
                    with capture.migration_step(competing, 5):
                        pytest.fail("another migration acquired the resume fence")
            with engine.begin() as ddl:
                ddl.exec_driver_sql("SET LOCAL lock_timeout='100ms'")
                with pytest.raises(DBAPIError), ddl.begin_nested():
                    ddl.exec_driver_sql("LOCK TABLE market.fact_versions IN ACCESS EXCLUSIVE MODE NOWAIT")
            # Actual QT archive/header/raw publication commits under the fence;
            # the fence owns no writer lock and requires no destination rehash.
            _raw_book_fixture(storage, source, monkeypatch,
                definition_id="rollback-fence-publication",
                provider_product_id="BTC-USD-ROLLBACK-FENCE",
                event_start=BASE+timedelta(hours=3))
            assert check()["database_resume_fence_held"]
            with pytest.raises(RuntimeError, match="commit_state_invalid"):
                worker.commit_database(deadline=time.monotonic()+30)
        assert worker.state == "aborted"
        with pytest.raises(RuntimeError, match="fence_lost"):
            check()
        with pytest.raises(RuntimeError, match="sequence_or_state_invalid"):
            _command(worker, "sql_copy")
    with engine.begin() as conn:
        assert dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one()) == original
        assert _frozen_records(conn) == frozen
        with capture.migration_step(conn, 5):
            pass


def test_controller_rollback_fence_loss_expiry_and_committed_refusal(
        storage, tmp_path, monkeypatch):
    engine, settings, _ = _setup(storage, tmp_path, monkeypatch)
    with OnlineController(engine, **settings) as worker:
        for invalid in (True, float("inf"), time.monotonic()-1,
                        time.monotonic()+settings["resource_limits"]["movement_timeout_seconds"]+10):
            with pytest.raises(ValueError, match="rollback_deadline"):
                with worker.rollback_source_fence(deadline=invalid):
                    pytest.fail("invalid rollback allowance admitted")
        with pytest.raises(DBAPIError):
            with worker.rollback_source_fence(deadline=time.monotonic()+30) as check:
                with engine.begin() as killer:
                    pid = killer.scalar(text("""SELECT pid FROM pg_locks
                        WHERE locktype='advisory' AND granted AND pid<>pg_backend_pid()
                          AND classid=((hashtextextended(:key,0)>>32)&4294967295)::oid
                          AND objid=(hashtextextended(:key,0)&4294967295)::oid"""),
                                        {"key": capture.LOCK})
                    assert pid
                    assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"), {"pid": pid})
                check()
        assert worker.state == "failed"
    with OnlineController(engine, **settings) as worker:
        deadline = time.monotonic()+10
        with pytest.raises(RuntimeError, match="rollback_deadline_expired|step_timeout"):
            with worker.rollback_source_fence(deadline=deadline) as check:
                # Expire the same absolute deadline after admission, without
                # waiting ten seconds or widening any production allowance.
                with monkeypatch.context() as clock:
                    clock.setattr("scripts.automation.storage_online_controller.monotonic",
                                  lambda: deadline+1)
                    check()
        assert worker.state == "failed"
    with OnlineController(engine, **settings) as worker:
        _drain(worker)
        _reprove(worker)
        # Lost successful COMMIT reply cannot lead to old-source admission.
        original_commit = handoff.commit_handoff
        def lost_reply(*args, **kwargs):
            original_commit(*args, **kwargs)
            raise RuntimeError("rollback fence lost successful reply")
        with monkeypatch.context() as lost:
            lost.setattr(handoff, "commit_handoff", lost_reply)
            with pytest.raises(RuntimeError, match="lost successful reply"):
                worker.commit_database(deadline=time.monotonic()+30)
        assert worker.state == "commit_unknown"
        with pytest.raises(RuntimeError, match="rollback_committed_refused"):
            with worker.rollback_source_fence(deadline=time.monotonic()+30):
                pytest.fail("committed database admitted for source restart")
        assert worker.state == "committed"


def test_controller_final_delta_refuses_baseline_then_copies_only_new_tail(
        storage, tmp_path, monkeypatch):
    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    with engine.begin() as conn:
        original = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
        frozen = _frozen_records(conn)
    with OnlineController(engine, **settings) as worker:
        with pytest.raises(RuntimeError, match="archive_baseline_required"):
            worker.final_delta(deadline=time.monotonic()+30)
        assert worker.state == "failed"
    with OnlineController(engine, **settings) as worker:
        _drain(worker)
        _reprove(worker)
        _raw_book_fixture(storage, source, monkeypatch,
            definition_id="final-delta-publication", provider_product_id="BTC-USD-FINAL-DELTA",
            event_start=BASE+timedelta(hours=3))
        deadline = time.monotonic()+30
        for invalid in (True, float("inf"), time.monotonic()-1):
            with pytest.raises(ValueError, match="delta_deadline"):
                worker.final_delta(deadline=invalid)
        for _ in range(8):
            report = worker.final_delta(deadline=deadline)
            assert not report["final_switch_authorized"]
            if (report["sql"]["outcome"] == "both_tails_observed_empty"
                    and all(x["captured_tail_empty_at_observation"] for x in report["archives"])):
                break
        else:
            pytest.fail("tiny final tails did not converge")
        with pytest.raises(ValueError, match="delta_deadline"):
            worker.final_delta(deadline=deadline+1)
        with pytest.raises(ValueError, match="deadline_widened"):
            worker.commit_database(deadline=deadline+1)
        with pytest.raises(RuntimeError, match="background_work_refused"):
            _command(worker, "sql_copy")
        assert worker.proof.hashed_bytes > 0
        with engine.begin() as conn:
            assert dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one()) == original
            assert _frozen_records(conn) == frozen
        # All existing exact switch checks still execute; delta is not a receipt
        # substitute, and the same absolute window/file leases reach COMMIT.
        worker.commit_database(deadline=deadline)
        assert worker.state == "committed"


def test_controller_final_delta_timeout_keeps_committed_sql_and_archive_queue(
        storage, tmp_path, monkeypatch):
    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    with OnlineController(engine, **settings) as worker:
        _drain(worker)
        _reprove(worker)
        _raw_book_fixture(storage, source, monkeypatch,
            definition_id="final-delta-timeout", provider_product_id="BTC-USD-DELTA-TIMEOUT",
            event_start=BASE+timedelta(hours=3))
        with engine.begin() as conn:
            original = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
            frozen = _frozen_records(conn)
            pending = conn.scalar(text(f"SELECT count(*) FROM {online.QUEUE}"))
            assert pending > 0
        copy_page = archives._copy_archive_page
        entered = []
        deadline = time.monotonic()+8
        def stall_record(*args, **kwargs):
            assert kwargs["deadline"] == deadline
            record = kwargs["record_page"]
            def blocked(conn, rows):
                entered.append(True)
                conn.exec_driver_sql("SELECT pg_sleep(12)")
                return record(conn, rows)
            return copy_page(*args, **(kwargs | {"record_page": blocked}))
        with monkeypatch.context() as stalled:
            stalled.setattr(archives, "_copy_archive_page", stall_record)
            with pytest.raises((RuntimeError, DBAPIError)):
                worker.final_delta(deadline=deadline)
        assert entered and worker.state == "failed"
        assert time.monotonic() < deadline+3
        with engine.begin() as conn:
            assert conn.scalar(text(f"SELECT count(*) FROM {capture.QUEUE}")) == 0
            assert conn.scalar(text(f"SELECT count(*) FROM {online.QUEUE}")) == pending
            assert dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one()) == original
            assert _frozen_records(conn) == frozen
    with OnlineController(engine, **settings) as replacement:
        assert replacement.proof.hashed_bytes == 0
        with pytest.raises(RuntimeError, match="background_reproof_required"):
            replacement.final_delta(deadline=time.monotonic()+20)


def test_controller_final_delta_never_starts_sql_baseline(storage, tmp_path, monkeypatch):
    engine, settings, _ = _setup(storage, tmp_path, monkeypatch, stage=False)
    with engine.begin() as conn:
        before = dict(conn.execute(text(f"SELECT * FROM {handoff.headers.STATE}")).mappings().one())
        assert not before["baseline_complete"]
    with OnlineController(engine, **settings) as worker:
        with pytest.raises(RuntimeError, match="sql_baseline_required"):
            worker.final_delta(deadline=time.monotonic()+20)
    with engine.begin() as conn:
        after = dict(conn.execute(text(f"SELECT * FROM {handoff.headers.STATE}")).mappings().one())
        assert after == before


def test_controller_outcome_wire_pending_negative_and_lost_real_commit(storage, tmp_path, monkeypatch):
    engine, settings, _ = _setup(storage, tmp_path, monkeypatch)
    with engine.begin() as conn:
        frozen = _frozen_records(conn)
    with OnlineController(engine, **settings) as worker:
        _drain(worker);_reprove(worker)
        hashed = worker.proof.hashed_bytes
        def inspect():
            request = dict(controller_id=worker.controller_id, sequence=worker._sequence+1,
                           operation="inspect_outcome", deadline=time.monotonic()+4)
            reply = worker.command(request)
            with pytest.raises(RuntimeError, match="fresh_sequence_required"):
                worker.command(request)
            return reply["result"]
        assert inspect()["outcome"] == "uncommitted"
        with engine.connect() as other, other.begin():
            other.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"),
                          {"name": capture.LOCK})
            result = inspect()
            assert result["outcome"] == "pending" and result["database_handoff_committed"] is None
            assert worker.state == "background" and worker.proof.hashed_bytes == hashed
        assert inspect()["outcome"] == "uncommitted"
        original = Connection._commit_impl
        switching = set()
        def observe(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("ALTER TABLE market.fact_versions SET SCHEMA"):
                switching.add(id(conn))
        def lost(conn):
            original(conn)
            if id(conn) in switching:
                raise RuntimeError("outcome fixture actual commit reply lost")
        event.listen(engine, "after_cursor_execute", observe)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(Connection, "_commit_impl", lost)
                with pytest.raises(RuntimeError, match="actual commit reply lost"):
                    worker.commit_database(deadline=time.monotonic()+30)
        finally:event.remove(engine, "after_cursor_execute", observe)
        assert worker.state == "commit_unknown" and switching
        errors = []
        client, server = socket.socketpair();client.settimeout(10)
        def host():
            try:
                with client, client.makefile("rwb", buffering=0) as channel:
                    greeting = json.loads(channel.readline(16385))
                    assert greeting["state"] == "commit_unknown"
                    seq = greeting["last_sequence"]
                    for operation in ("inspect_outcome", "close"):
                        seq += 1
                        request = dict(controller_id=greeting["controller_id"], sequence=seq, operation=operation)
                        if operation == "inspect_outcome":request["deadline"] = time.monotonic()+4
                        channel.write(json.dumps(request).encode()+b"\n")
                        reply = json.loads(channel.readline(16385))
                        if operation == "inspect_outcome":
                            assert reply["state"] == "commit_unknown"
                            assert reply["result"]["outcome"] == "committed"
                            assert not reply["result"]["collection_resume_authorized"]
                            assert not reply["result"]["runtime_activation_authorized"]
            except BaseException as exc:errors.append(exc)
        thread = threading.Thread(target=host);thread.start()
        try:
            with server:serve(worker, input_fd=server.fileno(), output_fd=server.fileno())
        finally:thread.join(timeout=15)
        assert not thread.is_alive() and not errors
        assert worker.state == "closed" and worker.proof.hashed_bytes == hashed
        worker.proof.verify_all()
    with engine.begin() as conn:assert _frozen_records(conn) == frozen


@pytest.mark.parametrize("ending", ["end", "eof", "malformed", "killed_backend", "replay"])
def test_controller_rollback_fence_wire_retains_live_lock_and_releases_on_channel_loss(
        storage, tmp_path, monkeypatch, ending):
    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    with engine.begin() as conn:
        original = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
        frozen = _frozen_records(conn)
    errors = []
    with OnlineController(engine, **settings) as worker:
        _drain(worker);_reprove(worker)
        deadline = time.monotonic()+30
        worker.final_delta(deadline=deadline)
        hashed = worker.proof.hashed_bytes
        client, server = socket.socketpair();client.settimeout(10)
        def host():
            try:
                with client, client.makefile("rwb", buffering=0) as channel:
                    greeting = json.loads(channel.readline(16385))
                    seq = greeting["last_sequence"]
                    def request(operation, **fields):
                        nonlocal seq
                        seq += 1
                        value = dict(controller_id=greeting["controller_id"], sequence=seq,
                                     operation=operation, deadline=deadline)
                        value.update(fields)
                        channel.write(json.dumps(value).encode()+b"\n")
                        return json.loads(channel.readline(16385))
                    reply = request("rollback_fence_begin")
                    assert reply["state"] == "resume_fenced"
                    assert reply["result"]["database_resume_fence_held"]
                    assert not reply["collection_resume_authorized"]
                    assert not reply["result"]["runtime_activation_authorized"]
                    with engine.begin() as competing:
                        with pytest.raises(RuntimeError, match="migration_busy"):
                            with capture.migration_step(competing, 5):pass
                    with engine.begin() as ddl:
                        with pytest.raises(DBAPIError), ddl.begin_nested():
                            ddl.exec_driver_sql("LOCK TABLE market.fact_versions IN ACCESS EXCLUSIVE MODE NOWAIT")
                    # Real source writes remain legal while this same SQL fence
                    # spans separate wire exchanges; no Docker restart occurs.
                    _raw_book_fixture(storage, source, monkeypatch,
                        definition_id="wire-rollback-"+ending,
                        provider_product_id="BTC-USD-WIRE-"+ending,
                        event_start=BASE+timedelta(hours=3))
                    assert request("rollback_fence_check")["result"]["database_resume_fence_held"]
                    if ending == "replay":
                        # Even a fully received prior reply is not fresh live
                        # ownership. Replaying its sequence closes the channel.
                        channel.write(json.dumps(dict(controller_id=greeting["controller_id"],
                            sequence=seq,operation="rollback_fence_check",deadline=deadline)).encode()+b"\n")
                    elif ending == "end":
                        reply = request("rollback_fence_end")
                        assert reply["state"] == "aborted"
                        assert not reply["result"]["database_resume_fence_held"]
                        # End is terminal for mutations but keeps this exact pipe
                        # readable for lost-acknowledgement reconciliation.
                        inspected = request("inspect_outcome", deadline=min(deadline, time.monotonic()+3))
                        assert inspected["state"] == "aborted"
                        assert inspected["result"]["outcome"] == "uncommitted"
                        assert not inspected["result"]["collection_resume_authorized"]
                        assert not inspected["result"]["runtime_activation_authorized"]
                    elif ending == "malformed":
                        channel.write(b'{"sequence":1,"sequence":2}\n')
                    elif ending == "killed_backend":
                        with engine.begin() as killer:
                            pid = killer.scalar(text("""SELECT pid FROM pg_locks
                                WHERE locktype='advisory' AND granted AND pid<>pg_backend_pid()
                                  AND classid=((hashtextextended(:key,0)>>32)&4294967295)::oid
                                  AND objid=(hashtextextended(:key,0)&4294967295)::oid"""),
                                                {"key": capture.LOCK})
                            assert pid and killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"), {"pid":pid})
                        # No request: the idle serve loop must detect lost SQL
                        # ownership instead of keeping a stale fence alive.
                        assert channel.readline(16385) == b""
            except BaseException as exc:errors.append(exc)
        thread = threading.Thread(target=host);thread.start()
        try:
            with server:
                if ending == "replay":
                    with pytest.raises(RuntimeError, match="fresh_sequence_required"):
                        serve(worker,input_fd=server.fileno(),output_fd=server.fileno(),channel_seconds=.5)
                elif ending == "malformed":
                    with pytest.raises(ValueError, match="duplicate_command_field"):
                        serve(worker,input_fd=server.fileno(),output_fd=server.fileno(),channel_seconds=.5)
                elif ending == "killed_backend":
                    with pytest.raises(DBAPIError):
                        serve(worker,input_fd=server.fileno(),output_fd=server.fileno(),channel_seconds=.5)
                else:
                    serve(worker,input_fd=server.fileno(),output_fd=server.fileno(),channel_seconds=.5)
            assert worker.proof.hashed_bytes == hashed
            if ending == "end":assert worker.state == "aborted"
        finally:
            thread.join(timeout=15)
        assert not thread.is_alive() and not errors
    # Context cleanup releases every SQL fence after EOF/malformed input/loss.
    with engine.begin() as conn:
        with capture.migration_step(conn,5):pass
        assert dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one()) == original
        assert _frozen_records(conn) == frozen
