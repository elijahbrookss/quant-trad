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
            if result["outcome"] == "raw_relocation_required":
                phase(first, "raw_history")
            elif result["outcome"] == "identity_relocation_required":
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
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool
        fault_actor = create_engine(engine.url, poolclass=NullPool)
        try:
            with pytest.raises(DBAPIError):
                with worker.rollback_source_fence(deadline=time.monotonic()+30) as check:
                    with fault_actor.begin() as killer:
                        pid = killer.scalar(text("""SELECT pid FROM pg_locks
                            WHERE locktype='advisory' AND granted AND pid<>pg_backend_pid()
                              AND classid=((hashtextextended(:key,0)>>32)&4294967295)::oid
                              AND objid=(hashtextextended(:key,0)&4294967295)::oid"""),
                                            {"key": capture.LOCK})
                        assert pid
                        assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"), {"pid": pid})
                    check()
        finally:
            # A retired diagnostic actor must close its physical connection,
            # not leave an idle third client in the migration engine's pool.
            fault_actor.dispose()
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
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool
        competitor = create_engine(engine.url, poolclass=NullPool)
        try:
            with competitor.connect() as other, other.begin():
                other.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"),
                              {"name": capture.LOCK})
                result = inspect()
                assert result["outcome"] == "pending" and result["database_handoff_committed"] is None
                assert worker.state == "background" and worker.proof.hashed_bytes == hashed
        finally:
            # This actor has its own non-pooling connection. Retire it without
            # leaving an extra idle session in the migration engine's pool.
            competitor.dispose()
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



def test_controller_idle_sql_peer_refuses_before_switch(storage, tmp_path, monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    engine, settings, source = _setup(storage, tmp_path, monkeypatch, stage=False)
    other = create_engine(engine.url, poolclass=NullPool)
    try:
        with OnlineController(engine, **settings) as worker, other.connect() as peer:
            # Even a spoofed name and an idle non-transactional connection are
            # not migration ownership. The actual live connections own that.
            peer.exec_driver_sql("SET application_name='qt.storage.online.controller.v1'")
            peer.commit()
            with pytest.raises(RuntimeError, match="sql_publishers_not_drained"):
                worker.commit_database(deadline=time.monotonic()+20)
            assert worker.state == "commit_unknown"
            assert not worker.reconcile_database()["database_handoff_committed"]
            with engine.connect() as conn:
                assert conn.scalar(text("SELECT to_regnamespace('qt_fact_header_retained_v1')")) is None
                assert capture.inspect_capture(conn)["started_at"] == settings["expected_started_at"]
    finally:
        other.dispose()


def test_controller_sql_peer_arriving_during_switch_rolls_back(storage, tmp_path, monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    other = create_engine(engine.url, poolclass=NullPool)
    peer = []
    with engine.connect() as conn:
        frozen = _frozen_records(conn)
    def arriving(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("ALTER TABLE market.fact_versions SET SCHEMA") and not peer:
            peer.append(other.connect())
            peer[0].exec_driver_sql("SELECT 1")
            peer[0].commit()
    try:
        with OnlineController(engine, **settings) as worker:
            _drain(worker);_reprove(worker)
            event.listen(engine, "after_cursor_execute", arriving)
            try:
                with pytest.raises(RuntimeError, match="sql_publishers_not_drained"):
                    worker.commit_database(deadline=time.monotonic()+30)
            finally:
                event.remove(engine, "after_cursor_execute", arriving)
            assert peer  # The real rename happened before the second refusal.
            assert not worker.reconcile_database()["database_handoff_committed"]
            with engine.connect() as conn:
                assert conn.scalar(text("SELECT to_regnamespace('qt_fact_header_retained_v1')")) is None
                assert _frozen_records(conn) == frozen
                assert capture.inspect_capture(conn)["started_at"] == settings["expected_started_at"]
    finally:
        for conn in peer:conn.close()
        other.dispose()



def test_controller_prepared_transaction_refuses_without_client(storage, tmp_path, monkeypatch):
    from uuid import uuid4
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    engine, settings, source = _setup(storage, tmp_path, monkeypatch, stage=False)
    with engine.begin() as conn:
        if int(conn.scalar(text("SHOW max_prepared_transactions"))) == 0:
            pytest.skip("requires disposable max_prepared_transactions > 0")
        conn.exec_driver_sql("CREATE TABLE public.qt_owned_prepared_probe(id integer PRIMARY KEY)")
    gid = "qt_storage_"+uuid4().hex
    other = create_engine(engine.url, poolclass=NullPool)
    pending = False
    try:
        raw = other.raw_connection()
        try:
            cursor = raw.cursor()
            cursor.execute("INSERT INTO public.qt_owned_prepared_probe VALUES(1)")
            cursor.execute("PREPARE TRANSACTION '"+gid+"'")
            pending = True
            cursor.close()
        finally:
            raw.close()
        with OnlineController(engine, **settings) as worker:
            with pytest.raises(RuntimeError, match="sql_publishers_not_drained"):
                worker.commit_database(deadline=time.monotonic()+20)
            assert not worker.reconcile_database()["database_handoff_committed"]
            with engine.connect() as conn:
                assert conn.scalar(text("SELECT count(*) FROM pg_prepared_xacts WHERE gid=:gid"), {"gid":gid}) == 1
                assert conn.scalar(text("SELECT count(*) FROM public.qt_owned_prepared_probe")) == 0
    finally:
        if pending:
            with other.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.exec_driver_sql("ROLLBACK PREPARED '"+gid+"'")
        other.dispose()


@pytest.mark.parametrize("fault", ["lost_reply", "session_loss"])
def test_final_session_switch_and_inspection_with_new_logins_closed(
        storage, tmp_path, monkeypatch, fault):
    """Fixture-owned login gate only; no host gate/restart authority implied."""
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    with engine.begin() as conn:
        frozen = _frozen_records(conn)
        original_capture = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
        database = conn.scalar(text("SELECT current_database()"))
        assert conn.scalar(text("SELECT datallowconn FROM pg_database WHERE datname=current_database()"))
    quoted = engine.dialect.identifier_preparer.quote_identifier(database)
    outsider = create_engine(engine.url, poolclass=NullPool, connect_args={"connect_timeout":2})
    control = create_engine(engine.url.set(database="postgres"), poolclass=NullPool,
                            isolation_level="AUTOCOMMIT", connect_args={"connect_timeout":2})
    try:
        with OnlineController(engine, **settings) as worker:
            _drain(worker);_reprove(worker)
            deadline = time.monotonic()+30
            with worker.final_database_session(deadline=deadline):
                connection = worker._final_connection
                pid = worker._final_pid
                with control.connect() as admin:
                    admin.exec_driver_sql("SET statement_timeout = '5s'")
                    admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS false")
                denied = []
                def new_login_refused():
                    with pytest.raises(DBAPIError, match="not currently accepting connections"):
                        with outsider.connect():pytest.fail("new login bypassed fixture gate")
                    denied.append(True)
                try:
                    new_login_refused()
                    assert worker.inspect_outcome(deadline=time.monotonic()+4)["outcome"] == "uncommitted"
                    assert worker._final_connection is connection
                    if fault == "session_loss":
                        connection.invalidate()
                        with monkeypatch.context() as patch:
                            patch.setattr(engine, "connect", lambda: pytest.fail("lost final session reconnected"))
                            with pytest.raises(RuntimeError, match="final_session_lost"):
                                worker.inspect_outcome(deadline=time.monotonic()+4)
                            with pytest.raises(RuntimeError, match="final_session_lost"):
                                worker.commit_database(deadline=deadline)
                    else:
                        from market_data.archive import FilesystemRawArchiveObjectStore
                        import hashlib
                        namespace_source = tmp_path / "namespace-source"
                        namespace_source.write_bytes(b"fixture namespace object outside catalog")
                        FilesystemRawArchiveObjectStore(worker.destination_root).put_verified(
                            object_key="retained/item", source_path=namespace_source,
                            expected_sha256=hashlib.sha256(namespace_source.read_bytes()).hexdigest())
                        original_commit = Connection._commit_impl
                        switched = set()
                        lost = []
                        def after_statement(conn, cursor, statement, parameters, context, executemany):
                            if statement.startswith("ALTER TABLE market.fact_versions SET SCHEMA"):
                                assert conn is connection
                                new_login_refused()
                                from tests.test_market_data.test_archive_namespace import _peer
                                # A process that starts AFTER the real rename cannot
                                # publish or retire destination names while this SAME
                                # controller retains its namespace fence.
                                _peer(worker.destination_root, namespace_source, blocked=True)
                                switched.add(id(conn))
                        def lose_reply(conn):
                            original_commit(conn)
                            if id(conn) in switched and not lost:
                                lost.append(True)
                                raise RuntimeError("closed-login actual COMMIT reply lost")
                        event.listen(engine, "after_cursor_execute", after_statement)
                        try:
                            with monkeypatch.context() as patch:
                                patch.setattr(Connection, "_commit_impl", lose_reply)
                                with pytest.raises(RuntimeError, match="actual COMMIT reply lost"):
                                    worker.commit_database(deadline=deadline)
                        finally:event.remove(engine, "after_cursor_execute", after_statement)
                        assert lost and worker.state == "commit_unknown"
                        new_login_refused()
                        result = worker.inspect_outcome(deadline=time.monotonic()+4)
                        assert result["outcome"] == "committed"
                        assert not result["collection_resume_authorized"]
                        assert not result["runtime_activation_authorized"]
                        assert worker._final_connection is connection and worker._final_pid == pid
                        # Lost COMMIT reply and fresh committed inspection do not
                        # release archive ownership or authorize another publisher.
                        from tests.test_market_data.test_archive_namespace import _peer
                        _peer(worker.destination_root, namespace_source, blocked=True)
                        assert len(denied) == 3
                        worker.proof.verify_all()
                    assert worker._final_deadline == deadline
                    with worker._owner.begin():
                        assert not worker._owner.scalar(text(
                            "SELECT datallowconn FROM pg_database WHERE datname=current_database()"))
                        assert dict(worker._owner.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one()) == original_capture
                finally:
                    # Disposable fixture cleanup, not production gate reconciliation.
                    with control.connect() as admin:
                        admin.exec_driver_sql("SET statement_timeout = '5s'")
                        admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS true")
            with pytest.raises(RuntimeError, match="final_session_state_invalid"):
                with worker.final_database_session(deadline=deadline):pytest.fail("session replay")
            assert worker._final_connection is None and connection.closed
        if fault == "lost_reply":
            _peer(settings["destination_root"], namespace_source, blocked=False)
        with engine.begin() as conn:
            assert _frozen_records(conn) == frozen
            assert conn.scalar(text("SELECT datallowconn FROM pg_database WHERE datname=current_database()"))
    finally:
        outsider.dispose()
        control.dispose()


def test_gated_timescale_job_stop_preserves_schedule_and_committed_work(storage, tmp_path, monkeypatch):
    """Actual scheduled publisher, retained switch session and fixture-only restart."""
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    control = create_engine(engine.url.set(database="postgres"), poolclass=NullPool,
                            isolation_level="AUTOCOMMIT", connect_args={"connect_timeout":2})
    with engine.begin() as conn:
        frozen = _frozen_records(conn)
        original = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
        database = conn.scalar(text("SELECT current_database()"))
        conn.exec_driver_sql("CREATE TABLE public.qt_job_progress(kind text)")
        conn.exec_driver_sql("CREATE TABLE public.qt_job_release(released boolean)")
        conn.exec_driver_sql("INSERT INTO public.qt_job_release VALUES(false)")
        conn.exec_driver_sql("""CREATE PROCEDURE public.qt_owned_storage_job(job_id int, config jsonb)
            LANGUAGE plpgsql AS $$ BEGIN
            INSERT INTO public.qt_job_progress VALUES('started'); COMMIT;
            IF NOT (SELECT released FROM public.qt_job_release) THEN
                PERFORM pg_sleep(60);
            END IF;
            INSERT INTO public.qt_job_progress VALUES('finished');
            END $$""")
        job = conn.scalar(text("SELECT add_job('public.qt_owned_storage_job','1 hour',initial_start=>now())"))
    quoted = engine.dialect.identifier_preparer.quote_identifier(database)
    def wait_for(conn, sql, seconds=15):
        until = time.monotonic()+seconds
        while time.monotonic() < until:
            with conn.begin():
                conn.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                conn.exec_driver_sql("SELECT pg_stat_clear_snapshot()")
                if conn.scalar(text(sql)):return
            time.sleep(.05)
        with conn.begin():
            for label, diagnostic in {
                "stats":"SELECT to_jsonb(s) FROM timescaledb_information.job_stats s",
                "errors":"SELECT to_jsonb(s) FROM timescaledb_information.job_errors s LIMIT 10",
                "activity":"SELECT json_build_object('type',backend_type,'state',state,'wait',wait_event) FROM pg_stat_activity WHERE datname=current_database()",
                "progress":"SELECT to_jsonb(s) FROM public.qt_job_progress s"}.items():
                print("JOB_DIAGNOSTIC="+label+":"+json.dumps(conn.scalars(text(diagnostic)).all(),default=str))
        pytest.fail("owned scheduled job did not reach expected lifecycle")
    try:
        with OnlineController(engine, **settings) as worker:
            _drain(worker);_reprove(worker)
            deadline = time.monotonic()+30
            with worker.final_database_session(deadline=deadline):
                conn = worker._final_connection
                wait_for(conn, "SELECT EXISTS(SELECT 1 FROM public.qt_job_progress WHERE kind='started')")
                with conn.begin():
                    before = worker._job_catalog(conn)
                    assert conn.scalar(text("SELECT count(*) FROM public.qt_job_progress WHERE kind='finished'")) == 0
                with control.connect() as admin:
                    admin.exec_driver_sql("SET statement_timeout='5s'")
                    admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS false")
                try:
                    # The already connected scheduled job remains inside the
                    # closed database. No application-name exemption is used.
                    with conn.begin():
                        assert conn.scalar(text("""SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                            WHERE datname=current_database() AND backend_type<>'client backend')"""))
                    result = worker.quiesce_final_database_jobs(deadline=deadline)
                    assert result["database_jobs_stopped"] and result["job_definitions_preserved"]
                    assert not result["database_switch_authorized"]
                    with conn.begin():
                        assert worker._job_catalog(conn) == before
                        assert conn.scalar(text("SELECT count(*) FROM public.qt_job_progress WHERE kind='started'")) == 1
                        assert conn.scalar(text("SELECT count(*) FROM public.qt_job_progress WHERE kind='finished'")) == 0
                    with pytest.raises(RuntimeError, match="closed_gate_required"):
                        worker.quiesce_final_database_jobs(deadline=deadline)
                    worker.commit_database(deadline=deadline)
                    assert worker.inspect_outcome(deadline=min(deadline,time.monotonic()+4))["outcome"] == "committed"
                    with conn.begin():
                        assert worker._job_catalog(conn) == before
                        assert _frozen_records(conn) == frozen
                        assert dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one()) == original
                finally:
                    # Fixture-owned restoration only; never production authority.
                    with control.connect() as admin:
                        admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS true")
        with engine.begin() as conn:
            conn.exec_driver_sql("UPDATE public.qt_job_release SET released=true")
            assert conn.scalar(text("SELECT _timescaledb_functions.start_background_workers()"))
        with engine.connect() as conn:
            wait_for(conn, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND backend_type<>'client backend')")
            with conn.begin():
                conn.execute(text("SELECT alter_job(:id,next_start=>now())"), {"id":job})
                print("JOB_RESTART="+json.dumps([dict(r) for r in conn.execute(text(
                    "SELECT * FROM timescaledb_information.job_stats WHERE job_id=:id"), {"id":job}).mappings()],default=str))
            # Timescale 2.14.2 intentionally backs off a terminated job for at
            # least five minutes, with positive retry jitter up to about 13%.
            # A prior 325-second observation expired; this new fixture allows
            # 360 seconds for this separate recovery measurement. This is AFTER the original final context has
            # closed, solely fixture scheduler recovery, never a pause renewal.
            restarted_at = time.monotonic()
            wait_for(conn,"SELECT EXISTS(SELECT 1 FROM public.qt_job_progress WHERE kind='finished')", seconds=360)
            print("JOB_RESTART_SECONDS="+str(time.monotonic()-restarted_at))
            with conn.begin():
                assert worker._job_catalog(conn) == before
                assert conn.scalar(text("SELECT count(*) FROM public.qt_job_progress WHERE kind='started'")) >= 2
        print("GATED_JOB=started_before_gate,stopped_with_committed_work_preserved,same_session_commit,fixture_scheduler_resumed")
    finally:
        control.dispose()


@pytest.mark.parametrize("fault", [None, "archive_failure", "gate_drift"])
def test_gated_residual_catchup_reuses_session_and_preserves_pages(storage, tmp_path, monkeypatch, fault):
    """Real late QT publication, closed logins, same-backend residual pages."""
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    with engine.begin() as conn:
        frozen = _frozen_records(conn)
        original = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
        database = conn.scalar(text("SELECT current_database()"))
    quoted = engine.dialect.identifier_preparer.quote_identifier(database)
    control = create_engine(engine.url.set(database="postgres"), poolclass=NullPool,
                            isolation_level="AUTOCOMMIT", connect_args={"connect_timeout":2})
    try:
        with OnlineController(engine, **settings) as worker:
            _drain(worker); _reprove(worker)
            deadline = time.monotonic()+30
            worker.final_delta(deadline=deadline)
            # Publication after the previous tail observation must survive job
            # retirement. This real QT publisher is not a scheduled-job fixture.
            _raw_book_fixture(storage, source, monkeypatch,
                definition_id="gated-residual", provider_product_id="BTC-USD-RESIDUAL",
                event_start=BASE+timedelta(hours=3))
            with engine.begin() as conn:
                pending = conn.scalar(text(f"SELECT count(*) FROM {online.QUEUE}"))
                assert pending > 0
            with worker.final_database_session(deadline=deadline):
                connection, pid = worker._final_connection, worker._final_pid
                engine.dispose()  # Only fixture-idle pool sessions; live owners remain.
                with control.connect() as admin:
                    admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS false")
                worker.quiesce_final_database_jobs(deadline=deadline)
                copy_page = archives._copy_archive_page
                entered = []
                def interrupted(*args, **kwargs):
                    record = kwargs["record_page"]
                    def fail(conn, rows):
                        entered.append(True)
                        assert conn is connection
                        record(conn, rows)
                        raise RuntimeError("fixture residual archive transaction interrupted")
                    return copy_page(*args, **(kwargs | {"record_page": fail}))
                if fault == "gate_drift":
                    with control.connect() as admin:
                        admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS true")
                with monkeypatch.context() as patch:
                    patch.setattr(engine, "connect", lambda: pytest.fail("residual work opened a new session"))
                    if fault == "archive_failure":patch.setattr(archives, "_copy_archive_page", interrupted)
                    if fault:
                        with pytest.raises(RuntimeError):worker.final_delta(deadline=deadline)
                        assert worker.state == "failed"
                    else:
                        for _ in range(8):
                            reply = worker.command(dict(controller_id=worker.controller_id,
                                sequence=worker._sequence+1,operation="final_delta",deadline=deadline))
                            result = reply["result"]
                            assert not result["final_switch_authorized"]
                            if (result["sql"]["outcome"] == "both_tails_observed_empty" and
                                    all(x["captured_tail_empty_at_observation"] for x in result["archives"])):break
                        else:pytest.fail("tiny gated residual did not converge")
                        worker.commit_database(deadline=deadline)
                        assert worker.inspect_outcome(deadline=min(deadline,time.monotonic()+4))["outcome"] == "committed"
                    assert worker._final_connection is connection and worker._final_pid == pid
                    assert worker._final_deadline == deadline
                    assert not connection.closed
                with worker._owner.begin():
                    assert dict(worker._owner.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one()) == original
                    if fault == "archive_failure":
                        assert entered
                        assert worker._owner.scalar(text(f"SELECT count(*) FROM {capture.QUEUE}")) == 0
                        assert worker._owner.scalar(text(f"SELECT count(*) FROM {online.QUEUE}")) == pending
                    if fault == "gate_drift":
                        assert worker._owner.scalar(text(f"SELECT count(*) FROM {online.QUEUE}")) == pending
            assert connection.closed
        # Only disposable cleanup after both controller connections retire.
        with control.connect() as admin:
            admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS true")
        with engine.begin() as conn:assert _frozen_records(conn) == frozen
    finally:
        with control.connect() as admin:
            admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS true")
        control.dispose()


def test_operator_admits_only_pinned_builtin_jobs(storage, tmp_path, monkeypatch):
    """Actual catalog and SQL bodies, with every unsupported change rolled back."""
    engine, settings, _ = _setup(storage, tmp_path, monkeypatch, stage=False)
    with OnlineController(engine, **settings) as worker:
        worker.admit_builtin_database_jobs()
        admitted = worker._builtin_catalog
        assert admitted and worker.state == "background" and not worker._jobs_stop_requested
        with engine.connect() as conn, conn.begin():
            assert worker._supported_builtin_catalog(conn) == admitted
            mutations = (
                "UPDATE _timescaledb_config.bgw_job SET config=NULL WHERE id=2",
                "UPDATE _timescaledb_config.bgw_job SET proc_name='custom_job' WHERE id=2",
                "UPDATE _timescaledb_config.bgw_job SET scheduled=false WHERE id=1",
                "UPDATE _timescaledb_config.bgw_job SET schedule_interval=INTERVAL '1 minute' WHERE id=2",
                "CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_job_error_retention_check(config jsonb) RETURNS void LANGUAGE plpgsql SET search_path TO pg_catalog, pg_temp AS 'BEGIN RETURN; END'",
            )
            for mutation in mutations:
                point = conn.begin_nested()
                conn.exec_driver_sql(mutation)
                with pytest.raises(RuntimeError, match="builtin_jobs_unqualified"):
                    worker._check_builtin_jobs(conn)
                point.rollback()
                assert worker._supported_builtin_catalog(conn) == admitted
            point = conn.begin_nested()
            conn.exec_driver_sql("UPDATE _timescaledb_config.bgw_job SET application_name='changed builtin' WHERE id=2")
            with pytest.raises(RuntimeError, match="builtin_job_definitions_changed"):
                worker._check_builtin_jobs(conn)
            point.rollback()
            assert worker._supported_builtin_catalog(conn) == admitted
            assert conn.scalar(text("SELECT datallowconn FROM pg_database WHERE datname=current_database()"))
        assert not worker._jobs_stop_requested and not worker._jobs_stopped


@pytest.mark.parametrize("lost_commit_reply", [False, True])
def test_held_host_dispatch_commits_once_and_reconciles_same_worker(
        storage, tmp_path, monkeypatch, lost_commit_reply):
    """Real SQL/leases/kernel hold; host Docker observations are fixture adapters.

    This connects final intent/gate/residual/COMMIT/outcome on one actual worker.
    It does not certify a production source image or the later recovery mounts.
    """
    from contextlib import ExitStack
    from pathlib import Path
    import stat
    from sqlalchemy import create_engine, literal
    from sqlalchemy.pool import NullPool
    from scripts.automation import storage_host_boundary as host
    from scripts.automation import storage_online_final as final

    engine, settings, source = _setup(storage, tmp_path, monkeypatch)
    state = tmp_path / "host-state"
    state.mkdir(mode=0o700)
    with engine.begin() as conn:
        frozen = _frozen_records(conn)
        original_capture = conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c WHERE id=1"))
        database = conn.scalar(text("SELECT current_database()"))
    quoted = engine.dialect.identifier_preparer.quote_identifier(database)
    target = str(literal(database).compile(dialect=engine.dialect, compile_kwargs={"literal_binds": True}))
    control = create_engine(engine.url.set(database="postgres"), poolclass=NullPool,
                            isolation_level="AUTOCOMMIT", connect_args={"connect_timeout": 2})
    rows = {name: dict(id=f"{index+1:064x}", running=name != "initialize",
                      status="exited" if name == "initialize" else "running", exit_code=0)
            for index, name in enumerate(host.STOP+("tsdb",))}
    root_metadata = {str(p): [p.stat().st_dev, p.stat().st_ino, p.stat().st_uid,
                            p.stat().st_gid, stat.S_IMODE(p.stat().st_mode)]
                     for p in [source, source / "objects"]}
    preparation = dict(source_roots=root_metadata, clients={n: dict(was_running=rows[n]["running"]) for n in host.STOP})
    image = "sha256:"+"a"*64  # Controlled Docker adapter, not a release image claim.
    revision = "a"*40
    root_target = "/app/logs/market-structure"
    details = {rows[service]["id"]: dict(image=image,
        config=dict(Entrypoint=None, Cmd=["python", "-m", module], Env=[
            "QT_IMAGE_SOURCE_REVISION="+revision, "QT_STORAGE_SOURCE_FENCE_ROOT="+root_target,
            "MARKET_STRUCTURE_STORAGE_ROOT="+root_target]),
        mounts=[dict(Type="bind", Source=str(source), Destination=root_target, RW=True)])
        for service, module in final._SOURCE_WRITERS.items()}
    monkeypatch.setattr(host, "database_details", lambda identity: details[identity])
    monkeypatch.setattr(final.initial, "_source_healthy", lambda _rows: True)
    def stop_client(action, *args, **kwargs):
        assert action == "stop" and args[:4] == ("--signal", "SIGTERM", "--timeout", "-1")
        assert final._load(state/final.STATE)["phase"] == "stopping"
        row = next(row for row in rows.values() if row["id"] == args[-1])
        row.update(running=False, status="exited")
        return ""
    monkeypatch.setattr(host, "docker", stop_client)
    def maintenance(identity, sql):
        assert identity == rows['tsdb']['id']
        with control.connect() as admin:
            admin.exec_driver_sql("SET statement_timeout = '5s'")
            if "ALTER DATABASE" in sql:
                assert "ALLOW_CONNECTIONS false" in sql
                assert final._load(state/final.STATE)['phase'] == 'login_closing'
                admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS false")
            return json.dumps(admin.exec_driver_sql(final._GATE_OBSERVE.replace(":'target'", target)).scalar_one())
    monkeypatch.setattr(host, "maintenance_query", maintenance)
    try:
        with ExitStack() as holds:
            with OnlineController(engine, **settings) as worker:
                _drain(worker);_reprove(worker)
                binding = dict(project="qt-held-fixture", source_revision=revision,
                    worker_id="b"*64, controller_id=worker.controller_id, capture=original_capture)
                def observe(_state, **kwargs):
                    assert _state == state
                    assert all(kwargs[k] == binding[k] for k in ('project','source_revision','worker_id','controller_id'))
                    if 'session' in kwargs: assert kwargs['session']['capture'] == original_capture
                    return binding, preparation, rows, dict(seconds=60, capture_deadline=time.time()+60)
                monkeypatch.setattr(final, "_observe", observe)
                calls = []
                def exchange(operation, **kwargs):
                    calls.append(operation)
                    request = dict(controller_id=worker.controller_id, sequence=worker._sequence+1, operation=operation)
                    if 'deadline' in kwargs: request['deadline'] = kwargs['deadline']
                    if operation == 'commit_database':
                        intent = final._load(state/final.STATE)
                        assert intent['phase'] == 'commit_dispatching'
                        assert intent['commit']['worker_sequence'] == request['sequence']
                    return worker.command(request)
                paused = final.stop_online_source_locked(state,
                    **{k: binding[k] for k in ('project','source_revision','worker_id','controller_id')},
                    max_duration_seconds=30)
                check = holds.enter_context(final.held_source_writers_locked(state, source_image=image))
                deadline = time.monotonic()+final._remaining(paused)-1
                exchange('final_delta', deadline=deadline)
                final.record_switch_entry_locked(state, deadline=deadline,
                    observe_worker=lambda **kw: exchange('status'))
                with control.connect() as admin:
                    preparation['cluster'] = str(admin.exec_driver_sql('SELECT system_identifier FROM pg_control_system()').scalar_one())
                final.close_database_logins_locked(state, exchange=exchange)
                connection = worker._final_connection
                final.copy_final_delta_locked(state, exchange=exchange, deadline=deadline, max_rounds=2)
                actual_commit = Connection._commit_impl
                renamed = []
                lost = []
                def after_statement(conn, cursor, statement, parameters, context, executemany):
                    if statement.startswith('ALTER TABLE market.fact_versions SET SCHEMA'):
                        assert conn is connection
                        renamed.append(True)
                def commit(conn):
                    actual_commit(conn)
                    if lost_commit_reply and conn is connection and renamed and not lost:
                        lost.append(True)
                        raise RuntimeError('fixture lost actual COMMIT result')
                event.listen(engine, 'after_cursor_execute', after_statement)
                try:
                    with monkeypatch.context() as patch:
                        patch.setattr(Connection, '_commit_impl', commit)
                        outcome = final.commit_online_handoff_locked(state, exchange=exchange)
                finally:
                    event.remove(engine, 'after_cursor_execute', after_statement)
                assert renamed and bool(lost) == lost_commit_reply
                assert outcome['outcome'] == 'committed'
                assert outcome['database_handoff_committed'] is True
                assert not outcome['collection_resume_authorized'] and not outcome['runtime_activation_authorized']
                receipt = final._load(state/final.STATE)
                assert receipt['phase'] == 'committed'
                assert receipt['deadline'] == paused['deadline'] and receipt['deadline_boot'] == paused['deadline_boot']
                assert worker._final_connection is connection
                assert calls.count('commit_database') == 1
                with pytest.raises(RuntimeError, match='commit_closed_gate_required'):
                    final.commit_online_handoff_locked(state, exchange=exchange)
                assert calls.count('commit_database') == 1
                worker.proof.verify_all()
                check()
            # Host kernel hold is still alive after controller/proof retirement.
            check()
        with control.connect() as admin:
            admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS true")
        with engine.begin() as conn:
            assert _frozen_records(conn) == frozen
            assert conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c WHERE id=1")) == original_capture
        assert final._load(state/final.STATE)['phase'] == 'committed'
    finally:
        with control.connect() as admin:
            admin.exec_driver_sql("ALTER DATABASE "+quoted+" ALLOW_CONNECTIONS true")
        control.dispose()


def test_controller_atomic_policy_rolls_back_with_switch_and_reconciles_lost_reply(storage, tmp_path, monkeypatch):
    engine, settings, _ = _setup(storage, tmp_path, monkeypatch)
    inspect_args = {k: settings[k] for k in ("policy", "source_root", "destination_root")}
    with engine.begin() as conn:
        frozen = _frozen_records(conn)
    actual_policy = handoff._stage_initial_policy
    def interrupted_policy(*args, **kwargs):
        result = actual_policy(*args, **kwargs)
        assert result["policy_activated"] and result["policy_current"]
        raise RuntimeError("owned fixture interruption after initial policy staging")
    with OnlineController(engine, **settings) as worker:
        _drain(worker)
        _reprove(worker)
        with monkeypatch.context() as interrupted:
            interrupted.setattr(handoff, "_stage_initial_policy", interrupted_policy)
            with pytest.raises(RuntimeError, match="interruption after initial policy staging"):
                worker.commit_database(deadline=time.monotonic()+30, activate_policy=True)
        assert worker.state == "commit_unknown"
    with engine.begin() as conn:
        assert not handoff.inspect_handoff(conn, **inspect_args)["database_handoff_committed"]
        for table in ("portal_storage_targets", "portal_storage_policy", "portal_storage_header_tablespaces", "portal_storage_plans"):
            assert conn.scalar(text("SELECT count(*) FROM public."+table)) == 0
        assert _frozen_records(conn) == frozen
    engine.dispose()  # Retire diagnostic connections before fresh publisher admission.
    with OnlineController(engine, **settings) as worker:
        _reprove(worker)
        actual_commit = handoff.commit_handoff
        def lost_reply(*args, **kwargs):
            result = actual_commit(*args, **kwargs)
            assert result["initial_policy_activated"]
            raise RuntimeError("owned fixture lost atomic switch reply")
        with monkeypatch.context() as lost:
            lost.setattr(handoff, "commit_handoff", lost_reply)
            with pytest.raises(RuntimeError, match="lost atomic switch reply"):
                worker.commit_database(deadline=time.monotonic()+30, activate_policy=True)
        outcome = worker.inspect_outcome(deadline=time.monotonic()+4)
        assert outcome["database_handoff_committed"] and outcome["initial_policy_activated"]
        assert not outcome["collection_resume_authorized"] and not outcome["runtime_activation_authorized"]
    with engine.begin() as conn:
        assert handoff.inspect_handoff_policy(conn, **inspect_args)["policy_current"]
        assert conn.scalar(text("SELECT count(*) FROM public.portal_storage_header_tablespaces")) == 1
        assert _frozen_records(conn) == frozen


def test_operation_catches_late_baseline_queue_before_identity_fence(storage, tmp_path, monkeypatch):
    from scripts.automation.storage_online_operation import prepare_background
    from tests.test_market_data.test_fact_header_online_prepare_db import _setup
    from tests.test_market_data.test_fact_header_copy_db import _insert
    from scripts.db import fact_header_v2_online as online
    from scripts.db import fact_header_v2_copy as headers
    engine, options, _ = _setup(storage, tmp_path, monkeypatch)
    prepared = online.prepare_attempt(engine, **options)
    with engine.connect() as conn:
        frozen = _frozen_records(conn)
    settings = {key: value for key, value in options.items() if key != 'attempt_seconds'}
    settings.update(expected_started_at=prepared['started_at'], max_objects=128,
                    max_bytes=64*1024**2, max_page_bytes=32*1024**2, page_rows=2, command_seconds=30)
    injected = []
    with OnlineController(engine, **settings) as worker:
        def exchange(operation, **kwargs):
            answer = worker.command(dict(controller_id=worker.controller_id,
                sequence=worker._sequence+1, operation=operation, **kwargs))
            report = answer.get('result', {})
            if operation == 'sql_copy' and report.get('phase') == 'header_baseline' and not injected:
                with engine.begin() as conn:
                    for i in range(10):
                        _insert(conn, storage, 'late-baseline-driver-'+str(i))
                    queued = conn.scalar(text('SELECT count(*) FROM '+headers.QUEUE))
                assert queued > worker.sql_page_rows
                injected.append(queued)
            return answer
        result = prepare_background(exchange, preparation_seconds=30)
        assert injected and not result['final_switch_authorized']
        with engine.connect() as conn:
            assert capture.inspect_capture(conn)['started_at'] == prepared['started_at']
            assert _frozen_records(conn) == frozen
            assert headers._inspect_progress(conn)['identity_capture']
            assert not conn.scalar(text('SELECT EXISTS(SELECT 1 FROM '+headers.QUEUE+')'))
            columns = ','.join(headers.HEADER_COLUMNS)
            assert not conn.scalar(text('SELECT EXISTS((SELECT '+columns+' FROM '+headers.SOURCE+
                ' EXCEPT SELECT '+columns+' FROM '+headers.SCHEMA+'.fact_versions) UNION ALL (SELECT '+
                columns+' FROM '+headers.SCHEMA+'.fact_versions EXCEPT SELECT '+columns+' FROM '+headers.SOURCE+'))'))
