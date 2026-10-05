"""Native retained-target adoption on owned two-filesystem storage fixtures.

Small correctness fixtures, not production HDD throughput or host admission.
"""
from dataclasses import replace
from datetime import timedelta
import os

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from market_data.canonical import build_fact_version_id
from scripts.db import fact_header_forward_adoption as adoption
from scripts.db import fact_header_forward_keys as keys
from scripts.db import fact_header_v2_references as references
from tests.test_market_data.tiered_v1_fixture import ensure_v1_payload_partition
from scripts.db import fact_header_v2_copy as headers, fact_header_v2_capture as capture
from scripts.db import fact_header_v2_cancel as cancellation, raw_mapping_v2_copy as raw
from tests.test_market_data.test_fact_header_copy_placement_db import placed, source
from tests.test_market_data.test_fact_header_copy_db import _finish as finish_headers, _insert, _frozen_records
from tests.test_market_data.test_raw_mapping_copy_db import _finish as finish_raw, _rows
from tests.test_market_data.test_fact_raw_lineage_db import _raw_trade_fixture, _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
    reason="requires owned two-filesystem storage-demo topology")]
CANCEL = "a" * 64
OPERATION = "b" * 64


@pytest.fixture
def retained(placed, tmp_path, monkeypatch):
    engine = placed.database._engine
    _raw_trade_fixture(placed, tmp_path, monkeypatch)
    with engine.begin() as conn:
        headers.prepare_copy(conn, placement=placed.copy_plan)
        raw.prepare_copy(conn)
    finish_headers(engine)
    finish_raw(engine)
    with engine.begin() as conn:
        original = cancellation._capture_binding(conn)
        cancellation.cancel_attempt(conn, expected_started_at=capture.inspect_capture(conn)["started_at"],
            expected_capture=original, intent_sha256=CANCEL)
    keys.prepare_keys(engine, expected_capture=original, intent_sha256=CANCEL)
    placed.original_capture = original
    placed.adoption_args = dict(expected_capture=original, cancellation_intent_sha256=CANCEL,
        operation_sha256=OPERATION, attempt_seconds=600)
    return placed


def _old(conn):
    return dict(headers=adoption._json_row(conn, headers.STATE), raw=adoption._json_row(conn, raw.STATE),
        capture=adoption._json_row(conn, capture.STATE), terminal=adoption._json_row(conn, capture.CANCELLED),
        queue=conn.execute(text("SELECT id FROM " + capture.QUEUE + " ORDER BY id")).scalars().all(),
        raw_queue=conn.execute(text("SELECT raw_record_id,manifest_id FROM " + raw.QUEUE + " ORDER BY 1,2")).all())


def _finish(engine):
    for _ in range(64):
        with engine.begin() as conn:
            result = adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
        if result["retained_targets_verified"]:
            assert not result["migration_ready"] and not result["final_switch_authorized"]
            return result
    pytest.fail("bounded adoption fixture did not finish")


def test_adoption_covers_uncaptured_gap_late_keys_and_interrupted_page(retained, tmp_path, monkeypatch):
    engine = retained.database._engine
    with engine.begin() as conn:
        original = _old(conn)
        frozen = _frozen_records(conn)
        _insert(conn, retained, "uncaptured-before-forward-adoption")
    _raw_book_fixture(retained, tmp_path, monkeypatch, definition_id="forward-gap")
    # Existing IDs alone are insufficient: the reverse physical scan must catch
    # a retained value that disagrees with its source before key-only coverage.
    with pytest.raises(RuntimeError, match="content_mismatch: identity:target"), engine.begin() as conn:
        conn.exec_driver_sql("UPDATE " + adoption.IDENTITY + " SET observation_key=observation_key||'-changed' "
            "WHERE id=(SELECT id FROM " + adoption.IDENTITY + " ORDER BY id LIMIT 1)")
        adoption.prepare_adoption(conn, **retained.adoption_args)
        for _ in range(64):
            adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
        pytest.fail("changed retained identity was accepted")
    child_sql = "CREATE TABLE " + keys.SCHEMA + ".unowned_identity_child () INHERITS (" + adoption.IDENTITY + ")"
    # No inherited child may be silently folded into a single-heap cursor.
    with pytest.raises(RuntimeError, match="identity_heap_required"), engine.begin() as conn:
        conn.exec_driver_sql(child_sql)
        adoption.prepare_adoption(conn, **retained.adoption_args)
    original_commit = Connection._commit_impl
    def lose_reply(conn):
        original_commit(conn)
        raise RuntimeError("lost adoption prepare commit reply")
    with monkeypatch.context() as lost:
        lost.setattr(Connection, "_commit_impl", lose_reply)
        with pytest.raises(RuntimeError, match="lost adoption prepare commit reply"), engine.begin() as conn:
            adoption.prepare_adoption(conn, **retained.adoption_args)
    with engine.begin() as conn:
        prepared = adoption._state(conn)
        assert adoption.prepare_adoption(conn, **retained.adoption_args)["reused"]
        assert adoption._state(conn) == prepared
        assert prepared["progress"]["identity_target"]["scan"] == adoption.IDENTITY_HEAP_SCAN
        assert prepared["progress"]["identity_target"]["high_block"] > 0
    # A physical row cursor commits with its exact comparison. Losing that
    # transaction neither skips a retained row nor advances source coverage.
    def interrupt_heap(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE " + adoption.STATE + " SET progress="):
            raise RuntimeError("interrupt identity heap cursor")
    event.listen(engine, "after_cursor_execute", interrupt_heap)
    try:
        with pytest.raises(RuntimeError, match="interrupt identity heap cursor"), engine.begin() as conn:
            adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
    finally:
        event.remove(engine, "after_cursor_execute", interrupt_heap)
    with engine.begin() as conn:
        assert adoption._state(conn) == prepared
        adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
        physical = adoption._state(conn)
        assert physical["progress"]["identity_target"]["after_tid"] is not None
        assert physical["progress"]["identity_target"]["verified"] == 1
        assert physical["progress"]["identity_source"]["after"] is None
    with pytest.raises(RuntimeError, match="identity_heap_required"), engine.begin() as conn:
        conn.exec_driver_sql(child_sql)
        adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
    with engine.begin() as conn:
        assert adoption._state(conn) == physical
    # Native heap rewrite invalidates a physical cursor even with identical rows.
    # This tiny owned rewrite rolls back; production never repairs it implicitly.
    with pytest.raises(RuntimeError, match="adoption_binding_changed"), engine.begin() as conn:
        primary = conn.scalar(text("SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
            "WHERE i.indrelid=CAST(:relation AS regclass) AND i.indisprimary"), {"relation":adoption.IDENTITY})
        conn.exec_driver_sql("CLUSTER " + adoption.IDENTITY + " USING " + conn.dialect.identifier_preparer.quote(primary))
        adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
    with engine.begin() as conn:
        assert adoption._state(conn) == physical
    failed = []
    def interrupt(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO " + adoption.IDENTITY) and "SELECT" in statement:
            failed.append(adoption._state(conn)["progress"])
            raise RuntimeError("interrupt missing identity after insert")
    event.listen(engine, "after_cursor_execute", interrupt)
    try:
        with pytest.raises(RuntimeError, match="interrupt missing identity after insert"):
            for _ in range(32):
                with engine.begin() as conn:
                    adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
    finally:
        event.remove(engine, "after_cursor_execute", interrupt)
    with engine.begin() as conn:
        assert adoption._state(conn)["progress"] == failed[0]
    # Finish the source-side identity cursor, then publish a key behind it.
    for _ in range(32):
        with engine.begin() as conn:
            adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
            current = adoption._state(conn)
        if current["progress"]["identity_source"]["complete"]:
            break
    else:
        pytest.fail("identity source scan did not finish")
    assert current["progress"]["identity_target"]["complete"]
    assert current["progress"]["identity_target"]["next_block"] == current["progress"]["identity_target"]["high_block"]
    high = current["progress"]["identity_source"]["high"][0]
    for number in range(10000):
        key = "late-forward-key-" + str(number)
        fact = replace(retained.fact, observation_key=key, observation_time=BASE + timedelta(days=2))
        identity = build_fact_version_id(series_id=retained.series_id, observation_key=key,
                                        revision=1, row_hash=fact.row_hash)
        if identity < high:
            break
    else:
        pytest.fail("could not construct owned late key behind cursor")
    with engine.begin() as conn:
        row = _insert(conn, retained, key)
        assert row["id"] == identity
        assert conn.scalar(text("SELECT EXISTS(SELECT 1 FROM " + adoption.IDENTITY + " WHERE id=:id)"), row)
    _raw_book_fixture(retained, tmp_path, monkeypatch, definition_id="forward-live", provider_product_id="ETH-USD")
    _finish(engine)
    with engine.begin() as conn:
        assert _rows(conn, raw.SOURCE) == _rows(conn, raw.TARGET)
        expected = conn.execute(text("SELECT " + ",".join(headers.IDENTITY_COLUMNS) + " FROM " + headers.SOURCE + " ORDER BY id")).all()
        actual = conn.execute(text("SELECT " + ",".join(headers.IDENTITY_COLUMNS) + " FROM " + adoption.IDENTITY + " ORDER BY id")).all()
        assert actual == expected
        assert _old(conn) == original
        assert _frozen_records(conn) == frozen
        final = adoption._state(conn)
        assert final["started_at"] == prepared["started_at"] and final["expires_at"] == prepared["expires_at"]
        assert conn.scalar(text("SELECT to_regclass('market.fact_header_legacy')")) is None
    # Stage references under the new owner after the old capture is canceled.
    # A lost ADD reply reuses its durable OID; old native source FKs survive.
    with engine.begin() as conn:
        originals = {row["relation"]: row["oid"] for row in references._incoming_references(conn)}
        initial = adoption.inspect_references(conn, operation_sha256=OPERATION)
        assert not initial["references_complete"]
    ordinary = [name for name in initial["references"] if name != references.PARENT]
    with monkeypatch.context() as lost:
        lost.setattr(Connection, "_commit_impl", lose_reply)
        with pytest.raises(RuntimeError, match="lost adoption prepare commit reply"), engine.begin() as conn:
            adoption.prepare_reference(conn, operation_sha256=OPERATION, relation=ordinary[0])
    for relation in ordinary:
        with engine.begin() as conn:
            adoption.prepare_reference(conn, operation_sha256=OPERATION, relation=relation)
    with engine.begin() as conn:
        staged = adoption._state(conn)
        assert adoption.prepare_reference(conn, operation_sha256=OPERATION, relation=ordinary[0])["reused"]
        assert adoption._state(conn) == staged
    # Rollback validation together with its progress; earlier staging survives.
    with pytest.raises(RuntimeError, match="interrupt forward validation"), engine.begin() as conn:
        adoption.validate_reference(conn, operation_sha256=OPERATION, relation=ordinary[0])
        raise RuntimeError("interrupt forward validation")
    with engine.begin() as conn:
        assert adoption._state(conn) == staged
    # Both an uncommitted writer and a newly committed writer coexist with
    # native validation locks, including synchronous identity mirroring.
    with engine.begin() as early:
        _insert(early, retained, "forward-reference-inflight")
        with engine.begin() as validator:
            for relation in ordinary:
                assert adoption.validate_reference(validator, operation_sha256=OPERATION, relation=relation)["validated"]
            with engine.begin() as writer:
                writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                _insert(writer, retained, "forward-reference-concurrent")
    retained.open_day += timedelta(days=3)
    with engine.begin() as conn:
        late_leaf = ensure_v1_payload_partition(conn, retained.open_day)
        _insert(conn, retained, "forward-reference-new-before-parent")
    with pytest.raises(RuntimeError, match="prevalidation_incomplete"), engine.begin() as conn:
        adoption.adopt_payload_references(conn, operation_sha256=OPERATION)
    with engine.begin() as conn:
        adoption.prepare_reference(conn, operation_sha256=OPERATION, relation=late_leaf)
    with engine.begin() as conn:
        adoption.validate_reference(conn, operation_sha256=OPERATION, relation=late_leaf)
        leaves = adoption._state(conn)["reference_progress"]
    with engine.begin() as conn:
        assert adoption.adopt_payload_references(conn, operation_sha256=OPERATION)["references_complete"]
        assert adoption.adopt_payload_references(conn, operation_sha256=OPERATION)["reused"]
        after = adoption.inspect_references(conn, operation_sha256=OPERATION)["references"]
        assert all(after[name]["oid"] == previous["oid"] for name, previous in leaves.items())
    retained.open_day += timedelta(days=1)
    with engine.begin() as conn:
        inherited_leaf = ensure_v1_payload_partition(conn, retained.open_day)
        _insert(conn, retained, "forward-reference-new-after-parent")
    with engine.begin() as conn:
        current = adoption.inspect_references(conn, operation_sha256=OPERATION)
        assert current["references_complete"] and current["references"][inherited_leaf]["convalidated"]
        header_fk = current["references"][headers.SOURCE]
        assert header_fk["columns"] == ["id", "storage_day"]
        assert header_fk["references"] == ["id", "storage_day"] and not header_fk["condeferrable"]
        assert not current["migration_ready"] and not current["final_switch_authorized"]
        assert all(row["oid"] == originals[row["relation"]] for row in references._incoming_references(conn)
                   if row["relation"] in originals)
        assert _old(conn) == original and _frozen_records(conn) == frozen
        final = adoption._state(conn)
        assert final["started_at"] == prepared["started_at"] and final["expires_at"] == prepared["expires_at"]
    # Referencing-side enforcement is checked even outside the mirrored tables.
    with engine.connect() as conn:
        tx = conn.begin()
        try:
            trigger = conn.scalar(text("SELECT t.tgname FROM pg_trigger t JOIN pg_constraint c ON c.oid=t.tgconstraint "
                "JOIN pg_proc p ON p.oid=t.tgfoid WHERE c.conrelid=to_regclass(:leaf) "
                "AND c.conname=:name AND p.proname='RI_FKey_check_ins'"),
                {"leaf": inherited_leaf, "name": references.STAGED})
            conn.exec_driver_sql("ALTER TABLE " + inherited_leaf + " DISABLE TRIGGER " +
                                conn.dialect.identifier_preparer.quote(trigger))
            with pytest.raises(RuntimeError, match="enforcement_changed"):
                adoption.inspect_references(conn, operation_sha256=OPERATION)
        finally:
            tx.rollback()
    # Changed ownership, native protection or clocks cannot become a new pass.
    with engine.begin() as conn:
        native_trigger = conn.scalar(text("SELECT tgname FROM pg_trigger WHERE tgrelid=to_regclass(:name) "
            "AND tgisinternal ORDER BY tgname LIMIT 1"), {"name": adoption.IDENTITY})
        assert native_trigger is not None
        native_disable = "ALTER TABLE " + adoption.IDENTITY + " DISABLE TRIGGER " + conn.dialect.identifier_preparer.quote(native_trigger)
    for sql, expected in (
        (native_disable, "binding_changed"),
        ("ALTER TABLE " + headers.SOURCE + ' DISABLE TRIGGER "' + adoption.IDENTITY_MIRROR + '"', "binding_changed"),
        ("ALTER TABLE " + adoption.IDENTITY + " DISABLE TRIGGER trg_qt_forward_identity_target_seal", "binding_changed"),
        ("UPDATE " + adoption.STATE + " SET started_at=started_at-interval '1 hour',expires_at=expires_at-interval '1 hour'", "adoption_expired"),
    ):
        with engine.connect() as conn:
            tx = conn.begin()
            try:
                conn.exec_driver_sql(sql)
                with pytest.raises(RuntimeError, match=expected):
                    adoption.adoption_page(conn, operation_sha256=OPERATION)
            finally:
                tx.rollback()
    with pytest.raises(RuntimeError, match="intent_changed"), engine.begin() as conn:
        adoption.prepare_adoption(conn, **{**retained.adoption_args, "attempt_seconds": 601})
    for relation in (headers.SOURCE, adoption.IDENTITY, raw.SOURCE, raw.TARGET):
        with pytest.raises(DBAPIError, match="immutable"), engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM " + relation)

    # Retirement is atomic even if trigger publication is interrupted.
    def interrupt_retirement(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("DROP TRIGGER trg_qt_forward_"):
            raise RuntimeError("interrupted adoption retirement")
    event.listen(engine, "after_cursor_execute", interrupt_retirement)
    try:
        with pytest.raises(RuntimeError, match="interrupted adoption retirement"), engine.begin() as conn:
            adoption.retire_adoption(conn, operation_sha256=OPERATION)
    finally:
        event.remove(engine, "after_cursor_execute", interrupt_retirement)
    with engine.begin() as conn:
        assert adoption._state(conn) == final
        assert adoption._snapshot(conn) == final["binding"]
    with monkeypatch.context() as lost:
        lost.setattr(Connection, "_commit_impl", lose_reply)
        with pytest.raises(RuntimeError, match="lost adoption prepare commit reply"), engine.begin() as conn:
            adoption.retire_adoption(conn, operation_sha256=OPERATION)
    with engine.begin() as conn:
        terminal = adoption._state(conn)
        assert adoption.retire_adoption(conn, operation_sha256=OPERATION)["reused"]
        assert adoption._state(conn) == terminal
        assert terminal["progress"] == final["progress"]
        assert terminal["started_at"] == final["started_at"] and terminal["expires_at"] == final["expires_at"]
        assert len(terminal["terminal"]["removed_triggers"]) == 8
        assert set(terminal["terminal"]["removed_reference_roots"]) == {
            headers.SOURCE, references.PARENT, "market.fact_archive_material_aliases",
            "market.fact_archive_canonical_dependencies"}
        assert not any(references._states(conn, references._native_inventory(conn, forward_header=True)).values())
        assert all(row["oid"] == originals[row["relation"]] for row in references._incoming_references(conn)
                   if row["relation"] in originals)
        preserved_raw = _rows(conn, raw.TARGET)
        preserved_identity = conn.execute(text("SELECT * FROM " + adoption.IDENTITY + " ORDER BY id")).all()
        _insert(conn, retained, "after-adoption-retirement")
    _raw_book_fixture(retained, tmp_path, monkeypatch, definition_id="after-adoption-retirement", provider_product_id="LTC-USD")
    with engine.begin() as conn:
        assert _rows(conn, raw.TARGET) == preserved_raw
        assert conn.execute(text("SELECT * FROM " + adoption.IDENTITY + " ORDER BY id")).all() == preserved_identity
        assert _old(conn) == original
        assert _frozen_records(conn) == frozen
    for action in (lambda conn: adoption.adoption_page(conn, operation_sha256=OPERATION),
                   lambda conn: adoption.prepare_adoption(conn, **retained.adoption_args)):
        with pytest.raises(RuntimeError, match="adoption_retired"), engine.begin() as conn:
            action(conn)


def test_adoption_reverse_scan_refuses_preexisting_foreign_target(retained):
    engine = retained.database._engine
    with engine.begin() as conn:
        conn.exec_driver_sql("INSERT INTO " + adoption.IDENTITY + "(id,storage_day,series_id,observation_key,revision) "
            "SELECT 'foreign-extra-target',storage_day,series_id,'foreign-extra-target',1 FROM " + headers.SOURCE + " LIMIT 1")
        adoption.prepare_adoption(conn, **retained.adoption_args)
    with pytest.raises(RuntimeError, match="content_mismatch: identity:target"):
        _finish(engine)
    with engine.begin() as conn:
        assert conn.scalar(text("SELECT EXISTS(SELECT 1 FROM " + adoption.IDENTITY + " WHERE id='foreign-extra-target')"))
        assert cancellation._capture_binding(conn) == retained.original_capture
        assert not adoption._report(adoption._state(conn), reused=True)["retained_targets_verified"]


def test_adoption_naturally_expired_phase_can_only_retire(retained):
    import time
    engine = retained.database._engine
    args = {**retained.adoption_args, "attempt_seconds": 45}
    with engine.begin() as conn:
        original = _old(conn)
        adoption.prepare_adoption(conn, **args)
    _finish(engine)
    with engine.begin() as conn:
        relation = next(name for name in references._native_inventory(conn, forward_header=True)
                        if name.startswith(references.PARENT + "_"))
        adoption.prepare_reference(conn, operation_sha256=OPERATION, relation=relation)
        prepared = adoption._state(conn)
        assert not prepared["reference_progress"][relation]["convalidated"]
        remaining = conn.scalar(text("SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM " + adoption.STATE))
    time.sleep(float(remaining) + 0.1)
    with pytest.raises(RuntimeError, match="adoption_expired"), engine.begin() as conn:
        adoption.adoption_page(conn, operation_sha256=OPERATION)
    with pytest.raises(RuntimeError, match="intent_changed"), engine.begin() as conn:
        adoption.retire_adoption(conn, operation_sha256="c" * 64)
    with engine.begin() as conn:
        assert adoption.retire_adoption(conn, operation_sha256=OPERATION)["retired"]
        terminal = adoption._state(conn)
        assert terminal["started_at"] == prepared["started_at"]
        assert terminal["expires_at"] == prepared["expires_at"]
        assert terminal["progress"] == prepared["progress"]
        assert terminal["terminal"]["removed_reference_roots"] == [relation]
        assert not any(references._states(conn, references._native_inventory(conn, forward_header=True)).values())
        assert _old(conn) == original
        _insert(conn, retained, "after-expired-adoption-retirement")



@pytest.mark.parametrize("held_relation", [headers.SOURCE, "market.raw_archive_manifests"])
def test_terminal_recovery_waits_for_writer_and_preserves_committed_rows(retained, held_relation):
    import queue
    import threading
    import time
    from scripts.automation import storage_online_terminal as terminal
    engine = retained.database._engine
    with engine.begin() as conn:
        adoption.prepare_adoption(conn, **retained.adoption_args)
        before = adoption._state(conn)
        frozen = _frozen_records(conn)
        old = _old(conn)
    ready = queue.Queue()
    outcome = queue.Queue()
    archive_contended = threading.Event()
    def observe_lock_error(context):
        if (getattr(context.original_exception, "pgcode", None) == "55P03"
                and "ACCESS EXCLUSIVE MODE NOWAIT" in (context.statement or "")):
            archive_contended.set()
    event.listen(engine, "handle_error", observe_lock_error)
    if held_relation != headers.SOURCE:
        with engine.begin() as conn:
            _insert(conn, retained, "writer-before-terminal-recovery")
    writer = engine.connect()
    transaction = writer.begin()
    if held_relation == headers.SOURCE:
        _insert(writer, retained, "writer-before-terminal-recovery")
    else:
        writer.exec_driver_sql("LOCK TABLE " + held_relation + " IN ROW EXCLUSIVE MODE")
    def recover():
        try:
            with engine.begin() as conn:
                ready.put(conn.scalar(text("SELECT pg_backend_pid()")))
                with adoption._step(conn, 10):
                    terminal._wait_for_retirement_sources(conn)
                    result = adoption.retire_adoption(conn, operation_sha256=OPERATION)
            outcome.put(result)
        except BaseException as exc:
            outcome.put(exc)
    thread = threading.Thread(target=recover)
    try:
        if held_relation == headers.SOURCE:
            with pytest.raises(DBAPIError) as failed, engine.begin() as conn:
                adoption.retire_adoption(conn, operation_sha256=OPERATION)
            assert failed.value.orig.pgcode == "55P03"
        thread.start()
        pid = ready.get(timeout=5)
        if held_relation == headers.SOURCE:
            deadline = time.monotonic()+5
            waiting = False
            while time.monotonic() < deadline:
                with engine.connect() as conn:
                    waiting = conn.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid "
                        "AND relation=CAST(:relation AS regclass) AND mode='AccessExclusiveLock' AND NOT granted)"),
                        {"pid": pid, "relation": held_relation})
                if waiting: break
                time.sleep(.02)
            assert waiting, "recovery did not queue behind the existing writer"
        else:
            assert archive_contended.wait(5), "archive publisher was not fenced"
            # The publisher can still visit Facts: recovery must release its
            # first gate whenever the rest of its lock set cannot be obtained.
            writer.exec_driver_sql("SET LOCAL lock_timeout='2s'")
            writer.exec_driver_sql("LOCK TABLE " + headers.SOURCE + " IN ROW EXCLUSIVE MODE")
        transaction.commit()
        thread.join(timeout=10)
        assert not thread.is_alive()
        result = outcome.get_nowait()
        if isinstance(result, BaseException): raise result
        assert result["retired"]
    finally:
        if transaction.is_active: transaction.rollback()
        writer.close()
        if thread.ident is not None: thread.join(timeout=12)
        event.remove(engine, "handle_error", observe_lock_error)
    with engine.begin() as conn:
        after = adoption._state(conn)
        assert after["terminal"]["rows_preserved"]
        assert all(after[name] == before[name] for name in ("started_at", "expires_at", "progress"))
        assert _old(conn) == old and _frozen_records(conn) == frozen
        assert conn.scalar(text("SELECT count(*) FROM " + headers.SOURCE + " WHERE observation_key='writer-before-terminal-recovery'")) == 1
        assert conn.scalar(text("SELECT count(*) FROM " + adoption.IDENTITY + " WHERE observation_key='writer-before-terminal-recovery'")) == 1
        _insert(conn, retained, "writer-after-terminal-recovery")
        assert conn.scalar(text("SELECT count(*) FROM " + adoption.IDENTITY + " WHERE observation_key='writer-after-terminal-recovery'")) == 0


def test_terminal_recovery_lock_wait_keeps_enclosing_deadline(retained):
    import time
    from scripts.automation import storage_online_terminal as terminal
    engine = retained.database._engine
    with engine.begin() as conn:
        adoption.prepare_adoption(conn, **retained.adoption_args)
        before = adoption._state(conn)
    with engine.connect() as writer:
        transaction = writer.begin()
        try:
            _insert(writer, retained, "held-terminal-recovery-writer")
            started = time.monotonic()
            with pytest.raises(DBAPIError), engine.begin() as conn:
                with adoption._step(conn, 1):
                    terminal._wait_for_retirement_sources(conn)
                    pytest.fail("expired recovery unexpectedly obtained its lock")
            assert time.monotonic()-started < 5
        finally:
            transaction.rollback()
    with engine.begin() as conn:
        assert adoption._state(conn) == before
        assert adoption._snapshot(conn) == before["binding"]


def test_successor_owns_new_proof_and_clocks_without_reopening_retired_attempt(retained, monkeypatch):
    engine = retained.database._engine
    successor = "c" * 64
    with engine.begin() as conn:
        adoption.prepare_adoption(conn, **retained.adoption_args)
        adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
        with pytest.raises(RuntimeError, match="successor_retirement_required"):
            adoption.prepare_adoption(conn, **{**retained.adoption_args, "operation_sha256": successor},
                predecessor_operation_sha256=OPERATION, predecessor_terminal_sha256="0" * 64)
        adoption.retire_adoption(conn, operation_sha256=OPERATION)
        previous = adoption._json_row(conn, adoption.STATE)
        old = _old(conn)
        frozen = _frozen_records(conn)
        terminal = adoption.inspect_retirement(conn, operation_sha256=OPERATION)
        terminal_hash = adoption._digest(terminal)
        _insert(conn, retained, "uncaptured-successor-gap")
    args = {**retained.adoption_args, "operation_sha256": successor,
            "predecessor_operation_sha256": OPERATION, "predecessor_terminal_sha256": terminal_hash}
    with pytest.raises(RuntimeError, match="retirement_required"), engine.begin() as conn:
        adoption.prepare_adoption(conn, **{**retained.adoption_args, "operation_sha256": successor})
    with pytest.raises(RuntimeError, match="retirement_required"), engine.begin() as conn:
        adoption.prepare_adoption(conn, **{**args, "predecessor_terminal_sha256": "0" * 64})
    schema = adoption.successor_schema(successor)
    with pytest.raises(RuntimeError, match="namespace_unowned"), engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA " + schema)
        adoption.prepare_adoption(conn, **args)
    with pytest.raises(RuntimeError, match="rollback successor prepare"), engine.begin() as conn:
        adoption.prepare_adoption(conn, **args)
        raise RuntimeError("rollback successor prepare")
    with engine.begin() as conn:
        assert conn.scalar(text("SELECT to_regnamespace(:name)"), {"name": schema}) is None
        assert adoption._json_row(conn, adoption.STATE) == previous
    commit = Connection._commit_impl
    def lose_reply(conn):
        commit(conn)
        raise RuntimeError("lost successor commit reply")
    with monkeypatch.context() as lost:
        lost.setattr(Connection, "_commit_impl", lose_reply)
        with pytest.raises(RuntimeError, match="lost successor commit reply"), engine.begin() as conn:
            adoption.prepare_adoption(conn, **args)
    with engine.begin() as conn:
        state = adoption._state(conn, successor)
        assert state["started_at"].isoformat() > previous["started_at"]
        assert state["progress"]["identity_target"]["verified"] == 0
        assert adoption.prepare_adoption(conn, **args)["reused"]
        assert adoption._state(conn, successor) == state
        assert adoption._json_row(conn, adoption.STATE) == previous
        with pytest.raises(RuntimeError, match="successor_intent_changed"):
            adoption.prepare_adoption(conn, **{**retained.adoption_args, "operation_sha256": successor})
        with pytest.raises(RuntimeError, match="adoption_retired"):
            adoption.adoption_page(conn, operation_sha256=OPERATION)
        _insert(conn, retained, "live-successor-publisher")
    with pytest.raises(RuntimeError, match="terminal_changed"), engine.begin() as conn:
        adoption.prepare_adoption(conn, **{**args, "operation_sha256": "d" * 64})
    with engine.begin() as conn:
        assert conn.scalar(text("SELECT to_regnamespace(:name)"),
                           {"name": adoption.successor_schema("d" * 64)}) is None
    # An old journal mutation cannot be accepted as the successor's lineage.
    with pytest.raises(RuntimeError, match="predecessor_changed"), engine.begin() as conn:
        conn.exec_driver_sql("UPDATE " + adoption.STATE + " SET progress='{}'::jsonb")
        adoption.adoption_page(conn, operation_sha256=successor)
    for _ in range(64):
        with engine.begin() as conn:
            result = adoption.adoption_page(conn, operation_sha256=successor, page_rows=1)
        if result["retained_targets_verified"]:
            break
    else:
        pytest.fail("successor proof did not finish within fixture budget")
    with engine.begin() as conn:
        assert adoption._json_row(conn, adoption.STATE) == previous
        assert _old(conn) == old and _frozen_records(conn) == frozen
        current = adoption._state(conn, successor)
        assert current["started_at"] == state["started_at"]
        assert current["expires_at"] == state["expires_at"]
        columns = ",".join(headers.IDENTITY_COLUMNS)
        assert conn.execute(text("SELECT " + columns + " FROM " + headers.SOURCE + " ORDER BY id")).all() == conn.execute(
            text("SELECT " + columns + " FROM " + adoption.IDENTITY + " ORDER BY id")).all()
        adoption.retire_adoption(conn, operation_sha256=successor)
        assert adoption.inspect_retirement(conn, operation_sha256=successor)["rows_preserved"]
        assert adoption._json_row(conn, adoption.STATE) == previous
        assert _old(conn) == old


def test_retained_lookup_placement_is_atomic_recoverable_and_explicit(retained, monkeypatch):
    from scripts.db import fact_header_forward_placement as lookup
    from scripts.db import fact_header_v2_placement as physical
    from tests.test_market_data.test_archive_reference_placement_db import _options
    engine = retained.database._engine
    plan = replace(retained.copy_plan, recent_lookup_indexes=True)
    options = dict(operation_sha256="d"*64, predecessor_operation_sha256=OPERATION,
        predecessor_terminal_sha256="e"*64, placement=plan, **_options(retained))
    with engine.begin() as conn:
        adoption.prepare_adoption(conn, **retained.adoption_args)
    with pytest.raises(RuntimeError, match="retirement_required"):
        lookup.move_lookup_indexes(engine, **options)
    with engine.begin() as conn:
        adoption.retire_adoption(conn, operation_sha256=OPERATION)
        terminal = adoption.inspect_retirement(conn, operation_sha256=OPERATION)
        old_state = adoption._json_row(conn, adoption.STATE)
        old = _old(conn)
        old_keys = adoption._json_row(conn, keys.STATE)
        frozen = _frozen_records(conn)
        before = adoption._snapshot(conn, OPERATION)
    options["predecessor_terminal_sha256"] = adoption._digest(terminal)
    # Another controller/storage owner must be refused, including idle owners.
    with engine.connect() as owner:
        owner.exec_driver_sql("SELECT pg_advisory_lock(hashtextextended('qt.storage.management.v1',0))")
        owner.commit()
        try:
            with pytest.raises(RuntimeError, match="owner_busy"):
                lookup.move_lookup_indexes(engine, **options)
        finally:
            owner.invalidate()
    with pytest.raises(RuntimeError, match="storage_move_cancelled"):
        lookup.move_lookup_indexes(engine, **options, cancelled=lambda: True)
    with engine.connect() as conn:
        assert lookup._row(conn) is None
    # Kill the real backend after its first ALTER. Source writes still commit;
    # PostgreSQL rolls every index back, and the durable original clock survives.
    interrupted = [False]
    def kill_first(conn, cursor, statement, parameters, context, executemany):
        if not interrupted[0] and statement.startswith("ALTER INDEX "+capture.SCHEMA+"."):
            interrupted[0] = True
            with engine.begin() as writer:
                writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                _insert(writer, retained, "collected-during-lookup-move")
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid": conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine, "after_cursor_execute", kill_first)
    try:
        with pytest.raises(DBAPIError):
            lookup.move_lookup_indexes(engine, **options)
    finally:
        event.remove(engine, "after_cursor_execute", kill_first)
    assert interrupted[0]
    with engine.begin() as conn:
        pending = lookup._row(conn)
        assert pending["completion"] is None
        assert adoption._snapshot(conn, OPERATION) == before
        assert adoption.inspect_retirement(conn, operation_sha256=OPERATION) == terminal
        assert conn.scalar(text("SELECT count(*) FROM market.fact_versions WHERE observation_key='collected-during-lookup-move'")) == 1
    changed = dict(options, operation_sha256="f"*64)
    with pytest.raises(RuntimeError, match="intent_changed"):
        lookup.move_lookup_indexes(engine, **changed)
    # Cancel while the first move is staged. Same-session watch must roll back
    # SQL before returning the connection; this is separate from ownership.
    cancel = [False]
    def cancel_first(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("ALTER INDEX "+capture.SCHEMA+"."):
            cancel[0] = True
    event.listen(engine, "after_cursor_execute", cancel_first)
    try:
        with pytest.raises(RuntimeError, match="storage_move_cancelled"):
            lookup.move_lookup_indexes(engine, **options, cancelled=lambda: cancel[0])
    finally:
        event.remove(engine, "after_cursor_execute", cancel_first)
    with engine.begin() as conn:
        assert lookup._row(conn) == pending
        assert adoption._snapshot(conn, OPERATION) == before
    # Lost COMMIT reply after all three moves and completion, then read-only
    # reconciliation. No second ALTER and no replacement attempt clock.
    commit = Connection._commit_impl
    lost = [False]
    def lose_completion(conn):
        if not lost[0] and not conn.invalidated:
            value = conn.scalar(text("SELECT completion IS NOT NULL FROM "+lookup.STATE+" WHERE id=1"))
            if value:
                commit(conn)
                lost[0] = True
                raise RuntimeError("lost lookup commit reply")
        commit(conn)
    with monkeypatch.context() as patch:
        patch.setattr(Connection, "_commit_impl", lose_completion)
        with pytest.raises(RuntimeError, match="lost lookup commit reply"):
            lookup.move_lookup_indexes(engine, **options)
    assert lost[0]
    def forbid_alter(conn, cursor, statement, parameters, context, executemany):
        assert not statement.startswith("ALTER INDEX")
    event.listen(engine, "before_cursor_execute", forbid_alter)
    try:
        complete = lookup.move_lookup_indexes(engine, **options)
    finally:
        event.remove(engine, "before_cursor_execute", forbid_alter)
    assert complete["reused"] and not complete["migration_ready"]
    assert complete["started_at"] == pending["started_at"] and complete["expires_at"] == pending["expires_at"]
    files = complete["completion"]
    assert {r["oid"] for r in files["before_files"]} == {r["oid"] for r in files["after_files"]}
    assert all(r["reltablespace"] in (0, 1663) for r in files["after_files"])
    budget = complete["completion"]["resource_budget"]["filesystems"]
    assert next(r for r in budget if r["target_id"] == plan.recent.target_id)["copy_bytes"] == complete["completion"]["copy_bytes"]
    assert next(r for r in budget if r["target_id"] == plan.history.target_id)["copy_bytes"] == 0
    with engine.begin() as conn:
        assert _old(conn) == old and adoption._json_row(conn, keys.STATE) == old_keys
        assert adoption._json_row(conn, adoption.STATE) == old_state
        assert _frozen_records(conn) == frozen
        assert adoption.inspect_retirement(conn, operation_sha256=OPERATION) == terminal
        with pytest.raises(RuntimeError, match="wrong_tablespace"), conn.begin_nested():
            physical.verify_group(conn, adoption.IDENTITY, history=True, saved=old["headers"]["placement"],
                pid=physical.verify(conn, old["headers"]["placement"]))
    successor = dict(retained.adoption_args, operation_sha256="c"*64,
        predecessor_operation_sha256=OPERATION, predecessor_terminal_sha256=adoption._digest(terminal))
    with pytest.raises(RuntimeError, match="explicit_lookup_placement_required"), engine.begin() as conn:
        adoption.prepare_adoption(conn, **successor)
    successor["lookup_operation_sha256"] = options["operation_sha256"]
    with engine.begin() as conn:
        adoption.prepare_adoption(conn, **successor)
        assert adoption.placement_binding(adoption._state(conn, "c"*64))["plan"] == plan.describe()
        _insert(conn, retained, "collected-after-lookup-adoption")
    for _ in range(64):
        with engine.begin() as conn:
            result = adoption.adoption_page(conn, operation_sha256="c"*64, page_rows=2)
        if result["retained_targets_verified"]:
            break
    assert result["retained_targets_verified"]
    with engine.begin() as conn:
        adoption.retire_adoption(conn, operation_sha256="c"*64)
        assert _old(conn) == old and adoption._json_row(conn, adoption.STATE) == old_state
        assert _frozen_records(conn) == frozen


def test_retained_lookup_intent_expiry_cannot_be_renewed(retained, monkeypatch):
    import time
    from scripts.db import fact_header_forward_placement as lookup
    from tests.test_market_data.test_archive_reference_placement_db import _options
    engine = retained.database._engine
    with engine.begin() as conn:
        adoption.prepare_adoption(conn, **retained.adoption_args)
        adoption.retire_adoption(conn, operation_sha256=OPERATION)
        terminal = adoption.inspect_retirement(conn, operation_sha256=OPERATION)
        before = adoption._snapshot(conn, OPERATION)
    options = dict(operation_sha256="d"*64, predecessor_operation_sha256=OPERATION,
        predecessor_terminal_sha256=adoption._digest(terminal),
        placement=replace(retained.copy_plan, recent_lookup_indexes=True), **_options(retained))
    options["resource_limits"]["movement_timeout_seconds"] = 15
    commit = Connection._commit_impl
    interrupted = [False]
    def interrupt_after_intent(conn):
        exists = conn.scalar(text("SELECT to_regclass(:name)"), {"name": lookup.STATE}) is not None
        commit(conn)
        if exists and not interrupted[0]:
            interrupted[0] = True
            raise RuntimeError("lost committed lookup intent reply")
    with monkeypatch.context() as patch:
        patch.setattr(Connection, "_commit_impl", interrupt_after_intent)
        with pytest.raises(RuntimeError, match="lost committed lookup intent reply"):
            lookup.move_lookup_indexes(engine, **options)
    assert interrupted[0]
    with engine.begin() as conn:
        pending = lookup._row(conn)
        assert pending["completion"] is None
        remaining = conn.scalar(text("SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM "+lookup.STATE))
    if remaining > 0:
        time.sleep(float(remaining) + .05)
    with pytest.raises(RuntimeError, match="lookup_expired"):
        lookup.move_lookup_indexes(engine, **options)
    with pytest.raises(RuntimeError, match="lookup_intent_changed"):
        lookup.move_lookup_indexes(engine, **{**options, "resource_limits": {
            **options["resource_limits"], "movement_timeout_seconds": 60}})
    with engine.begin() as conn:
        assert lookup._row(conn) == pending
        assert adoption._snapshot(conn, OPERATION) == before
        assert adoption.inspect_retirement(conn, operation_sha256=OPERATION) == terminal
        _insert(conn, retained, "collected-after-lookup-expiry")
