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
        ("ALTER TABLE " + headers.SOURCE + " DISABLE TRIGGER trg_qt_forward_identity_mirror", "binding_changed"),
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
            references.PARENT, "market.fact_archive_material_aliases",
            "market.fact_archive_canonical_dependencies"}
        assert not any(references._states(conn, references._native_inventory(conn)).values())
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
        relation = next(name for name in references._native_inventory(conn)
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
        assert not any(references._states(conn, references._native_inventory(conn)).values())
        assert _old(conn) == original
        _insert(conn, retained, "after-expired-adoption-retirement")
