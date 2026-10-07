"""Atomic retained-header switch; tiny native fixture, not host admission."""
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
import os

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Connection

from portal.backend.db import Database
from scripts.db import fact_header_forward_adoption as adoption
from scripts.db import fact_header_v2_handoff as handoff
from scripts.db import fact_header_v2_references as references
from scripts.db import fact_header_v2_copy as headers
from scripts.db import raw_mapping_v2_copy as raw
from tests.test_market_data.test_fact_storage_tiers_db import storage as native_storage, _placement, BASE
from tests.test_market_data.test_fact_header_copy_placement_db import placed, source
from tests.test_market_data.test_fact_header_forward_adoption_db import retained, _finish, _old, OPERATION
from tests.test_market_data.test_fact_header_copy_db import _insert, _headers, _frozen_records
from tests.test_market_data.tiered_v1_fixture import ensure_v1_payload_partition

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
    reason="requires owned two-filesystem storage-demo topology")]


@pytest.fixture
def storage(native_storage, monkeypatch):
    # Historical placement only. The actual database clock and cutover boundary
    # are never patched; a production host must separately qualify its pause.
    native_storage.today -= timedelta(days=3)
    _placement(monkeypatch, native_storage.today)
    return native_storage


@pytest.mark.parametrize("retain_raw", [False, True])
def test_forward_switch_rolls_back_interruptions_and_preserves_native_history(retained, monkeypatch, retain_raw):
    engine = retained.database._engine
    with engine.begin() as conn:
        if retain_raw:
            # An untrusted private copy cannot affect canonical reads when it is
            # retained solely as evidence. The old path must prove it before use.
            conn.exec_driver_sql("UPDATE " + raw.TARGET + " SET raw_frame_sha256=repeat('f',64)")
        adoption.prepare_adoption(conn, **retained.adoption_args)
        today = conn.scalar(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date"))
        args = dict(operation_sha256=OPERATION, end_day=today, evidence={"fixture": "forward"})
        original = _old(conn)
        old_copy = _headers(conn, headers.SCHEMA + ".fact_versions")
        assert old_copy  # Existing private copies are deliberately nonempty.
        frozen = _frozen_records(conn)
    with pytest.raises(RuntimeError, match="adoption_incomplete"), engine.begin() as conn:
        handoff.stage_forward_tables(conn, **args)
    if retain_raw:
        with engine.begin() as conn:
            with pytest.raises(RuntimeError, match="completed_identity_required"):
                adoption._retain_raw_source(conn, adoption._state(conn))
        for _ in range(64):
            with engine.begin() as conn:
                state = adoption._state(conn)
                if all(state["progress"]["identity_" + side]["complete"] for side in ("source", "target")):
                    raw_progress = {k:v for k,v in state["progress"].items() if k.startswith("raw_")}
                    original_raw_oid = handoff._oid(conn, raw.SOURCE)
                    adoption._retain_raw_source(conn, state)
                    break
                adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
        else:
            pytest.fail("identity fixture did not finish")
        with engine.begin() as conn:
            after = adoption._state(conn)
            assert {k:v for k,v in after["progress"].items() if k.startswith("raw_")} == raw_progress
            assert not all(p["complete"] for p in raw_progress.values())
            assert adoption.adoption_page(conn, operation_sha256=OPERATION)["retained_targets_verified"]
            assert adoption._state(conn)["progress"] == after["progress"]
    else:
        _finish(engine)
    with engine.begin() as conn:
        inventory = adoption.inspect_references(conn, operation_sha256=OPERATION)["references"]
    for relation in inventory:
        if relation == references.PARENT:
            continue
        with engine.begin() as conn:
            adoption.prepare_reference(conn, operation_sha256=OPERATION, relation=relation)
        with engine.begin() as conn:
            adoption.validate_reference(conn, operation_sha256=OPERATION, relation=relation)
    with engine.begin() as conn:
        adoption.adopt_payload_references(conn, operation_sha256=OPERATION)
        admitted = adoption._state(conn)
        before = _headers(conn, headers.SOURCE)
    with pytest.raises(RuntimeError, match="utc_boundary_not_current"), engine.begin() as conn:
        handoff.stage_forward_tables(conn, **{**args, "end_day":today + timedelta(days=1)})
    # Missing the real UTC boundary must refuse, not redate source records.
    with pytest.raises(RuntimeError, match="new_day_already_written"), engine.begin() as conn:
        ensure_v1_payload_partition(conn, today)
        _insert(conn, SimpleNamespace(**{**vars(retained), "open_day":today}), "new-day-before-switch")
        handoff.stage_forward_tables(conn, **args)
    # Catching the owner's refusal inside an outer transaction still cannot
    # commit removed mirrors or other unfinished switch work.
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="atomic_switch_required"):
            with adoption.verified_adoption(conn, operation_sha256=OPERATION):
                adoption.release_for_switch(conn)
        assert adoption._state(conn) == admitted
        assert adoption._snapshot(conn) == admitted["binding"]
    def interrupt(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("ALTER TABLE market.fact_versions ADD CONSTRAINT qt_forward_legacy_bound"):
            raise RuntimeError("interrupt after native range validation")
    event.listen(engine, "after_cursor_execute", interrupt)
    try:
        with engine.begin() as conn:
            with pytest.raises(RuntimeError, match="interrupt after native range validation"):
                handoff.stage_forward_tables(conn, **args)
    finally:
        event.remove(engine, "after_cursor_execute", interrupt)
    with engine.begin() as conn:
        assert adoption._state(conn) == admitted
        assert adoption._snapshot(conn) == admitted["binding"]
        assert _headers(conn, headers.SOURCE) == before
        assert conn.scalar(text("SELECT to_regnamespace(:schema)"), {"schema":handoff.RETAINED}) is None
        _insert(conn, retained, "writer-after-interrupted-range")
    # A completed switch also rolls back every rename and constraint if its
    # outer transaction fails before COMMIT.
    with pytest.raises(RuntimeError, match="interrupt before forward commit"), engine.begin() as conn:
        assert handoff.stage_forward_tables(conn, **args)["source_preserved"]
        raise RuntimeError("interrupt before forward commit")
    with engine.begin() as conn:
        assert adoption._snapshot(conn) == admitted["binding"]
        assert _old(conn) == original and _frozen_records(conn) == frozen
    commit = Connection._commit_impl
    def lose_reply(conn):
        commit(conn)
        raise RuntimeError("lost forward commit reply")
    with monkeypatch.context() as lost:
        lost.setattr(Connection, "_commit_impl", lose_reply)
        with pytest.raises(RuntimeError, match="lost forward commit reply"), engine.begin() as conn:
            result = handoff.stage_forward_tables(conn, **args)
    with engine.begin() as conn:
        assert adoption._state(conn)["terminal"] == result["receipt"]
        assert _old(conn) == original and _frozen_records(conn) == frozen
        assert _headers(conn, headers.SCHEMA + ".fact_versions") == old_copy
        assert conn.scalar(text("SELECT count(*) FROM " + handoff.RETAINED + "." + raw.NAME)) > 0
        if retain_raw:
            assert handoff._oid(conn, raw.SOURCE) == original_raw_oid
            assert result["receipt"]["raw_history_placement_pending"] is True
            assert result["receipt"]["canonical_raw_oid"] == original_raw_oid
            assert not adoption._state(conn)["progress"]["raw_target"]["complete"]
            for statement in ("UPDATE " + handoff.RETAINED + "." + raw.NAME + " SET object_row_index=0",
                              "DELETE FROM " + handoff.RETAINED + "." + raw.NAME,
                              "TRUNCATE " + handoff.RETAINED + "." + raw.NAME):
                with pytest.raises(Exception, match="immutable"), conn.begin_nested():
                    conn.exec_driver_sql(statement)
        assert conn.scalar(text("SELECT market.fact_header_legacy_end_day()")) == today
        assert _headers(conn, "market.fact_versions_legacy") == _headers(conn, headers.SOURCE)
        with pytest.raises(RuntimeError, match="already_switched"):
            adoption.retire_adoption(conn, operation_sha256=OPERATION)
    restarted = Database(retained.dsn)
    try:
        assert restarted.ensure_schema(), str(restarted.last_error)
    finally:
        restarted._reset_engine()
    _placement(monkeypatch, today)
    current = replace(retained.fact, observation_key="after-forward-switch", observation_time=BASE+timedelta(days=4))
    retained.repo.ingest_facts(series_id=retained.series_id, source_id=retained.source_id, facts=[current])
    assert retained.repo.read_dataset_fact_revisions(dataset_id=retained.frozen_dataset_id,
        series_id=retained.series_id) == retained.frozen_result
    rows = retained.repo.read_facts(series_id=retained.series_id, start=BASE-timedelta(hours=1), end=BASE+timedelta(days=5))
    assert any(row.fact.observation_key == "after-forward-switch" for row in rows)
    old = retained.original_facts[0]
    corrected = replace(old, payload={**old.payload, "rate":"0.2", "raw_rate":"0.2"},
                        accepted_at=BASE+timedelta(seconds=10), known_at=BASE+timedelta(seconds=10))
    assert retained.repo.ingest_facts(series_id=retained.series_id, source_id=retained.source_id,
        facts=[corrected]).corrected_count == 1
    query = dict(series_id=retained.series_id, start=BASE-timedelta(hours=1), end=BASE+timedelta(days=5))
    latest = retained.repo.read_facts(**query)
    assert next(row for row in latest if row.fact.observation_key == old.observation_key).revision == 2
    causal = retained.repo.read_facts(**query, known_at_lte=BASE)
    assert next(row for row in causal if row.fact.observation_key == old.observation_key).revision == 1
    assert retained.repo.read_dataset_fact_revisions(dataset_id=retained.frozen_dataset_id,
        series_id=retained.series_id) == retained.frozen_result
    assert retained.archive_path.read_bytes() == retained.archive_bytes
