"""Protected online copy with live v1 payload partitions and prevalidated FKs."""
from dataclasses import replace
from datetime import timedelta
import os
from time import monotonic

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from scripts.db import fact_header_v2_references as references
from scripts.db import fact_header_v2_online_proof as proof
from scripts.db import fact_header_v2_copy as headers
from scripts.db.fact_header_v2_capture import SCHEMA
from tests.test_market_data.test_fact_header_copy_db import _insert, _frozen_records
from tests.test_market_data.test_fact_header_online_db import _advance
from tests.test_market_data.test_fact_header_online_proof_db import _protected
from tests.test_market_data.test_fact_header_copy_placement_db import placed, source
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement, BASE
from tests.test_market_data.tiered_v1_fixture import (
    ensure_v1_payload_partition, stage_shadow_handoff_fixture,
)

pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                      reason="requires owned SSD/HDD storage-demo topology"),
]


def _ordinary(engine):
    with engine.begin() as conn:
        return [row["relation"] for row in references.inspect_references(conn)["references"]
                if row["relation"] != references.PARENT]


def _prepare_slots(engine, slots):
    for relation in slots:
        with engine.begin() as conn:
            references.prepare_reference(conn, relation=relation)


def test_online_references_cover_new_partitions_and_concurrent_collection(
        placed, tmp_path, monkeypatch):
    engine, options = _protected(placed, tmp_path, monkeypatch)
    initial = _ordinary(engine)
    _prepare_slots(engine, initial)
    with engine.connect() as conn:
        prepared_at = conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture"))
    committed = []

    # Real VALIDATE locks are still owned when another v1 writer inserts both
    # header and payload and commits. An earlier writer remains uncommitted too.
    with engine.begin() as early:
        early.exec_driver_sql("SET LOCAL statement_timeout='2s'")
        earlier = _insert(early, placed, "reference-earlier-late-commit")
        with engine.begin() as validator:
            for relation in initial:
                references.validate_reference(validator, relation=relation)
            with engine.begin() as writer:
                writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                started = monotonic()
                committed.append(_insert(writer, placed, "reference-during-validation"))
                print("ONLINE_REFERENCE_CONCURRENT_INSERT_SECONDS="+str(monotonic()-started))
    committed.append(earlier)

    # A new populated payload partition after inventory/validation is not
    # silently accepted by adoption. Source FKs and identity mirroring apply.
    placed.open_day += timedelta(days=3)
    with engine.begin() as writer:
        writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
        late_leaf = ensure_v1_payload_partition(writer, placed.open_day)
        committed.append(_insert(writer, placed, "reference-new-before-adoption"))
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="prevalidation_incomplete"):
            references.adopt_payload_references(conn)
        assert not references.inspect_references(conn)["references_complete"]
    _prepare_slots(engine, [late_leaf])
    with engine.begin() as conn:
        references.validate_reference(conn, relation=late_leaf)
        before = {row["relation"]: row["constraint_oid"]
                  for row in references.inspect_references(conn)["references"]}
    with engine.begin() as conn:
        started = monotonic()
        assert references.adopt_payload_references(conn)["references_complete"]
        print("ONLINE_REFERENCE_ADOPTION_SECONDS="+str(monotonic()-started))
        after = references.inspect_references(conn)
        assert all(row["constraint_oid"] == before[row["relation"]]
                   for row in after["references"] if row["relation"] != references.PARENT)

    # A partition created after parent adoption inherits the validated staged
    # FK as well as the original authoritative source reference.
    placed.open_day += timedelta(days=1)
    with engine.begin() as writer:
        writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
        inherited_leaf = ensure_v1_payload_partition(writer, placed.open_day)
        committed.append(_insert(writer, placed, "reference-new-after-adoption"))
    with engine.begin() as conn:
        state = references.inspect_references(conn)
        inherited = next(row for row in state["references"] if row["relation"] == inherited_leaf)
        assert inherited["prepared"] and inherited["validated"] and state["references_complete"]
        assert conn.scalar(text("""SELECT count(*) FROM pg_constraint
            WHERE conrelid=to_regclass(:leaf) AND contype='f'
              AND confrelid IN ('market.fact_versions'::regclass,
                               'qt_fact_header_cutover_v2.fact_identities'::regclass)"""),
                           {"leaf": inherited_leaf}) == 2
        for row in committed:
            assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_identities WHERE id=:id"),
                               {"id": row["id"]}) == 1
        with pytest.raises(RuntimeError, match="pending_capture"):
            with proof.verified_copy(conn):
                pytest.fail("references alone certified an uncopied tail")
    _advance(engine, options)

    def no_full_scan(*args, **kwargs):
        pytest.fail("protected reference handoff invoked the full-history verifier")
    monkeypatch.setattr(headers, "_verified_pages", no_full_scan)
    with engine.begin() as conn:
        assert conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture")) == prepared_at
        before = references.inspect_references(conn)
        with pytest.raises(RuntimeError, match="switch abort"), conn.begin_nested():
            stage_shadow_handoff_fixture(conn, placed, prevalidated=True, raw_mapping=True)
            raise RuntimeError("switch abort")
        assert references.inspect_references(conn) == before
        with proof.verified_copy(conn):
            pass
        stage_shadow_handoff_fixture(conn, placed, prevalidated=True, raw_mapping=True)
        assert _frozen_records(conn) == placed.frozen_before
        for row in committed:
            assert conn.scalar(text("SELECT count(*) FROM market.fact_versions WHERE id=:id"),
                               {"id": row["id"]}) == 1
    assert placed.repo.read_dataset_fact_revisions(
        dataset_id=placed.frozen_dataset_id, series_id=placed.series_id) == placed.frozen_result
    # The matching runtime can append a correction on the newly created day;
    # current reads select it, but the original frozen cold binding is stable.
    _placement(monkeypatch, placed.open_day)
    original = placed.original_facts[0]
    corrected = replace(original, payload={**original.payload, "rate": "0.2", "raw_rate": "0.2"},
                        accepted_at=BASE+timedelta(days=7), known_at=BASE+timedelta(days=7))
    assert placed.repo.ingest_facts(series_id=placed.series_id, source_id=placed.source_id,
                                   facts=[corrected]).corrected_count == 1
    current = placed.repo.read_facts(series_id=placed.series_id, start=BASE-timedelta(seconds=1),
                                    end=BASE+timedelta(seconds=10))
    revision = next(row for row in current if row.fact.observation_key == original.observation_key)
    assert revision.revision == 2 and revision.fact.payload == corrected.payload
    assert placed.repo.read_dataset_fact_revisions(
        dataset_id=placed.frozen_dataset_id, series_id=placed.series_id) == placed.frozen_result
    assert placed.archive_path.read_bytes() == placed.archive_bytes


def test_online_reference_validation_interruption_preserves_prior_commits(
        placed, tmp_path, monkeypatch):
    engine, options = _protected(placed, tmp_path, monkeypatch)
    slots = _ordinary(engine)
    _prepare_slots(engine, slots)
    with engine.begin() as conn:
        references.validate_reference(conn, relation=slots[0])
        prepared_at = conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture"))
    killed = [False]
    statement = f"ALTER TABLE {slots[1]} VALIDATE CONSTRAINT {references.STAGED}"
    def kill(conn, cursor, sql, parameters, context, executemany):
        if sql == statement and not killed[0]:
            killed[0] = True
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid": conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine, "after_cursor_execute", kill)
    try:
        with pytest.raises(DBAPIError), engine.begin() as conn:
            references.validate_reference(conn, relation=slots[1])
    finally:
        event.remove(engine, "after_cursor_execute", kill)
    assert killed[0]
    with engine.begin() as conn:
        states = {row["relation"]: row for row in references.inspect_references(conn)["references"]}
        assert states[slots[0]]["validated"] and not states[slots[1]]["validated"]
        conn.exec_driver_sql("SET LOCAL statement_timeout='2s'")
        _insert(conn, placed, "after-validator-interruption")
    for relation in slots:
        with engine.begin() as conn:
            references.validate_reference(conn, relation=relation)
    with engine.begin() as conn:
        references.adopt_payload_references(conn)
        assert conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture")) == prepared_at
    _advance(engine, options)
    with engine.begin() as conn:
        with proof.verified_copy(conn):
            pass
        assert _frozen_records(conn) == placed.frozen_before
