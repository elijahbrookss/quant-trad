"""Actual QT reference prevalidation keeps source collection and frozen data."""
from dataclasses import replace
from datetime import timedelta
import hashlib

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from scripts.db import fact_header_v2_copy as copy
from scripts.db import fact_header_v2_references as references
from scripts.db.fact_header_v2_capture import SCHEMA
from tests.test_market_data.test_fact_header_copy_db import source, _finish, _insert, _headers
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement, BASE
from tests.test_market_data.tiered_v1_fixture import restore_tiered_v1_fixture, stage_shadow_handoff_fixture

pytestmark=pytest.mark.db


def _prepare(engine):
    with engine.begin() as conn:
        copy.prepare_copy(conn)
    _finish(engine)
    with engine.begin() as conn:
        copy.enable_identity_capture(conn)
        return references.inspect_references(conn)


def _stage_all(engine):
    with engine.begin() as conn:
        slots=references.inspect_references(conn)["references"]
    ordinary=[r["relation"] for r in slots if r["relation"]!=references.PARENT]
    for relation in ordinary:
        with engine.begin() as conn:
            references.prepare_reference(conn,relation=relation)
        with engine.begin() as conn:
            references.validate_reference(conn,relation=relation)
    with engine.begin() as conn:
        assert references.adopt_payload_references(conn)["references_complete"]
        assert references.inspect_references(conn)["references_complete"]
    return ordinary


def test_reference_validation_allows_collection_and_reuses_verified_constraints(source):
    engine=source.database._engine
    initial=_prepare(engine)
    assert not initial["references_complete"] and not initial["migration_ready"]
    ordinary=[row["relation"] for row in initial["references"] if row["relation"]!=references.PARENT]
    for relation in ordinary:
        with engine.begin() as conn:
            before=conn.scalar(text("SHOW statement_timeout"))
            assert not references.prepare_reference(conn,relation=relation)["validated"]
            assert conn.scalar(text("SHOW statement_timeout"))==before
    # Keep one collection transaction open during validation, then commit another
    # while validation still owns its locks. No source writer fence is held.
    with engine.begin() as writer:
        first=_insert(writer,source,"validation-inflight")
        with engine.begin() as validator:
            for relation in ordinary:
                assert references.validate_reference(validator,relation=relation)["validated"]
            with engine.begin() as second:
                second.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                later=_insert(second,source,"validation-concurrent")
    with engine.begin() as conn:
        before=references.inspect_references(conn)
        assert not before["references_complete"]
        assert references.adopt_payload_references(conn)["references_complete"]
        after=references.inspect_references(conn)
        previous={row["relation"]:row["constraint_oid"] for row in before["references"]}
        assert all(row["constraint_oid"]==previous[row["relation"]]
                   for row in after["references"] if row["relation"]!=references.PARENT)
        assert references.adopt_payload_references(conn)["reused"]
        assert references.prepare_reference(conn,relation=ordinary[0])["reused"]
        assert references.validate_reference(conn,relation=ordinary[0])["reused"]
        # Original references stay authoritative throughout preparation.
        assert conn.scalar(text("""
            SELECT count(*) FROM pg_constraint WHERE contype='f'
            AND confrelid='market.fact_versions'::regclass AND conparentid=0
        """))==3
        for row in (first,later):
            assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_identities WHERE id=:id"),
                               {"id":row["id"]})==1
    _finish(engine)
    with engine.connect() as conn:
        assert _headers(conn,copy.SOURCE)==_headers(conn,SCHEMA+".fact_versions")


def test_missing_target_and_changed_enforcement_block_reference_completion(source):
    engine=source.database._engine
    initial=_prepare(engine)
    payload=next(r["relation"] for r in initial["references"] if r["relation"].startswith(references.PARENT+"_"))
    with engine.begin() as conn:
        row=dict(conn.execute(text("SELECT * FROM market.fact_versions WHERE storage_day=:day"),
                              {"day":source.open_day}).mappings().one())
        conn.execute(text(f"DELETE FROM {SCHEMA}.fact_versions WHERE id=:id"),{"id":row["id"]})
        conn.execute(text(f"DELETE FROM {SCHEMA}.fact_identities WHERE id=:id"),{"id":row["id"]})
        references.prepare_reference(conn,relation=payload)
    with engine.begin() as conn:
        with pytest.raises(DBAPIError,match="foreign key constraint"):
            references.validate_reference(conn,relation=payload)
        assert not next(r["validated"] for r in references.inspect_references(conn)["references"]
                        if r["relation"]==payload)
        with pytest.raises(RuntimeError,match="prevalidation_incomplete"):
            references.adopt_payload_references(conn)
        assert _headers(conn,copy.SOURCE)==source.source_before
        copy._copy_rows(conn,[row])
    _stage_all(engine)
    with engine.connect() as conn:
        transaction=conn.begin()
        try:
            trigger=conn.scalar(text("""
                SELECT t.tgname FROM pg_trigger t JOIN pg_constraint c ON c.oid=t.tgconstraint
                JOIN pg_proc p ON p.oid=t.tgfoid
                WHERE c.conrelid=to_regclass(:relation) AND c.conname=:name
                  AND p.proname='RI_FKey_check_ins'
            """),{"relation":payload,"name":references.STAGED})
            quoted=conn.dialect.identifier_preparer.quote(trigger)
            conn.exec_driver_sql(f"ALTER TABLE {payload} DISABLE TRIGGER {quoted}")
            with pytest.raises(RuntimeError,match="enforcement_changed"):
                references.inspect_references(conn)
        finally:
            transaction.rollback()
    with engine.begin() as conn:
        assert references.inspect_references(conn)["references_complete"]


def test_prevalidated_cold_references_survive_interrupted_handoff_and_frozen_reads(storage,tmp_path,monkeypatch):
    from tests.test_market_data.test_fact_book_retention_db import (
        test_cold_book_handoff_preserves_frozen_features_checkpoint_and_replay,
    )
    from portal.backend.db.session import Database
    test_cold_book_handoff_preserves_frozen_features_checkpoint_and_replay(
        storage,tmp_path,monkeypatch,split_sources=False)
    storage.open_day=storage.today+timedelta(days=1)
    _placement(monkeypatch,storage.open_day)
    recent=replace(storage.fact,observation_key="reference-live",observation_time=BASE+timedelta(days=2))
    assert storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,
                                    facts=[recent]).inserted_count==1
    engine=storage.database._engine
    with engine.connect() as conn:
        source_before=_headers(conn,copy.SOURCE)
        frozen_pairs=conn.execute(text("SELECT dataset_id,series_id FROM market.dataset_series ORDER BY dataset_id,series_id")).all()
        relationships={name:conn.execute(text(f"SELECT to_jsonb(t) FROM market.{name} t ORDER BY to_jsonb(t)::text")).scalars().all()
                       for name in ("fact_archive_material_aliases","fact_archive_canonical_dependencies")}
        assert all(relationships.values())
        ranges=conn.execute(text("""
            SELECT series_id,min(observation_time),max(observation_time) FROM market.fact_versions GROUP BY series_id
        """)).all()
    frozen={tuple(pair):storage.repo.read_dataset_fact_revisions(dataset_id=pair[0],series_id=pair[1]) for pair in frozen_pairs}
    queries={(series,start,end):storage.repo.read_facts(series_id=series,start=start-timedelta(seconds=1),end=end+timedelta(seconds=1))
             for series,start,end in ranges}
    archive_bytes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in tmp_path.rglob("*") if p.is_file()}
    restore_tiered_v1_fixture(storage)
    _prepare(engine)
    _stage_all(engine)
    with engine.begin() as conn:
        reference_before=references.inspect_references(conn)
    killed=[False]
    def terminate(conn,cursor,statement,parameters,context,executemany):
        if not killed[0] and statement==f"ALTER TABLE {SCHEMA}.fact_versions SET SCHEMA market":
            killed[0]=True
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                                     {"pid":conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine,"after_cursor_execute",terminate)
    try:
        with pytest.raises(DBAPIError),engine.begin() as conn:
            stage_shadow_handoff_fixture(conn,storage,prevalidated=True)
    finally:
        event.remove(engine,"after_cursor_execute",terminate)
    assert killed[0]
    with engine.begin() as conn:
        assert references.inspect_references(conn)==reference_before
        assert _headers(conn,copy.SOURCE)==source_before
        stage_shadow_handoff_fixture(conn,storage,prevalidated=True)
    restarted=Database(storage.dsn)
    try:
        assert restarted.ensure_schema(),str(restarted.last_error)
    finally:
        restarted._reset_engine()
    for (dataset_id,series_id),records in frozen.items():
        assert storage.repo.read_dataset_fact_revisions(dataset_id=dataset_id,series_id=series_id)==records
    for (series,start,end),records in queries.items():
        assert storage.repo.read_facts(series_id=series,start=start-timedelta(seconds=1),end=end+timedelta(seconds=1))==records
    with engine.connect() as conn:
        for name,rows in relationships.items():
            assert conn.execute(text(f"SELECT to_jsonb(t) FROM market.{name} t ORDER BY to_jsonb(t)::text")).scalars().all()==rows
    assert archive_bytes=={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in tmp_path.rglob("*") if p.is_file()}
