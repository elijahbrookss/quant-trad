"""Preserving-copy rehearsal on an owned, disposable tiered-v1 source."""
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from market_data.canonical import build_fact_version_id
from market_data.contracts import DatasetSeriesRequest
from portal.backend.service.storage.repos.market_data import _json_text
from scripts.db import fact_header_v2_copy as copy
from scripts.db.fact_header_v2_capture import QUEUE, SCHEMA
from tests.test_market_data.test_fact_storage_tiers_db import (
    storage, _placement, _verified_cold_fixture, BASE,
)
from tests.test_market_data.tiered_v1_fixture import restore_tiered_v1_fixture, stage_shadow_handoff_fixture

pytestmark = pytest.mark.db


@pytest.fixture
def source(storage, monkeypatch, tmp_path):
    facts = [replace(storage.fact, observation_key=f"preserved-{i}",
                     observation_time=BASE+timedelta(seconds=i)) for i in range(6)]
    storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,facts=facts)
    request = DatasetSeriesRequest(storage.series_id,BASE-timedelta(hours=1),BASE+timedelta(hours=1))
    frozen = storage.repo.freeze_dataset([request])
    frozen_before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id,series_id=storage.series_id)
    assert len(frozen_before)==6
    archive_path = _verified_cold_fixture(storage,tmp_path,monkeypatch)
    archive_bytes = archive_path.read_bytes()
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id,series_id=storage.series_id)==frozen_before
    storage.open_day = storage.today+timedelta(days=1)
    _placement(monkeypatch,storage.open_day)
    recent = replace(storage.fact,observation_key="preserved-recent",observation_time=BASE+timedelta(days=2))
    storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,facts=[recent])
    storage.original_facts=facts
    storage.frozen_result=frozen_before
    storage.frozen_dataset_id=frozen.dataset_id
    storage.dataset_request=request
    storage.query_before=storage.repo.read_facts(
        series_id=storage.series_id,start=request.start,end=BASE+timedelta(days=3))
    engine = storage.database._engine
    with engine.connect() as conn:
        storage.source_before = _headers(conn,copy.SOURCE)
        storage.payload_before = conn.execute(text("SELECT * FROM market.fact_hot_payloads ORDER BY id")).mappings().all()
        storage.frozen_before = _frozen_records(conn)
    restore_tiered_v1_fixture(storage)
    with engine.connect() as conn:
        assert _headers(conn,copy.SOURCE)==storage.source_before
        assert _frozen_records(conn)==storage.frozen_before
        assert conn.scalar(text("SELECT count(*) FROM market.fact_hot_payloads"))==1
    storage.archive_path,storage.archive_bytes=archive_path,archive_bytes
    yield storage


def _headers(conn,table):
    return [dict(row) for row in conn.execute(text(
        f"SELECT {','.join(copy.HEADER_COLUMNS)} FROM {table} ORDER BY id")).mappings()]


def _frozen_records(conn):
    return {name:conn.execute(text(f"SELECT to_jsonb(t) FROM market.{name} t ORDER BY to_jsonb(t)::text")).scalars().all()
            for name in ("datasets","dataset_series")}


def _finish(engine):
    for _ in range(32):
        with engine.begin() as conn:
            report=copy.copy_page(conn,page_rows=2)
        if report["caught_up_at_observation"]:
            return report
    pytest.fail("bounded fixture copy failed to catch up")


def _insert(conn,storage,key):
    """Native v1 transaction using canonical hashes and the real source guards."""
    fact=replace(storage.fact,observation_key=key,observation_time=BASE+timedelta(days=2))
    row={name:getattr(fact,name) for name in copy.HEADER_COLUMNS if hasattr(fact,name)}
    row.update(id=build_fact_version_id(series_id=storage.series_id,observation_key=key,
                                      revision=1,row_hash=fact.row_hash),
               storage_day=storage.open_day,series_id=storage.series_id,revision=1,
               source_id=storage.source_id,ingestion_run_id=None,state=fact.state.value,
               market_commit_seq=conn.scalar(text("SELECT nextval('market.fact_commit_seq')")))
    conn.execute(text(f"INSERT INTO {copy.SOURCE}({','.join(copy.HEADER_COLUMNS)}) "
                      f"VALUES({','.join(':'+name for name in copy.HEADER_COLUMNS)})"),row)
    conn.execute(text("""
        INSERT INTO market.fact_hot_payloads(storage_day,id,series_id,payload_schema_id,observation_time,
                                             payload,provenance,quality)
        VALUES(:storage_day,:id,:series_id,:payload_schema_id,:observation_time,
               CAST(:payload AS jsonb),CAST(:provenance AS jsonb),CAST(:quality AS jsonb))
    """),{**row,**{name:_json_text(getattr(fact,name)) for name in ("payload","provenance","quality")}})
    return row


def test_shadow_copy_preserves_hot_cold_headers_and_frozen_sources(source):
    engine=source.database._engine
    with engine.begin() as conn:
        assert not copy.prepare_copy(conn)["migration_ready"]
    with engine.begin() as conn:
        first=copy.copy_page(conn,page_rows=2)
        assert first["verified_page_rows"]==2
        assert copy.prepare_copy(conn)["reused"]
    report=_finish(engine)
    assert not report["migration_ready"] and report["source_authoritative"]
    with engine.connect() as conn:
        assert _headers(conn,copy.SOURCE)==source.source_before==_headers(conn,SCHEMA+".fact_versions")
        assert conn.execute(text("SELECT * FROM market.fact_hot_payloads ORDER BY id")).mappings().all()==source.payload_before
        assert _frozen_records(conn)==source.frozen_before
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_identities"))==7
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_header_partitions"))==2
        bounds=conn.execute(text(f"""
            SELECT storage_day,min_observation_time,max_observation_time
            FROM {SCHEMA}.fact_header_series_days ORDER BY storage_day
        """)).all()
        assert bounds==[(source.today,BASE,BASE+timedelta(seconds=5)),
                       (source.open_day,BASE+timedelta(days=2),BASE+timedelta(days=2))]
        assert conn.scalar(text("SELECT state FROM market.fact_storage_state WHERE layout_version='market.fact_storage_tiers.v1'"))=="ready"
        assert not conn.scalar(text("SELECT EXISTS(SELECT 1 FROM market.fact_storage_state WHERE layout_version='market.fact_storage_tiers.v2')"))
    assert source.archive_path.read_bytes()==source.archive_bytes


def test_later_committing_lower_sequence_is_not_missed(source):
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
    with engine.connect() as slow:
        transaction=slow.begin()
        lower=_insert(slow,source,"late-commit")
        with engine.begin() as fast:
            higher=_insert(fast,source,"early-commit")
        assert lower["market_commit_seq"]<higher["market_commit_seq"]
        assert _finish(engine)["caught_up_at_observation"]
        with engine.connect() as conn:
            assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions WHERE id=:id"),{"id":lower["id"]})==0
        transaction.commit()
    with engine.begin() as conn:
        assert conn.scalar(text(f"SELECT count(*) FROM {QUEUE}"))==1
        assert copy.copy_page(conn,page_rows=1)["verified_page_rows"]==1
    with engine.connect() as conn:
        assert _headers(conn,copy.SOURCE)==_headers(conn,SCHEMA+".fact_versions")


def test_backend_kill_rolls_back_shadow_and_cursor_then_retry_preserves_rows(source):
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
        inserted=_insert(conn,source,"before-kill")
    killed=[False]
    def terminate(conn,cursor,statement,parameters,context,executemany):
        if not killed[0] and statement.startswith("INSERT INTO "+SCHEMA+".fact_versions"):
            killed[0]=True
            pid=conn.connection.driver_connection.get_backend_pid()
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),{"pid":pid})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine,"after_cursor_execute",terminate)
    try:
        with pytest.raises(DBAPIError),engine.begin() as conn:
            copy.copy_page(conn,page_rows=2)
    finally:
        event.remove(engine,"after_cursor_execute",terminate)
    assert killed[0]
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT after_id FROM {copy.STATE}")) is None
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions"))==0
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_identities"))==0
        assert conn.scalar(text(f"SELECT count(*) FROM {QUEUE} WHERE id=:id"),{"id":inserted["id"]})==1
    _finish(engine)
    with engine.connect() as conn:
        assert _headers(conn,copy.SOURCE)==_headers(conn,SCHEMA+".fact_versions")


def test_failed_page_savepoint_cannot_commit_partial_progress(source,monkeypatch):
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
    original=copy._copy_rows
    def fail_after_copy(conn,rows):
        original(conn,rows)
        raise RuntimeError("injected failure after verification")
    monkeypatch.setattr(copy,"_copy_rows",fail_after_copy)
    with engine.begin() as conn:
        with pytest.raises(RuntimeError,match="injected failure"):
            copy.copy_page(conn,page_rows=2)
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions"))==0
        assert conn.scalar(text(f"SELECT after_id FROM {copy.STATE}")) is None
    monkeypatch.setattr(copy,"_copy_rows",original)
    _finish(engine)
    with engine.begin() as conn:
        queued=_insert(conn,source,"queue-retirement-rollback")
    monkeypatch.setattr(copy,"_copy_rows",fail_after_copy)
    with engine.begin() as conn:
        with pytest.raises(RuntimeError,match="injected failure"):
            copy.copy_page(conn,page_rows=2)
        assert conn.scalar(text(f"SELECT count(*) FROM {QUEUE} WHERE id=:id"),{"id":queued["id"]})==1
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions WHERE id=:id"),{"id":queued["id"]})==0
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_identities WHERE id=:id"),{"id":queued["id"]})==0
    monkeypatch.setattr(copy,"_copy_rows",original)
    _finish(engine)
    with engine.connect() as conn:
        assert _headers(conn,copy.SOURCE)==_headers(conn,SCHEMA+".fact_versions")


@pytest.mark.parametrize("change",["source_index","target_definition","target_content"])
def test_incompatible_retry_refuses_without_retiring_pending_rows(source,change):
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
    _finish(engine)
    with engine.begin() as conn:
        row=_insert(conn,source,"pending-after-copy")
        if change=="source_index":
            conn.exec_driver_sql("DROP INDEX market.ix_market_fact_storage_page")
        elif change=="target_definition":
            conn.exec_driver_sql(f"ALTER TABLE {SCHEMA}.fact_identities ADD COLUMN unexpected text")
        else:
            # Pre-existing conflicting data must never be accepted as a retry.
            copy._copy_rows(conn,[row])
            conn.execute(text(f"INSERT INTO {QUEUE}(id) VALUES(:id)"),{"id":row["id"]})
            conn.execute(text(f"UPDATE {SCHEMA}.fact_versions SET row_hash=:hash WHERE id=:id"),
                         {"hash":"e"*64,"id":row["id"]})
    with engine.begin() as conn:
        with pytest.raises(RuntimeError,match="source_index_required|definition_changed|full_row_mismatch"):
            copy.copy_page(conn)
        assert conn.scalar(text(f"SELECT count(*) FROM {QUEUE} WHERE id=:id"),{"id":row["id"]})==1
        assert conn.scalar(text(f"SELECT row_hash FROM {copy.SOURCE} WHERE id=:id"),{"id":row["id"]})==row["row_hash"]


def _assert_application_after_handoff(source):
    from portal.backend.db.session import Database
    restarted=Database(source.dsn)
    try:
        assert restarted.ensure_schema(),str(restarted.last_error)
    finally:
        restarted._reset_engine()
    assert source.repo.read_facts(series_id=source.series_id,start=source.dataset_request.start,
                                  end=BASE+timedelta(days=3))==source.query_before
    assert source.repo.read_dataset_fact_revisions(dataset_id=source.frozen_dataset_id,
                                                   series_id=source.series_id)==source.frozen_result


def test_copied_v1_handoff_preserves_reads_freezes_and_new_collection(source):
    engine=source.database._engine
    with engine.begin() as conn:
        original_sequence=conn.scalar(text("SELECT 'market.fact_commit_seq'::regclass::oid::bigint"))
        copy.prepare_copy(conn)
    _finish(engine)
    with engine.begin() as conn:
        copy.enable_identity_capture(conn)
    with engine.begin() as conn:
        assert stage_shadow_handoff_fixture(conn,source)["source_rows_retained"]==7
    _assert_application_after_handoff(source)
    new=replace(source.fact,observation_key="after-handoff",observation_time=BASE+timedelta(days=2))
    result=source.repo.ingest_facts(series_id=source.series_id,source_id=source.source_id,facts=[new])
    assert result.inserted_count==1
    corrected=replace(source.original_facts[0],
        known_at=BASE+timedelta(days=2),accepted_at=BASE+timedelta(days=2),
        payload={**source.fact.payload,"rate":"0.2","raw_rate":"0.2"})
    assert source.repo.ingest_facts(series_id=source.series_id,source_id=source.source_id,
                                   facts=[corrected]).corrected_count==1
    history=source.repo.read_facts(series_id=source.series_id,start=source.dataset_request.start,
                                  end=source.dataset_request.end)
    assert next(row for row in history if row.fact.observation_key=="preserved-0").row_hash==corrected.row_hash
    assert source.repo.read_dataset_fact_revisions(dataset_id=source.frozen_dataset_id,
                                                   series_id=source.series_id)==source.frozen_result
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT 'market.fact_commit_seq'::regclass::oid::bigint"))==original_sequence
        assert conn.scalar(text("SELECT count(*) FROM qt_fact_header_retained_v1.fact_versions"))==7
        assert conn.scalar(text("SELECT count(*) FROM market.fact_versions"))==9
        assert conn.scalar(text("SELECT count(*) FROM market.fact_identities"))==9
        assert conn.scalar(text("SELECT count(*) FROM qt_fact_header_retained_v1.fact_storage_state"))==1
        assert not conn.scalar(text("SELECT EXISTS(SELECT 1 FROM market.fact_storage_state WHERE layout_version='market.fact_storage_tiers.v1')"))
    with pytest.raises(DBAPIError,match="immutable"),engine.begin() as conn:
        conn.exec_driver_sql("""
            INSERT INTO qt_fact_header_retained_v1.fact_versions
            SELECT * FROM market.fact_versions LIMIT 1
        """)
    assert source.archive_path.read_bytes()==source.archive_bytes


def test_interrupted_handoff_restores_original_layout_before_retry(source):
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
    _finish(engine)
    with engine.begin() as conn:
        copy.enable_identity_capture(conn)
    killed=[False]
    def terminate(conn,cursor,statement,parameters,context,executemany):
        if not killed[0] and statement==f"ALTER TABLE {SCHEMA}.fact_versions SET SCHEMA market":
            killed[0]=True
            pid=conn.connection.driver_connection.get_backend_pid()
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),{"pid":pid})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine,"after_cursor_execute",terminate)
    try:
        with pytest.raises(DBAPIError),engine.begin() as conn:
            stage_shadow_handoff_fixture(conn,source)
    finally:
        event.remove(engine,"after_cursor_execute",terminate)
    assert killed[0]
    with engine.connect() as conn:
        assert _headers(conn,copy.SOURCE)==source.source_before
        assert _frozen_records(conn)==source.frozen_before
        assert conn.scalar(text("SELECT state FROM market.fact_storage_state WHERE layout_version='market.fact_storage_tiers.v1'"))=="ready"
        assert conn.scalar(text("SELECT to_regnamespace('qt_fact_header_retained_v1')")) is None
        assert conn.scalar(text("SELECT to_regclass('market.fact_identities')")) is None
        assert conn.scalar(text("SELECT count(*) FROM pg_constraint WHERE contype='f' AND confrelid='market.fact_versions'::regclass AND conparentid=0"))==3
    with engine.begin() as conn:
        stage_shadow_handoff_fixture(conn,source)
    _assert_application_after_handoff(source)


def test_source_identity_and_queue_commit_or_roll_back_together(source):
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
    _finish(engine)
    with engine.begin() as conn:
        assert copy.enable_identity_capture(conn)["identity_capture_active"]
        row=_insert(conn,source,"identity-before-copy")
        identity=dict(conn.execute(text(f"""
            SELECT {",".join(copy.IDENTITY_COLUMNS)} FROM {SCHEMA}.fact_identities WHERE id=:id
        """),{"id":row["id"]}).mappings().one())
        assert identity=={name:row[name] for name in copy.IDENTITY_COLUMNS}
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions WHERE id=:id"),
                           {"id":row["id"]})==0
    # A terminated collection transaction must leave no source, identity or queue entry.
    with pytest.raises(DBAPIError):
        with engine.begin() as conn:
            killed=_insert(conn,source,"identity-killed")
            pid=conn.connection.driver_connection.get_backend_pid()
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),{"pid":pid})
            conn.exec_driver_sql("SELECT 1")
    with engine.begin() as conn:
        for relation in (copy.SOURCE,SCHEMA+".fact_identities",QUEUE):
            assert not conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {relation} WHERE id=:id)"),
                                   {"id":killed["id"]})
        # Stage a conflicting identity without committing its source record.
        transaction=conn.begin_nested()
        conflicting=_insert(conn,source,"conflicting-identity")
        transaction.rollback()
        wrong={name:conflicting[name] for name in copy.IDENTITY_COLUMNS}
        wrong["storage_day"]+=timedelta(days=1)
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.fact_identities({",".join(copy.IDENTITY_COLUMNS)})
            VALUES({",".join(":"+name for name in copy.IDENTITY_COLUMNS)})
        """),wrong)
        with pytest.raises(DBAPIError,match="identity_capture_content_mismatch"):
            with conn.begin_nested():
                _insert(conn,source,"conflicting-identity")
        for relation in (copy.SOURCE,QUEUE):
            assert not conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {relation} WHERE id=:id)"),
                                   {"id":conflicting["id"]})


def test_identity_capture_requires_no_private_access_and_drift_blocks_resume(source):
    from uuid import uuid4
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
    _finish(engine)
    with engine.begin() as conn:
        copy.enable_identity_capture(conn)
    role="qt_identity_writer_"+uuid4().hex[:16]
    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE ROLE {role} NOLOGIN")
        conn.exec_driver_sql(f"GRANT USAGE ON SCHEMA market TO {role}")
        conn.exec_driver_sql(f"GRANT SELECT ON ALL TABLES IN SCHEMA market TO {role}")
        conn.exec_driver_sql(f"GRANT INSERT ON market.fact_versions,market.fact_hot_payloads TO {role}")
        conn.exec_driver_sql(f"GRANT USAGE ON SEQUENCE market.fact_commit_seq TO {role}")
        # Existing payload validation takes FOR SHARE on this row, which needs
        # UPDATE privilege as well as SELECT; no private-schema grant is added.
        conn.exec_driver_sql(f"GRANT UPDATE(state) ON market.fact_retention_partitions TO {role}")
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(f"SET LOCAL ROLE {role}")
            row=_insert(conn,source,"identity-restricted-writer")
            conn.exec_driver_sql("SET CONSTRAINTS ALL IMMEDIATE")
            with pytest.raises(DBAPIError,match="permission denied"):
                with conn.begin_nested():
                    conn.exec_driver_sql(f"SELECT * FROM {SCHEMA}.fact_identities")
        with engine.connect() as conn:
            assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_identities WHERE id=:id"),
                               {"id":row["id"]})==1
        for sql in (
            "ALTER TABLE market.fact_versions DISABLE TRIGGER trg_qt_header_v2_capture_identity",
            f"""CREATE OR REPLACE FUNCTION {SCHEMA}.capture_fact_identity()
                RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
                AS $$ BEGIN RETURN NULL; END; $$""",
        ):
            with engine.connect() as conn:
                transaction=conn.begin()
                try:
                    conn.exec_driver_sql(sql)
                    with pytest.raises(RuntimeError,match="identity_capture_changed"):
                        copy.copy_page(conn)
                    with pytest.raises(RuntimeError,match="identity_capture_changed"):
                        copy.prepare_copy(conn)
                    assert conn.scalar(text(f"SELECT count(*) FROM {QUEUE} WHERE id=:id"),
                                       {"id":row["id"]})==1
                finally:
                    transaction.rollback()
        _finish(engine)
        with engine.connect() as conn:
            assert _headers(conn,copy.SOURCE)==_headers(conn,SCHEMA+".fact_versions")
    finally:
        with engine.begin() as conn:
            conn.exec_driver_sql(f"DROP OWNED BY {role}")
            conn.exec_driver_sql(f"DROP ROLE {role}")


def test_identity_capture_boundary_requires_catchup_and_rolls_back_on_failure(source,monkeypatch):
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
        with pytest.raises(RuntimeError,match="baseline_required"):
            copy.enable_identity_capture(conn)
    _finish(engine)
    with engine.begin() as writer:
        row=_insert(writer,source,"before-identity-boundary")
        with engine.begin() as conn:
            with pytest.raises(DBAPIError,match="could not obtain lock"):
                copy.enable_identity_capture(conn)
    with engine.begin() as conn:
        second=_insert(conn,source,"second-before-boundary")
        with pytest.raises(RuntimeError,match="backlog_exceeds_fence_budget"):
            copy.enable_identity_capture(conn,page_rows=1)
        assert conn.scalar(text(f"SELECT count(*) FROM {QUEUE}"))==2
    original=copy.install_identity_capture
    def fail_after_install(conn):
        original(conn)
        raise RuntimeError("injected mirror installation failure")
    monkeypatch.setattr(copy,"install_identity_capture",fail_after_install)
    with engine.begin() as conn:
        with pytest.raises(RuntimeError,match="injected mirror"):
            copy.enable_identity_capture(conn,page_rows=2)
        assert not conn.scalar(text(f"SELECT identity_capture FROM {copy.STATE}"))
        assert conn.scalar(text(f"SELECT count(*) FROM {QUEUE}"))==2
        assert not conn.scalar(text("SELECT to_regprocedure(:name)"),
                               {"name":SCHEMA+".capture_fact_identity()"})
        for saved in (row,second):
            assert not conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {SCHEMA}.fact_versions WHERE id=:id)"),
                                   {"id":saved["id"]})
    monkeypatch.setattr(copy,"install_identity_capture",original)
    with engine.begin() as conn:
        result=copy.enable_identity_capture(conn,page_rows=2)
        assert result["identity_capture_active"] and not result["migration_ready"]
        assert result["caught_up_at_observation"]
        assert _headers(conn,copy.SOURCE)==_headers(conn,SCHEMA+".fact_versions")
    with engine.begin() as conn:
        assert copy.enable_identity_capture(conn)["reused"]
        assert copy.prepare_copy(conn)["reused"]


def test_new_day_after_identity_activation_respects_caller_timeout_and_retries(source):
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
    _finish(engine)
    with engine.begin() as conn:
        copy.enable_identity_capture(conn)
        source.open_day+=timedelta(days=1)
        end=source.open_day+timedelta(days=1)
        child="market.fact_hot_payloads_"+source.open_day.strftime("%Y%m%d")
        conn.exec_driver_sql(f"CREATE TABLE {child} PARTITION OF market.fact_hot_payloads "
                             f"FOR VALUES FROM ('{source.open_day}') TO ('{end}')")
        conn.execute(text("INSERT INTO market.fact_retention_partitions(storage_day) VALUES(:day)"),
                     {"day":source.open_day})
    with engine.begin() as slow:
        late=_insert(slow,source,"new-day-late")
        with engine.begin() as fast:
            earlier=_insert(fast,source,"new-day-earlier")
        with engine.begin() as copier:
            copier.exec_driver_sql("SET LOCAL statement_timeout='500ms'")
            with pytest.raises(DBAPIError,match="statement timeout"):
                copy.copy_page(copier)
            # Page savepoint restores the target and queue. The committed source
            # and its identity survive; the unfinished writer remains independent.
            assert copier.scalar(text(f"SELECT count(*) FROM {QUEUE}"))==1
            assert copier.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions WHERE id=:id"),
                                 {"id":earlier["id"]})==0
            assert copier.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_identities WHERE id=:id"),
                                 {"id":earlier["id"]})==1
    _finish(engine)
    with engine.connect() as conn:
        assert _headers(conn,copy.SOURCE)==_headers(conn,SCHEMA+".fact_versions")
        assert not conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {QUEUE})"))
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions WHERE id=:id"),
                           {"id":late["id"]})==1
