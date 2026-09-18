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
from tests.test_market_data.tiered_v1_fixture import restore_tiered_v1_fixture

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
