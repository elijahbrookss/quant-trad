"""Original archive lookup remains authoritative during an interrupted HDD copy."""
from dataclasses import replace
from datetime import timedelta
import hashlib
import os
from pathlib import Path

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from portal.backend.db.session import Database
from portal.backend.service.market.market_structure_service import MarketStructureService
from portal.backend.service.storage.repos import market_structure
from scripts.db import fact_header_v2_copy as headers, raw_mapping_v2_copy as copy
from scripts.db.fact_header_v2_capture import SCHEMA
from tests.test_market_data.test_fact_header_copy_placement_db import (
    placed, source, _assert_disk, _configure_placement,
)
from tests.test_market_data.test_fact_header_copy_db import _finish as finish_headers, _insert, _frozen_records
from tests.test_market_data.test_fact_raw_lineage_db import _raw_trade_fixture, _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.tiered_v1_fixture import restore_tiered_v1_fixture, stage_shadow_handoff_fixture

pytestmark=[
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO")!="1",
                      reason="requires owned two-filesystem storage-demo topology"),
]


def _rows(conn,relation=copy.SOURCE):
    return [dict(row) for row in conn.execute(text(
        f"SELECT {','.join(copy.COLUMNS)} FROM {relation} ORDER BY raw_record_id,manifest_id")).mappings()]


def _finish(engine):
    for _ in range(64):
        with engine.begin() as conn:
            report=copy.copy_page(conn,page_rows=1)
        if report["caught_up_at_observation"]:
            return report
    pytest.fail("raw mapping copy did not catch up within fixture budget")


def test_raw_mapping_copy_resumes_killed_page_and_captures_new_archives(placed,tmp_path,monkeypatch):
    storage=placed
    engine=storage.database._engine
    _raw_trade_fixture(storage,tmp_path,monkeypatch)
    with engine.begin() as conn:
        headers.prepare_copy(conn,placement=storage.copy_plan)
        before=_rows(conn)
        assert len(before)==2
        original_timeout=conn.scalar(text("SHOW statement_timeout"))
        report=copy.prepare_copy(conn)
        assert report["source_authoritative"] and not report["migration_ready"]
        assert conn.scalar(text("SHOW statement_timeout"))==original_timeout
        assert copy.prepare_copy(conn)["reused"]
        _assert_disk(conn,copy.TARGET,Path("/qt-history"))
        _assert_disk(conn,copy.SOURCE,Path("/qt-source/pgdata"))
        _assert_disk(conn,copy.QUEUE,Path("/qt-source/pgdata"))
    killed=[False]
    def terminate(conn,cursor,statement,parameters,context,executemany):
        if not killed[0] and statement.startswith("INSERT INTO "+copy.TARGET):
            killed[0]=True
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                                     {"pid":conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine,"after_cursor_execute",terminate)
    try:
        with pytest.raises(DBAPIError),engine.begin() as conn:
            copy.copy_page(conn,page_rows=1)
    finally:
        event.remove(engine,"after_cursor_execute",terminate)
    assert killed[0]
    with engine.begin() as conn:
        assert _rows(conn)==before and not _rows(conn,copy.TARGET)
        assert conn.scalar(text(f"SELECT verified_rows FROM {copy.STATE}"))==0
        copy.copy_page(conn,page_rows=1)
    # Real publication while the baseline is incomplete; source writers never
    # need the private HDD target. Exact keys are captured in their transaction.
    _raw_book_fixture(storage,tmp_path,monkeypatch,definition_id="raw-copy-during")
    with engine.begin() as conn:
        assert conn.scalar(text(f"SELECT count(*) FROM {copy.QUEUE}"))>0
        _insert(conn,storage,"raw-copy-concurrent-fact")
    assert _finish(engine)["caught_up_at_observation"]
    _raw_book_fixture(storage,tmp_path,monkeypatch,definition_id="raw-copy-after",provider_product_id="BTC-USDT")
    assert _finish(engine)["caught_up_at_observation"]
    with engine.begin() as conn:
        assert _rows(conn)==_rows(conn,copy.TARGET)
        assert len(_rows(conn))>len(before)
        _assert_disk(conn,copy.TARGET,Path("/qt-history"))
    with engine.connect() as conn:
        assert _frozen_records(conn)==storage.frozen_before
    assert storage.archive_path.read_bytes()==storage.archive_bytes


def test_raw_mapping_copy_refuses_changed_guards_content_and_mount(placed,tmp_path,monkeypatch):
    engine=placed.database._engine
    _raw_trade_fixture(placed,tmp_path,monkeypatch)
    with engine.begin() as conn:
        headers.prepare_copy(conn,placement=placed.copy_plan)
        before=_rows(conn)
        # Unrecognized source behavior refuses preparation; nested DDL must
        # leave neither a target nor a capture queue behind.
        conn.exec_driver_sql(f"CREATE INDEX unexpected_raw_index ON {copy.SOURCE}(known_at)")
        with pytest.raises(RuntimeError,match="source_definition_changed"):
            copy.prepare_copy(conn)
        assert conn.scalar(text("SELECT to_regclass(:target)"),{"target":copy.TARGET}) is None
        conn.exec_driver_sql("DROP INDEX market.unexpected_raw_index")
        conn.exec_driver_sql(f"CREATE VIEW market.unexpected_raw_view AS SELECT raw_record_id FROM {copy.SOURCE}")
        with pytest.raises(RuntimeError,match="source_dependency_changed"):
            copy.prepare_copy(conn)
        assert conn.scalar(text("SELECT to_regclass(:queue)"),{"queue":copy.QUEUE}) is None
        conn.exec_driver_sql("DROP VIEW market.unexpected_raw_view")
        copy.prepare_copy(conn)
    with pytest.raises(DBAPIError,match="immutable"),engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE {copy.SOURCE}")
    with engine.begin() as conn:
        with conn.begin_nested() as temporary:
            conn.exec_driver_sql(f"ALTER TABLE {copy.SOURCE} DISABLE TRIGGER trg_qt_raw_mapping_v2_capture")
            with pytest.raises(RuntimeError,match="bound_layout_changed"):
                copy.copy_page(conn)
            temporary.rollback()
        # A damaged pre-existing target row must never retire progress.
        row={**before[0],"raw_frame_sha256":"0"*64}
        conn.execute(copy._table().insert(),row)
        with pytest.raises(RuntimeError,match="full_row_mismatch"):
            copy.copy_page(conn,page_rows=1)
        assert conn.scalar(text(f"SELECT verified_rows FROM {copy.STATE}"))==0
        assert _rows(conn)==before
        conn.exec_driver_sql(f"DELETE FROM {copy.TARGET}")
        with conn.begin_nested() as temporary:
            conn.exec_driver_sql(f"ALTER INDEX {SCHEMA}.ix_market_raw_archive_mapping_segment SET TABLESPACE pg_default")
            with pytest.raises(RuntimeError,match="relation_on_wrong_tablespace"):
                copy.copy_page(conn)
            temporary.rollback()
    dev=Path("/qt-history").stat().st_dev
    identity=placed.copy_udev/f"b{os.major(dev)}:{os.minor(dev)}"
    original=identity.read_text()
    identity.write_text("E:ID_FS_UUID=wrong-disk\n")
    try:
        with pytest.raises((RuntimeError,ValueError)),engine.begin() as conn:
            copy.copy_page(conn)
    finally:
        identity.write_text(original)
    _finish(engine)
    with engine.begin() as conn:
        assert _rows(conn)==before==_rows(conn,copy.TARGET)


def test_cold_book_and_frozen_results_survive_raw_mapping_handoff(storage,tmp_path,monkeypatch):
    from tests.test_market_data.test_fact_book_retention_db import (
        test_cold_book_handoff_preserves_frozen_features_checkpoint_and_replay,
    )
    test_cold_book_handoff_preserves_frozen_features_checkpoint_and_replay(
        storage,tmp_path,monkeypatch,split_sources=False)
    storage.open_day=storage.today+timedelta(days=1)
    _placement(monkeypatch,storage.open_day)
    recent=replace(storage.fact,observation_key="raw-copy-recent",observation_time=BASE+timedelta(days=2))
    storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,facts=[recent])
    engine=storage.database._engine
    with engine.connect() as conn:
        before=_rows(conn)
        frozen_pairs=conn.execute(text("SELECT dataset_id,series_id FROM market.dataset_series")).all()
        ranges=conn.execute(text("SELECT series_id,min(observation_time),max(observation_time) FROM market.fact_versions GROUP BY series_id")).all()
        sessions=conn.execute(text("""
            SELECT DISTINCT m.definition_id,m.session_id FROM market.raw_archive_manifests m
            JOIN market.stream_definitions d ON d.id=m.definition_id WHERE d.channels ? 'level2'
        """)).all()
    frozen={tuple(pair):storage.repo.read_dataset_fact_revisions(dataset_id=pair[0],series_id=pair[1]) for pair in frozen_pairs}
    queries={(series,start,end):storage.repo.read_facts(series_id=series,start=start-timedelta(seconds=1),end=end+timedelta(seconds=1))
             for series,start,end in ranges}
    replay=MarketStructureService(repository=market_structure.PostgresMarketStructureRepository())
    replays={tuple(pair):replay.replay_book_session(definition_id=pair[0],session_id=pair[1],storage_root=tmp_path)
             for pair in sessions}
    assert replays
    archive_bytes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in tmp_path.rglob("*") if p.is_file()}
    restore_tiered_v1_fixture(storage)
    _configure_placement(storage,tmp_path,monkeypatch)
    with engine.begin() as conn:
        headers.prepare_copy(conn,placement=storage.copy_plan)
        copy.prepare_copy(conn)
    finish_headers(engine)
    _finish(engine)
    with engine.begin() as conn:
        headers.enable_identity_capture(conn)
    with engine.begin() as conn:
        # This switch is still a tiny guarded fixture, never an operator command.
        with pytest.raises(RuntimeError,match="injected switch interruption"),conn.begin_nested():
            stage_shadow_handoff_fixture(conn,storage,raw_mapping=True)
            raise RuntimeError("injected switch interruption")
        assert _rows(conn)==before
        assert _rows(conn,copy.TARGET)==before
        stage_shadow_handoff_fixture(conn,storage,raw_mapping=True)
        assert _rows(conn)==before
        assert _rows(conn,"qt_fact_header_retained_v1."+copy.NAME)==before
        _assert_disk(conn,copy.SOURCE,Path("/qt-history"))
    restarted=Database(storage.dsn)
    try:
        assert restarted.ensure_schema(),str(restarted.last_error)
    finally:
        restarted._reset_engine()
    for (dataset,series),records in frozen.items():
        assert storage.repo.read_dataset_fact_revisions(dataset_id=dataset,series_id=series)==records
    for (series,start,end),records in queries.items():
        assert storage.repo.read_facts(series_id=series,start=start-timedelta(seconds=1),end=end+timedelta(seconds=1))==records
    for (definition,session),result in replays.items():
        assert replay.replay_book_session(definition_id=definition,session_id=session,storage_root=tmp_path)==result
    assert all(hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest
               for name,digest in archive_bytes.items())
    _raw_book_fixture(storage,tmp_path,monkeypatch,definition_id="raw-copy-new-active",provider_product_id="BTC-USDT")
    with engine.begin() as conn:
        assert len(_rows(conn))>len(before)
        assert _rows(conn,"qt_fact_header_retained_v1."+copy.NAME)==before
        _assert_disk(conn,copy.SOURCE,Path("/qt-history"))



def test_lookup_final_verification_refuses_content_drift_and_unfenced_use(placed,tmp_path,monkeypatch):
    engine=placed.database._engine
    _raw_trade_fixture(placed,tmp_path,monkeypatch)
    with engine.begin() as conn:
        headers.prepare_copy(conn,placement=placed.copy_plan)
        copy.prepare_copy(conn)
    finish_headers(engine)
    _finish(engine)
    with engine.begin() as conn:
        headers.enable_identity_capture(conn)
    with engine.begin() as conn:
        with pytest.raises(RuntimeError,match="header_fence_required"):
            with copy.verified_copy(conn):
                pytest.fail("unfenced raw verification admitted")
        with headers.verified_copy(conn,page_rows=2,timeout_seconds=60):
            original=_rows(conn)
            with copy.verified_copy(conn,page_rows=1) as report:
                assert report["verified_lookup_rows"]==len(original)==2
                with engine.begin() as reader:
                    reader.exec_driver_sql("SET LOCAL lock_timeout='250ms'")
                    assert _rows(reader)==original==_rows(reader,copy.TARGET)
                assert not report["migration_ready"]
            with conn.begin_nested() as damage:
                conn.exec_driver_sql(f"UPDATE {copy.TARGET} SET raw_frame_sha256=repeat('e',64)")
                with pytest.raises(RuntimeError,match="raw_lookup"):
                    with copy.verified_copy(conn,page_rows=1):
                        pytest.fail("same-count changed lookup admitted")
                assert _rows(conn)==original
                damage.rollback()
            with copy.verified_copy(conn,page_rows=1) as report:
                assert report["verified_lookup_rows"]==2
    assert placed.archive_path.read_bytes()==placed.archive_bytes
