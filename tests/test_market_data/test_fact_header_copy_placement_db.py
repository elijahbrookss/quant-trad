"""Preserving copy places new files on two actual disposable filesystems."""
from dataclasses import replace
from datetime import timedelta
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import event,text
from sqlalchemy.exc import DBAPIError

from core.storage_targets import StorageTarget
from scripts.db import fact_header_v2_copy as copy
from scripts.db.fact_header_v2_placement import CopyPlacement
from scripts.db.fact_header_v2_capture import SCHEMA
from tests.test_market_data.test_fact_header_copy_db import source,_headers,_finish,_insert
from tests.test_market_data.tiered_v1_fixture import stage_shadow_handoff_fixture
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark=[
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO")!="1",
                      reason="requires owned two-filesystem storage-demo topology"),
]


@pytest.fixture
def placed(source,tmp_path,monkeypatch):
    assert os.getenv("QT_DB_TEST_ISOLATED")=="1" and os.getuid()==70
    recent,history=Path("/qt-source/pgdata"),Path("/qt-history")
    assert recent.stat().st_dev!=history.stat().st_dev
    udev=tmp_path/"udev-placement"
    udev.mkdir()
    targets=(StorageTarget("ssd","Recent","uuid-copy-ssd",str(recent),"ssd"),
             StorageTarget("hdd","History","uuid-copy-hdd",str(history),"hdd"))
    for target in targets:
        dev=Path(target.root).stat().st_dev
        (udev/f"b{os.major(dev)}:{os.minor(dev)}").write_text("E:ID_FS_UUID="+target.filesystem_uuid+"\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT",str(udev))
    name="qt_copy_history_"+uuid4().hex[:12]
    directory=history/name
    directory.mkdir()
    with source.database._engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql(f"CREATE TABLESPACE {name} LOCATION '{directory}'")
        oid=conn.scalar(text("SELECT oid::bigint FROM pg_tablespace WHERE spcname=:name"),{"name":name})
    plan=CopyPlacement(targets[0],targets[1],oid,source.open_day,
                       Path("/usr/lib/postgresql/15/bin/pg_controldata"))
    source.copy_plan=plan
    source.copy_udev=udev
    source.copy_history_name=name
    return source


def _files(conn, relation):
    return conn.execute(text("""
        WITH heap AS (SELECT oid,reltoastrelid FROM pg_class WHERE oid=to_regclass(:relation)),
        heaps AS (SELECT oid FROM heap UNION ALL SELECT reltoastrelid FROM heap WHERE reltoastrelid<>0),
        members AS (SELECT oid FROM heaps UNION ALL SELECT indexrelid FROM pg_index WHERE indrelid IN (SELECT oid FROM heaps))
        SELECT c.relkind,pg_relation_filepath(c.oid) AS path FROM members JOIN pg_class c ON c.oid=members.oid
        WHERE c.relkind NOT IN ('p','I') ORDER BY c.oid
    """),{"relation":relation}).all()


def _assert_disk(conn, relation, root):
    members=_files(conn,relation)
    assert members and any(kind=="i" for kind,path in members)
    expected=root.stat().st_dev
    assert all((Path("/qt-source/pgdata")/path).stat().st_dev==expected for kind,path in members)


def test_copy_starts_on_correct_drives_and_recovers_before_preserving_handoff(placed):
    storage=placed
    engine=storage.database._engine
    with engine.begin() as conn:
        before=conn.scalar(text("SHOW default_tablespace"))
        assert copy.prepare_copy(conn,placement=storage.copy_plan)["physical_placement_configured"]
        assert conn.scalar(text("SHOW default_tablespace"))==before
        _assert_disk(conn,SCHEMA+".fact_identities",Path("/qt-history"))
        _assert_disk(conn,SCHEMA+".pending_fact_ids",Path("/qt-source/pgdata"))
        with pytest.raises(RuntimeError,match="placement_cannot_change"):
            copy.prepare_copy(conn)
        with pytest.raises(RuntimeError,match="placement_cannot_change"):
            copy.prepare_copy(conn,placement=replace(storage.copy_plan,
                              history_before=storage.copy_plan.history_before-timedelta(days=1)))
        assert copy.prepare_copy(conn,placement=storage.copy_plan)["reused"]
    killed=[False]
    def terminate(conn,cursor,statement,parameters,context,executemany):
        if not killed[0] and statement.startswith("INSERT INTO "+SCHEMA+".fact_versions"):
            killed[0]=True
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                       {"pid":conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine,"after_cursor_execute",terminate)
    try:
        with pytest.raises(DBAPIError),engine.begin() as conn:
            copy.copy_page(conn,page_rows=2)
    finally:
        event.remove(engine,"after_cursor_execute",terminate)
    assert killed[0]
    with engine.begin() as conn:
        assert _headers(conn,copy.SOURCE)==storage.source_before
        assert conn.scalar(text(f"SELECT verified_rows FROM {copy.STATE}"))==0
        assert not _headers(conn,SCHEMA+".fact_versions")
    _finish(engine)
    with engine.begin() as conn:
        copy.enable_identity_capture(conn)
        later=_insert(conn,storage,"physical-copy-late")
    _finish(engine)
    with engine.begin() as conn:
        assert _headers(conn,copy.SOURCE)==_headers(conn,SCHEMA+".fact_versions")
        _assert_disk(conn,SCHEMA+".fact_versions_"+storage.today.strftime("%Y%m%d"),Path("/qt-history"))
        _assert_disk(conn,SCHEMA+".fact_versions_"+storage.open_day.strftime("%Y%m%d"),Path("/qt-source/pgdata"))
        _assert_disk(conn,SCHEMA+".fact_identities",Path("/qt-history"))
        stage_shadow_handoff_fixture(conn,storage)
        _assert_disk(conn,"market.fact_identities",Path("/qt-history"))
    from portal.backend.db.session import Database
    restarted=Database(storage.dsn)
    try:
        assert restarted.ensure_schema(),str(restarted.last_error)
    finally:
        restarted._reset_engine()
    new=replace(storage.original_facts[0],observation_key="physical-after-switch",
                observation_time=storage.original_facts[0].observation_time+timedelta(days=2))
    assert storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,
                                    facts=[new]).inserted_count==1
    with engine.connect() as conn:
        _assert_disk(conn,"market.fact_identities",Path("/qt-history"))
        _assert_disk(conn,"market.fact_versions_"+storage.open_day.strftime("%Y%m%d"),Path("/qt-source/pgdata"))
    assert storage.repo.read_dataset_fact_revisions(
        dataset_id=storage.frozen_dataset_id,series_id=storage.series_id)==storage.frozen_result
    records=storage.repo.read_facts(series_id=storage.series_id,start=storage.dataset_request.start,
                                   end=storage.dataset_request.end+timedelta(days=3))
    assert len(records)==len(storage.query_before)+2
    assert any(record.fact.observation_key==later["observation_key"] and
               record.market_commit_seq==later["market_commit_seq"] for record in records)
    assert storage.archive_path.read_bytes()==storage.archive_bytes


def test_copy_refuses_missing_mount_and_misplaced_index_without_advancing(placed):
    storage=placed
    engine=storage.database._engine
    dev=Path("/qt-history").stat().st_dev
    identity=storage.copy_udev/f"b{os.major(dev)}:{os.minor(dev)}"
    original=identity.read_text()
    identity.write_text("E:ID_FS_UUID=wrong-filesystem\n")
    try:
        with pytest.raises(RuntimeError,match="storage_mount_identity_mismatch"),engine.begin() as conn:
            copy.prepare_copy(conn,placement=storage.copy_plan)
        with engine.connect() as conn:
            assert conn.scalar(text("SELECT to_regnamespace(:name)"),{"name":SCHEMA}) is None
            assert _headers(conn,copy.SOURCE)==storage.source_before
    finally:
        identity.write_text(original)
    with engine.begin() as conn:
        copy.prepare_copy(conn,placement=storage.copy_plan)
        copy.copy_page(conn,page_rows=2)
        cursor=conn.execute(text(f"SELECT after_day,after_seq,after_id,verified_rows FROM {copy.STATE}")).one()
        partition=SCHEMA+".fact_versions_"+storage.today.strftime("%Y%m%d")
        index=conn.scalar(text("""
            SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
            WHERE i.indrelid=to_regclass(:relation) ORDER BY i.indisprimary,c.oid LIMIT 1
        """),{"relation":partition})
        quoted=conn.dialect.identifier_preparer.quote(index)
        conn.exec_driver_sql(f"ALTER INDEX {SCHEMA}.{quoted} SET TABLESPACE pg_default")
    with engine.begin() as conn:
        with pytest.raises(RuntimeError,match="relation_on_wrong_tablespace"):
            copy.copy_page(conn,page_rows=2)
        assert conn.execute(text(f"SELECT after_day,after_seq,after_id,verified_rows FROM {copy.STATE}")).one()==cursor
        assert _headers(conn,copy.SOURCE)==storage.source_before
        conn.exec_driver_sql(f"ALTER INDEX {SCHEMA}.{quoted} SET TABLESPACE {storage.copy_history_name}")
    identity.write_text("E:ID_FS_UUID=wrong-filesystem\n")
    try:
        with engine.begin() as conn:
            with pytest.raises(RuntimeError,match="storage_mount_identity_mismatch"):
                copy.copy_page(conn,page_rows=2)
            assert conn.execute(text(f"SELECT after_day,after_seq,after_id,verified_rows FROM {copy.STATE}")).one()==cursor
    finally:
        identity.write_text(original)
    _finish(engine)
    with engine.connect() as conn:
        assert _headers(conn,copy.SOURCE)==_headers(conn,SCHEMA+".fact_versions")
        _assert_disk(conn,partition,Path("/qt-history"))
