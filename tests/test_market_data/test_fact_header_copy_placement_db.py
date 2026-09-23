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
    return _configure_placement(source,tmp_path,monkeypatch)


def _configure_placement(source,tmp_path,monkeypatch):
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
        _assert_disk(conn,SCHEMA+".fact_identities",Path("/qt-source/pgdata"))
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


def test_history_destination_recovers_lost_create_without_replacing_files(placed):
    from scripts.db.fact_header_v2_placement import prepare_history_tablespace, observe
    engine = placed.database._engine
    old_plan = placed.copy_plan
    options = dict(recent=old_plan.recent,history=old_plan.history,
        history_before=old_plan.history_before,pg_controldata=old_plan.pg_controldata)
    lost = [False]
    def lose_reply(conn,cursor,statement,parameters,context,executemany):
        if not lost[0] and statement.startswith("CREATE TABLESPACE "):
            lost[0] = True
            raise RuntimeError("injected_tablespace_reply_lost")
    event.listen(engine,"after_cursor_execute",lose_reply)
    try:
        with pytest.raises(RuntimeError,match="injected_tablespace_reply_lost"):
            prepare_history_tablespace(engine,**options)
    finally:
        event.remove(engine,"after_cursor_execute",lose_reply)
    assert lost[0]
    with engine.connect() as conn:
        oid = conn.scalar(text("SELECT oid FROM pg_tablespace WHERE spcname='qt_history_hdd'"))
        assert oid
        assert _headers(conn,copy.SOURCE) == placed.source_before
    directory = Path(old_plan.history.root)/"postgres"
    inode = directory.stat().st_ino
    plan = prepare_history_tablespace(engine,**options)
    assert plan.history_tablespace_oid == oid
    assert prepare_history_tablespace(engine,**options) == plan
    assert directory.stat().st_ino == inode
    with engine.begin() as conn:
        binding,_ = observe(conn,plan)
        assert binding["history_location"] == str(directory)
        copy.prepare_copy(conn,placement=plan)
        _assert_disk(conn,SCHEMA+".fact_identities",Path("/qt-source/pgdata"))
    # An existing directory with catalog data is never adopted under another
    # target identity or cleared to make preparation succeed.
    with pytest.raises(RuntimeError,match="nonempty_unregistered_directory"):
        prepare_history_tablespace(engine,**(options | {
            "history": replace(old_plan.history,target_id="other_hdd")}))
    assert directory.stat().st_ino == inode


def test_private_identity_relocation_rolls_back_then_retries(placed):
    engine=placed.database._engine
    target=SCHEMA+".fact_identities"
    with engine.begin() as conn:
        copy.prepare_copy(conn,placement=placed.copy_plan)
        with pytest.raises(RuntimeError,match="history_baseline_incomplete"):
            copy.place_identity_on_history(conn)
    for _ in range(8):
        with engine.begin() as conn:report=copy.copy_page(conn,page_rows=2)
        if report["caught_up_at_observation"]:break
    assert report["caught_up_at_observation"]
    killed=[False]
    def terminate(conn,cursor,statement,parameters,context,executemany):
        if not killed[0] and statement.startswith("ALTER TABLE "+target+" SET TABLESPACE"):
            killed[0]=True
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid":conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine,"after_cursor_execute",terminate)
    try:
        with pytest.raises(DBAPIError),engine.begin() as conn:copy.place_identity_on_history(conn)
    finally:event.remove(engine,"after_cursor_execute",terminate)
    assert killed[0]
    with engine.begin() as conn:
        assert not copy._inspect_progress(conn)["identity_history_ready"]
        _assert_disk(conn,target,Path("/qt-source/pgdata"))
        assert _headers(conn,copy.SOURCE)==placed.source_before
        assert not copy.place_identity_on_history(conn)["reused"]
    with engine.begin() as conn:
        assert copy.place_identity_on_history(conn)["reused"]
        _assert_disk(conn,target,Path("/qt-history"))
        copy.enable_identity_capture(conn)
        with copy.verified_copy(conn,timeout_seconds=60) as verified:
            assert verified["verified_header_rows"]==verified["verified_identity_rows"]==7
        assert _headers(conn,copy.SOURCE)==placed.source_before
