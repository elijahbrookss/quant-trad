"""Native preserving placement of archive metadata, with owned disposable disks."""
from dataclasses import replace
from datetime import timedelta
import hashlib
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from core.storage_targets import StoragePolicy
from portal.backend.db.session import Database
from scripts.db import archive_reference_v2_placement as move
from scripts.db import fact_header_v2_copy as headers
from scripts.db.fact_header_v2_capture import SCHEMA
from tests.test_market_data.test_fact_header_copy_db import _finish, _insert, source
from tests.test_market_data.test_fact_header_copy_placement_db import placed, _configure_placement, _assert_disk
from tests.test_market_data.test_fact_header_references_db import _stage_all
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement, BASE
from tests.test_market_data.test_fact_book_retention_db import (
    test_cold_book_handoff_preserves_frozen_features_checkpoint_and_replay as _cold_book,
)
from tests.test_market_data.tiered_v1_fixture import restore_tiered_v1_fixture, stage_shadow_handoff_fixture

pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO")!="1", reason="requires owned two-filesystem topology"),
]


def _prepare(storage):
    engine=storage.database._engine
    with engine.begin() as conn:
        headers.prepare_copy(conn,placement=storage.copy_plan)
    _finish(engine)
    with engine.begin() as conn:
        headers.enable_identity_capture(conn)
    return engine


def _options(storage):
    recent,history=storage.copy_plan.recent,storage.copy_plan.history
    return {
        "policy": StoragePolicy(recent=(recent.target_id,),history=(history.target_id,),
                                archives=(history.target_id,),backups=(history.target_id,)),
        "resource_limits": {
            "wal_bytes": 16*1024**2,
            "temporary_bytes": {recent.target_id:1024**2,history.target_id:0},
            "growth_bytes_per_second": {recent.target_id:0,history.target_id:0},
            "maintenance_bytes": {recent.target_id:1024**2,history.target_id:1024**2},
            "movement_timeout_seconds": 60,
            "cancellation_grace_seconds": 5,
        },
    }


def _rows(conn,relation):
    return conn.execute(text(f"SELECT to_jsonb(t) FROM {relation} t ORDER BY to_jsonb(t)::text")).scalars().all()


def _nodes(conn,relation):
    return conn.execute(text("""
        SELECT oid::bigint,relfilenode::bigint FROM pg_class
        WHERE oid=to_regclass(:relation) OR oid IN
          (SELECT indexrelid FROM pg_index WHERE indrelid=to_regclass(:relation))
        ORDER BY oid
    """), {"relation":relation}).all()


def test_reference_move_retains_rows_and_new_collection_through_interruption_and_handoff(storage,tmp_path,monkeypatch):
    _cold_book(storage,tmp_path,monkeypatch,split_sources=False)
    storage.open_day=storage.today+timedelta(days=1)
    _placement(monkeypatch,storage.open_day)
    recent=replace(storage.fact,observation_key="reference-move-recent",observation_time=BASE+timedelta(days=2))
    storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,facts=[recent])
    engine=storage.database._engine
    with engine.connect() as conn:
        before={relation:_rows(conn,relation) for relation in move.RELATIONS}
        assert all(before.values())
        pairs=conn.execute(text("SELECT dataset_id,series_id FROM market.dataset_series")).all()
        ranges=conn.execute(text("SELECT series_id,min(observation_time),max(observation_time) FROM market.fact_versions GROUP BY series_id")).all()
    frozen={tuple(pair):storage.repo.read_dataset_fact_revisions(dataset_id=pair[0],series_id=pair[1]) for pair in pairs}
    queries={(series,start,end):storage.repo.read_facts(series_id=series,start=start-timedelta(seconds=1),end=end+timedelta(seconds=1))
             for series,start,end in ranges}
    archives={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in tmp_path.rglob("*") if p.is_file()}
    restore_tiered_v1_fixture(storage)
    _configure_placement(storage,tmp_path,monkeypatch)
    _prepare(storage)
    _stage_all(engine)
    options=_options(storage)
    relation=move.RELATIONS[0]
    with engine.connect() as conn:
        nodes=_nodes(conn,relation)
    interrupted=[False]
    def kill_after_heap(conn,cursor,statement,parameters,context,executemany):
        if not interrupted[0] and statement.startswith(f"ALTER TABLE {relation} SET TABLESPACE"):
            interrupted[0]=True
            # The selected metadata table is fenced, while a real v1 source
            # transaction can still collect and mirror a new global identity.
            with engine.begin() as writer:
                writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                _insert(writer,storage,"during-reference-move")
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid":conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine,"after_cursor_execute",kill_after_heap)
    try:
        with pytest.raises(DBAPIError):
            move.move_reference_catalog(engine,relation=relation,**options)
    finally:
        event.remove(engine,"after_cursor_execute",kill_after_heap)
    assert interrupted[0]
    with engine.connect() as conn:
        assert _nodes(conn,relation)==nodes
        assert _rows(conn,relation)==before[relation]
        _assert_disk(conn,relation,Path("/qt-source/pgdata"))
        assert conn.scalar(text("SELECT count(*) FROM market.fact_versions WHERE observation_key='during-reference-move'"))==1
    for relation in move.RELATIONS:
        result=move.move_reference_catalog(engine,relation=relation,**options)
        assert result["committed"] and not result["reused"] and not result["migration_ready"]
        with engine.connect() as conn:
            assert _rows(conn,relation)==before[relation]
            _assert_disk(conn,relation,Path("/qt-history"))
            moved=_nodes(conn,relation)
        assert move.move_reference_catalog(engine,relation=relation,**options)["reused"]
        with engine.connect() as conn:
            assert _nodes(conn,relation)==moved
    _finish(engine)
    with engine.begin() as conn:
        stage_shadow_handoff_fixture(conn,storage,prevalidated=True)
    restarted=Database(storage.dsn)
    try:
        assert restarted.ensure_schema(),str(restarted.last_error)
    finally:
        restarted._reset_engine()
    for (dataset,series),expected in frozen.items():
        assert storage.repo.read_dataset_fact_revisions(dataset_id=dataset,series_id=series)==expected
    for (series,start,end),expected in queries.items():
        actual=storage.repo.read_facts(series_id=series,start=start-timedelta(seconds=1),end=end+timedelta(seconds=1))
        assert [row for row in actual if row.fact.observation_key!="during-reference-move"]==expected
    recent_rows=storage.repo.read_facts(series_id=storage.series_id,start=BASE+timedelta(days=2),
                                       end=BASE+timedelta(days=3))
    assert any(row.fact.observation_key=="during-reference-move" for row in recent_rows)
    for path,digest in archives.items():
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest
    after=replace(recent,observation_key="after-reference-move-and-handoff")
    assert storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,facts=[after]).inserted_count==1
    with engine.connect() as conn:
        for relation in move.RELATIONS:
            _assert_disk(conn,relation,Path("/qt-history"))


def test_reference_move_reconciles_lost_commit_without_recopying(placed,monkeypatch):
    engine=_prepare(placed)
    relation=move.RELATIONS[0]
    options=_options(placed)
    with engine.connect() as conn:
        before=_rows(conn,relation)
    original=Connection._commit_impl
    lost=[False]
    def committed_then_lost(conn):
        original(conn)
        if not lost[0]:
            lost[0]=True
            raise RuntimeError("injected_lost_commit_reply")
    with monkeypatch.context() as fault:
        fault.setattr(Connection,"_commit_impl",committed_then_lost)
        with pytest.raises(RuntimeError,match="injected_lost_commit_reply"):
            move.move_reference_catalog(engine,relation=relation,**options)
    assert lost[0]
    with engine.connect() as conn:
        assert _rows(conn,relation)==before
        _assert_disk(conn,relation,Path("/qt-history"))
        nodes=_nodes(conn,relation)
    assert move.move_reference_catalog(engine,relation=relation,**options)["reused"]
    with engine.connect() as conn:
        assert _nodes(conn,relation)==nodes
    # Reconciliation remains read-only after the original attempt expires.
    with engine.begin() as conn:
        conn.exec_driver_sql(f"UPDATE {SCHEMA}.capture SET prepared_at=clock_timestamp()-interval '25 hours'")
    with engine.connect() as conn:
        assert move.inspect_reference_catalog(conn,relation=relation)["placement"]=="history"
    with pytest.raises(RuntimeError,match="attempt_expired"):
        move.move_reference_catalog(engine,relation=relation,**options)


def test_reference_move_refuses_busy_mount_mixed_placement_and_insufficient_capacity(placed):
    engine=_prepare(placed)
    relation=move.RELATIONS[0]
    options=_options(placed)
    with engine.begin() as reader:
        reader.exec_driver_sql(f"SELECT 1 FROM {relation}")
        with pytest.raises(DBAPIError,match="could not obtain lock"):
            move.move_reference_catalog(engine,relation=relation,**options)
    excessive={**options,"resource_limits":{**options["resource_limits"],"wal_bytes":2**62}}
    with pytest.raises(RuntimeError,match="capacity_blocked"):
        move.move_reference_catalog(engine,relation=relation,**excessive)
    with engine.begin() as conn:
        indexes=conn.execute(text("SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid=to_regclass(:relation)"),
                             {"relation":relation}).scalars().all()
        conn.exec_driver_sql(f"ALTER INDEX {indexes[0]} SET TABLESPACE {placed.copy_history_name}")
    with pytest.raises(RuntimeError,match="mixed_or_unknown_placement"):
        move.move_reference_catalog(engine,relation=relation,**options)
    with engine.begin() as conn:
        conn.exec_driver_sql(f"ALTER INDEX {indexes[0]} SET TABLESPACE pg_default")
    dev=Path("/qt-history").stat().st_dev
    identity=placed.copy_udev/f"b{os.major(dev)}:{os.minor(dev)}"
    saved=identity.read_text()
    identity.write_text("E:ID_FS_UUID=wrong-disk\n")
    try:
        with pytest.raises((ValueError,RuntimeError)):
            move.move_reference_catalog(engine,relation=relation,**options)
    finally:
        identity.write_text(saved)
    with engine.connect() as conn:
        _assert_disk(conn,relation,Path("/qt-source/pgdata"))


def test_reference_move_space_pressure_rolls_back_all_files_and_releases_connection(placed):
    engine=_prepare(placed)
    relation=move.RELATIONS[0]
    options=_options(placed)
    pressure=Path("/qt-history")/("reference-pressure-"+uuid4().hex)
    allocated=[False]
    with engine.connect() as conn:
        nodes=_nodes(conn,relation)
        before=_rows(conn,relation)
    def consume_after_heap(conn,cursor,statement,parameters,context,executemany):
        if not allocated[0] and statement.startswith(f"ALTER TABLE {relation} SET TABLESPACE"):
            allocated[0]=True
            # Actual private tmpfs allocation, not mocked free-space evidence.
            with pressure.open("xb") as stream:
                stream.write(b"x"*(4*1024**2))
                stream.flush()
                os.fsync(stream.fileno())
    event.listen(engine,"after_cursor_execute",consume_after_heap)
    try:
        with pytest.raises(RuntimeError,match="space_budget_exceeded"):
            move.move_reference_catalog(engine,relation=relation,**options)
    finally:
        event.remove(engine,"after_cursor_execute",consume_after_heap)
        pressure.unlink(missing_ok=True)
    assert allocated[0]
    with engine.begin() as conn:
        assert _nodes(conn,relation)==nodes
        assert _rows(conn,relation)==before
        _assert_disk(conn,relation,Path("/qt-source/pgdata"))
        _insert(conn,placed,"after-reference-capacity-refusal")
    assert move.move_reference_catalog(engine,relation=relation,**options)["committed"]
