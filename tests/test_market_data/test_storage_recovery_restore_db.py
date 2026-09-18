"""Disposable dump+archive restore proof; not a scheduler or rotation executor."""
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess

import pytest
from sqlalchemy import create_engine,text
from sqlalchemy.engine import make_url

from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.db.session import Database
from core.storage_targets import StorageTarget
from portal.backend.service.storage.recovery_copies import LocalRecoveryCopies
from portal.backend.service.market.market_structure_service import MarketStructureService
from portal.backend.service.storage.repos import market_data,market_structure,market_lifecycle
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from portal.backend.service.storage.repos.market_lifecycle import PostgresMarketStorageLifecycleRepository
from tests.test_market_data.migration_test_support import fresh_migration_database
from tests.test_market_data.test_fact_storage_tiers_db import storage,BASE,_placement
from tests.test_market_data.test_fact_book_retention_db import (
    test_cold_book_handoff_preserves_frozen_features_checkpoint_and_replay as _cold_book,
)
from tests.test_market_data.storage_metadata_fixture import prepare_metadata_candidate,observe_metadata

pytestmark=[
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO")!="1",
                      reason="requires owned storage-demo filesystems"),
]


def _client(tool,dsn,*arguments):
    url=make_url(dsn)
    assert os.getenv("QT_DB_TEST_ISOLATED")=="1"
    assert url.host=="timescaledb" and url.database.startswith("qt_migration_")
    assert url.username.startswith("qt_test_") and url.password
    binary=Path("/usr/lib/postgresql/15/bin")/tool
    env={"PATH":os.defpath,"LC_ALL":"C","HOME":"/tmp",
         "PGHOST":url.host,"PGPORT":str(url.port or 5432),"PGDATABASE":url.database,
         "PGUSER":url.username,"PGPASSWORD":url.password,"PGCONNECT_TIMEOUT":"10"}
    result=subprocess.run([str(binary),*arguments],env=env,stdin=subprocess.DEVNULL,
                          capture_output=True,text=True,timeout=120)
    assert result.returncode==0,result.stderr
    return result.stdout


def _files(root):
    paths=[p for p in root.rglob("*") if p.is_file()]
    assert paths and all(not p.is_symlink() for p in paths)
    assert sum(p.stat().st_size for p in paths)<16*1024**2
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


def test_consistent_local_copy_restores_hdd_metadata_cold_books_and_frozen_results(storage,tmp_path,monkeypatch):
    assert os.getuid()==70
    source_root=tmp_path/"source-archives"
    source_root.mkdir()
    _cold_book(storage,source_root,monkeypatch,split_sources=False)
    _placement(monkeypatch,storage.today)
    recent=replace(storage.fact,observation_key="recovery-recent",observation_time=BASE+timedelta(days=2))
    assert storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,facts=[recent]).inserted_count==1
    engine=storage.database._engine
    history_root=Path("/qt-history")
    tablespace=history_root/"qt_demo_history_cold"
    tablespace.mkdir()
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql(f"CREATE TABLESPACE qt_demo_history_cold LOCATION '{tablespace}'")
    prepare_metadata_candidate(engine,Path("/qt-source/pgdata"),history_root,
                               tablespace_name="qt_demo_history_cold")
    metadata=observe_metadata(engine,Path("/qt-source/pgdata"))
    assert all(item["rows"]>0 for item in metadata.values())
    with engine.connect() as conn:
        ranges=conn.execute(text("""
            SELECT series_id,min(observation_time),max(observation_time)
            FROM market.fact_versions GROUP BY series_id
        """)).all()
        pairs=conn.execute(text("SELECT dataset_id,series_id FROM market.dataset_series")).all()
        sessions=conn.execute(text("""
            SELECT DISTINCT m.definition_id,m.session_id FROM market.raw_archive_manifests m
            JOIN market.stream_definitions d ON d.id=m.definition_id
            WHERE d.channels @> '["level2"]'::jsonb
        """)).all()
        assert sessions
        assert all(conn.scalar(text("SELECT count(*) FROM market."+name))>0
                   for name in ("raw_archive_manifests","book_checkpoint_manifests","fact_archive_manifests"))
    reads={(series,start,end):storage.repo.read_facts(series_id=series,start=start-timedelta(seconds=1),
             end=end+timedelta(seconds=1)) for series,start,end in ranges}
    frozen={tuple(pair):storage.repo.read_dataset_fact_revisions(dataset_id=pair[0],series_id=pair[1]) for pair in pairs}
    replay=MarketStructureService(repository=market_structure.PostgresMarketStructureRepository())
    replays={tuple(pair):replay.replay_book_session(definition_id=pair[0],session_id=pair[1],
                                                   storage_root=source_root) for pair in sessions}
    archives=_files(source_root/"objects")
    original_inodes={str(p.relative_to(source_root/"objects")):p.stat().st_ino
                     for p in (source_root/"objects").rglob("*") if p.is_file()}
    udev=tmp_path/"recovery-udev"
    udev.mkdir()
    device=history_root.stat().st_dev
    (udev/f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=uuid-recovery-history\\n".replace("\\n","\n"))
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT",str(udev))
    target=StorageTarget("hdd","History","uuid-recovery-history",str(history_root),"hdd")
    def copies(identity,*,max_bytes=16*1024**2):
        return LocalRecoveryCopies(target=target,database_identity=identity,max_bytes=max_bytes,
                                   reserve_bytes=1024**2,timeout_seconds=120,max_objects=1000)
    with PostgresMarketStorageLifecycleRepository.dataset_snapshot_session(database=storage.database) as snapshot:
        identity=snapshot.scalar(text("""
            SELECT c.system_identifier::text||'/'||d.oid::text
            FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()
        """))
        snapshot_rows=snapshot.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one()
        with engine.connect() as contender:
            assert not contender.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"),
                                        {"name":market_lifecycle._LIFECYCLE_LOCK_NAME})
        # This commit must remain on the source but outside the recovery snapshot.
        after=replace(recent,observation_key="recovery-after-snapshot")
        assert storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,facts=[after]).inserted_count==1
        objects=FilesystemRawArchiveObjectStore(source_root/"objects",writable=False)
        options={"objects":objects,"pg_dump":Path("/usr/lib/postgresql/15/bin/pg_dump"),"keep_copies":2}
        first_manager=copies(identity)
        first=first_manager.create(snapshot,**options)
        first_path=first_manager.root/first["name"]
        # A failed new copy cannot retire the known completed generation.
        with pytest.raises(RuntimeError,match="recovery_byte_budget_exceeded"):
            copies(identity,max_bytes=1).create(snapshot,**options)
        assert first_path.is_dir()
        second=copies(identity).create(snapshot,**options)
        second_path=first_manager.root/second["name"]
        assert first_path.is_dir() and second_path.is_dir()
        third=copies(identity).create(snapshot,**options)
        recovery=first_manager.root/third["name"]
        dump=recovery/"database.dump"
        assert not first_path.exists() and second_path.is_dir() and recovery.is_dir()
        assert not list(first_manager.root.glob(".copy_*"))
        inventory=[json.loads(line) for line in (recovery/"objects.jsonl").read_text().splitlines()]
        copied=_files(recovery/"objects")
        assert copied=={item["object_key"]:item["sha256"] for item in inventory}
        assert copied=={key:archives[key] for key in copied}
        assert third["archive_objects"]==len(copied)
        assert third["database"]["sha256"]==hashlib.sha256(dump.read_bytes()).hexdigest()
    assert dump.stat().st_size>0
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT count(*) FROM market.fact_versions"))==snapshot_rows+1
        assert conn.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"),
                           {"name":market_lifecycle._LIFECYCLE_LOCK_NAME})
        assert conn.scalar(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"),
                           {"name":market_lifecycle._LIFECYCLE_LOCK_NAME})
    # Replace the fixture's archive mount path with copied bytes; keep originals
    # out of the reader path. No hardlinks or fallback to source files qualify.
    retained=tmp_path/"retained-source-archives"
    source_root.rename(retained)
    source_root.mkdir()
    shutil.copytree(recovery/"objects",source_root/"objects")
    assert _files(source_root/"objects")==copied
    assert all(p.stat().st_ino!=original_inodes[str(p.relative_to(source_root/"objects"))]
               for p in (source_root/"objects").rglob("*") if p.is_file())
    with fresh_migration_database("storage_restore") as restored_dsn:
        restore_engine=create_engine(restored_dsn)
        try:
            with restore_engine.begin() as conn:
                conn.execute(text("SELECT timescaledb_pre_restore()"))
            try:
                # PostgreSQL cannot infer the lookup inside the payload CHECK.
                # Keep validation enabled and load its immutable registry first.
                toc=_client("pg_restore",restored_dsn,"--list",str(dump)).splitlines()
                registry=[line for line in toc if re.fullmatch(
                    r"\d+; \d+ \d+ TABLE DATA market fact_schemas \S+",line)]
                assert len(registry)==1,"expected exactly one canonical schema registry in dump"
                ordered=recovery/"restore-order.list"
                ordered.write_text("\n".join(registry+[line for line in toc if line not in registry])+"\n")
                options=("--exit-on-error","--no-owner","--no-privileges",
                         "--dbname="+make_url(restored_dsn).database)
                _client("pg_restore",restored_dsn,*options,"--section=pre-data",str(dump))
                _client("pg_restore",restored_dsn,*options,"--section=data",
                        "--use-list="+str(ordered),str(dump))
                _client("pg_restore",restored_dsn,*options,"--section=post-data",str(dump))
            finally:
                with restore_engine.begin() as conn:
                    conn.execute(text("SELECT timescaledb_post_restore()"))
        finally:
            restore_engine.dispose()
        restored=Database(restored_dsn)
        try:
            assert restored.ensure_schema(),str(restored.last_error)
            for module in (market_data,market_structure,market_lifecycle):
                monkeypatch.setattr(module,"db",restored)
            reader=FilesystemRawArchiveObjectStore(source_root/"objects",writable=False)
            tiered=PostgresCanonicalFactStorageRepository(object_store_factory=lambda:reader)
            monkeypatch.setattr(market_data,"canonical_fact_storage_repository",tiered)
            monkeypatch.setattr(market_structure,"canonical_fact_storage_repository",tiered)
            repo=market_data.PostgresMarketDataRepository()
            with restored.session() as session:
                assert session.scalar(text("SELECT count(*) FROM market.fact_versions"))==snapshot_rows
                assert not session.scalar(text("SELECT EXISTS(SELECT 1 FROM market.fact_versions WHERE observation_key='recovery-after-snapshot')"))
            for (series,start,end),expected in reads.items():
                assert repo.read_facts(series_id=series,start=start-timedelta(seconds=1),
                                       end=end+timedelta(seconds=1))==expected
            for (dataset,series),expected in frozen.items():
                assert repo.read_dataset_fact_revisions(dataset_id=dataset,series_id=series)==expected
            restored_replay=MarketStructureService(repository=market_structure.PostgresMarketStructureRepository())
            for (definition,session),expected in replays.items():
                actual=restored_replay.replay_book_session(definition_id=definition,session_id=session,
                                                          storage_root=source_root)
                assert actual==expected
            after_metadata=observe_metadata(restored._engine,Path("/qt-source/pgdata"))
            for name,before in metadata.items():
                assert (after_metadata[name]["rows"],after_metadata[name]["content_hash"])==(before["rows"],before["content_hash"])
                assert all(member["device"]==history_root.stat().st_dev for member in after_metadata[name]["files"])
        finally:
            restored._reset_engine()
    print("QT_RECOVERY_RESTORE_RESULT="+json.dumps({
        "consistent_snapshot_excludes_later_commit":True,"collection_continued":True,
        "archive_expiry_fenced":True,"copied_archive_hashes_match":True,
        "startup_recent_history_frozen_and_book_replay_preserved":True,
        "shared_metadata_and_indexes_restored_on_history":True,
        "database_dump_bytes":dump.stat().st_size,
        "archive_copy_bytes":sum(p.stat().st_size for p in (recovery/"objects").rglob("*") if p.is_file()),
        "local_generation_rotation_implemented":True,
        "failed_copy_preserves_completed_and_retry_cleans_partial":True,
        "routine_scheduling_implemented":False,
        "limits":["tiny disposable data","not production capacity or restore-duration qualification"],
    },sort_keys=True))
