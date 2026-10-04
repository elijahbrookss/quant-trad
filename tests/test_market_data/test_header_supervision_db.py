"""Cancel real staged movement and retain readable source/journal state."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
import os
from pathlib import Path
import threading
from time import monotonic, sleep

import pytest
from sqlalchemy import event, select, text

from core.storage_targets import StoragePolicy, StorageTarget
from market_data.contracts import DatasetSeriesRequest
from portal.backend.db.storage_target_models import (
    StorageTargetRecord, StoragePolicyRecord, StoragePlanRecord, StorageHeaderMoveRecord,
)
from portal.backend.service.storage import header_movement as movement
from portal.backend.service.storage.header_catalog import read_header_catalog
from portal.backend.service.storage.header_destinations import register_header_tablespaces, review_header_moves
from portal.backend.service.storage.header_filesystem import verify_header_filesystem
from portal.backend.service.storage.header_journal import reserve_header_batch
from portal.backend.service.storage.header_resource_claims import reserve_header_resources
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.test_fact_header_copy_placement_db import _configure_placement, _assert_disk

pytestmark=[
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO")!="1",
                      reason="requires owned two-filesystem storage-demo topology"),
]
CONTROL=Path("/usr/lib/postgresql/15/bin/pg_controldata")


def test_supervised_move_cancels_faults_then_recovers_and_commits_once(storage,tmp_path,monkeypatch):
    storage.open_day=storage.today
    _configure_placement(storage,tmp_path,monkeypatch)
    targets=(storage.copy_plan.recent,storage.copy_plan.history)
    engine=storage.database._engine
    day=storage.today-timedelta(days=45)
    _placement(monkeypatch,day)
    facts=[replace(storage.fact,observation_key=f"supervised-{i}",
                   observation_time=BASE+timedelta(seconds=i)) for i in range(16)]
    assert storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,
                                    facts=facts).inserted_count==len(facts)
    frozen=storage.repo.freeze_dataset([
        DatasetSeriesRequest(storage.series_id,BASE-timedelta(seconds=1),BASE+timedelta(minutes=1))])
    before=storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id,series_id=storage.series_id)
    _placement(monkeypatch,storage.today)
    policy=StoragePolicy(recent=("ssd",),history=("hdd",),archives=("hdd",),backups=("hdd",),
                         recent_days=30,movement_enabled=True)
    with storage.database.session() as session:
        for target in targets:
            session.add(StorageTargetRecord(id=target.target_id,label=target.label,
                filesystem_uuid=target.filesystem_uuid,root=target.root,medium=target.medium,roles=list(target.roles)))
        session.add(StoragePolicyRecord(id=1,revision=0,policy=policy.to_dict()))
        session.add(StoragePlanRecord(id="supervised",request_id="supervised",base_revision=0,
            policy=policy.to_dict(),policy_hash=policy.fingerprint,state="queued",impact={},progress={}))
    verified=verify_header_filesystem(read_header_catalog(engine,
        destination_tablespace_oids=(storage.copy_plan.history_tablespace_oid,)),
        targets,pg_controldata=CONTROL,
        destination_assignments={"hdd":storage.copy_plan.history_tablespace_oid})
    review=review_header_moves(verified=verified,policy=policy,targets=targets,
                               reserved_bytes={"ssd":0,"hdd":0})
    with storage.database.session() as session:
        register_header_tablespaces(session,verified=verified)
        reserved=reserve_header_batch(session,plan_id="supervised",
            review_hash=review["plan_hash"],verified=verified)
    assert len(reserved["moves"])==1
    move_id=reserved["moves"][0]["id"]

    def invoke(cancelled=None):
        return movement.execute_reserved_header_move(storage.database,move_id=move_id,
            review_hash=review["plan_hash"],pg_controldata=CONTROL,cancelled=cancelled)

    with pytest.raises(ValueError,match="resource_claim_required"):
        invoke()
    limits=dict(wal_bytes=64*1024**2,temporary_bytes={"ssd":16*1024**2,"hdd":16*1024**2},
        growth_bytes_per_second={"ssd":1024**2,"hdd":1024**2},
        maintenance_bytes={"ssd":0,"hdd":0},movement_timeout_seconds=30,cancellation_grace_seconds=2)
    with storage.database.session() as session:
        session.connection()  # Begin the transaction owned by this context.
        reserve_header_resources(session,move_id=move_id,review_hash=review["plan_hash"],
            pg_controldata=CONTROL,**limits)
        counters={row.id:(row.reserved_bytes,row.auxiliary_reserved_bytes)
                  for row in session.scalars(select(StorageTargetRecord))}

    watches=[]
    start=movement._MoveWatch.start
    def observe_start(watch):
        watches.append(watch)
        return start(watch)
    monkeypatch.setattr(movement._MoveWatch,"start",observe_start)
    original_inspect=StorageTarget.inspect
    space_fault=threading.Event()
    def inspect(target,**kwargs):
        result=original_inspect(target,**kwargs)
        if target.target_id=="hdd" and space_fault.is_set():
            return replace(result,available_bytes=0,used_bytes=result.total_bytes)
        return result
    monkeypatch.setattr(StorageTarget,"inspect",inspect)
    dev=Path("/qt-history").stat().st_dev
    identity=storage.copy_udev/f"b{os.major(dev)}:{os.minor(dev)}"
    identity_bytes=identity.read_bytes()

    for fault in ("shutdown","deadline","space","identity"):
        boundary=threading.Event()
        cancelled=threading.Event()
        details={}
        def hold_after_copy(conn,cursor,statement,parameters,context,executemany):
            if not boundary.is_set() and statement.startswith("ALTER TABLE ONLY"):
                details["pid"]=conn.connection.driver_connection.get_backend_pid()
                boundary.set()
                # Hold a genuine staged copy in an active SQL statement. The
                # watcher must cancel this backend; another connection observes
                # PgSleep before injection, removing the idle-cancel race.
                conn.exec_driver_sql("SELECT pg_sleep(20)")
        event.listen(engine,"after_cursor_execute",hold_after_copy)
        try:
            with ThreadPoolExecutor(max_workers=1) as workers:
                future=workers.submit(invoke,cancelled.is_set)
                assert boundary.wait(15)
                end=monotonic()+5
                while True:
                    with engine.connect() as observer:
                        sleeping=observer.scalar(text("""
                            SELECT wait_event='PgSleep' FROM pg_stat_activity WHERE pid=:pid
                        """),{"pid":details["pid"]})
                    if sleeping:
                        break
                    assert monotonic()<end,"staged-copy SQL boundary not observed"
                    sleep(.02)
                if fault=="shutdown":
                    cancelled.set()
                    expected="storage_move_cancelled"
                elif fault=="deadline":
                    watches[-1].deadline=monotonic()-1
                    expected="storage_move_time_budget_exceeded"
                elif fault=="space":
                    space_fault.set()
                    expected="storage_move_space_budget_exceeded"
                else:
                    identity.write_text("E:ID_FS_UUID=replaced-device\n")
                    expected="storage_mount_identity_mismatch"
                with pytest.raises(RuntimeError,match=expected):
                    future.result(timeout=5)
        finally:
            event.remove(engine,"after_cursor_execute",hold_after_copy)
            space_fault.clear()
            identity.write_bytes(identity_bytes)
        assert not any(item.name=="qt-history-move-watch" for item in threading.enumerate())
        with storage.database.session() as session:
            assert session.get(StorageHeaderMoveRecord,move_id).state=="reserved"
            assert {row.id:(row.reserved_bytes,row.auxiliary_reserved_bytes)
                    for row in session.scalars(select(StorageTargetRecord))}==counters
            relation="market.fact_versions_"+day.strftime("%Y%m%d")
            _assert_disk(session.connection(),relation,Path("/qt-source/pgdata"))
            assert session.scalar(text("SELECT 42"))==42
        assert storage.repo.read_dataset_fact_revisions(
            dataset_id=frozen.dataset_id,series_id=storage.series_id)==before

    completed=invoke()
    assert completed["state"]=="completed" and not completed["commit_required"]
    assert completed["supervised_execution"] and not completed["reused"]
    statements=[]
    def record(conn,cursor,statement,parameters,context,executemany):
        if statement.startswith("ALTER"):
            statements.append(statement)
    event.listen(engine,"after_cursor_execute",record)
    try:
        assert invoke()["reused"]
    finally:
        event.remove(engine,"after_cursor_execute",record)
    assert not statements
    with storage.database.session() as session:
        assert all(row.reserved_bytes==row.auxiliary_reserved_bytes==0
                   for row in session.scalars(select(StorageTargetRecord)))
        _assert_disk(session.connection(),relation,Path("/qt-history"))
    assert storage.repo.read_dataset_fact_revisions(
        dataset_id=frozen.dataset_id,series_id=storage.series_id)==before
    assert not any(item.name=="qt-history-move-watch" for item in threading.enumerate())
