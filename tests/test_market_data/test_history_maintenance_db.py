"""Saved policy advances one real day, recovers, and leaves existing HDD work alone."""
from dataclasses import replace
from datetime import timedelta
import os
from pathlib import Path

import pytest
from sqlalchemy import event, select, text

from core.market_storage_lifecycle import MarketStorageLifecyclePolicy
from core.storage_targets import StoragePolicy
from market_data.contracts import DatasetSeriesRequest
from portal.backend.db.storage_target_models import (
    StorageHeaderMoveRecord,StoragePlanRecord,StoragePolicyRecord,StorageTargetRecord,
)
from portal.backend.service.market.market_storage_lifecycle import MarketStorageLifecycleSupervisor
from portal.backend.service.storage import history_maintenance as history
from portal.backend.service.storage.header_catalog import read_header_catalog
from portal.backend.service.storage.header_destinations import register_header_tablespaces
from portal.backend.service.storage.header_filesystem import verify_header_filesystem
from tests.test_market_data.test_fact_storage_tiers_db import storage,BASE,_placement
from tests.test_market_data.test_fact_header_copy_placement_db import _configure_placement,_assert_disk

pytestmark=[
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO")!="1",
                      reason="requires owned two-filesystem storage-demo topology"),
]
CONTROL=Path("/usr/lib/postgresql/15/bin/pg_controldata")


def test_saved_history_policy_recovers_and_advances_without_duplicate_moves(storage,tmp_path,monkeypatch):
    storage.open_day=storage.today
    _configure_placement(storage,tmp_path,monkeypatch)
    engine=storage.database._engine
    targets=(storage.copy_plan.recent,storage.copy_plan.history)
    limits=dict(wal_bytes=64*1024**2,temporary_bytes={"ssd":16*1024**2,"hdd":16*1024**2},
        growth_bytes_per_second={"ssd":1024**2,"hdd":1024**2},
        maintenance_bytes={"ssd":0,"hdd":0},movement_timeout_seconds=30,cancellation_grace_seconds=2)
    def invoke(**overrides):
        return history.run_history_maintenance(storage.database,pg_controldata=CONTROL,
            resource_limits=overrides.pop("resource_limits",limits),**overrides)
    assert invoke()["state"]=="unconfigured"
    days=[storage.today-timedelta(days=45),storage.today-timedelta(days=44)]
    for ordinal,day in enumerate([*days,storage.today]):
        _placement(monkeypatch,day)
        facts=[replace(storage.fact,observation_key=f"policy-{ordinal}-{i}",
                       observation_time=BASE+timedelta(days=ordinal,seconds=i)) for i in range(4)]
        assert storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,
                                        facts=facts).inserted_count==4
    frozen=storage.repo.freeze_dataset([
        DatasetSeriesRequest(storage.series_id,BASE-timedelta(seconds=1),BASE+timedelta(days=3))])
    before=storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id,series_id=storage.series_id)
    policy=StoragePolicy(recent=("ssd",),history=("hdd",),archives=("hdd",),backups=("hdd",),
                         recent_days=30,movement_enabled=True)
    with storage.database.session() as session:
        for target in targets:
            session.add(StorageTargetRecord(id=target.target_id,label=target.label,
                filesystem_uuid=target.filesystem_uuid,root=target.root,medium=target.medium,roles=list(target.roles)))
        session.add(StoragePolicyRecord(id=1,revision=0,policy=replace(policy,movement_enabled=False).to_dict()))
    assert invoke()["state"]=="disabled"
    with storage.database.session() as session:
        session.get(StoragePolicyRecord,1).policy=policy.to_dict()
    assert invoke()=={"state":"blocked","reason":"history_tablespace_unconfigured"}
    verified=verify_header_filesystem(read_header_catalog(engine,
        destination_tablespace_oids=(storage.copy_plan.history_tablespace_oid,)),
        targets,pg_controldata=CONTROL,
        destination_assignments={"hdd":storage.copy_plan.history_tablespace_oid})
    with storage.database.session() as session:
        register_header_tablespaces(session,verified=verified)
    with engine.begin() as owner:
        owner.exec_driver_sql("SELECT pg_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))")
        assert invoke()["state"]=="busy"
    with pytest.raises(ValueError,match="capacity"):
        invoke(resource_limits={**limits,"wal_bytes":2**40})
    with storage.database.session() as session:
        assert not list(session.scalars(select(StoragePlanRecord)))
        assert all(row.reserved_bytes==row.auxiliary_reserved_bytes==0
                   for row in session.scalars(select(StorageTargetRecord)))

    recoveries=[]
    supervisor=MarketStorageLifecycleSupervisor(
        policy=MarketStorageLifecyclePolicy(enabled=False,execution_enabled=True),
        history_runner=lambda **kwargs:invoke(**kwargs),
        recovery_runner=lambda **kwargs:recoveries.append("ran") or {"state":"not_due"})
    killed=[False]
    def kill(conn,cursor,statement,parameters,context,executemany):
        if not killed[0] and statement.startswith("ALTER TABLE ONLY"):
            killed[0]=True
            with engine.begin() as other:
                assert other.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                                    {"pid":conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine,"after_cursor_execute",kill)
    try:
        result=supervisor.run_once()
    finally:
        event.remove(engine,"after_cursor_execute",kill)
    assert killed[0] and result["history_movement"]["state"]=="failed"
    assert result["local_recovery"]["state"]=="not_due" and recoveries==["ran"]
    assert supervisor.snapshot()["state"]=="degraded"
    with storage.database.session() as session:
        first_plan=session.scalar(select(StoragePlanRecord))
        first_id=first_plan.id
        assert first_plan.state=="blocked" and first_plan.progress["error"]
        assert first_plan.impact["deferred_storage_days"]==[days[1].isoformat()]
        first_move=session.scalar(select(StorageHeaderMoveRecord))
        first_move_id=first_move.id
        assert first_move.state=="reserved"
        _assert_disk(session.connection(),"market.fact_versions_"+days[0].strftime("%Y%m%d"),
                     Path("/qt-source/pgdata"))
    assert storage.repo.read_dataset_fact_revisions(
        dataset_id=frozen.dataset_id,series_id=storage.series_id)==before

    # Lose only the client's completion response after a real committed move.
    execute=history.execute_reserved_header_move
    def lost_response(*args,**kwargs):
        execute(*args,**kwargs)
        raise RuntimeError("injected completion response lost")
    with monkeypatch.context() as fault:
        fault.setattr(history,"execute_reserved_header_move",lost_response)
        result=supervisor.run_once()
    assert result["history_movement"]["state"]=="failed"
    with storage.database.session() as session:
        assert session.get(StorageHeaderMoveRecord,first_move_id).state=="completed"
        assert session.get(StoragePlanRecord,first_id).state=="running"
        assert all(row.reserved_bytes==row.auxiliary_reserved_bytes==0
                   for row in session.scalars(select(StorageTargetRecord)))
    result=supervisor.run_once()["history_movement"]
    assert result["state"]=="completed" and result["reused"] and result["plan_id"]==first_id
    with storage.database.session() as session:
        _assert_disk(session.connection(),"market.fact_versions_"+days[0].strftime("%Y%m%d"),Path("/qt-history"))
        _assert_disk(session.connection(),"market.fact_versions_"+days[1].strftime("%Y%m%d"),Path("/qt-source/pgdata"))
        assert session.get(StoragePlanRecord,first_id).state=="completed"

    # Simulate a stop after reserving the next day but before issuing any DDL.
    pending=history._prepare(storage.database,pg_controldata=CONTROL,limits=limits,cancelled=None)
    assert pending["plan_id"]!=first_id
    with storage.database.session() as session:
        config=session.get(StoragePolicyRecord,1)
        config.revision+=1
        config.policy=replace(policy,movement_enabled=False).to_dict()
    assert invoke()["state"]=="cancelled"
    assert invoke()["state"]=="disabled"
    with storage.database.session() as session:
        assert session.get(StoragePlanRecord,pending["plan_id"]).state=="cancelled"
        assert all(row.reserved_bytes==row.auxiliary_reserved_bytes==0
                   for row in session.scalars(select(StorageTargetRecord)))
        config=session.get(StoragePolicyRecord,1)
        config.revision+=1
        config.policy=policy.to_dict()
    result=supervisor.run_once()["history_movement"]
    assert result["state"]=="completed" and result["storage_day"]==days[1].isoformat()
    with storage.database.session() as session:
        assert len(list(session.scalars(select(StoragePlanRecord))))==3
        for day in days:
            _assert_disk(session.connection(),"market.fact_versions_"+day.strftime("%Y%m%d"),Path("/qt-history"))
        _assert_disk(session.connection(),"market.fact_versions_"+storage.today.strftime("%Y%m%d"),Path("/qt-source/pgdata"))
        assert all(row.reserved_bytes==row.auxiliary_reserved_bytes==0
                   for row in session.scalars(select(StorageTargetRecord)))
    assert supervisor.run_once()["history_movement"]["state"]=="idle"
    with storage.database.session() as session:
        assert len(list(session.scalars(select(StoragePlanRecord))))==3
    assert storage.repo.read_dataset_fact_revisions(
        dataset_id=frozen.dataset_id,series_id=storage.series_id)==before
