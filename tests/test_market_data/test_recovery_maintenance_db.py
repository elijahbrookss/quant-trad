"""Due-copy policy, namespace and capacity ownership on disposable filesystems."""
import os
import json
from datetime import datetime, UTC, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text

from core.storage_targets import StoragePolicy,StorageTarget
from core.market_storage_lifecycle import MarketStorageLifecyclePolicy
from portal.backend.service.market.market_storage_lifecycle import MarketStorageLifecycleSupervisor
from portal.backend.db.storage_target_models import StorageTargetRecord,StoragePolicyRecord
from portal.backend.db.market_data_models import MarketCollectorWorkerStateRecord
from portal.backend.service.storage_management import StorageManagementService
from portal.backend.service.storage.recovery_maintenance import run_due_local_recovery
from portal.backend.service.storage.maintenance_runtime import storage_maintenance_runners
from tests.test_storage_maintenance_runtime import configuration
from portal.backend.service.storage.recovery_copies import _identity, LocalRecoveryCopies
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark=[pytest.mark.db,pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO")!="1",
            reason="requires owned two-filesystem topology")]


def test_due_recovery_owns_capacity_skips_busy_and_preserves_completed_copy(storage,tmp_path,monkeypatch):
    recent,history=Path("/qt-source/pgdata"),Path("/qt-history")
    assert os.getenv("QT_DB_TEST_ISOLATED")=="1" and os.getuid()==70
    udev=tmp_path/"recovery-maintenance-udev"
    udev.mkdir()
    targets=(StorageTarget("ssd","Recent","uuid-maintenance-ssd",str(recent),"ssd"),
             StorageTarget("hdd","History","uuid-maintenance-hdd",str(history),"hdd"))
    for target in targets:
        dev=Path(target.root).stat().st_dev
        (udev/f"b{os.major(dev)}:{os.minor(dev)}").write_text("E:ID_FS_UUID="+target.filesystem_uuid+"\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT",str(udev))
    archive=history/("qt_recovery_sources_"+uuid4().hex)
    (archive/"objects").mkdir(parents=True)
    policy=StoragePolicy(recent=("ssd",),history=("hdd",),archives=("hdd",),backups=("hdd",),
                         backup_enabled=False)
    with storage.database.session() as session:
        for target in targets:
            session.add(StorageTargetRecord(id=target.target_id,label=target.label,
                filesystem_uuid=target.filesystem_uuid,root=target.root,medium=target.medium,
                roles=list(target.roles),state="active"))
        session.add(StoragePolicyRecord(id=1,revision=1,policy=policy.to_dict()))
    args={"storage_root":archive,"pg_dump":Path("/usr/lib/postgresql/15/bin/pg_dump"),
          "pg_controldata":Path("/usr/lib/postgresql/15/bin/pg_controldata"),
          "max_bytes":16*1024**2,"timeout_seconds":120,
          "headroom_bytes":{"ssd":1024**2,"hdd":1024**2},"max_objects":1000}
    def run(**overrides):
        return run_due_local_recovery(storage.database,**(args|overrides))
    with storage.database.session() as session:
        _,namespace=_identity(session)
    copy_root=history/"recovery"/namespace
    assert run()["state"]=="disabled"
    assert not copy_root.exists()
    with storage.database.session() as session:
        config=session.get(StoragePolicyRecord,1)
        config.policy=policy.to_dict()|{"backup_enabled":True}
    with storage.database._engine.begin() as owner:
        assert owner.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))"))
        assert run()=={"state":"busy","reason":"storage_operation_running"}
    assert run(max_bytes=2**62)=={"state":"blocked","reason":"recovery_creation_capacity"}
    with storage.database._engine.connect().execution_options(isolation_level="AUTOCOMMIT") as holder:
        assert holder.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"),
                             {"name":_LIFECYCLE_LOCK_NAME})
        try:
            assert run()=={"state":"busy","reason":"archive_lifecycle_running"}
        finally:
            assert holder.scalar(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"),
                                 {"name":_LIFECYCLE_LOCK_NAME})
    # The resource guard is invoked during streaming, not only before starting.
    called=[]
    def cancellation():
        called.append(True)
        return False
    runtime_config=configuration()
    runtime_config["recovery"]={key:args[key] for key in
        ("max_bytes","timeout_seconds","headroom_bytes","max_objects")}
    runtime_path=tmp_path/"maintenance-limits.json"
    runtime_path.write_text(json.dumps(runtime_config))
    runners=storage_maintenance_runners(storage.database,storage_root=archive,limits_path=runtime_path)
    recovery=runners["recovery_runner"]
    runners["recovery_runner"]=lambda *,cancelled:recovery(cancelled=lambda:cancellation() or cancelled())
    worker=MarketStorageLifecycleSupervisor(policy=MarketStorageLifecyclePolicy(enabled=False),**runners)
    result=worker.run_once()["local_recovery"]
    assert worker.snapshot()["last_run"]["local_recovery"]==result
    assert result["state"]=="completed" and result["policy_revision"]==1 and len(called)>5
    generations=list(copy_root.glob("copy_*"))
    assert len(generations)==1 and (generations[0]/"database.dump").is_file()
    assert worker.run_once()["local_recovery"]["state"]=="not_due"
    assert list(copy_root.glob("copy_*"))==generations
    assert result["storage_layout"]["layout_version"]=="market.fact_storage_tiers.v2"
    # A recent legacy receipt, or even a completed copy of a different layout,
    # must not postpone the first recovery copy covering the current snapshot.
    for stale in (None, {"layout_version":"market.fact_storage_tiers.v1",
                         "certificate_sha256":"a"*64}):
        previous=copy_root/result["generation"]
        certificate=previous/"complete.json"
        old=json.loads(certificate.read_text())
        if stale is None:
            old.pop("storage_layout")
        else:
            old["storage_layout"]=stale
        certificate.write_text(json.dumps(old))
        before={p.name for p in copy_root.glob("copy_*")}
        create=LocalRecoveryCopies.create
        def lost_reply(manager,*positional,**keywords):
            create(manager,*positional,**keywords)
            raise TimeoutError("injected lost completed-copy reply")
        with monkeypatch.context() as patch:
            patch.setattr(LocalRecoveryCopies,"create",lost_reply)
            with pytest.raises(TimeoutError,match="lost completed-copy"):
                run()
        after={p.name for p in copy_root.glob("copy_*")}
        assert len(after-before)==1 and previous.is_dir()
        result=worker.run_once()["local_recovery"]
        assert result["state"]=="not_due" and result["generation"] in after-before
        assert result["storage_layout"]["layout_version"]=="market.fact_storage_tiers.v2"
        assert {p.name for p in copy_root.glob("copy_*")}==after
    generations=list(copy_root.glob("copy_*"))
    with pytest.raises(RuntimeError,match="recovery_cancelled"):
        run(cancelled=lambda:True)
    with pytest.raises(RuntimeError,match="capacity_or_filesystem_changed"):
        run(headroom_bytes={"ssd":2**62,"hdd":1024**2})
    assert (generations[0]/"complete.json").is_file()
    with storage.database._engine.begin() as retry:
        assert retry.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))"))
        assert retry.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                            {"name":_LIFECYCLE_LOCK_NAME})


    now = datetime.now(UTC)
    with storage.database.session() as session:
        session.add(MarketCollectorWorkerStateRecord(
            worker_id="recovery-status", worker_role="scheduled_market_fact_collector",
            worker_version="disposable", state="idle", started_at=now,
            heartbeat_at=now, expires_at=now+timedelta(seconds=30),
            capabilities={}, context={"storage_lifecycle": worker.snapshot()}))
    status_service = StorageManagementService(storage.database, inventory_path=tmp_path/"no-inventory.json")
    snapshot = status_service.snapshot()
    assert snapshot["backup"]["state"] == "not_due"
    assert snapshot["backup"]["last_completed_at"] == result["last_completed_at"]
    assert snapshot["movement"]["state"] == "disabled"
    assert snapshot["health"] == "available"
    # A later phase failure must replace the earlier completed-copy observation.
    original_runner = worker.recovery_runner
    def failed(**kwargs):
        raise RuntimeError("injected recovery failure")
    worker.recovery_runner = failed
    worker.run_once()
    worker.recovery_runner = original_runner
    with storage.database.session() as session:
        session.get(MarketCollectorWorkerStateRecord, "recovery-status").context = {
            "storage_lifecycle": worker.snapshot()}
    snapshot = status_service.snapshot()
    assert snapshot["backup"]["state"] == "failed"
    assert snapshot["backup"]["last_completed_at"] is None
    assert snapshot["health"] == "needs_attention"
