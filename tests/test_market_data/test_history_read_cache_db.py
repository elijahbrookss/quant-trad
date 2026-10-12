"""Real transaction/ownership tests, using only the isolated test database."""
from dataclasses import replace
import hashlib
from threading import Thread, Event
from time import monotonic

import pytest
from sqlalchemy import text

from core.execution_control import ExecutionControl
from core.settings import get_settings
from core.storage_mounts import FilesystemEvidence, StorageMountError
from core.storage_targets import StorageTarget
from portal.backend.db.storage_target_models import StorageTargetRecord
from portal.backend.service.storage.history_policy import configured_history_read_cache
from portal.backend.service.research import execution_limits
from tests.test_portal.test_storage_management_db import service, apply_existing

pytestmark=pytest.mark.db


@pytest.fixture
def configured(service,tmp_path,monkeypatch):
    import core.settings as settings_module
    import core.storage_mounts as mounts
    import portal.backend.db.session as database_module
    apply_existing(service)
    root=tmp_path/"history-read-cache"
    root.mkdir(mode=0o700)
    monkeypatch.setenv("MARKET_STRUCTURE_WORKING_ROOT",str(tmp_path))
    settings=get_settings()
    monkeypatch.setattr(settings_module,"get_settings",lambda:replace(settings,
        storage=replace(settings.storage,history_cache_bytes=1024**2,history_cache_min_free_bytes=100)))
    monkeypatch.setattr(database_module,"db",service.database)
    with service.database.session() as session:
        session.get(StorageTargetRecord,"ssd").root=str(tmp_path)
    evidence=FilesystemEvidence(path=str(tmp_path),device_id="fixture-device",filesystem_uuid="uuid-ssd",
        total_bytes=10**7,available_bytes=9*10**6,used_bytes=10**6,read_only=False)
    monkeypatch.setattr(mounts,"require_configured_working_mount",lambda *a,**kw:evidence)
    monkeypatch.setattr(StorageTarget,"inspect",lambda self,**kw:replace(evidence,filesystem_uuid=self.filesystem_uuid))
    source=tmp_path/"source.parquet"
    source.write_bytes(b"source bytes")
    digest=hashlib.sha256(source.read_bytes()).hexdigest()
    return service,configured_history_read_cache(),source,digest


def test_cache_respects_storage_owner_and_existing_claims(configured):
    service,cache,source,digest=configured
    with service.database.session() as owner:
        owner.execute(text("SELECT pg_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))"))
        with cache.open_copy(source,digest=digest,size=source.stat().st_size) as handle:
            assert handle is None
    assert not list(cache.root.iterdir())
    with service.database.session() as owner:
        owner.get(StorageTargetRecord,"ssd").auxiliary_reserved_bytes=8*10**6
    with cache.open_copy(source,digest=digest,size=source.stat().st_size) as handle:
        assert handle is None
    with service.database.session() as owner:
        owner.get(StorageTargetRecord,"ssd").auxiliary_reserved_bytes=0
    with cache.open_copy(source,digest=digest,size=source.stat().st_size) as handle:
        assert handle.read()==b"source bytes"
    assert source.read_bytes()==b"source bytes"


def test_cache_fill_excludes_new_maintenance(configured):
    service,cache,source,digest=configured
    with cache.capacity_scope() as check:
        assert check(1)>0
        with service.database.session() as peer:
            assert not peer.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))"))
    with service.database.session() as peer:
        assert peer.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))"))


def test_cache_fills_beside_read_only_registered_target(configured, monkeypatch):
    service,cache,source,digest=configured
    inspect = StorageTarget.inspect

    def read_only_target(target, *, require_writable=False):
        if require_writable:
            raise StorageMountError("storage_mount_read_only: registered PGDATA bind")
        return replace(inspect(target), read_only=True)

    monkeypatch.setattr(StorageTarget, "inspect", read_only_target)
    with cache.open_copy(source, digest=digest, size=source.stat().st_size) as handle:
        assert handle.read() == source.read_bytes()
    with cache.open_copy(source, digest=digest, size=source.stat().st_size) as handle:
        assert handle.read() == source.read_bytes()

    # A read-only working mount still refuses even an existing cache entry.
    import core.storage_mounts as mounts
    def read_only_working(*args, **kwargs):
        raise StorageMountError("storage_mount_read_only: cache working mount")
    monkeypatch.setattr(mounts, "require_configured_working_mount", read_only_working)
    unavailable = configured_history_read_cache()
    with unavailable.open_copy(source, digest=digest, size=source.stat().st_size) as handle:
        assert handle is None
    assert source.read_bytes() == b"source bytes"


def test_cache_bypasses_unavailable_registered_target(configured, monkeypatch):
    service,cache,source,digest=configured
    def unavailable(*args, **kwargs):
        raise StorageMountError("storage_mount_unavailable: registered target")
    monkeypatch.setattr(StorageTarget, "inspect", unavailable)
    with cache.open_copy(source, digest=digest, size=source.stat().st_size) as handle:
        assert handle is None
    assert not list(cache.root.iterdir())
    assert source.read_bytes() == b"source bytes"


def test_research_global_admission_uses_real_postgres_exclusion(service,monkeypatch):
    settings=get_settings()
    monkeypatch.setattr(execution_limits,"get_settings",lambda:replace(settings,
        async_jobs=replace(settings.async_jobs,research_global_serialization=True)))
    failures=[]
    def contender():
        try:
            with execution_limits._global_admission(ExecutionControl(),session_factory=service.database.session):
                failures.append("unexpected")
        except execution_limits.ResearchAdmissionError as error:
            failures.append(str(error))
    with execution_limits._global_admission(ExecutionControl(),session_factory=service.database.session):
        thread=Thread(target=contender)
        thread.start()
        thread.join(5)
        assert not thread.is_alive()
        assert failures==["research_execution_busy: global capacity occupied"]
    with execution_limits._global_admission(ExecutionControl(),session_factory=service.database.session):
        pass


def test_real_admission_connection_loss_cancels_execution(service, monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(execution_limits, "get_settings", lambda: replace(settings,
        async_jobs=replace(settings.async_jobs, research_global_serialization=True)))
    control = ExecutionControl()
    with pytest.raises(execution_limits.ResearchAdmissionError, match="owner_lost"):
        with execution_limits._global_admission(control, session_factory=service.database.session):
            with service.database.session() as peer:
                owner_pid = peer.scalar(text("""
                    WITH key AS (SELECT hashtextextended(:name,0) AS value)
                    SELECT pid FROM pg_locks,key WHERE locktype='advisory'
                      AND mode='ExclusiveLock' AND granted AND objsubid=1
                      AND classid::bigint=((key.value>>32)&4294967295)
                      AND objid::bigint=(key.value&4294967295)
                      AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
                """), {"name": execution_limits._GLOBAL_LOCK})
                assert owner_pid and peer.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": owner_pid})
            deadline = monotonic() + 3
            while not control.stop_requested and monotonic() < deadline:
                Event().wait(.01)
            control.check()
    with execution_limits._global_admission(ExecutionControl(), session_factory=service.database.session):
        pass
