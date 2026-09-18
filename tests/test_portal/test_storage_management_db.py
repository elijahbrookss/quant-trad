from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from core.storage_mounts import FilesystemEvidence, StorageMountError
from core.storage_targets import StoragePolicy, StorageTarget
from portal.backend.db.storage_target_models import StoragePlanRecord, StoragePolicyRecord, StorageTargetRecord, StorageObjectLocationRecord
from portal.backend.service.storage_management import StorageConflict, StorageManagementService
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db


@pytest.fixture
def service(tmp_path, monkeypatch):
    with fresh_migration_database("storage_policy") as dsn:
        engine = create_engine(dsn)
        for model in (StorageTargetRecord, StoragePolicyRecord, StoragePlanRecord, StorageObjectLocationRecord):
            model.__table__.create(engine)
        class TestDatabase:
            @contextmanager
            def session(self):
                with Session(engine) as session, session.begin():
                    yield session

        path = tmp_path / "inventory.json"
        targets = [
            StorageTarget("ssd", "Fast drive", "uuid-ssd", "/qt/ssd", "ssd"),
            StorageTarget("hdd", "History drive", "uuid-hdd", "/qt/hdd", "hdd"),
        ]
        path.write_text(json.dumps({"schema_version": "qt.storage_inventory.v1", "targets": [asdict(t) for t in targets]}))
        def inspect(target, **kwargs):
            return FilesystemEvidence(path=target.root, filesystem_uuid=target.filesystem_uuid,
                device_id="8:1", used_bytes=10**11,
                total_bytes=10**12, available_bytes=9*10**11, read_only=False)
        monkeypatch.setattr(StorageTarget, "inspect", inspect)
        service = StorageManagementService(TestDatabase(), inventory_path=path)
        try:
            yield service
        finally:
            engine.dispose()


def policy():
    return StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",), backups=("hdd",)).to_dict()


def enroll(service):
    service.register("ssd")
    service.register("hdd")


def test_enrollment_is_idempotent_and_plan_does_not_activate(service):
    enroll(service)
    assert service.register("ssd")["reused"] is True
    plan = service.plan(policy=policy(), base_revision=0, request_id="review-1")
    assert service.plan(policy=policy(), base_revision=0, request_id="review-1")["id"] == plan["id"]
    assert service.snapshot()["policy"] is None
    assert service.snapshot()["health"] == "unconfigured"
    assert service.snapshot()["movement"]["state"] == "unconfigured"


def test_request_id_cannot_be_reused_for_different_policy(service):
    enroll(service)
    service.plan(policy=policy(), base_revision=0, request_id="same")
    changed = {**policy(), "reserve_percent": 25}
    with pytest.raises(StorageConflict, match="request_conflict"):
        service.plan(policy=changed, base_revision=0, request_id="same")


def test_stale_review_and_hash_are_rejected(service):
    enroll(service)
    plan = service.plan(policy=policy(), base_revision=0, request_id="one")
    with pytest.raises(StorageConflict, match="review_mismatch"):
        service.queue_plan(plan["id"], policy_hash="wrong")
    with service.database.session() as session:
        session.get(StoragePolicyRecord, 1).revision = 1
    with pytest.raises(StorageConflict, match="policy_changed"):
        service.queue_plan(plan["id"], policy_hash=plan["policy_hash"])


def test_missing_mount_prevents_enrollment(service, monkeypatch):
    def missing(*args, **kwargs):
        raise StorageMountError("storage_uuid_mismatch")
    monkeypatch.setattr(StorageTarget, "inspect", missing)
    with pytest.raises(StorageMountError):
        service.register("ssd")
    assert service.snapshot()["targets"] == []


def test_unimplemented_execution_never_reports_queued_success(service):
    enroll(service)
    plan = service.plan(policy=policy(), base_revision=0, request_id="one")
    assert any(b["code"] == "storage_execution_unavailable" for b in plan["impact"]["blockers"])
    with pytest.raises(StorageConflict, match="storage_execution_unavailable"):
        service.queue_plan(plan["id"], policy_hash=plan["policy_hash"])
    assert service.get_plan(plan["id"])["state"] == "planned"


def test_active_plan_outside_recent_history_still_changes_health(service):
    enroll(service)
    with service.database.session() as session:
        session.add(StoragePolicyRecord(id=1, revision=1, policy=policy()))
        for i in range(12):
            session.add(StoragePlanRecord(id=f"plan_{i}", request_id=f"request_{i}",
                base_revision=0, policy=policy(), policy_hash="a"*64,
                state="running" if i == 0 else "planned", impact={}, progress={},
                created_at=datetime.now(UTC)+timedelta(seconds=i)))
    assert service.snapshot()["health"] == "changing"
