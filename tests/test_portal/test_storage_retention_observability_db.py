from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy, MarketStorageLifecyclePolicy
from core.storage_mounts import FilesystemEvidence
from core.storage_targets import StoragePolicy, StorageTarget
from portal.backend.db.market_storage_models import (
    MarketFactArchiveManifestRecord,
    MarketFactArchiveVerificationRecord,
    MarketFactRetentionPartitionRecord,
)
from portal.backend.db.storage_target_models import (
    StorageObjectLocationRecord,
    StoragePlanRecord,
    StoragePolicyRecord,
    StorageTargetRecord,
)
from portal.backend.service.storage import retention_observability as module
from portal.backend.service.storage_management import StorageManagementService
from tests.test_market_data.migration_test_support import fresh_migration_database


pytestmark = pytest.mark.db
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


class _Database:
    def __init__(self, engine):
        self.engine = engine

    @contextmanager
    def session(self):
        with Session(self.engine) as session, session.begin():
            yield session


def _settings():
    return SimpleNamespace(
        market_data_lifecycle=MarketStorageLifecyclePolicy(
            canonical_retention=CanonicalFactRetentionPolicy(
                hot_days=17, hot_days_by_fact_type={"inactive.base": 90}
            )
        ),
        storage=SimpleNamespace(maintenance_limits_path=None),
    )


@pytest.fixture
def postgres(tmp_path, monkeypatch):
    with fresh_migration_database("retention_observe", install_extensions=False) as dsn:
        engine = create_engine(dsn, future=True)
        with engine.begin() as connection:
            connection.execute(text("CREATE SCHEMA market"))
        for model in (
            StorageTargetRecord,
            StoragePolicyRecord,
            StoragePlanRecord,
            StorageObjectLocationRecord,
            MarketFactRetentionPartitionRecord,
            MarketFactArchiveManifestRecord,
            MarketFactArchiveVerificationRecord,
        ):
            model.__table__.create(engine)
        monkeypatch.setattr(module, "get_settings", _settings)
        monkeypatch.setattr(module, "source_revision", lambda: "d" * 40)
        try:
            yield engine, tmp_path
        finally:
            engine.dispose()


def _policy(**changes):
    values = StoragePolicy(
        recent=("ssd",),
        history=("hdd",),
        archives=("hdd",),
        backups=("hdd",),
    ).to_dict()
    values.update(changes)
    return values


def _manifest(day, *, ordinal, byte_count, acknowledged_at):
    manifest_id = f"manifest-{ordinal}"
    object_key = f"canonical/{day.isoformat()}/{ordinal}.parquet"
    descriptor = {
        "schema_version": "market.canonical_fact_archive.v1",
        "record_selection": "all_canonical_revisions.v1",
        "manifest_id": manifest_id,
        "object_key": object_key,
        "object_sha256": HASH_A,
        "row_count": 10,
        "byte_count": byte_count,
    }
    return MarketFactArchiveManifestRecord(
        id=manifest_id,
        storage_day=day,
        page_ordinal=ordinal,
        object_key=object_key,
        object_sha256=HASH_A,
        manifest_hash=HASH_B,
        row_count=10,
        byte_count=byte_count,
        first_commit_seq=ordinal + 1,
        first_id=f"fact-{ordinal}-a",
        last_commit_seq=ordinal + 1,
        last_id=f"fact-{ordinal}-z",
        descriptor=descriptor,
        acknowledged_at=acknowledged_at,
    )


def _snapshot(session, policy=None):
    return module.RetentionObservability().snapshot(
        session,
        policy_payload=policy or _policy(),
        revision=4,
        targets=[],
        workers=[],
    )


def test_projection_executes_read_only_against_empty_real_schema_and_keeps_timeouts(postgres):
    engine, _ = postgres
    with Session(engine) as session, session.begin():
        session.execute(text("SET TRANSACTION READ ONLY"))
        result = _snapshot(session)
        assert session.scalar(text("SHOW transaction_read_only")) == "on"
        assert session.scalar(text("SHOW statement_timeout")) == "5s"
        assert session.scalar(text("SHOW lock_timeout")) == "250ms"

    assert result["state"] == "ready"
    assert result["backlog"]["eligible_partitions"] == 0
    assert result["storage"]["physical_hot"]["source_bytes"] == 0
    assert result["storage"]["compressed_archive"]["bytes"] == 0
    assert result["lifecycle"]["last_durable_advancement"] is None
    assert result["policy"]["effective_default_hot_days"] == 30
    assert result["policy"]["effective_fact_type_hot_days"] == {}
    assert result["policy"]["configured_base_fact_type_hot_days"] == {"inactive.base": 90}


def test_projection_preserves_nulls_and_separates_populated_storage_domains(postgres):
    engine, _ = postgres
    now = datetime.now(UTC)
    open_day = (now - timedelta(days=90)).date()
    sealed_day = (now - timedelta(days=89)).date()
    verified_day = (now - timedelta(days=88)).date()
    reclaimed_day = (now - timedelta(days=87)).date()
    with Session(engine) as session, session.begin():
        session.add_all([
            MarketFactRetentionPartitionRecord(storage_day=open_day, state="open"),
            MarketFactRetentionPartitionRecord(
                storage_day=sealed_day,
                state="sealed",
                expected_rows=10,
                source_bytes=100,
                sealed_at=now - timedelta(hours=4),
            ),
            MarketFactRetentionPartitionRecord(
                storage_day=verified_day,
                state="verified",
                expected_rows=20,
                source_bytes=200,
                sealed_at=now - timedelta(hours=3),
                verified_at=now - timedelta(hours=1),
                manifest_set_hash=HASH_A,
            ),
            MarketFactRetentionPartitionRecord(
                storage_day=reclaimed_day,
                state="reclaimed",
                expected_rows=30,
                source_bytes=300,
                sealed_at=now - timedelta(hours=2),
                verified_at=now - timedelta(hours=1),
                manifest_set_hash=HASH_B,
                reclaimed_at=now - timedelta(minutes=30),
                reclaimed_bytes=280,
            ),
        ])
        first = _manifest(
            sealed_day, ordinal=0, byte_count=80, acknowledged_at=now - timedelta(hours=2)
        )
        second = _manifest(
            verified_day, ordinal=1, byte_count=120, acknowledged_at=now - timedelta(hours=1)
        )
        session.add_all([first, second])
        session.flush()
        session.add(MarketFactArchiveVerificationRecord(
            manifest_id=second.id,
            verifier_version="fixture.v1",
            manifest_hash=second.manifest_hash,
            catalog_hash=HASH_C,
            verification_hash=HASH_A,
            verified_at=now - timedelta(minutes=10),
        ))

    with Session(engine) as session, session.begin():
        session.execute(text("SET TRANSACTION READ ONLY"))
        result = _snapshot(session)

    assert result["state"] == "partial"
    assert result["completeness"]["unknowns"] == ["open_partition_source_bytes_unmeasured"]
    assert result["backlog"]["eligible_partitions"] == 3
    assert result["backlog"]["eligible_source_bytes"] == 300
    assert result["backlog"]["verified_guarded_source_bytes"] == 200
    assert result["storage"]["physical_hot"] == {
        "source_bytes": 300,
        "estimated_rows": 30,
        "basis": "non_reclaimed_sealed_partition_catalog",
    }
    assert result["storage"]["compressed_archive"] == {
        "bytes": 200,
        "rows": 20,
        "verified_bytes": 120,
    }
    assert result["storage"]["cumulative_reclaimed_bytes"] == 280
    six_hours = result["rates"]["windows"][0]
    assert six_hours["intake"]["estimated_source_bytes"] == 600
    assert six_hours["archive"]["compressed_bytes"] == 200
    assert six_hours["verify"]["compressed_bytes"] == 120
    assert six_hours["reclaim"]["physical_hot_bytes"] == 280
    assert result["lifecycle"]["last_durable_advancement"]["stage"] == "archive_page_verified"


def test_projection_reports_real_query_truncation_with_small_test_bounds(postgres, monkeypatch):
    engine, _ = postgres
    now = datetime.now(UTC)
    monkeypatch.setattr(module, "PARTITION_SCAN_LIMIT", 1)
    monkeypatch.setattr(module, "MANIFEST_SCAN_LIMIT", 1)
    with Session(engine) as session, session.begin():
        for ordinal in range(2):
            day = (now - timedelta(days=90 - ordinal)).date()
            session.add(MarketFactRetentionPartitionRecord(
                storage_day=day,
                state="sealed",
                expected_rows=10,
                source_bytes=100 + ordinal,
                sealed_at=now - timedelta(hours=ordinal + 1),
            ))
            session.flush()
            session.add(_manifest(
                day,
                ordinal=ordinal,
                byte_count=80 + ordinal,
                acknowledged_at=now - timedelta(minutes=ordinal + 1),
            ))

    with Session(engine) as session, session.begin():
        session.execute(text("SET TRANSACTION READ ONLY"))
        result = _snapshot(session)

    assert result["state"] == "partial"
    assert result["completeness"]["truncated"] is True
    assert result["completeness"]["partition_scan_truncated"] is True
    assert result["completeness"]["manifest_scan_truncated"] is True
    assert result["bounds"]["partition_rows"] == 1
    assert result["bounds"]["manifest_rows"] == 1


def test_storage_snapshot_adds_projection_without_changing_status_contract(postgres, monkeypatch):
    engine, tmp_path = postgres
    inventory = tmp_path / "inventory.json"
    targets = [
        StorageTarget("ssd", "Fast", "uuid-ssd", "/qt/ssd", "ssd"),
        StorageTarget("hdd", "History", "uuid-hdd", "/qt/hdd", "hdd"),
    ]
    inventory.write_text(json.dumps({
        "schema_version": "qt.storage_inventory.v1",
        "targets": [asdict(target) for target in targets],
    }))

    def inspect(target, **_):
        return FilesystemEvidence(
            path=target.root,
            filesystem_uuid=target.filesystem_uuid,
            device_id="8:1",
            used_bytes=100,
            total_bytes=1000,
            available_bytes=900,
            read_only=False,
        )

    monkeypatch.setattr(StorageTarget, "inspect", inspect)
    service = StorageManagementService(_Database(engine), inventory_path=inventory)
    service.register("ssd")
    service.register("hdd")
    with service.database.session() as session:
        session.add(StoragePolicyRecord(id=1, revision=4, policy=_policy()))

    snapshot = service.snapshot()
    assert snapshot["schema_version"] == "qt.storage_status.v1"
    assert snapshot["health"] == "available"
    assert snapshot["retention"]["schema_version"] == "qt.storage_retention_observability.v1"
    assert snapshot["retention"]["state"] == "ready"
    assert snapshot["retention"]["policy"]["revision"] == 4
