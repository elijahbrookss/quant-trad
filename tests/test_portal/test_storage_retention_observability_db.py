from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from time import monotonic
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


def _snapshot(session, policy=None, *, sampled_at=None):
    return module.RetentionObservability().snapshot(
        session,
        policy_payload=policy or _policy(),
        revision=4,
        targets=[],
        workers=[],
        sampled_at=sampled_at,
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
        session.add_all([
            MarketFactArchiveVerificationRecord(
                manifest_id=second.id,
                verifier_version="fixture.v1",
                manifest_hash=second.manifest_hash,
                catalog_hash=HASH_C,
                verification_hash=HASH_A,
                verified_at=now - timedelta(minutes=20),
            ),
            MarketFactArchiveVerificationRecord(
                manifest_id=second.id,
                verifier_version="fixture.v2",
                manifest_hash=second.manifest_hash,
                catalog_hash=HASH_C,
                verification_hash=HASH_B,
                verified_at=now - timedelta(minutes=10),
            ),
        ])

    with Session(engine) as session, session.begin():
        session.execute(text("SET TRANSACTION READ ONLY"))
        result = _snapshot(session, sampled_at=now)

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
    monkeypatch.setattr(module, "PARTITION_SCAN_LIMIT", 2)
    monkeypatch.setattr(module, "MANIFEST_SCAN_LIMIT", 2)
    with Session(engine) as session, session.begin():
        for ordinal in range(3):
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
        result = _snapshot(session, sampled_at=now)

    assert result["state"] == "partial"
    assert result["completeness"]["truncated"] is True
    assert result["completeness"]["partition_scan_truncated"] is True
    assert result["completeness"]["manifest_scan_truncated"] is True
    assert result["bounds"]["partition_rows"] == 2
    assert result["bounds"]["manifest_rows"] == 2
    assert result["bounds"]["partition_scan_rows"] == 3
    assert result["bounds"]["manifest_scan_rows"] == 3


def _configured_service(engine, tmp_path, monkeypatch):
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
    return service


def test_storage_snapshot_adds_projection_without_changing_status_contract(postgres, monkeypatch):
    engine, tmp_path = postgres
    service = _configured_service(engine, tmp_path, monkeypatch)

    snapshot = service.snapshot()
    assert snapshot["schema_version"] == "qt.storage_status.v1"
    assert snapshot["health"] == "available"
    assert snapshot["retention"]["schema_version"] == "qt.storage_retention_observability.v1"
    assert snapshot["retention"]["state"] == "ready"
    assert snapshot["retention"]["policy"]["revision"] == 4


def test_latest_durable_event_keeps_timestamp_and_day_from_same_row(postgres):
    engine, _ = postgres
    sampled_at = datetime(2026, 10, 19, 18, 0, tzinfo=UTC)
    older_day = (sampled_at - timedelta(days=90)).date()
    newer_day = (sampled_at - timedelta(days=89)).date()
    with Session(engine) as session, session.begin():
        session.add_all([
            MarketFactRetentionPartitionRecord(
                storage_day=older_day,
                state="sealed",
                expected_rows=1,
                source_bytes=10,
                sealed_at=sampled_at - timedelta(minutes=5),
            ),
            MarketFactRetentionPartitionRecord(
                storage_day=newer_day,
                state="sealed",
                expected_rows=1,
                source_bytes=10,
                sealed_at=sampled_at - timedelta(hours=1),
            ),
        ])
    with Session(engine) as session, session.begin():
        session.execute(text("SET TRANSACTION READ ONLY"))
        result = _snapshot(session, sampled_at=sampled_at)

    assert result["lifecycle"]["last_durable_advancement"] == {
        "stage": "sealed",
        "storage_day": older_day.isoformat(),
        "happened_at": (sampled_at - timedelta(minutes=5)).isoformat(),
        "authority": "durable_catalog",
    }


def test_exact_rolling_window_edges_are_inclusive_and_keep_domains_separate(postgres):
    engine, _ = postgres
    sampled_at = datetime(2026, 10, 19, 18, 0, tzinfo=UTC)
    days = [(sampled_at - timedelta(days=90 - offset)).date() for offset in range(4)]
    edge = sampled_at - timedelta(hours=6)
    before = edge - timedelta(microseconds=1)
    with Session(engine) as session, session.begin():
        session.add_all([
            MarketFactRetentionPartitionRecord(
                storage_day=days[0], state="sealed", expected_rows=1,
                source_bytes=10, sealed_at=edge,
            ),
            MarketFactRetentionPartitionRecord(
                storage_day=days[1], state="sealed", expected_rows=1,
                source_bytes=20, sealed_at=before,
            ),
            MarketFactRetentionPartitionRecord(
                storage_day=days[2], state="reclaimed", expected_rows=1,
                source_bytes=30, sealed_at=sampled_at - timedelta(days=1),
                verified_at=sampled_at - timedelta(hours=7),
                manifest_set_hash=HASH_A, reclaimed_at=edge, reclaimed_bytes=11,
            ),
            MarketFactRetentionPartitionRecord(
                storage_day=days[3], state="reclaimed", expected_rows=1,
                source_bytes=40, sealed_at=sampled_at - timedelta(days=1),
                verified_at=sampled_at - timedelta(hours=7),
                manifest_set_hash=HASH_B, reclaimed_at=before, reclaimed_bytes=13,
            ),
        ])
        first = _manifest(days[0], ordinal=0, byte_count=5, acknowledged_at=edge)
        second = _manifest(days[1], ordinal=1, byte_count=7, acknowledged_at=before)
        session.add_all([first, second])
        session.flush()
        session.add_all([
            MarketFactArchiveVerificationRecord(
                manifest_id=first.id, verifier_version="edge", manifest_hash=first.manifest_hash,
                catalog_hash=HASH_C, verification_hash=HASH_A, verified_at=edge,
            ),
            MarketFactArchiveVerificationRecord(
                manifest_id=second.id, verifier_version="before", manifest_hash=second.manifest_hash,
                catalog_hash=HASH_C, verification_hash=HASH_B, verified_at=before,
            ),
        ])

    with Session(engine) as session, session.begin():
        session.execute(text("SET TRANSACTION READ ONLY"))
        result = _snapshot(session, sampled_at=sampled_at)

    six_hours = result["rates"]["windows"][0]
    assert six_hours["intake"]["estimated_source_bytes"] == 10
    assert six_hours["archive"]["compressed_bytes"] == 5
    assert six_hours["verify"]["compressed_bytes"] == 5
    assert six_hours["reclaim"]["physical_hot_bytes"] == 11


def test_projection_lock_timeout_is_unknown_without_poisoning_base_snapshot(postgres, monkeypatch):
    engine, tmp_path = postgres
    service = _configured_service(engine, tmp_path, monkeypatch)
    locker = engine.connect()
    transaction = locker.begin()
    try:
        locker.execute(text(
            "LOCK TABLE market.fact_retention_partitions IN ACCESS EXCLUSIVE MODE"
        ))
        started = monotonic()
        snapshot = service.snapshot()
        assert monotonic() - started < 2
        assert snapshot["health"] == "available"
        assert snapshot["retention"]["state"] == "unknown"
        assert snapshot["retention"]["reason"] == "bounded_projection_unavailable"
    finally:
        transaction.rollback()
        locker.close()

    assert service.snapshot()["retention"]["state"] == "ready"


def test_projection_statement_timeout_cancels_delayed_query_on_fresh_connection(
        postgres, monkeypatch):
    engine, tmp_path = postgres
    service = _configured_service(engine, tmp_path, monkeypatch)
    engine.dispose()
    monkeypatch.setattr(module, "STATEMENT_TIMEOUT_MS", 50)
    monkeypatch.setattr(module, "_PROJECTION_SQL", text("SELECT pg_sleep(5)"))

    started = monotonic()
    snapshot = service.snapshot()
    assert monotonic() - started < 1
    assert snapshot["health"] == "available"
    assert snapshot["retention"]["state"] == "unknown"
    assert snapshot["retention"]["reason"] == "bounded_projection_unavailable"


def test_grafana_metadata_panels_refuse_unbounded_queries_even_when_tables_are_locked(postgres):
    engine, _ = postgres
    dashboard = json.loads((Path(__file__).parents[2] /
        "docker/grafana/provisioning/dashboards/capacity-database-growth.json").read_text())
    panels_by_id = {
        panel["id"]: panel
        for panel in dashboard["panels"] if panel.get("id") in {28, 29, 30, 31}
    }
    assert set(panels_by_id) == {28, 29, 30, 31}
    assert all("bounded Storage API/UI projection" in panel["description"]
               for panel in panels_by_id.values())
    assert all(panel["fieldConfig"]["defaults"]["noValue"] ==
               "Unavailable: Grafana pre-query deadline unavailable"
               for panel in (panels_by_id[28], panels_by_id[29], panels_by_id[30]))
    sql_by_id = {
        panel_id: panel["targets"][0]["rawSql"]
        for panel_id, panel in panels_by_id.items()
    }
    for sql in sql_by_id.values():
        assert "bounded_storage_projection_unavailable" in sql
        assert "fact_retention_partitions" not in sql
        assert "fact_archive_manifests" not in sql
        assert "fact_archive_verifications" not in sql
        assert "set_config" not in sql

    engine.dispose()
    locker = engine.connect()
    transaction = locker.begin()
    try:
        locker.execute(text(
            "LOCK TABLE market.fact_retention_partitions,"
            "market.fact_archive_manifests,market.fact_archive_verifications "
            "IN ACCESS EXCLUSIVE MODE"
        ))
        started = monotonic()
        with Session(engine) as session, session.begin():
            session.execute(text("SET TRANSACTION READ ONLY"))
            results = {
                panel_id: list(session.execute(text(sql)).mappings())
                for panel_id, sql in sql_by_id.items()
            }
        assert monotonic() - started < 1
    finally:
        transaction.rollback()
        locker.close()

    assert all(rows == [] for rows in results.values())
