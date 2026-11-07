from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
import json
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
        session.flush()
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
        session.flush()
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


def test_storage_metrics_function_respects_pre_dispatch_lock_deadline(postgres):
    from portal.backend.db.storage_metrics_schema import install_storage_metrics_functions
    from sqlalchemy.exc import DBAPIError
    from portal.backend.db.models import DatabaseCapacitySampleRecord, DatabaseRelationCapacitySampleRecord
    engine, _ = postgres
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA observability_metrics"))
        DatabaseCapacitySampleRecord.__table__.create(conn)
        DatabaseRelationCapacitySampleRecord.__table__.create(conn)
        install_storage_metrics_functions(conn)
    locker = engine.connect()
    transaction = locker.begin()
    try:
        locker.execute(text("LOCK TABLE market.fact_archive_manifests IN ACCESS EXCLUSIVE MODE"))
        started = monotonic()
        with pytest.raises(DBAPIError):
            with Session(engine) as session, session.begin():
                session.execute(text("SET TRANSACTION READ ONLY"))
                session.execute(text("SET LOCAL statement_timeout='5s'"))
                session.execute(text("SET LOCAL lock_timeout='250ms'"))
                session.execute(text("SELECT * FROM observability_metrics.storage_series_v1("
                    "'archive_compressed_bytes',now()-interval '1 hour',now(),300)" )).all()
        assert monotonic()-started < 2
    finally:
        transaction.rollback()
        locker.close()


def _install_metric_queries(engine, *, small_manifest_limit=False):
    from portal.backend.db.models import DatabaseCapacitySampleRecord, DatabaseRelationCapacitySampleRecord
    from portal.backend.db.storage_metrics_schema import install_storage_metrics_functions, STORAGE_SERIES_SQL
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA observability_metrics"))
        DatabaseCapacitySampleRecord.__table__.create(conn)
        DatabaseRelationCapacitySampleRecord.__table__.create(conn)
        install_storage_metrics_functions(conn)
        if small_manifest_limit:
            conn.execute(text(STORAGE_SERIES_SQL.replace("LIMIT 10001","LIMIT 3").replace("ELSE 10000 END","ELSE 2 END")))


def test_first_verification_counts_manifest_once_in_original_window(postgres):
    engine, _=postgres
    _install_metric_queries(engine)
    start=datetime(2026,1,1,tzinfo=UTC)
    day=start.date()
    with Session(engine) as session,session.begin():
        session.add(MarketFactRetentionPartitionRecord(storage_day=day,state="open"))
        session.flush()
        session.add(_manifest(day,ordinal=1,byte_count=123,acknowledged_at=start))
        session.flush()
        session.add_all([
            MarketFactArchiveVerificationRecord(manifest_id="manifest-1",verifier_version=f"market.canonical_archive_verification.v{v}",
               manifest_hash=HASH_B,catalog_hash=HASH_A,verification_hash=HASH_C,verified_at=at)
            for v,at in [(1,start+timedelta(minutes=10)),(12,start+timedelta(hours=2))]
        ])
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout='5s'"))
        conn.execute(text("SET LOCAL lock_timeout='250ms'"))
        def series(metric,lower,upper):
            return list(conn.execute(text("SELECT * FROM observability_metrics.storage_series_v1(:m,:lo,:hi,300)"),
                dict(m=metric,lo=lower,hi=upper)).mappings())
        original=series("verified_first_compressed_bytes",start,start+timedelta(hours=1))
        retry=series("verified_first_compressed_bytes",start+timedelta(hours=1),start+timedelta(hours=3))
        archive=series("archive_compressed_bytes",start,start+timedelta(hours=1))
        assert sum(r["value"] or 0 for r in original)==123
        assert all(r["value"] is None for r in retry)
        assert sum(r["value"] or 0 for r in archive)==123
        assert conn.scalar(text("SELECT count(*) FROM pg_catalog.pg_proc p, LATERAL pg_catalog.aclexplode(p.proacl) a "
            "WHERE p.pronamespace='observability_metrics'::regnamespace AND p.proname LIKE 'storage_%_v1' "
            "AND a.grantee=0 AND a.privilege_type='EXECUTE'"))==0


def test_source_cap_hides_partial_sum_and_reports_sentinel(postgres):
    engine,_=postgres
    _install_metric_queries(engine,small_manifest_limit=True)
    start=datetime(2026,1,1,tzinfo=UTC)
    with Session(engine) as session,session.begin():
        session.add(MarketFactRetentionPartitionRecord(storage_day=start.date(),state="open"))
        session.flush()
        session.add_all([_manifest(start.date(),ordinal=i,byte_count=100,acknowledged_at=start+timedelta(minutes=i))
                         for i in range(4)])
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout='5s'"))
        conn.execute(text("SET LOCAL lock_timeout='250ms'"))
        rows=list(conn.execute(text("SELECT * FROM observability_metrics.storage_series_v1("
            "'archive_compressed_bytes',:lo,:hi,300)"),dict(lo=start,hi=start+timedelta(hours=1))).mappings())
    assert all(r["truncated"] and not r["complete"] and r["source_count"]==3 for r in rows)
    assert all(r["value"] is None and r["reason"]=="source_limit" for r in rows)


def test_backlog_is_unknown_for_eligible_unsealed_partition(postgres):
    engine,_=postgres
    _install_metric_queries(engine)
    day=(datetime.now(UTC)-timedelta(days=90)).date()
    with Session(engine) as session,session.begin():
        session.add(StoragePolicyRecord(id=1,revision=1,policy=_policy(recent_days=30)))
        session.add(MarketFactRetentionPartitionRecord(storage_day=day,state="open"))
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout='5s'"))
        conn.execute(text("SET LOCAL lock_timeout='250ms'"))
        rows=list(conn.execute(text("SELECT * FROM observability_metrics.storage_metric_status_v1()")).mappings())
    assert all(r["value"] is None and r["reason"]=="eligible_unsealed_size_unknown" for r in rows)
    assert all(r["oldest_day"]==day for r in rows)


def test_coarse_stock_growth_uses_observed_elapsed_and_market_requires_whole_sample(postgres):
    from portal.backend.db.models import DatabaseCapacitySampleRecord, DatabaseRelationCapacitySampleRecord
    engine,_=postgres
    _install_metric_queries(engine)
    start=datetime(2026,1,1,tzinfo=UTC)
    times=[start+timedelta(minutes=1+5*i,seconds=20) for i in range(6)]
    times += [start+timedelta(minutes=31+5*i,seconds=50) for i in range(6)]
    with engine.begin() as conn:
        for i,at in enumerate(times):
            # Complete DB sample says two relation rows; deliberately retain one.
            d={c.name:0 for c in DatabaseCapacitySampleRecord.__table__.columns}
            d.update(sampled_at=at.replace(tzinfo=None),created_at=at.replace(tzinfo=None),
                     database_size_bytes=100+i*10,relation_count=2)
            conn.execute(DatabaseCapacitySampleRecord.__table__.insert(),d)
            r={c.name:0 for c in DatabaseRelationCapacitySampleRecord.__table__.columns}
            r.update(sampled_at=at.replace(tzinfo=None),created_at=at.replace(tzinfo=None),
                     schema_name="market",relation_name="synthetic",relation_kind="table",total_bytes=10)
            conn.execute(DatabaseRelationCapacitySampleRecord.__table__.insert(),r)
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout='5s'"))
        conn.execute(text("SET LOCAL lock_timeout='250ms'"))
        growth=list(conn.execute(text("SELECT * FROM observability_metrics.storage_growth_v1("
            "'database_allocated_bytes',:lo,:hi,1800)"),dict(lo=start,hi=start+timedelta(hours=1))).mappings())
        market=list(conn.execute(text("SELECT * FROM observability_metrics.storage_series_v1("
            "'market_allocated_bytes',:lo,:hi,1800)"),dict(lo=start,hi=start+timedelta(hours=1))).mappings())
    assert growth[0]["value"] is None
    assert growth[1]["value"]==pytest.approx(60*86400/1830)
    assert growth[1]["observed_at"]==times[-1]
    assert all(r["value"] is None and not r["complete"] for r in market)
    assert all(r["reason"]=="relation_sample_incomplete" for r in market)


def test_metrics_refuses_unbounded_session_before_data_read(postgres):
    from sqlalchemy.exc import DBAPIError
    engine,_=postgres
    _install_metric_queries(engine)
    with engine.connect() as conn:
        conn.execute(text("SET LOCAL statement_timeout='0'"))
        with pytest.raises(DBAPIError,match="storage_metrics_session_bounds_required"):
            conn.execute(text("SELECT * FROM observability_metrics.storage_metric_status_v1()" )).all()


def test_restricted_definer_transition_does_not_grant_raw_market_access(postgres):
    from pathlib import Path
    from uuid import uuid4
    from sqlalchemy.exc import DBAPIError
    engine,_=postgres
    _install_metric_queries(engine)
    token=uuid4().hex[:10]
    roles={name:f"{name}_{token}" for name in
           ("qt_storage_metrics_owner","qt_storage_metrics_reader","qt_storage_metrics_grafana")}
    api=f"synthetic_metrics_api_{token}"
    sql=(Path(__file__).parents[2]/"scripts/db/manual_grant_storage_metrics_v1.sql").read_text()
    for original,replacement in roles.items(): sql=sql.replace(original,replacement)
    sql=sql.replace(':"api_role"',api)
    sql="\n".join(line for line in sql.splitlines() if not line.startswith("\\"))
    owner=roles["qt_storage_metrics_owner"]
    reader=roles["qt_storage_metrics_reader"]
    login=roles["qt_storage_metrics_grafana"]
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
        if not admin.scalar(text("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user")):
            pytest.skip("isolated role administrator required")
        database=admin.scalar(text("SELECT current_database()"))
        quote=admin.dialect.identifier_preparer.quote
        try:
            admin.execute(text(f"CREATE ROLE {login} LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"))
            admin.execute(text(f"CREATE ROLE {api} NOLOGIN"))
            # Only this generated fixture database may have its TEMP baseline changed.
            admin.execute(text(f"GRANT TEMP ON DATABASE {quote(database)} TO PUBLIC"))
            with pytest.raises(DBAPIError,match="ambient_temporary_privilege"):
                admin.execute(text(sql))
            admin.execute(text("ROLLBACK"))
            assert not admin.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=:name)"),dict(name=owner))
            admin.execute(text(f"REVOKE TEMP ON DATABASE {quote(database)} FROM PUBLIC"))
            admin.execute(text(f"GRANT CREATE ON DATABASE {quote(database)} TO {login}"))
            with pytest.raises(DBAPIError,match="database_create_privilege"):
                admin.execute(text(sql))
            admin.execute(text("ROLLBACK"))
            assert not admin.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=:name)"),dict(name=owner))
            admin.execute(text(f"REVOKE CREATE ON DATABASE {quote(database)} FROM {login}"))
            admin.execute(text(sql))
            # A new authenticated connection is required: SET ROLE does not load
            # ALTER ROLE defaults. This password belongs only to a generated
            # disposable fixture role and is never printed or stored in source.
            synthetic_password=uuid4().hex+uuid4().hex
            admin.engine.hide_parameters=True
            admin.execute(text("SELECT set_config('qt.metrics_fixture_password',:password,false)"),
                          dict(password=synthetic_password))
            admin.execute(text(f"""DO $fixture_password$ BEGIN
                EXECUTE format('ALTER ROLE %I PASSWORD %L','{login}',
                    current_setting('qt.metrics_fixture_password'));
                END $fixture_password$"""))
            admin.execute(text("RESET qt.metrics_fixture_password"))
            client_engine=create_engine(engine.url.set(username=login,password=synthetic_password),
                future=True,pool_size=1,max_overflow=0,hide_parameters=True,
                connect_args={"connect_timeout":3,"options":""})
            try:
                with client_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as client:
                    assert client.scalar(text("SELECT current_user"))==login
                    assert client.scalar(text("SHOW default_transaction_read_only"))=="on"
                    assert client.scalar(text("SHOW transaction_read_only"))=="on"
                    assert client.scalar(text("SHOW statement_timeout"))=="5s"
                    assert client.scalar(text("SHOW lock_timeout"))=="250ms"
                    assert len(client.execute(text("SELECT * FROM observability_metrics.storage_metric_status_v1()")).all())==2
                    assert not client.scalar(text("SELECT has_database_privilege(current_user,current_database(),'CREATE')"))
                    assert not client.scalar(text("SELECT has_database_privilege(current_user,current_database(),'TEMP')"))
                    for statement in (
                        "CREATE TEMP TABLE synthetic_denied_temp(value integer)",
                        "CREATE SCHEMA synthetic_denied_schema",
                        "SELECT byte_count FROM market.fact_archive_manifests LIMIT 1",
                        "INSERT INTO public.portal_storage_policy (id,revision,policy) VALUES (1,1,'{}'::jsonb)",
                    ):
                        with pytest.raises(DBAPIError): client.execute(text(statement))
            finally:
                client_engine.dispose()

            admin.execute(text(f"SET ROLE {login}"))
            admin.execute(text("BEGIN READ ONLY"))
            admin.execute(text("SET LOCAL statement_timeout='5s'"))
            admin.execute(text("SET LOCAL lock_timeout='250ms'"))
            rows=admin.execute(text("SELECT * FROM observability_metrics.storage_metric_status_v1()" )).all()
            assert len(rows)==2
            admin.execute(text("ROLLBACK"))
            with pytest.raises(DBAPIError):
                admin.execute(text("SELECT byte_count FROM market.fact_archive_manifests LIMIT 1"))
            with pytest.raises(DBAPIError):
                admin.execute(text("INSERT INTO public.portal_storage_policy (id,revision,policy) VALUES (1,1,'{}'::jsonb)"))
            admin.execute(text("RESET ROLE"))
            assert not admin.scalar(text("SELECT has_schema_privilege(:role,'observability_metrics','CREATE')"),dict(role=owner))
        finally:
            admin.execute(text("ROLLBACK"))
            admin.execute(text("RESET ROLE"))
            current=admin.scalar(text("SELECT current_user"))
            for role in (login,api,reader,owner):
                if admin.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=:name)"),dict(name=role)):
                    admin.execute(text(f"REASSIGN OWNED BY {role} TO {quote(current)}"))
                    admin.execute(text(f"DROP OWNED BY {role}"))
                    admin.execute(text(f"DROP ROLE {role}"))


def _ambient_monitoring_queries(login):
    from pathlib import Path
    source=(Path(__file__).parents[2]/"scripts/db/manual_grant_storage_metrics_v1.sql").read_text()
    result=[]
    for name in ("ambient_relation","reset_privilege"):
        begin=f"-- storage_metrics_{name}_query_begin\n"
        end=f"-- storage_metrics_{name}_query_end"
        result.append(source.split(begin,1)[1].split(end,1)[0].replace("qt_storage_metrics_grafana",login))
    return result


@pytest.mark.parametrize("scenario,blocked,reset_blocked",[
    ("namesake",True,False),
    ("unrelated_public_select",True,False),
    ("genuine",False,False),
    ("public_write",True,False),
    ("public_column_write",True,False),
    ("external_relation",True,False),
    ("builtin_expression",True,False),
    ("definer_function",True,False),
    ("reset_execute",False,True),
])
def test_monitoring_views_require_exact_standard_extension_visibility(postgres,scenario,blocked,reset_blocked):
    from uuid import uuid4
    engine,_=postgres
    login=f"synthetic_monitor_reader_{uuid4().hex[:10]}"
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
        if not admin.scalar(text("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user")):
            pytest.skip("isolated extension/role administrator required")
        # The fixture owns a generated isolated database; no operational extension
        # or role is read or changed. No password or LOGIN access is introduced.
        try:
            admin.execute(text(f"CREATE ROLE {login} NOLOGIN INHERIT"))
            admin.execute(text("DROP EXTENSION IF EXISTS pg_stat_statements"))
            if scenario in ("namesake","unrelated_public_select"):
                name="pg_stat_statements" if scenario=="namesake" else "synthetic_unrelated_monitor"
                admin.execute(text(f"CREATE VIEW public.{name} AS SELECT 1 AS synthetic_value"))
                admin.execute(text(f"GRANT SELECT ON public.{name} TO PUBLIC"))
            else:
                assert admin.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_available_extension_versions "
                    "WHERE name='pg_stat_statements' AND version='1.10')")), "qualified pg_stat_statements 1.10 image required"
                admin.execute(text("CREATE EXTENSION pg_stat_statements WITH SCHEMA public VERSION '1.10'"))
                assert admin.scalar(text("SELECT has_table_privilege(:role,'public.pg_stat_statements','SELECT')"),dict(role=login))
                assert admin.scalar(text("SELECT has_table_privilege(:role,'public.pg_stat_statements_info','SELECT')"),dict(role=login))
                if scenario=="public_write":
                    admin.execute(text("GRANT UPDATE ON public.pg_stat_statements_info TO PUBLIC"))
                elif scenario=="public_column_write":
                    admin.execute(text("GRANT UPDATE(stats_reset) ON public.pg_stat_statements_info TO PUBLIC"))
                elif scenario=="external_relation":
                    admin.execute(text("CREATE TABLE public.synthetic_monitor_source(value integer)"))
                    admin.execute(text("CREATE OR REPLACE VIEW public.pg_stat_statements_info AS "
                        "SELECT info.dealloc,info.stats_reset FROM public.pg_stat_statements_info() info "
                        "CROSS JOIN public.synthetic_monitor_source"))
                elif scenario=="builtin_expression":
                    admin.execute(text("CREATE OR REPLACE VIEW public.pg_stat_statements_info AS "
                        "SELECT info.dealloc,clock_timestamp() AS stats_reset FROM public.pg_stat_statements_info() info"))
                elif scenario=="definer_function":
                    admin.execute(text("ALTER FUNCTION public.pg_stat_statements_info() SECURITY DEFINER"))
                elif scenario=="reset_execute":
                    admin.execute(text("GRANT EXECUTE ON FUNCTION public.pg_stat_statements_reset(oid,oid,bigint) TO PUBLIC"))
            # Match the installer's transaction-local deparse path and inspect the
            # exact source predicates, not a copied/adapted classification formula.
            admin.execute(text("SET search_path=pg_catalog,pg_temp"))
            relation_query,reset_query=_ambient_monitoring_queries(login)
            assert bool(admin.execute(text(relation_query)).all()) is blocked
            assert bool(admin.execute(text(reset_query)).all()) is reset_blocked
            assert not admin.scalar(text("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=:role"),dict(role=login))
            assert not admin.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members m "
                "JOIN pg_catalog.pg_roles r ON r.oid=m.member WHERE r.rolname=:role)"),dict(role=login))
        finally:
            admin.execute(text("RESET search_path"))
            if admin.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=:role)"),dict(role=login)):
                admin.execute(text(f"DROP OWNED BY {login}"))
                admin.execute(text(f"DROP ROLE {login}"))
