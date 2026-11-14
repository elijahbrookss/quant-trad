from datetime import UTC, datetime
import os

import pytest
from sqlalchemy import create_engine, text

from market_data.fact_archive import archive_evidence_hash
from scripts.db.canonical_cold_pressure import _read_database, build_receipt
from tests.test_market_data.test_canonical_cold_capacity import _sample


UUIDS = {resource: resource for resource in ("docker-engine-storage", "market-archive-storage")}


@pytest.fixture
def samples():
    return [
        _sample("docker-engine-storage", "docker_engine_storage", "engine_storage_filesystem", "8:1", 650, 18),
        _sample("market-archive-storage", "market_archive_storage", "archive_filesystem", "8:2", 450, 18),
    ]


@pytest.fixture
def observed():
    return {
        "observed_at": datetime(2026, 11, 14, 18, 0, 5, tzinfo=UTC),
        "archive_status": {
            "supported": True, "ready_markers": 1,
            "oldest_ready_marker_at": datetime(2026, 11, 14, 17, 59, tzinfo=UTC),
        },
        "resident_wal": {"files": 3, "bytes": 50331648},
        "database_temp_since_reset": {"files": 5, "bytes": 100_000_000},
    }


def test_assembles_hashed_observation_with_separate_resident_and_cumulative_values(observed, samples):
    result = build_receipt(observed, samples, expected_filesystem_uuids=UUIDS, max_sample_age_seconds=5)
    assert result["filesystems"]["docker-engine-storage"]["available_bytes"] == 350
    assert result["postgresql"]["resident_wal"]["bytes"] == 50331648
    assert result["postgresql"]["database_temp_since_reset"]["bytes"] == 100_000_000
    assert result["postgresql"]["archive_status"]["oldest_ready_marker_at"] == "2026-11-14T17:59:00+00:00"
    assert result.pop("receipt_hash") == archive_evidence_hash(result)


@pytest.mark.parametrize(("changes", "allowed_age", "error"), [
    ({}, 4, "outside_allowed_age"),
    ({"filesystem_uuid": "unapproved-medium"}, 5, "uuid_mismatch"),
    ({"device_id": "8:2"}, 5, "aligned_distinct"),
    ({"observed_at": "2026-11-14T18:00:01+00:00"}, 5, "aligned_distinct"),
    ({"physical_host_visible": False}, 5, "native_device_identity"),
])
def test_refuses_stale_or_misdirected_observations(observed, samples, changes, allowed_age, error):
    samples[0].update(changes)
    with pytest.raises(ValueError, match=error):
        build_receipt(observed, samples, expected_filesystem_uuids=UUIDS, max_sample_age_seconds=allowed_age)


@pytest.mark.db
def test_native_monitor_reads_a_pinned_postgresql_fixture_only():
    dsn = os.environ.get("PG_DSN")
    if os.environ.get("QT_DB_TEST_ISOLATED") != "1" or not dsn:
        pytest.skip("requires an isolated PostgreSQL 15 fixture")
    engine = create_engine(dsn)
    try:
        if engine.url.host not in ("localhost", "127.0.0.1", "::1"):
            pytest.fail("requires a loopback PostgreSQL fixture")
        with engine.begin() as conn:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            expected = dict(conn.execute(text("""
                SELECT system_identifier::text AS system_identifier, d.oid::bigint AS database_oid,
                       d.datname AS database
                FROM pg_catalog.pg_control_system() CROSS JOIN pg_catalog.pg_database d
                WHERE d.datname=current_database()
            """)).mappings().one())
        with pytest.raises(ValueError, match="identity_mismatch"):
            _read_database(engine, expected | {"database_oid": expected["database_oid"] + 1})
        observed = _read_database(engine, expected)
        assert observed["database"] == expected
        assert observed["resident_wal"]["bytes"] > 0
        assert observed["resident_default_tablespace_temp"]["bytes"] >= 0
        assert observed["database_temp_since_reset"]["files"] >= 0
        assert observed["archive_status"]["supported"] is True
        assert observed["archive_status"]["ready_markers"] >= observed["archive_status"]["ready_segment_markers"]
        assert "max_wal_size" in observed["observer_settings"]
    finally:
        engine.dispose()
