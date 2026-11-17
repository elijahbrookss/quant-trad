"""Read PostgreSQL WAL and spill pressure alongside native QT filesystem samples."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ArgumentError, DBAPIError

from market_data.fact_archive import archive_evidence_hash
from scripts.db.canonical_cold_capacity import _RESOURCES, _sample


_IDENTITY = """
    SELECT system_identifier::text AS system_identifier, d.oid::bigint AS database_oid,
           d.datname AS database, current_setting('server_version_num')::int AS server_version_num,
           pg_has_role(current_user, 'pg_monitor', 'USAGE') AS monitor_access,
           to_regprocedure('pg_catalog.pg_ls_archive_statusdir()') IS NOT NULL AS archive_status_supported
    FROM pg_catalog.pg_control_system() CROSS JOIN pg_catalog.pg_database d
    WHERE d.datname=current_database()
"""
_PRESSURE = """
    SELECT clock_timestamp() AS observed_at, pg_postmaster_start_time() AS started_at,
           pg_is_in_recovery() AS recovery, current_setting('transaction_read_only')::bool AS read_only,
           (SELECT jsonb_build_object('files', count(*), 'bytes', coalesce(sum(size),0))
            FROM pg_catalog.pg_ls_waldir()) AS resident_wal,
           (SELECT jsonb_build_object('files', count(*), 'bytes', coalesce(sum(size),0))
            FROM pg_catalog.pg_ls_tmpdir()) AS resident_default_tablespace_temp,
           (SELECT jsonb_build_object('files',temp_files,'bytes',temp_bytes,'reset_at',stats_reset)
            FROM pg_catalog.pg_stat_database WHERE datname=current_database()) AS database_temp_since_reset,
           (SELECT jsonb_build_object('bytes',wal_bytes,'reset_at',stats_reset)
            FROM pg_catalog.pg_stat_wal) AS wal_since_reset,
           (SELECT jsonb_build_object('succeeded',archived_count,'failed',failed_count,
                    'last_success_at',last_archived_time,'last_failure_at',last_failed_time,'reset_at',stats_reset)
            FROM pg_catalog.pg_stat_archiver) AS archiver,
           (SELECT jsonb_build_object('total',count(*),'inactive',count(*) FILTER (WHERE NOT active),
                    'oldest_restart_lsn',min(restart_lsn))
            FROM pg_catalog.pg_replication_slots) AS replication_slots,
           (SELECT jsonb_build_object('total',count(*),
                    'active',count(*) FILTER (WHERE state='active'),
                    'idle_in_transaction',count(*) FILTER (WHERE state IN ('idle in transaction','idle in transaction (aborted)')),
                    'oldest_open_transaction_at',min(xact_start))
            FROM pg_catalog.pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()) AS database_backends,
           (SELECT jsonb_object_agg(name, jsonb_build_object('value',setting,'unit',unit,'source',source))
            FROM pg_catalog.pg_settings WHERE name IN (
                'archive_mode','archive_timeout','checkpoint_timeout','wal_segment_size','min_wal_size',
                'max_wal_size','wal_keep_size','max_slot_wal_keep_size','temp_file_limit',
                'max_parallel_workers_per_gather','max_parallel_maintenance_workers')) AS observer_settings,
           current_setting('temp_tablespaces')<>'' AS observer_temp_tablespaces_configured
"""
_ARCHIVE_STATUS = """
    SELECT count(*) FILTER (WHERE name LIKE '%.ready') AS ready_markers,
           count(*) FILTER (WHERE name ~ '^[0-9A-F]{24}[.]ready$') AS ready_segment_markers,
           count(*) FILTER (WHERE name LIKE '%.done') AS done_markers,
           min(modification) FILTER (WHERE name LIKE '%.ready') AS oldest_ready_marker_at
    FROM pg_catalog.pg_ls_archive_statusdir()
"""


def _read_database(engine, expected_database):
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
        conn.exec_driver_sql("SET LOCAL lock_timeout='250ms'")
        conn.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")
        identity = dict(conn.execute(text(_IDENTITY)).mappings().one())
        database = {key: identity[key] for key in ("system_identifier", "database_oid", "database")}
        if database != expected_database:
            raise ValueError("cold_pressure_database_identity_mismatch")
        if identity["server_version_num"] < 150000 or not identity["monitor_access"]:
            raise ValueError("cold_pressure_requires_postgresql_15_and_pg_monitor_access")
        observed = dict(conn.execute(text(_PRESSURE)).mappings().one())
        if not observed.pop("read_only"):
            raise ValueError("cold_pressure_requires_read_only_transaction")
        observed["archive_status"] = {"supported": identity["archive_status_supported"]}
        if identity["archive_status_supported"]:
            observed["archive_status"].update(conn.execute(text(_ARCHIVE_STATUS)).mappings().one())
        return observed | {"database": database, "server_version_num": identity["server_version_num"]}


def build_receipt(observed, samples, *, expected_filesystem_uuids, max_sample_age_seconds):
    """Align a native host observation with a separately pinned PostgreSQL snapshot."""
    if (type(max_sample_age_seconds) is not int or max_sample_age_seconds <= 0
            or set(expected_filesystem_uuids) != set(_RESOURCES)
            or any(not isinstance(sample, dict) for sample in samples)):
        raise ValueError("cold_pressure_invalid_sample_scope")
    observed_at = observed["observed_at"]
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise ValueError("cold_pressure_database_clock_unavailable")
    filesystems = {}
    timestamps = set()
    devices = set()
    for resource, (scope, authority) in _RESOURCES.items():
        sample, (device, uuid, _), captured = _sample(samples, resource, scope, authority)
        if uuid != expected_filesystem_uuids[resource]:
            raise ValueError(f"cold_pressure_filesystem_uuid_mismatch: {resource}")
        timestamps.add(captured)
        devices.add(device)
        filesystems[resource] = {key: sample[key] for key in (
            "device_id", "filesystem_uuid", "capacity_authority", "total_bytes", "used_bytes", "available_bytes",
        )}
    if (len(timestamps) != 1 or len(devices) != len(_RESOURCES)
            or len(set(expected_filesystem_uuids.values())) != len(_RESOURCES)):
        raise ValueError("cold_pressure_requires_aligned_distinct_filesystems")
    captured, = timestamps
    if not 0 <= (observed_at - captured).total_seconds() <= max_sample_age_seconds:
        raise ValueError("cold_pressure_sample_outside_allowed_age")
    result = {
        "schema": "qt.canonical_cold_pressure.v1",
        "postgresql": {key: value.isoformat() if isinstance(value, datetime) else value
                       for key, value in observed.items()},
        "filesystems": filesystems,
        "filesystems_observed_at": captured.isoformat(),
        "max_filesystem_sample_age_seconds": max_sample_age_seconds,
    }
    # The only nested native datetime comes from the optional archive-directory query.
    oldest = result["postgresql"]["archive_status"].get("oldest_ready_marker_at")
    if oldest is not None:
        result["postgresql"]["archive_status"] = observed["archive_status"] | {
            "oldest_ready_marker_at": oldest.isoformat(),
        }
    return result | {"receipt_hash": archive_evidence_hash(result)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("filesystem_samples", type=Path, help="one native observer capture in JSON Lines")
    parser.add_argument("--database", required=True)
    parser.add_argument("--database-oid", required=True, type=int)
    parser.add_argument("--system-identifier", required=True)
    parser.add_argument("--ssd-uuid", required=True)
    parser.add_argument("--hdd-uuid", required=True)
    parser.add_argument("--max-sample-age-seconds", required=True, type=int)
    args = parser.parse_args()
    if not os.environ.get("PG_DSN"):
        parser.exit(2, "cold_pressure_missing_pg_dsn\n")
    try:
        with args.filesystem_samples.open(encoding="utf-8") as source:
            payload = source.read(65_537)
        if len(payload) > 65_536:
            raise ValueError("cold_pressure_sample_input_too_large")
        samples = [json.loads(line) for line in payload.splitlines()]
        engine = create_engine(os.environ["PG_DSN"], connect_args={"connect_timeout": 5}, hide_parameters=True)
        try:
            observed = _read_database(engine, {
                "database": args.database, "database_oid": args.database_oid,
                "system_identifier": args.system_identifier,
            })
        finally:
            engine.dispose()
        result = build_receipt(observed, samples, expected_filesystem_uuids={
            "docker-engine-storage": args.ssd_uuid, "market-archive-storage": args.hdd_uuid,
        }, max_sample_age_seconds=args.max_sample_age_seconds)
    except DBAPIError as exc:
        parser.exit(2, f"cold_pressure_database_observation_failed sqlstate={getattr(exc.orig, 'pgcode', None)}\n")
    except ValueError as exc:
        error = str(exc)
        parser.exit(2, (error if error.startswith(("cold_pressure_", "cutover_capacity_"))
                        else "cold_pressure_invalid_input") + "\n")
    except (ArgumentError, OSError, KeyError, TypeError):
        parser.exit(2, "cold_pressure_invalid_input\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
