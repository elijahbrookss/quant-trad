"""Bounded database and relation capacity snapshots for Grafana."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert

from ._shared import (
    DatabaseCapacitySampleRecord,
    DatabaseRelationCapacitySampleRecord,
    db,
    delete,
    text,
)

_CAPACITY_SAMPLE_LOCK_KEY = 9021067

_DATABASE_CAPACITY_QUERY = text(
    """
    SELECT
        pg_database_size(current_database())::bigint AS database_size_bytes,
        current_setting('max_connections')::int AS max_connections,
        (
            SELECT count(*)::int
            FROM pg_stat_activity
            WHERE datname = current_database()
        ) AS connections_total,
        (
            SELECT count(*)::int
            FROM pg_stat_activity
            WHERE datname = current_database() AND state = 'active'
        ) AS connections_active,
        (
            SELECT count(*)::int
            FROM pg_stat_activity
            WHERE datname = current_database() AND state = 'idle'
        ) AS connections_idle,
        d.xact_commit::bigint AS xact_commit,
        d.xact_rollback::bigint AS xact_rollback,
        d.blks_read::bigint AS blocks_read,
        d.blks_hit::bigint AS blocks_hit,
        d.tup_returned::bigint AS tuples_returned,
        d.tup_fetched::bigint AS tuples_fetched,
        d.tup_inserted::bigint AS tuples_inserted,
        d.tup_updated::bigint AS tuples_updated,
        d.tup_deleted::bigint AS tuples_deleted,
        d.temp_files::bigint AS temp_files,
        d.temp_bytes::bigint AS temp_bytes,
        d.deadlocks::bigint AS deadlocks,
        d.blk_read_time::double precision AS block_read_time_ms,
        d.blk_write_time::double precision AS block_write_time_ms,
        COALESCE(w.wal_bytes, 0)::bigint AS wal_bytes
    FROM pg_stat_database AS d
    CROSS JOIN pg_stat_wal AS w
    WHERE d.datname = current_database()
    """
)

_RELATION_CAPACITY_QUERY = text(
    """
    WITH chunk_stats AS (
        SELECT
            chunks.hypertable_schema,
            chunks.hypertable_name,
            COALESCE(sum(stats.n_dead_tup), 0)::bigint AS dead_rows,
            COALESCE(sum(stats.n_tup_ins), 0)::bigint AS inserts_total,
            COALESCE(sum(stats.n_tup_upd), 0)::bigint AS updates_total,
            COALESCE(sum(stats.n_tup_del), 0)::bigint AS deletes_total,
            COALESCE(sum(stats.seq_scan), 0)::bigint AS sequential_scans_total,
            COALESCE(sum(stats.idx_scan), 0)::bigint AS index_scans_total
        FROM timescaledb_information.chunks AS chunks
        LEFT JOIN pg_stat_user_tables AS stats
          ON stats.schemaname = chunks.chunk_schema
         AND stats.relname = chunks.chunk_name
        GROUP BY chunks.hypertable_schema, chunks.hypertable_name
    ),
    hypertable_sizes AS (
        SELECT
            hypertables.hypertable_schema,
            hypertables.hypertable_name,
            sizes.table_bytes,
            sizes.index_bytes,
            sizes.toast_bytes,
            sizes.total_bytes
        FROM timescaledb_information.hypertables AS hypertables
        CROSS JOIN LATERAL hypertable_detailed_size(
            to_regclass(
                format(
                    '%I.%I',
                    hypertables.hypertable_schema,
                    hypertables.hypertable_name
                )
            )
        ) AS sizes
    )
    SELECT
        stats.schemaname AS schema_name,
        stats.relname AS relation_name,
        CASE
            WHEN hypertables.hypertable_name IS NULL THEN 'table'
            ELSE 'hypertable'
        END AS relation_kind,
        CASE
            WHEN hypertables.hypertable_name IS NULL
                THEN pg_relation_size(stats.relid)
            ELSE COALESCE(hypertable_sizes.table_bytes, 0)
        END::bigint AS table_bytes,
        CASE
            WHEN hypertables.hypertable_name IS NULL
                THEN pg_indexes_size(stats.relid)
            ELSE COALESCE(hypertable_sizes.index_bytes, 0)
        END::bigint AS index_bytes,
        CASE
            WHEN hypertables.hypertable_name IS NULL
                THEN COALESCE(pg_total_relation_size(classes.reltoastrelid), 0)
            ELSE COALESCE(hypertable_sizes.toast_bytes, 0)
        END::bigint AS toast_bytes,
        CASE
            WHEN hypertables.hypertable_name IS NULL
                THEN pg_total_relation_size(stats.relid)
            ELSE COALESCE(hypertable_sizes.total_bytes, 0)
        END::bigint AS total_bytes,
        COALESCE(approximate_row_count(stats.relid), 0)::bigint AS estimated_live_rows,
        CASE
            WHEN hypertables.hypertable_name IS NULL
                THEN COALESCE(stats.n_dead_tup, 0)
            ELSE COALESCE(chunk_stats.dead_rows, 0)
        END::bigint AS estimated_dead_rows,
        CASE
            WHEN hypertables.hypertable_name IS NULL
                THEN COALESCE(stats.n_tup_ins, 0)
            ELSE COALESCE(chunk_stats.inserts_total, 0)
        END::bigint AS inserts_total,
        CASE
            WHEN hypertables.hypertable_name IS NULL
                THEN COALESCE(stats.n_tup_upd, 0)
            ELSE COALESCE(chunk_stats.updates_total, 0)
        END::bigint AS updates_total,
        CASE
            WHEN hypertables.hypertable_name IS NULL
                THEN COALESCE(stats.n_tup_del, 0)
            ELSE COALESCE(chunk_stats.deletes_total, 0)
        END::bigint AS deletes_total,
        CASE
            WHEN hypertables.hypertable_name IS NULL
                THEN COALESCE(stats.seq_scan, 0)
            ELSE COALESCE(chunk_stats.sequential_scans_total, 0)
        END::bigint AS sequential_scans_total,
        CASE
            WHEN hypertables.hypertable_name IS NULL
                THEN COALESCE(stats.idx_scan, 0)
            ELSE COALESCE(chunk_stats.index_scans_total, 0)
        END::bigint AS index_scans_total
    FROM pg_stat_user_tables AS stats
    JOIN pg_class AS classes
      ON classes.oid = stats.relid
    LEFT JOIN timescaledb_information.hypertables AS hypertables
      ON hypertables.hypertable_schema = stats.schemaname
     AND hypertables.hypertable_name = stats.relname
    LEFT JOIN chunk_stats
      ON chunk_stats.hypertable_schema = stats.schemaname
     AND chunk_stats.hypertable_name = stats.relname
    LEFT JOIN hypertable_sizes
      ON hypertable_sizes.hypertable_schema = stats.schemaname
     AND hypertable_sizes.hypertable_name = stats.relname
    WHERE stats.schemaname NOT IN (
        '_timescaledb_cache',
        '_timescaledb_catalog',
        '_timescaledb_config',
        '_timescaledb_internal',
        'information_schema',
        'pg_catalog'
    )
    ORDER BY stats.schemaname, stats.relname
    """
)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def record_database_capacity_snapshot(
    *,
    sampled_at: datetime,
    retention_days: int,
) -> dict[str, Any]:
    """Record one leader-fenced, bounded database capacity snapshot."""

    sample_time = _naive_utc(sampled_at)
    retention_before = sample_time - timedelta(days=max(int(retention_days), 1))
    started = time.perf_counter()

    with db.session() as session:
        owns_sample = bool(
            session.execute(
                text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
                {"lock_key": _CAPACITY_SAMPLE_LOCK_KEY},
            ).scalar_one()
        )
        if not owns_sample:
            return {
                "sampled": False,
                "reason": "sampler_lock_busy",
                "sampled_at": sample_time,
            }

        database_row = dict(
            session.execute(_DATABASE_CAPACITY_QUERY).mappings().one()
        )
        relation_rows = [
            dict(row)
            for row in session.execute(_RELATION_CAPACITY_QUERY).mappings().all()
        ]
        sample_query_ms = max((time.perf_counter() - started) * 1000.0, 0.0)

        deleted_database = int(
            session.execute(
                delete(DatabaseCapacitySampleRecord).where(
                    DatabaseCapacitySampleRecord.sampled_at < retention_before
                )
            ).rowcount
            or 0
        )
        deleted_relations = int(
            session.execute(
                delete(DatabaseRelationCapacitySampleRecord).where(
                    DatabaseRelationCapacitySampleRecord.sampled_at < retention_before
                )
            ).rowcount
            or 0
        )

        database_payload = {
            **database_row,
            "sampled_at": sample_time,
            "relation_count": len(relation_rows),
            "sample_query_ms": sample_query_ms,
            "created_at": sample_time,
        }
        database_insert = pg_insert(DatabaseCapacitySampleRecord).values(
            database_payload
        )
        database_insert = database_insert.on_conflict_do_nothing(
            index_elements=["sampled_at"]
        )
        database_inserted = int(session.execute(database_insert).rowcount or 0)

        relation_inserted = 0
        if relation_rows:
            relation_payloads = [
                {
                    **row,
                    "sampled_at": sample_time,
                    "created_at": sample_time,
                }
                for row in relation_rows
            ]
            relation_insert = pg_insert(
                DatabaseRelationCapacitySampleRecord
            ).values(relation_payloads)
            relation_insert = relation_insert.on_conflict_do_nothing(
                index_elements=["sampled_at", "schema_name", "relation_name"]
            )
            relation_inserted = int(session.execute(relation_insert).rowcount or 0)

    return {
        "sampled": bool(database_inserted or relation_inserted),
        "sampled_at": sample_time,
        "database_size_bytes": int(database_row["database_size_bytes"]),
        "relation_count": len(relation_rows),
        "database_rows_inserted": database_inserted,
        "relation_rows_inserted": relation_inserted,
        "retention_rows_deleted": deleted_database + deleted_relations,
        "sample_query_ms": sample_query_ms,
    }


__all__ = ["record_database_capacity_snapshot"]

