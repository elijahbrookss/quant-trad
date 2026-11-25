"""Bounded database and relation capacity snapshots for Grafana."""

from __future__ import annotations

import time
import logging
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from typing import Any
from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy.dialects.postgresql import insert as pg_insert

from ._shared import (
    DatabaseCapacitySampleRecord,
    DatabaseRelationCapacitySampleRecord,
    db,
    delete,
    text,
)

_CAPACITY_SAMPLE_LOCK_KEY = 9021067
logger = logging.getLogger(__name__)

# These observations run once per existing capacity bucket, never on a scrape.
# Every source is metadata/index bounded; no Fact or archive object is opened.
_STORAGE_OPERATING_QUERY = text("""
WITH clock AS (SELECT CAST(:sampled_at AS timestamptz) AS at),
policy AS (
 SELECT CASE WHEN jsonb_typeof(policy->'recent_days')='number'
  AND (policy->>'recent_days')::numeric BETWEEN 1 AND 3650
  THEN (policy->>'recent_days')::integer END AS days
 FROM public.portal_storage_policy WHERE id=1 AND revision>0
), cohorts AS MATERIALIZED (
 SELECT f.storage_day,f.expected_rows,f.source_bytes,
  CASE WHEN retired.storage_day IS NOT NULL THEN 'reclaimed' ELSE f.state END AS state
 FROM market.fact_retention_partitions f
 LEFT JOIN market.cold_fact_retired_days retired USING(storage_day)
 ORDER BY f.storage_day LIMIT 2049
), eligible AS MATERIALIZED (
 SELECT c.* FROM (SELECT * FROM cohorts ORDER BY storage_day LIMIT 2048)c CROSS JOIN clock CROSS JOIN policy
 WHERE c.state<>'reclaimed' AND c.storage_day < (clock.at AT TIME ZONE 'UTC')::date-policy.days
), backlog_pages AS MATERIALIZED (
 SELECT m.storage_day,m.row_count FROM market.fact_archive_manifests m
 JOIN eligible e USING(storage_day)
 ORDER BY m.storage_day,m.page_ordinal LIMIT 10001
), remaining AS (
 SELECT e.*,coalesce(a.rows,0) AS archived_rows FROM eligible e LEFT JOIN (
  SELECT storage_day,sum(row_count) AS rows FROM backlog_pages GROUP BY storage_day
 )a USING(storage_day)
), published AS MATERIALIZED (
 SELECT id,row_count,byte_count FROM market.fact_archive_manifests,clock
 WHERE acknowledged_at>=clock.at-interval '5 minutes' AND acknowledged_at<clock.at
 ORDER BY acknowledged_at,id LIMIT 10001
), verification_page AS MATERIALIZED (
 SELECT v.manifest_id,v.manifest_hash,v.verified_at FROM market.fact_archive_verifications v,clock
 WHERE v.verified_at>=clock.at-interval '5 minutes' AND v.verified_at<clock.at
  AND v.verifier_version=:verifier
 ORDER BY v.verified_at,v.manifest_id,v.verifier_version LIMIT 10001
), verified AS (
 SELECT m.row_count,m.byte_count FROM verification_page v
 JOIN market.fact_archive_manifests m ON m.id=v.manifest_id
 WHERE v.manifest_hash=m.manifest_hash AND v.verified_at>=m.acknowledged_at
), retired AS MATERIALIZED (
 SELECT storage_day,expected_rows,reclaimed_bytes FROM market.fact_retention_partitions,clock
 WHERE state='reclaimed' AND reclaimed_at>=clock.at-interval '5 minutes' AND reclaimed_at<clock.at
 ORDER BY reclaimed_at,storage_day LIMIT 2049
)
SELECT
 EXISTS(SELECT FROM policy WHERE days IS NOT NULL) AS policy_available,
 (SELECT count(*)>2048 FROM cohorts) AS cohorts_truncated,
 (SELECT count(*)>10000 FROM backlog_pages) AS backlog_truncated,
 (SELECT count(*)>10000 FROM verification_page) AS verified_source_truncated,
 (SELECT count(*) FROM remaining) AS eligible_cohorts,
 (SELECT count(*) FROM remaining WHERE expected_rows IS NULL OR source_bytes IS NULL OR state='open') AS unknown_cohorts,
 (SELECT coalesce(sum(greatest(expected_rows-archived_rows,0)) FILTER (WHERE expected_rows IS NOT NULL),0) FROM remaining) AS known_backlog_rows,
 (SELECT EXISTS(SELECT FROM remaining WHERE expected_rows IS NOT NULL AND archived_rows>expected_rows)) AS backlog_count_inconsistent,
 (SELECT coalesce(sum(source_bytes),0) FROM remaining) AS backlog_source_bytes,
 (SELECT min(storage_day) FROM remaining) AS oldest_eligible_day,
 (SELECT count(*) FROM published) AS published_pages_window,
 (SELECT coalesce(sum(row_count),0) FROM published) AS published_rows_window,
 (SELECT coalesce(sum(byte_count),0) FROM published) AS published_bytes_window,
 (SELECT count(*) FROM verified) AS verified_pages_window,
 (SELECT coalesce(sum(row_count),0) FROM verified) AS verified_rows_window,
 (SELECT coalesce(sum(byte_count),0) FROM verified) AS verified_bytes_window,
 (SELECT count(*) FROM retired) AS retired_cohorts_window,
 (SELECT CASE WHEN count(*) FILTER (WHERE expected_rows IS NULL)>0 THEN NULL ELSE coalesce(sum(expected_rows),0) END FROM retired) AS retired_rows_window,
 (SELECT coalesce(sum(reclaimed_bytes),0) FROM retired) AS retired_relation_bytes_window,
 (SELECT extract(epoch FROM acknowledged_at) FROM market.fact_archive_manifests ORDER BY acknowledged_at DESC,id DESC LIMIT 1) AS last_publication_unixtime,
 (SELECT extract(epoch FROM v.verified_at) FROM market.fact_archive_verifications v
  JOIN market.fact_archive_manifests m ON m.id=v.manifest_id
  WHERE v.verifier_version=:verifier AND v.manifest_hash=m.manifest_hash AND v.verified_at>=m.acknowledged_at
  ORDER BY v.verified_at DESC,v.manifest_id DESC,v.verifier_version DESC LIMIT 1) AS last_verification_unixtime,
 (SELECT extract(epoch FROM reclaimed_at) FROM market.fact_retention_partitions WHERE state='reclaimed' ORDER BY reclaimed_at DESC,storage_day DESC LIMIT 1) AS last_retirement_unixtime,
 (SELECT coalesce(sum(size),0) FROM pg_ls_waldir()) AS wal_resident_bytes,
 (SELECT failed_count FROM pg_stat_archiver) AS archiver_failed_total,
 (SELECT extract(epoch FROM last_archived_time) FROM pg_stat_archiver) AS archiver_last_success_unixtime
""")

_RELATION_ALLOCATION_QUERY = text("""
WITH page AS MATERIALIZED (
 SELECT c.oid,c.relkind,coalesce(nullif(c.reltablespace,0),d.dattablespace) AS tablespace_oid,
  coalesce(owner.relname,base.relname) AS source_name,
  CASE WHEN owner.oid IS NOT NULL THEN 'toast' WHEN c.relkind='i' THEN 'index' ELSE 'heap' END AS allocation_kind
 FROM pg_class c
 LEFT JOIN pg_index i ON i.indexrelid=c.oid
 JOIN pg_class base ON base.oid=coalesce(i.indrelid,c.oid)
 LEFT JOIN pg_class owner ON owner.reltoastrelid=base.oid
 JOIN pg_namespace n ON n.oid=coalesce(owner.relnamespace,base.relnamespace)
 CROSS JOIN pg_database d
 WHERE d.datname=current_database() AND c.relkind IN ('r','i','m','t')
  AND n.nspname IN ('market','public','observability_metrics')
 ORDER BY c.oid LIMIT 4097
), sized AS (
 SELECT p.*,pg_relation_size(p.oid)::bigint AS allocated_bytes,
  CASE WHEN source_name LIKE 'fact_hot_payloads%' THEN 'hot_payload'
   WHEN source_name LIKE 'fact_versions%' OR source_name IN ('fact_identities','fact_header_series_days') THEN 'canonical_hot_metadata'
   WHEN source_name LIKE 'cold_fact_%' OR source_name='cold_book_session_facts' THEN 'canonical_cold_lookup'
   WHEN source_name='raw_archive_record_mappings' THEN 'raw_custody_lookup'
   ELSE 'other_database' END AS family FROM (SELECT * FROM page ORDER BY oid LIMIT 4096)p
)
SELECT family,t.spcname AS tablespace,allocation_kind,sum(allocated_bytes)::bigint AS allocated_bytes,
 count(*) AS relation_count,(SELECT count(*)>4096 FROM page) AS truncated
FROM sized s JOIN pg_tablespace t ON t.oid=s.tablespace_oid
GROUP BY family,t.spcname,allocation_kind ORDER BY family,t.spcname,allocation_kind
""")


def sample_storage_operating_metrics(*, sampled_at, database=db):
    """One capped read-only observation; an unavailable sample is never zero."""
    from .fact_archival import FACT_ARCHIVE_VERIFIER_VERSION
    sample_time = sampled_at.replace(tzinfo=timezone.utc) if sampled_at.tzinfo is None else sampled_at.astimezone(timezone.utc)
    envelope = {"schema_version": "qt.storage_operating_snapshot.v1", "sampled_at": sample_time.isoformat(),
        "window_seconds": 300, "source_bytes_basis": "sealed_partition_catalog_at_seal",
        "published_bytes_basis": "acknowledged_manifest_object_bytes_not_filesystem_delta",
        "verified_bytes_basis": "current_hash_bound_manifest_verification",
        "reclaim_basis": "recorded_retired_relation_bytes_not_filesystem_delta",
        "admitted_revisions_state": "unavailable", "admitted_revisions_reason": "no_universal_post_commit_counter",
        "research_cache_allocation_state": "unavailable", "hdd_recovery_inventory_state": "unavailable",
        "cohort_limit": 2048, "manifest_limit": 10000}
    started = time.monotonic()
    deadline = started + 5
    operating = None
    try:
        with database.session() as session:
            session.execute(text("SET TRANSACTION READ ONLY"))
            session.execute(text("SET LOCAL lock_timeout='250ms'"))
            def query(statement, params=None):
                remaining = int((deadline-time.monotonic())*1000)
                if remaining <= 0:
                    raise TimeoutError("storage_operating_sample_deadline")
                session.execute(text("SELECT set_config('statement_timeout',:timeout,true)"), {"timeout": str(remaining)})
                return session.execute(statement, params or {})
            row = dict(query(_STORAGE_OPERATING_QUERY, {"sampled_at": sample_time,
                "verifier": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().one())
            operating = _storage_operating_payload(envelope, row, sample_time)
            allocation = [dict(r) for r in query(_RELATION_ALLOCATION_QUERY).mappings()]
        operating["sample_query_ms"] = (time.monotonic()-started)*1000
        return {"operating": operating,
                "allocation": [{"schema_version": "qt.storage_relation_allocation.v1",
                    "sampled_at": sample_time.isoformat(), "basis": "postgres_main_fork_allocation_not_filesystem_delta",
                    "namespace_scope": "market_public_observability_metrics",
                    **r, "state": "partial" if r["truncated"] else "ready", "partial": bool(r["truncated"])} for r in allocation]}
    except (SQLAlchemyError, TimeoutError, OSError) as exc:
        logger.warning("storage_operating_sample_unavailable | error_type=%s", type(exc).__name__)
        if operating is None:
            operating = {**envelope, "state": "unavailable", "partial": True,
                "reason": "bounded_metadata_query_unavailable"}
        operating.update(allocation_state="unavailable", sample_query_ms=(time.monotonic()-started)*1000)
        return {"operating": operating, "allocation": []}


def _storage_operating_payload(envelope, row, sample_time):
    clipped = bool(row["cohorts_truncated"] or row["backlog_truncated"])
    inconsistent = row["backlog_count_inconsistent"]
    incomplete = clipped or bool(row["unknown_cohorts"]) or not row["policy_available"] or inconsistent
    value = {**envelope, **row, "state": "unavailable" if not row["policy_available"] or inconsistent else "partial" if incomplete else "ready",
        "partial": incomplete, "reason": "applied_policy_unavailable" if not row["policy_available"] else "retention_count_mismatch" if inconsistent else "source_limit" if clipped else "unsealed_cohort_size_unknown" if row["unknown_cohorts"] else None,
        "backlog_rows": None if incomplete else int(row["known_backlog_rows"])}
    value["known_backlog_source_bytes"] = row["backlog_source_bytes"]
    if incomplete:
        value["backlog_source_bytes"] = None
    if clipped or inconsistent or not row["policy_available"]:
        value["known_backlog_rows"] = value["known_backlog_source_bytes"] = None
    if row["retired_rows_window"] is None:
        value.update(state="partial", partial=True, reason="retired_rows_unknown")
    for prefix, limit, count in (("published", 10000, "pages"), ("verified", 10000, "pages"), ("retired", 2048, "cohorts")):
        if row[prefix+"_"+count+"_window"] > limit or (prefix == "verified" and row["verified_source_truncated"]):
            value.update(state="partial", partial=True, reason="source_limit")
            for key in list(value):
                if key.startswith(prefix+"_") and key.endswith("_window"):
                    value[key] = None
    oldest = value["oldest_eligible_day"]
    value["oldest_eligible_day"] = oldest.isoformat() if oldest else None
    value["oldest_eligible_age_seconds"] = (sample_time.date()-oldest).days*86400 if oldest else None
    for key in list(value):
        if key.endswith("_unixtime") and value[key] is not None:
            value[key] = float(value[key])
        elif isinstance(value[key], Decimal):
            value[key] = int(value[key])
    return value

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
        "storage_metrics": sample_storage_operating_metrics(sampled_at=sample_time)
            if database_inserted or relation_inserted else None,
    }


__all__ = ["record_database_capacity_snapshot"]

