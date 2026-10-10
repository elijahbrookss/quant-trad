#!/usr/bin/env python3
"""Read-only inventory for a future tiered-v1 to dated-header-v2 cutover.

PG_DSN is the only connection input. This command never loads application
settings, provisions schema, starts capture, copies rows, or approves a cutover.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

SOURCE = "market.fact_versions"
SCHEMA_VERSION = "qt.fact_header_cutover_preflight.v1"
MAX_CATALOG_ROWS = 4096
_REQUIRED_HEADER_COLUMNS = frozenset({
    "id", "storage_day", "series_id", "observation_key", "revision",
    "market_commit_seq", "observation_time", "known_at", "row_hash",
})
_MIGRATION_GATES = (
    "online_cutover_executor_not_implemented",
    "source_schema_and_guard_contract_must_be_verified",
    "capacity_including_retained_source_identity_indexes_wal_and_scratch_must_be_measured",
    "capture_backfill_and_verification_must_be_rehearsed_within_24_hours",
    "writer_fence_foreign_keys_views_and_post_resume_rollback_must_be_rehearsed",
    "full_horizon_query_performance_and_restore_must_be_proven",
)


def _rows(conn, sql, parameters=None, *, label):
    rows = [dict(row) for row in conn.execute(text(sql), parameters or {}).mappings()]
    if len(rows) > MAX_CATALOG_ROWS:
        raise RuntimeError(f"fact_header_preflight_catalog_limit: {label}")
    return rows


def _relation(conn, relation):
    row = conn.execute(text("""
        SELECT c.oid::bigint AS oid, c.relkind AS kind, c.relpersistence AS persistence,
               CASE WHEN c.reltuples < 0 THEN NULL ELSE c.reltuples::bigint END AS estimated_rows,
               pg_table_size(c.oid) AS table_bytes,
               pg_indexes_size(c.oid) AS index_bytes,
               pg_total_relation_size(c.oid) AS total_bytes,
               pg_get_partkeydef(c.oid) AS partition_key,
               COALESCE(t.spcname, 'database_default') AS tablespace
        FROM pg_class c LEFT JOIN pg_tablespace t ON t.oid=c.reltablespace
        WHERE c.oid=to_regclass(:relation)
    """), {"relation": relation}).mappings().one_or_none()
    return None if row is None else {"relation": relation, **dict(row)}


def _inspect(conn):
    context = dict(conn.execute(text("""
        SELECT clock_timestamp()::text AS observed_at,
               current_setting('server_version_num')::integer AS server_version_num,
               current_setting('transaction_read_only') AS transaction_read_only
    """)).mappings().one())
    if context["transaction_read_only"] != "on":
        raise RuntimeError("fact_header_preflight_requires_read_only_transaction")
    source = _relation(conn, SOURCE)
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "inspection_only",
        "migration_ready": False,
        "changes_performed": False,
        "context": context,
        "source": source,
        "source_findings": [],
        "required_gates": list(_MIGRATION_GATES),
        "estimate_note": "Row counts are planner estimates; file sizes are observations, not a capacity or duration proof.",
    }
    findings = report["source_findings"]
    if context["server_version_num"] // 10000 != 15:
        findings.append("postgresql_major_version_not_qualified")
    if source is None:
        findings.append("source_relation_missing")
        return report
    source["size_scope"] = "relation_only_excluding_partition_children"
    if source["persistence"] != "p":
        findings.append("source_is_not_durable_logged_storage")
    if source["kind"] != "r":
        findings.append("source_is_not_unpartitioned_tiered_v1")

    columns = _rows(conn, """
        SELECT a.attname AS name, format_type(a.atttypid,a.atttypmod) AS type,
               a.attnotnull AS not_null, pg_get_expr(d.adbin,d.adrelid) AS default_expression
        FROM pg_attribute a LEFT JOIN pg_attrdef d
          ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=to_regclass(:source) AND a.attnum>0 AND NOT a.attisdropped
        ORDER BY a.attnum LIMIT 4097
    """, {"source": SOURCE}, label="columns")
    report["columns"] = columns
    names = {column["name"] for column in columns}
    if not _REQUIRED_HEADER_COLUMNS <= names or names & {"payload", "provenance", "quality"}:
        findings.append("source_does_not_have_tiered_header_shape")

    constraints = _rows(conn, """
        SELECT c.conname AS name, c.contype AS kind, c.convalidated AS validated,
               c.condeferrable AS deferrable, pg_get_constraintdef(c.oid) AS definition,
               ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(num,pos)
                     JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.num ORDER BY k.pos) AS columns
        FROM pg_constraint c WHERE c.conrelid=to_regclass(:source)
        ORDER BY c.conname LIMIT 4097
    """, {"source": SOURCE}, label="source_constraints")
    report["source_constraints"] = constraints
    if not any(row["kind"] == "p" and row["validated"] and not row["deferrable"] and row["columns"] == ["id"]
               for row in constraints):
        findings.append("source_global_id_primary_key_missing")
    if not any(row["kind"] == "u" and row["validated"] and not row["deferrable"]
               and row["columns"] == ["series_id", "observation_key", "revision"]
               for row in constraints):
        findings.append("source_global_revision_uniqueness_missing")

    indexes = _rows(conn, """
        SELECT c.relname AS name, i.indisvalid AS valid, i.indisready AS ready,
               i.indisunique AS unique, am.amname AS method,
               i.indpred IS NULL AND i.indexprs IS NULL AS plain_unfiltered,
               ARRAY(SELECT pg_get_indexdef(i.indexrelid,p,true)
                     FROM generate_series(1,i.indnkeyatts) p ORDER BY p) AS keys,
               i.indoption::text AS options,
               pg_get_indexdef(i.indexrelid) AS definition
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_am am ON am.oid=c.relam
        WHERE i.indrelid=to_regclass(:source) ORDER BY c.relname LIMIT 4097
    """, {"source": SOURCE}, label="source_indexes")
    report["source_indexes"] = indexes
    if not any(row["name"] == "ix_market_fact_storage_page" and row["valid"] and row["ready"]
               and not row["unique"] and row["method"] == "btree" and row["plain_unfiltered"]
               and row["keys"] == ["storage_day", "market_commit_seq", "id"]
               and row["options"] == "0 0 0" for row in indexes):
        findings.append("bounded_source_copy_index_missing_or_incompatible")

    report["source_triggers"] = _rows(conn, """
        SELECT t.tgname AS name, t.tgenabled AS enabled,
               pg_get_triggerdef(t.oid) AS definition,
               n.nspname || '.' || p.proname AS function,
               md5(pg_get_functiondef(p.oid)) AS function_definition_md5
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE t.tgrelid=to_regclass(:source) AND NOT t.tgisinternal
        ORDER BY t.tgname LIMIT 4097
    """, {"source": SOURCE}, label="source_triggers")
    required_triggers = {
        "trg_assert_fact_version_valid", "trg_reject_mutation_fact_versions",
        "trg_require_fact_hot_payload",
    }
    if not required_triggers <= {row["name"] for row in report["source_triggers"]}:
        findings.append("source_required_triggers_missing")
    if any(row["enabled"] not in {"O", "A"} for row in report["source_triggers"]):
        findings.append("source_trigger_disabled_or_replica_only")

    incoming = _rows(conn, """
        SELECT format('%I.%I',n.nspname,r.relname) AS relation, c.conname AS name,
               c.convalidated AS validated, c.conparentid<>0 AS inherited,
               pg_get_constraintdef(c.oid) AS definition,
               ARRAY(SELECT a.attname::text FROM unnest(c.confkey) WITH ORDINALITY k(num,pos)
                     JOIN pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.num ORDER BY k.pos) AS referenced_columns
        FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid
        JOIN pg_namespace n ON n.oid=r.relnamespace
        WHERE c.contype='f' AND c.confrelid=to_regclass(:source)
        ORDER BY n.nspname,r.relname,c.conname LIMIT 4097
    """, {"source": SOURCE}, label="incoming_foreign_keys")
    known_owners = {
        "market.fact_hot_payloads", "market.fact_archive_material_aliases",
        "market.fact_archive_canonical_dependencies",
    }
    payload_children = _rows(conn, """
        SELECT format('%I.%I',n.nspname,c.relname) AS relation
        FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE i.inhparent=to_regclass('market.fact_hot_payloads')
        ORDER BY n.nspname,c.relname LIMIT 4097
    """, label="payload_children")
    known_owners.update(row["relation"] for row in payload_children)
    for row in incoming:
        row["expected_consumer"] = row["relation"] in known_owners and row["referenced_columns"] == ["id"]
    report["incoming_foreign_keys"] = incoming
    if any(not row["expected_consumer"] for row in incoming):
        findings.append("unhandled_incoming_foreign_key")
    if any(not row["validated"] for row in incoming):
        findings.append("incoming_foreign_key_not_validated")

    views = _rows(conn, """
        SELECT DISTINCT format('%I.%I',n.nspname,c.relname) AS relation, c.relkind AS kind
        FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid
        JOIN pg_class c ON c.oid=r.ev_class JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE d.classid='pg_rewrite'::regclass AND d.refclassid='pg_class'::regclass
          AND d.refobjid=to_regclass(:source) AND c.relkind IN ('v','m')
        ORDER BY relation LIMIT 4097
    """, {"source": SOURCE}, label="dependent_views")
    report["dependent_views"] = views
    if any(row["relation"] != "market.fact_rows" for row in views):
        findings.append("unhandled_dependent_view")

    state = _relation(conn, "market.fact_storage_state")
    certificates = [] if state is None else _rows(conn, """
        SELECT layout_version,state FROM market.fact_storage_state ORDER BY layout_version LIMIT 4097
    """, label="layout_certificates")
    report["layout_certificates"] = certificates
    if not any(row == {"layout_version": "market.fact_storage_tiers.v1", "state": "ready"}
               for row in certificates):
        findings.append("tiered_v1_ready_certificate_missing")

    report["existing_v2_objects"] = [
        relation for relation in ("market.fact_identities", "market.fact_header_partitions")
        if _relation(conn, relation) is not None
    ]
    if report["existing_v2_objects"]:
        findings.append("v2_identity_objects_already_exist")
    report["retained_legacy_source"] = _relation(conn, "qt_fact_storage_cutover_v1.fact_versions")
    # Fingerprint catalog structure for comparison only; it is not authorization
    # and cannot replace revalidation under the eventual writer fence.
    structural = {key: report[key] for key in (
        "columns", "source_constraints", "source_indexes", "source_triggers",
        "incoming_foreign_keys", "dependent_views", "layout_certificates", "existing_v2_objects",
    )}
    report["catalog_fingerprint"] = hashlib.sha256(
        json.dumps(structural, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return report


def inspect_preflight(engine, *, statement_timeout_seconds=15):
    if engine.dialect.name != "postgresql":
        raise ValueError("fact_header_preflight_requires_postgresql")
    if type(statement_timeout_seconds) is not int or not 1 <= statement_timeout_seconds <= 60:
        raise ValueError("fact_header_preflight_timeout_out_of_range")
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                         {"timeout": str(statement_timeout_seconds * 1000)})
            conn.execute(text("SET LOCAL lock_timeout='2s'"))
            return _inspect(conn)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--statement-timeout-seconds", type=int, default=15)
    args = parser.parse_args(argv)
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        parser.error("PG_DSN is required; dotenv and application settings are not loaded")
    if make_url(dsn).get_backend_name() != "postgresql":
        parser.error("PG_DSN must select PostgreSQL")
    engine = create_engine(dsn, future=True, connect_args={"connect_timeout": 5})
    try:
        print(json.dumps(inspect_preflight(
            engine, statement_timeout_seconds=args.statement_timeout_seconds,
        ), indent=2, sort_keys=True))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
