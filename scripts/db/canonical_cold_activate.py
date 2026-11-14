"""Transactional cold authority handoff in the maintenance database session.

The operation consumes an operator-bound exact cutover record. A release
launcher separately owns collector shutdown and restart, paired recovery,
application and provider fences, and the post-write rollback policy.
"""
from __future__ import annotations

import json
from datetime import date, datetime
import re

from sqlalchemy import text

from core.storage_mounts import require_configured_archive_mount
from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive_verification import ArchiveVerificationBatch
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_CUSTODY_TABLE_NAMES, COLD_HISTORY_LAYOUT_VERSION, COLD_TABLE_NAMES
from portal.backend.db.cold_retirement_schema import prepare_cold_dependency_bindings
from portal.backend.db.fact_identity_schema import _partition_bound
from portal.backend.db.fact_storage_schema import (
    FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract, fact_partition_name,
)
from portal.backend.service.storage.repos.cold_retirement import (
    _assert_partition, drop_verified_header, lock_header_namespace,
)
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db.canonical_cold_audit import _ensure_target, _plan
from scripts.db.canonical_cold_custody_placement import assert_custody_placement
from scripts.db.canonical_cold_database_fence import DatabaseFence
from scripts.db.canonical_cold_identity_rebuild import swap_identity_suffix
from scripts.db.canonical_cold_load import TargetDatabase
from scripts.db.canonical_cold_migration import PREPARED_CATALOG_TABLES, PostgresMigrationSource
from scripts.db.canonical_cold_receipts import manifest_inventory_hash, validate_audit_receipts

ACTIVATION_VERSION = "qt.canonical_cold_activate.v3"
_PHYSICAL_GATES = {
    "independent_recovery", "provider_boundary", "replay_equivalence",
    "legacy_reference_review", "physical_capacity", "rollback_rehearsal",
    "representative_performance", "candidate_release", "custody_catalog_placement",
}


def _valid_digest(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def validate_execution_record(execution, *, expected_execution_hash):
    """Pin the exact approved record separately from component audit receipts."""
    if not _valid_digest(expected_execution_hash) or archive_evidence_hash(execution) != expected_execution_hash:
        raise ValueError("canonical_cold_activation_execution_hash_mismatch")
    if (
        not isinstance(execution, dict) or execution.get("schema") != ACTIVATION_VERSION
        or execution.keys() != {
            "schema", "approved_package_hash", "evidence", "source_database", "target_database",
            "audit_set_hash", "loaded_inventory_hash", "source_commit_seq", "payload_children",
            "custody_target_id", "custody_tablespace_oid", "custody_relation_oids",
        }
        or not isinstance(execution.get("evidence"), dict)
        or not _valid_digest(execution.get("approved_package_hash"))
        or set(execution["evidence"]) != _PHYSICAL_GATES
        or not all(_valid_digest(digest) for digest in execution["evidence"].values())
        or not _valid_digest(execution.get("audit_set_hash"))
        or not _valid_digest(execution.get("loaded_inventory_hash"))
        or type(execution.get("source_commit_seq")) is not int
        or execution["source_commit_seq"] <= 0
        or not isinstance(execution.get("payload_children"), dict)
        or not isinstance(execution.get("target_database"), dict)
        or not isinstance(execution.get("source_database"), dict)
        or type(execution.get("custody_target_id")) is not str
        or not re.fullmatch(r"[a-z][a-z0-9_-]{0,47}", execution["custody_target_id"])
        or type(execution.get("custody_tablespace_oid")) is not int
        or not 0 < execution["custody_tablespace_oid"] <= 4_294_967_295
        or not isinstance(execution.get("custody_relation_oids"), dict)
        or execution["custody_relation_oids"].keys() != set(COLD_CUSTODY_TABLE_NAMES)
        or not all(type(oid) is int and 0 < oid <= 4_294_967_295
                   for oid in execution["custody_relation_oids"].values())
    ):
        raise ValueError("canonical_cold_activation_incomplete_execution_record")


def _inventory_hash(conn, *, objects=None):
    rows = conn.execute(text("""
        SELECT id,manifest_hash,row_count,object_key,object_sha256,byte_count,staged_storage_day
        FROM market.cold_fact_pages ORDER BY id COLLATE "C"
    """).execution_options(yield_per=256))

    def verified_rows():
        for identity, digest, count, key, sha, size, staged in rows:
            if staged is not None:
                raise RuntimeError(f"canonical_cold_activation_unexpected_staged_page: id={identity}")
            if objects is not None:
                objects.verify(key, sha, expected_bytes=size)
            yield identity, digest, count

    try:
        return manifest_inventory_hash(verified_rows())
    finally:
        rows.close()


def verify_mounted_candidate(conn, *, audit, store, limits, check_budget=None):
    """Verify mounted bytes before the critical transaction; keep inode witnesses."""
    require_configured_archive_mount(store.root, require_writable=True)
    objects = ArchiveVerificationBatch(store, limits=limits, check_budget=check_budget)
    if _inventory_hash(conn, objects=objects) != audit["loaded_inventory_hash"]:
        raise RuntimeError("canonical_cold_activation_loaded_inventory_hash_mismatch")
    objects.assert_unchanged()
    return objects


def _assert_source_authority_inventory(conn, cutoff, audit):
    """Refuse changed exact published generations before any source DDL."""
    # A concurrent archive writer may stage a page elsewhere but cannot
    # mutate the published scope once the lifecycle/SQL writers are fenced.
    tables = {"fact_archive_active_generations", "fact_archive_manifests",
              "fact_archive_preparations", "fact_archive_verification_generations",
              "fact_archive_verifications", "fact_archive_catalog_batches", *PREPARED_CATALOG_TABLES.values()}
    for table in sorted(tables):
        conn.execute(text(f'LOCK TABLE market."{table}" IN SHARE MODE NOWAIT'))
    rows = conn.execute(text("""
        SELECT * FROM market.fact_archive_manifests
        WHERE storage_day<:cutoff ORDER BY id COLLATE "C"
    """).execution_options(yield_per=128), {"cutoff": cutoff}).mappings()

    def witnesses():
        for page in rows:
            authority = PostgresMigrationSource.source_authority(conn, page)
            yield page["id"], page["storage_day"].isoformat(), archive_evidence_hash(authority)

    try:
        actual = manifest_inventory_hash(witnesses())
    finally:
        rows.close()
    if actual != audit["source_authority_inventory_hash"]:
        raise RuntimeError("canonical_cold_activation_source_authority_inventory_changed")


def _retention_sources(conn, cutoff, audit):
    sources = conn.execute(text("""
        SELECT storage_day,state,sealed_at,expected_rows FROM market.fact_retention_partitions
        WHERE storage_day<:cutoff ORDER BY storage_day
    """), {"cutoff": cutoff}).mappings()
    found = {}
    for row in sources:
        day = row["storage_day"].isoformat()
        seen = audit["days"].get(day)
        if seen is None:
            raise RuntimeError(f"canonical_cold_activation_unaudited_retention_day: day={day}")
        sealed = datetime.fromisoformat(seen["source_sealed_at"]) if seen["source_sealed_at"] else None
        actual = row["state"], row["sealed_at"], (row["expected_rows"] or 0)
        if actual != (seen["source_retention_state"], sealed, seen["expected_rows"]) or row["state"] == "open":
            raise RuntimeError(f"canonical_cold_activation_retention_changed: day={day}")
        found[day] = row["state"]
    if set(found) != {key for key, item in audit["days"].items() if item["source_retention_state"] is not None}:
        raise RuntimeError("canonical_cold_activation_retention_inventory_changed")
    return found


def _payload_children(conn, cutoff, planned, retention):
    rows = conn.execute(text("""
        SELECT c.relname,c.oid::bigint,pg_get_expr(c.relpartbound,0)
        FROM pg_inherits AS i JOIN pg_class AS c ON c.oid=i.inhrelid
        WHERE i.inhparent='market.fact_hot_payloads'::regclass ORDER BY c.relname
    """))
    expected = {
        fact_partition_name(date.fromisoformat(day)): date.fromisoformat(day)
        for day, state in retention.items() if state != "reclaimed"
    }
    historical = {}
    for name, oid, bound in rows:
        if not name.startswith("fact_hot_payloads_"):
            raise RuntimeError(f"canonical_cold_activation_unexpected_payload_child: relation={name}")
        day = datetime.strptime(name[-8:], "%Y%m%d").date()
        if name != fact_partition_name(day):
            raise RuntimeError(f"canonical_cold_activation_unexpected_payload_child: relation={name}")
        if day >= cutoff:
            continue
        if expected.get(name) != day or planned.get(name) != oid:
            raise RuntimeError(f"canonical_cold_activation_payload_binding_changed: relation={name}")
        if bound != _partition_bound(day):
            raise RuntimeError(f"canonical_cold_activation_payload_bound_changed: relation={name}")
        historical[name] = (day, oid, bound)
    if historical.keys() != expected.keys() or historical.keys() != planned.keys():
        raise RuntimeError("canonical_cold_activation_payload_inventory_changed")
    return historical


def activate_cold_storage(conn, *, execution, expected_execution_hash, receipts, verified_objects,
                          database_fence, statement_timeout_ms=900_000):
    """Perform one all-or-nothing authority change on the already fenced target."""
    validate_execution_record(execution, expected_execution_hash=expected_execution_hash)
    audit = validate_audit_receipts(receipts, target_identity=execution["target_database"],
                                   source_identity=execution["source_database"])
    if (audit["audit_set_hash"] != execution["audit_set_hash"]
            or audit["loaded_inventory_hash"] != execution["loaded_inventory_hash"] or not audit["verified_rows"]):
        raise RuntimeError("canonical_cold_activation_audit_set_mismatch")
    if (
        not conn.in_transaction()
        or conn.get_execution_options().get("isolation_level") == "AUTOCOMMIT"
        or conn.get_isolation_level() != "READ COMMITTED"
    ):
        raise RuntimeError("canonical_cold_activation_requires_transaction")
    if not isinstance(database_fence, DatabaseFence):
        raise ValueError("canonical_cold_activation_requires_live_database_fence")
    if type(statement_timeout_ms) is not int or not 1 <= statement_timeout_ms <= MAX_MIGRATION_SECONDS * 1000:
        raise ValueError("canonical_cold_activation_invalid_statement_timeout")
    conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"), {"timeout": str(statement_timeout_ms)})
    conn.execute(text("SET LOCAL lock_timeout='500ms'"))
    database_fence.assert_quiet(conn)
    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))")):
        raise RuntimeError("canonical_cold_activation_storage_management_busy")
    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                       {"name": _LIFECYCLE_LOCK_NAME}):
        raise RuntimeError("canonical_cold_activation_writer_fence_busy")
    conn.execute(text("LOCK TABLE market.fact_storage_state IN ACCESS EXCLUSIVE MODE NOWAIT"))
    source_cert = conn.execute(text(
        "SELECT layout_version,state FROM market.fact_storage_state ORDER BY layout_version LIMIT 2"
    )).all()
    if source_cert != [(FACT_STORAGE_LAYOUT_VERSION, "ready")]:
        raise RuntimeError("canonical_cold_activation_source_certificate_changed")
    lock_header_namespace(conn)
    conn.execute(text("LOCK TABLE ONLY market.fact_hot_payloads IN ACCESS EXCLUSIVE MODE NOWAIT"))
    conn.execute(text("LOCK TABLE market.fact_retention_partitions IN SHARE ROW EXCLUSIVE MODE NOWAIT"))
    for table in sorted(COLD_TABLE_NAMES):
        conn.execute(text(f'LOCK TABLE market."{table}" IN SHARE ROW EXCLUSIVE MODE NOWAIT'))
    if conn.scalar(text("SELECT EXISTS (SELECT FROM market.cold_fact_retired_days)")):
        raise RuntimeError("canonical_cold_activation_retirement_receipts_already_exist")
    conn.execute(text("LOCK TABLE public.portal_storage_plans IN SHARE ROW EXCLUSIVE MODE NOWAIT"))
    if conn.scalar(text("""
        SELECT EXISTS (
            SELECT FROM public.portal_storage_plans
            WHERE state IN ('queued','running','blocked') AND progress->>'operation'='qt.automatic_header_history.v1'
        )
    """)):
        raise RuntimeError("canonical_cold_activation_pending_legacy_header_move")

    identity = execution["target_database"]
    target = TargetDatabase(identity["system_identifier"], identity["database"], identity["database_oid"])
    source = PostgresMigrationSource(conn.engine, expected_system_identifier=target.system_identifier)
    if _ensure_target(conn, source, target) != identity:
        raise RuntimeError("canonical_cold_activation_target_identity_changed")
    cutoff = date.fromisoformat(audit["prefix"]["cold_before_day"])
    if _plan(conn, cutoff=cutoff, max_days=4096) != audit["prefix"]:
        raise RuntimeError("canonical_cold_activation_prefix_changed")
    if PostgresMarketDataRepository._current_commit_seq_with_session(conn) != execution["source_commit_seq"]:
        raise RuntimeError("canonical_cold_activation_commit_boundary_changed")
    if _inventory_hash(conn) != audit["loaded_inventory_hash"]:
        raise RuntimeError("canonical_cold_activation_loaded_inventory_hash_mismatch")
    _assert_source_authority_inventory(conn, cutoff, audit)
    _assert_verified_archive(conn, verified_objects)
    for table in COLD_CUSTODY_TABLE_NAMES:
        conn.exec_driver_sql(f"LOCK TABLE ONLY market.{table} IN SHARE MODE NOWAIT")
    conn.exec_driver_sql("LOCK TABLE public.portal_storage_targets, public.portal_storage_header_tablespaces "
                         "IN SHARE MODE NOWAIT")
    assert_custody_placement(
        conn, expected_database=f"{target.system_identifier}/{target.oid}",
        target_id=execution["custody_target_id"], expected_tablespace_oid=execution["custody_tablespace_oid"],
        expected_relation_oids=execution["custody_relation_oids"],
    )
    retention = _retention_sources(conn, cutoff, audit)
    payloads = _payload_children(conn, cutoff, execution["payload_children"], retention)
    if conn.scalar(text("""
        SELECT EXISTS (SELECT FROM market.fact_archive_preparations
          WHERE storage_day<:cutoff AND state IN ('building','ready'))
    """), {"cutoff": cutoff}):
        raise RuntimeError("canonical_cold_activation_unfinished_archive_preparations")
    prepare_cold_dependency_bindings(conn)

    header_bytes = {}
    payload_bytes = {}
    for name, (day, oid, bound) in payloads.items():
        _assert_partition(conn, name, oid, bound, "fact_hot_payloads")
        payload_bytes[day.isoformat()] = int(conn.scalar(text("SELECT pg_total_relation_size(:oid)"), {"oid": oid}))
        conn.execute(text(f'DROP TABLE market."{name}"'))
    legacy_end = audit["prefix"]["legacy_end_day"]
    for row in reversed(audit["prefix"]["days"]):
        name = row["child"]
        if name is None or name in header_bytes:
            continue
        header_bytes[name] = drop_verified_header(
            conn, name=name, oid=row["child_oid"], storage_day=date.fromisoformat(row["day"]),
            legacy_end_day=date.fromisoformat(legacy_end) if name == "fact_versions_legacy" else None,
        )
    retired_header_bytes = sum(header_bytes.values())
    conn.execute(text("""
        INSERT INTO market.cold_fact_retired_days
          (storage_day,manifest_set_hash,row_count,reclaimed_header_bytes,protected_dataset_ranges,handoff_kind)
        VALUES (:day,:audit_hash,:rows,:header_bytes,0,'initial')
    """), [{"day": date.fromisoformat(row["day"]), "audit_hash": audit["audit_set_hash"],
            "rows": audit["days"][row["day"]]["verified_rows"],
            "header_bytes": header_bytes.pop(row["child"], 0)}
            for row in audit["prefix"]["days"]])
    identity_result = swap_identity_suffix(
        conn, expected_database=f"{target.system_identifier}/{target.oid}", cutoff=cutoff,
        expected_retired_legacy_oid=next((row["child_oid"] for row in audit["prefix"]["days"]
                                          if row["child"] == "fact_versions_legacy"), None),
        statement_timeout_ms=statement_timeout_ms,
    )
    result = {
        "execution_hash": expected_execution_hash, "audit_set_hash": audit["audit_set_hash"],
        "loaded_inventory_hash": audit["loaded_inventory_hash"],
        "approved_package_hash": execution["approved_package_hash"],
        "database": identity, "source_commit_seq": execution["source_commit_seq"],
        "cold_before_day": cutoff.isoformat(),
        "verified_rows": audit["verified_rows"], "verified_pages": audit["verified_pages"],
        "retired_header_bytes": retired_header_bytes,
        "retired_payload_bytes": sum(payload_bytes.values()), "identity": identity_result,
        "custody": {"target_id": execution["custody_target_id"], "tablespace_oid": execution["custody_tablespace_oid"],
                    "relation_oids": execution["custody_relation_oids"]},
        "evidence": execution["evidence"],
    }
    written = conn.execute(text("""
        UPDATE market.fact_storage_state SET layout_version=:layout,completed_at=clock_timestamp(),
            evidence=CAST(:evidence AS jsonb)
        WHERE layout_version=:prior AND state='ready'
    """), {"layout": COLD_HISTORY_LAYOUT_VERSION, "prior": FACT_STORAGE_LAYOUT_VERSION,
            "evidence": json.dumps(result)}).rowcount
    if written != 1:
        raise RuntimeError("canonical_cold_activation_certificate_changed")
    assert_fact_storage_contract(conn)
    if PostgresMarketDataRepository._current_commit_seq_with_session(conn) != execution["source_commit_seq"]:
        raise RuntimeError("canonical_cold_activation_authority_watermark_mismatch")
    verified_objects.assert_unchanged()
    database_fence.assert_quiet(conn)
    return result


def read_activation_outcome(conn, *, execution, expected_execution_hash):
    """Reconcile durable SQL state for this exact operation on the pinned database."""
    validate_execution_record(execution, expected_execution_hash=expected_execution_hash)
    expected = execution["target_database"]
    identity = PostgresMigrationSource(conn.engine, expected_system_identifier=expected["system_identifier"])
    if identity.identity(conn) != expected:
        raise RuntimeError("canonical_cold_activation_outcome_wrong_database")
    rows = conn.execute(text("""
        SELECT layout_version,state,completed_at IS NOT NULL,evidence FROM market.fact_storage_state
        ORDER BY layout_version LIMIT 2
    """)).all()
    if len(rows) != 1 or tuple(rows[0][1:3]) != ("ready", True):
        raise RuntimeError("canonical_cold_activation_outcome_uncertain")
    layout, _, _, evidence = rows[0]
    if PostgresMarketDataRepository._current_commit_seq_with_session(conn) != execution["source_commit_seq"]:
        raise RuntimeError("canonical_cold_activation_outcome_commit_boundary_changed")
    if layout == FACT_STORAGE_LAYOUT_VERSION:
        assert_fact_storage_contract(conn)
        if _inventory_hash(conn) != execution["loaded_inventory_hash"]:
            raise RuntimeError("canonical_cold_activation_original_boundary_changed")
        return "original"
    if (layout != COLD_HISTORY_LAYOUT_VERSION or not isinstance(evidence, dict)
            or evidence.get("execution_hash") != expected_execution_hash
            or evidence.get("audit_set_hash") != execution["audit_set_hash"]
            or evidence.get("loaded_inventory_hash") != execution["loaded_inventory_hash"]
            or evidence.get("approved_package_hash") != execution["approved_package_hash"]
            or evidence.get("database") != expected
            or evidence.get("source_commit_seq") != execution["source_commit_seq"]):
        raise RuntimeError("canonical_cold_activation_outcome_uncertain")
    assert_fact_storage_contract(conn)
    return "committed"


def _assert_verified_archive(conn, batch):
    require_configured_archive_mount(batch.object_store.root, require_writable=True)
    rows = conn.execute(text("""
        SELECT object_key,object_sha256,byte_count FROM market.cold_fact_pages
        ORDER BY object_key COLLATE "C"
    """).execution_options(yield_per=256))
    count = 0
    try:
        for key, digest, size in rows:
            verified = batch.objects.get(key)
            if verified is None or (verified.object_sha256, verified.byte_count) != (digest, size):
                raise RuntimeError(f"canonical_cold_activation_archive_witness_mismatch: key={key}")
            count += 1
    finally:
        rows.close()
    if count != len(batch.objects):
        raise RuntimeError("canonical_cold_activation_archive_witness_scope_mismatch")
    batch.assert_unchanged()
