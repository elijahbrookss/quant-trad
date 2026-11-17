"""Measure the activation scope and source witness on a verified private clone."""

import argparse
from datetime import date
from hashlib import sha256
import json
import os
from pathlib import Path
import socket

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from core.storage_mounts import configured_archive_root, require_configured_archive_mount
from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.fact_storage_schema import fact_partition_name
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from scripts.automation.stage_encrypted_recovery_handoff import _private_root
from scripts.db.canonical_cold_activate import (
    _inventory_hash, _payload_children, _plan, _retention_sources, assert_original_identity_registry,
)
from scripts.db.canonical_cold_custody_placement import inspect_custody_placement
from scripts.db.canonical_cold_identity_rebuild import SOURCE, STAGED, _oid
from scripts.db.canonical_cold_migration import PostgresMigrationSource
from scripts.db.canonical_cold_private_activate import SCHEMA, _pinned_bytes, _validate_private_execution
from scripts.db.canonical_cold_private_placement import assert_private_placement, load_private_placement
from scripts.db.canonical_cold_private_seal import load_private_clone
from scripts.db.canonical_cold_raw_index import RECENT_TABLESPACE_OID, _fork_bytes, inspect_raw_mapping_index
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from scripts.db.canonical_cold_witness import MAX_WITNESS_BYTES, measure_witnesses
from scripts.db.fact_header_v2_capture import _bounded_step


def _json_bytes(body):
    return json.dumps(body, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")).encode("ascii")


def measure_private_activation(conn, *, clone, clone_sha256, placement, placement_sha256, audit_receipts, cases, store,
                               raw_copy_bytes, custody_copy_bytes, copy_timeout_seconds, witness_max_bytes,
                               witness_timeout_seconds):
    if (not conn.in_transaction() or conn.scalar(text("SHOW transaction_read_only")) != "on"
            or type(raw_copy_bytes) is not int or not 1 <= raw_copy_bytes < 2**63
            or type(custody_copy_bytes) is not int or not 0 <= custody_copy_bytes < 2**63
            or type(copy_timeout_seconds) is not int or not 1 <= copy_timeout_seconds <= MAX_MIGRATION_SECONDS):
        raise ValueError("private_activation_preparation_requires_read_only_and_bounded_copies")
    assert_private_placement(conn, clone=clone, placement=placement)
    expected_database = clone.proof["database_identity"]
    system_identifier, _ = expected_database.split("/")
    identity = PostgresMigrationSource(conn.engine, expected_system_identifier=system_identifier).identity(conn)
    audit = validate_audit_receipts(audit_receipts, target_identity=identity, source_identity=identity)
    cutoff = date.fromisoformat(audit["prefix"]["cold_before_day"])
    if not audit["verified_rows"] or _plan(conn, cutoff=cutoff, max_days=4096) != audit["prefix"]:
        raise RuntimeError("private_activation_preparation_requires_current_audited_history")
    if _inventory_hash(conn) != audit["loaded_inventory_hash"]:
        raise RuntimeError("private_activation_preparation_loaded_inventory_changed")
    retention = _retention_sources(conn, cutoff, audit)
    names = [fact_partition_name(date.fromisoformat(day)) for day, state in retention.items() if state != "reclaimed"]
    payloads = dict(conn.execute(text("""
        SELECT name, to_regclass('market.' || name)::oid::bigint
        FROM unnest(CAST(:names AS text[])) AS names(name)
    """), {"names": names}).tuples().all())
    _payload_children(conn, cutoff, payloads, retention)
    registry = {"old_oid": _oid(conn, SOURCE), "new_oid": _oid(conn, STAGED)}
    assert_original_identity_registry(conn, scope=registry)
    target_id = placement["history_target_id"]
    raw = inspect_raw_mapping_index(conn, expected_database=expected_database, target_id=target_id)
    member = raw["member"]
    custody = inspect_custody_placement(conn, expected_database=expected_database, target_id=target_id)
    raw_bytes = _fork_bytes(member)
    if member["tablespace_oid"] != RECENT_TABLESPACE_OID or raw_bytes > raw_copy_bytes:
        raise RuntimeError("private_activation_preparation_raw_index_placement_or_budget_changed")
    if custody["copy_bytes"] > custody_copy_bytes:
        raise RuntimeError("private_activation_preparation_custody_copy_budget_exceeded")
    witness = _json_bytes(measure_witnesses(
        conn, cases, store=store, timeout_seconds=witness_timeout_seconds, max_archive_bytes=witness_max_bytes,
    ))
    if len(witness) > MAX_WITNESS_BYTES:
        raise RuntimeError("private_activation_preparation_witness_size_exceeded")
    scope = {
        "schema": SCHEMA, "clone_proof_sha256": clone_sha256, "placement_sha256": placement_sha256,
        "witness_sha256": sha256(witness).hexdigest(), "cold_before_day": cutoff.isoformat(),
        "source_commit_seq": clone.proof["accepted_commit_seq"], "source_database": identity, "target_database": identity,
        "audit_set_hash": audit["audit_set_hash"], "loaded_inventory_hash": audit["loaded_inventory_hash"],
        "payload_children": payloads, "custody_target_id": target_id,
        "custody_tablespace_oid": custody["target"]["tablespace_oid"], "custody_relation_oids": custody["relation_oids"],
        "identity_registry": registry, "raw_mapping_index": {
            "target_id": target_id, "tablespace_oid": raw["target"]["tablespace_oid"], "table_oid": member["table_oid"],
            "index_oid": member["index_oid"], "max_copy_bytes": raw_copy_bytes, "timeout_seconds": copy_timeout_seconds,
        },
    }
    if any(relation["tablespace_oid"] != scope["custody_tablespace_oid"] for relation in custody["members"]):
        scope["custody_move"] = {"source_placement_hash": custody["placement_hash"], "max_copy_bytes": custody_copy_bytes,
                                 "timeout_seconds": copy_timeout_seconds}
    _validate_private_execution(scope, archive_evidence_hash(scope))
    return scope, witness, {"verified_rows": audit["verified_rows"], "verified_pages": audit["verified_pages"],
                            "raw_index_bytes": raw_bytes, "custody_copy_bytes": custody["copy_bytes"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("proof", "placement", "audit-receipts", "witness-cases"):
        parser.add_argument(f"--{name}", type=Path, required=True)
        parser.add_argument(f"--{name}-sha256", required=True)
    for name in ("max-audit-bytes", "raw-copy-bytes", "custody-copy-bytes", "copy-timeout-seconds",
                 "witness-max-bytes", "witness-timeout-seconds", "timeout-seconds"):
        parser.add_argument(f"--{name}", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        if ({name for _, name in socket.if_nameindex()} != {"lo"}
                or not 1 <= args.max_audit_bytes <= 512 * 1024**2 or not 1 <= args.timeout_seconds <= 900):
            raise ValueError("private_activation_preparation_requires_isolated_and_bounded_worker")
        _pinned_bytes(args.proof, args.proof_sha256, 65536)
        clone = load_private_clone(args.proof, args.proof_sha256, os.environ.get("PG_DSN"))
        placement = load_private_placement(args.placement, args.placement_sha256, clone, clone_proof_sha256=args.proof_sha256)
        boundaries = ((args.audit_receipts, args.audit_receipts_sha256, args.max_audit_bytes),
                      (args.witness_cases, args.witness_cases_sha256, MAX_WITNESS_BYTES))
        receipts, cases = (json.loads(_pinned_bytes(*values), object_pairs_hook=_unique_fields) for values in boundaries)
        archive = Path(placement["archive_root"])
        if (configured_archive_root() != archive or not os.statvfs(archive).f_flag & os.ST_RDONLY
                or not isinstance(receipts, list) or not isinstance(cases, list)):
            raise ValueError("private_activation_preparation_requires_bound_readonly_archive_and_inventory")
        require_configured_archive_mount(archive, require_writable=False)
        _private_root(archive)
        store = FilesystemRawArchiveObjectStore(archive / "objects", writable=False)
        engine = create_engine(clone.url, poolclass=NullPool, hide_parameters=True, isolation_level="REPEATABLE READ",
            connect_args={"connect_timeout": 5, "passfile": "/dev/null", "application_name": "qt_private_activation_prepare"})
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                with _bounded_step(conn, args.timeout_seconds):
                    scope, witness, measured = measure_private_activation(
                        conn, clone=clone, clone_sha256=args.proof_sha256, placement=placement,
                        placement_sha256=args.placement_sha256, audit_receipts=receipts, cases=cases, store=store,
                        raw_copy_bytes=args.raw_copy_bytes, custody_copy_bytes=args.custody_copy_bytes,
                        copy_timeout_seconds=args.copy_timeout_seconds, witness_max_bytes=args.witness_max_bytes,
                        witness_timeout_seconds=args.witness_timeout_seconds,
                    )
        finally:
            engine.dispose()
        scope_bytes = _json_bytes(scope)
        print(json.dumps({"schema": "qt.private_canonical_cold_activation_preparation.v1", **measured,
            "scope_json": scope_bytes.decode("ascii"), "scope_sha256": sha256(scope_bytes).hexdigest(),
            "witness_json": witness.decode("ascii"), "witness_sha256": scope["witness_sha256"]}, sort_keys=True))
    except SQLAlchemyError as error:
        parser.exit(1, f"private_activation_preparation_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, TypeError, ValueError) as error:
        parser.exit(1, f"private_activation_preparation_refused: {error}\n")


if __name__ == "__main__":
    main()
