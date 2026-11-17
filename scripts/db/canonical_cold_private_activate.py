"""Rehearse the canonical authority transaction on the exact private clone.

Use the network-disabled capsule and its verified qt_bench_* Unix socket.
Bind the clone proof, private placement, execution scope, audit receipt array
and reviewed source witness read-only. Bind the candidate archive writable
and preserve a fresh private journal path for the database fence and commit.
The old identity registry is retained on the rehearsal copy.
"""

import argparse
from datetime import date
from functools import partial
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import socket
import stat
from time import monotonic

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from core.storage_mounts import configured_archive_root
from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_namespace import archive_namespace
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_CUSTODY_TABLE_NAMES
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from portal.backend.service.storage.recovery_copies import _sync
from scripts.automation.stage_encrypted_recovery_handoff import _private_root
from scripts.ci.verify_benchmark_clone_boundary import _read_receipt
from scripts.db.canonical_cold_activate import (
    _EXECUTION_SCOPE, _activate_cold_storage, _read_activation_outcome, _valid_custody_move,
    _valid_digest, _valid_identity_registry, _valid_raw_index_scope, verify_mounted_candidate,
)
from scripts.db.canonical_cold_benchmark import _db_url
from scripts.db.canonical_cold_commit_outcome import prepare_activation_transaction
from scripts.db.canonical_cold_database_fence import close_database_fence, native_transaction_mode, reopen_database_fence
from scripts.db.canonical_cold_identity_rebuild import PREVIOUS, _oid
from scripts.db.canonical_cold_private_placement import assert_private_placement, load_private_placement
from scripts.db.canonical_cold_private_seal import load_private_clone
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from scripts.db.canonical_cold_witness import MAX_WITNESS_BYTES, check_witnesses, load_approved_witnesses
from scripts.db.fact_header_v2_capture import _bounded_step


SCHEMA = "qt.private_canonical_cold_activate.v1"
_PRIVATE_DIGESTS = ("clone_proof_sha256", "placement_sha256", "witness_sha256")


def _certificate_metadata(execution):
    return {"rehearsal": {"schema": SCHEMA, **{key: execution[key] for key in _PRIVATE_DIGESTS}}}


def _validate_private_execution(execution, expected_execution_hash):
    fields = _EXECUTION_SCOPE | {"schema", "source_commit_seq", "cold_before_day", *_PRIVATE_DIGESTS}
    if (not isinstance(execution, dict) or not _valid_digest(expected_execution_hash)
            or archive_evidence_hash(execution) != expected_execution_hash
            or execution.get("schema") != SCHEMA or execution.keys() - {"custody_move"} != fields
            or not all(_valid_digest(execution[key]) for key in (*_PRIVATE_DIGESTS, "audit_set_hash", "loaded_inventory_hash"))
            or type(execution["source_commit_seq"]) is not int or not 0 < execution["source_commit_seq"] < 2**63):
        raise ValueError("private_activation_scope_or_digest_changed")
    target = execution["target_database"]
    if (not isinstance(target, dict) or target.keys() != {"system_identifier", "database_oid", "database", "header_oid"}
            or target != execution["source_database"]
            or not isinstance(target["database"], str) or not re.fullmatch(r"qt_bench_[a-z0-9_]{1,48}", target["database"])
            or date.fromisoformat(execution["cold_before_day"]).isoformat() != execution["cold_before_day"]):
        raise ValueError("private_activation_requires_one_exact_benchmark_database_and_cutoff")

    def oid(value):
        return type(value) is int and 0 < value < 2**32

    raw = execution["raw_mapping_index"]
    payloads, custody = execution["payload_children"], execution["custody_relation_oids"]
    if (not _valid_identity_registry(execution["identity_registry"]) or not _valid_raw_index_scope(raw)
            or (execution["custody_target_id"], execution["custody_tablespace_oid"]) != (raw["target_id"], raw["tablespace_oid"])
            or not isinstance(custody, dict) or custody.keys() != set(COLD_CUSTODY_TABLE_NAMES)
            or not all(map(oid, custody.values())) or not isinstance(payloads, dict) or len(payloads) > 4096
            or any(not re.fullmatch(r"fact_hot_payloads_[0-9]{8}", name) or not oid(value) for name, value in payloads.items())
            or ("custody_move" in execution and not _valid_custody_move(execution["custody_move"]))):
        raise ValueError("private_activation_native_relation_scope_invalid")


def _assert_private_owner(conn, control, *, clone, placement, store):
    url = _db_url(clone.url.render_as_string(hide_password=False))
    archive = Path(placement["archive_root"])
    if ({name for _, name in socket.if_nameindex()} != {"lo"} or conn.engine.url != url
            or control.engine.url != url.set(database="postgres")
            or store.root != archive / "objects" or configured_archive_root() != archive
            or os.statvfs(archive).f_flag & os.ST_RDONLY or conn.scalar(text("SHOW fsync")) != "on"):
        raise ValueError("private_activation_requires_isolated_clone_and_bound_writable_archive")
    assert_private_placement(conn, clone=clone, placement=placement, require_writable=True)


def read_private_activation_outcome(conn, *, execution, expected_execution_hash):
    _validate_private_execution(execution, expected_execution_hash)
    outcome = _read_activation_outcome(conn, execution=execution, expected_execution_hash=expected_execution_hash,
                                       certificate_metadata=_certificate_metadata(execution))
    if outcome == "committed" and _oid(conn, PREVIOUS) != execution["identity_registry"]["old_oid"]:
        raise RuntimeError("private_activation_prior_identity_registry_not_retained")
    return outcome


def run_private_activation(conn, control, *, clone, placement, execution, expected_execution_hash, receipts,
                           candidate_witness, store, limits, timeout_seconds, record, supervise_capacity):
    """Activate and verify within one retained native writer fence."""
    _validate_private_execution(execution, expected_execution_hash)
    if (conn is control or any(not native_transaction_mode(item) or item.in_transaction() for item in (conn, control))
            or not isinstance(limits, ArchiveVerificationLimits) or not isinstance(store, FilesystemRawArchiveObjectStore)
            or type(timeout_seconds) is not int
            or not 1 <= timeout_seconds <= MAX_MIGRATION_SECONDS or not callable(record) or not callable(supervise_capacity)):
        raise ValueError("private_activation_requires_finite_native_transaction_boundaries")
    target = execution["target_database"]
    proof = clone.proof
    binding = placement["native_binding"]
    if (target["database"] != clone.url.database
            or f"{target['system_identifier']}/{target['database_oid']}" != proof["database_identity"]
            or execution["source_commit_seq"] != proof["accepted_commit_seq"]
            or placement["clone_proof_sha256"] != execution["clone_proof_sha256"]
            or (placement["history_target_id"], binding["tablespace_oid"])
            != (execution["custody_target_id"], execution["custody_tablespace_oid"])):
        raise ValueError("private_activation_scope_different_verified_clone")
    audit = validate_audit_receipts(receipts, target_identity=target, source_identity=target)
    if (not audit["verified_rows"] or audit["verified_pages"] > limits.max_pages
            or (audit["audit_set_hash"], audit["loaded_inventory_hash"], audit["prefix"]["cold_before_day"])
            != (execution["audit_set_hash"], execution["loaded_inventory_hash"], execution["cold_before_day"])):
        raise RuntimeError("private_activation_audit_set_changed_or_exceeds_budget")
    witness = load_approved_witnesses(candidate_witness, expected_sha256=execution["witness_sha256"])
    outcome = partial(read_private_activation_outcome, execution=execution, expected_execution_hash=expected_execution_hash)
    deadline = monotonic() + timeout_seconds

    def remaining():
        left = int(deadline - monotonic())
        if left < 1:
            raise TimeoutError("private_activation_deadline")
        return left

    with archive_namespace(store.root.parent, exclusive=True) as parent_held, \
            archive_namespace(store.root, exclusive=True) as objects_held:
        def check():
            parent_held()
            objects_held()
            return remaining()

        with conn.begin(), _bounded_step(conn, check()):
            _assert_private_owner(conn, control, clone=clone, placement=placement, store=store)
            workerless_epoch = (proof["postmaster_epoch"]
                if conn.scalar(text("SHOW timescaledb.max_background_workers")) == "0" else None)
            if outcome(conn) != "original":
                raise RuntimeError("private_activation_expected_original_authority")
            verified = verify_mounted_candidate(conn, audit=audit, store=store, limits=limits, check_budget=check)
        record("private_activation_scope", {"execution_hash": expected_execution_hash,
                    "audit_set_hash": audit["audit_set_hash"], "database": target, "source_commit_seq": proof["accepted_commit_seq"]})
        fence = close_database_fence(conn, control, expected_database=target, timeout_seconds=check(), record=record,
                                     private_workerless_epoch=workerless_epoch)
        try:
            with conn.begin(), _bounded_step(conn, check()):
                fence.assert_quiet(conn)
                _assert_private_owner(conn, control, clone=clone, placement=placement, store=store)
                original_witness_hash = check_witnesses(conn, witness, store=store, timeout_seconds=min(600, check()))
                transaction = prepare_activation_transaction(conn, fence)
                record("activation_committing", {"execution_hash": expected_execution_hash, "transaction": transaction})
                with supervise_capacity(conn, remaining) as physical:
                    result = _activate_cold_storage(conn, execution=execution, expected_execution_hash=expected_execution_hash,
                        receipts=receipts, verified_objects=verified, database_fence=fence,
                        certificate_metadata=_certificate_metadata(execution), statement_timeout_ms=check() * 1000)
                    witness_hash = check_witnesses(conn, witness, store=store, timeout_seconds=min(600, check()))
                    if witness_hash != original_witness_hash or outcome(conn) != "committed":
                        raise RuntimeError("private_activation_candidate_acceptance_changed")
                    fence.assert_quiet(conn)
                    verified.assert_unchanged()
                    check()
                final_tx = conn.execute(text("""
                    SELECT pg_current_xact_id_if_assigned()::text AS xid, current_setting('fsync') AS fsync,
                           current_setting('synchronous_commit') AS synchronous_commit, pg_current_wal_insert_lsn()::text AS wal
                """)).mappings().one()
                if any(final_tx[key] != transaction[key] for key in ("xid", "fsync", "synchronous_commit")):
                    raise RuntimeError("private_activation_transaction_changed")
                precommit = {"execution_hash": expected_execution_hash, "xid": transaction["xid"],
                             "witness_hash": witness_hash, "wal_before_commit": final_tx["wal"],
                             "physical_supervision": physical}
                record("activation_precommit_verified", precommit)
                check()
        except Exception as error:
            try:
                check()
                reopen_database_fence(fence, control, expected_outcome="original", read_outcome=outcome, record=record)
                record("activation_rolled_back", {"execution_hash": expected_execution_hash})
            except Exception as recovery_error:
                error.add_note(f"Private fence needs reconciliation ({type(recovery_error).__name__}).")
            raise
        check()
        reopen_database_fence(fence, control, expected_outcome="committed", read_outcome=outcome, record=record)
        record("activation_verified", precommit)
        check()
    return result | {"witness_hash": witness_hash, "transaction": transaction, "physical_supervision": physical}


def _pinned_bytes(path, digest, limit):
    if (path.resolve(strict=True) != path or not stat.S_ISREG(path.lstat().st_mode)
            or not os.statvfs(path).f_flag & os.ST_RDONLY or not _valid_digest(digest)):
        raise ValueError("private_activation_requires_reviewed_readonly_artifact")
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if not 0 < len(data) <= limit or sha256(data).hexdigest() != digest:
        raise ValueError("private_activation_reviewed_artifact_changed_or_exceeds_budget")
    return data


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("proof", "placement", "scope", "audit-receipts", "witness", "journal", "limits", "pg-controldata"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("proof-sha256", "placement-sha256", "scope-sha256", "audit-receipts-sha256", "limits-sha256"):
        parser.add_argument(f"--{name}", required=True)
    for name in ("max-audit-bytes", "max-archive-objects", "max-archive-bytes", "max-pages", "timeout-seconds"):
        parser.add_argument(f"--{name}", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        if not 1 <= args.max_audit_bytes <= 512 * 1024**2:
            raise ValueError("private_activation_invalid_audit_artifact_budget")
        proof = _read_receipt(args.proof, args.proof_sha256)
        clone = load_private_clone(args.proof, args.proof_sha256, os.environ.get("PG_DSN"))
        placement = load_private_placement(args.placement, args.placement_sha256, clone, clone_proof_sha256=args.proof_sha256)
        execution = json.loads(_pinned_bytes(args.scope, args.scope_sha256, 65536), object_pairs_hook=_unique_fields)
        if (proof != clone.proof or execution["clone_proof_sha256"] != args.proof_sha256
                or execution["placement_sha256"] != args.placement_sha256):
            raise ValueError("private_activation_reviewed_scope_uses_other_evidence")
        receipts = json.loads(_pinned_bytes(args.audit_receipts, args.audit_receipts_sha256, args.max_audit_bytes),
                              object_pairs_hook=_unique_fields)
        if not isinstance(receipts, list):
            raise ValueError("private_activation_expected_audit_receipts_array")
        witness = _pinned_bytes(args.witness, execution["witness_sha256"], MAX_WITNESS_BYTES)
        archive = Path(placement["archive_root"])
        if (args.journal.resolve() != args.journal or args.journal.is_relative_to(archive)):
            raise ValueError("private_activation_requires_disjoint_private_journal")
        _private_root(args.journal.parent)
        store = FilesystemRawArchiveObjectStore(archive / "objects", writable=False)
        from scripts.db.canonical_cold_private_activation_capacity import load_activation_limits, supervise_private_activation

        capacity = partial(supervise_private_activation, clone=clone, placement=placement, execution=execution,
            limits=load_activation_limits(_pinned_bytes(args.limits, args.limits_sha256, 8192)),
            pg_controldata=args.pg_controldata)
        limits = ArchiveVerificationLimits(max_objects=args.max_archive_objects, max_bytes=args.max_archive_bytes,
                                           max_pages=args.max_pages)
        engines = [create_engine(clone.url.set(database=name), poolclass=NullPool, hide_parameters=True,
            isolation_level="READ COMMITTED", connect_args={"connect_timeout": 5, "passfile": "/dev/null",
                "application_name": "qt_private_activation", "options": "-c default_transaction_read_only=off"})
            for name in (clone.url.database, "postgres")]
        try:
            fd = os.open(args.journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as journal, engines[0].connect() as conn, engines[1].connect() as control:
                _sync(args.journal.parent)

                def record(phase, value):
                    journal.write(json.dumps({"phase": phase, "evidence": value}, sort_keys=True, allow_nan=False) + "\n")
                    journal.flush()
                    os.fsync(journal.fileno())

                result = run_private_activation(conn, control, clone=clone, placement=placement, execution=execution,
                    expected_execution_hash=archive_evidence_hash(execution), receipts=receipts, candidate_witness=witness,
                    store=store, limits=limits, timeout_seconds=args.timeout_seconds, record=record, supervise_capacity=capacity)
        finally:
            for engine in engines:
                engine.dispose()
    except SQLAlchemyError as error:
        parser.exit(1, f"private_activation_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, TypeError, ValueError) as error:
        parser.exit(1, f"private_activation_refused: {error}\n")
    print(json.dumps(result, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
