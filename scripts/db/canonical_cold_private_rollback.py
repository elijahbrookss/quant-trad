"""Reverse a candidate on the isolated rehearsal database after accepted writes."""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass, fields
from datetime import date
from functools import partial
import json
import os
from pathlib import Path
import socket
import tempfile
from time import monotonic

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_namespace import archive_namespace
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import FactArchiveLimits, archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from portal.backend.service.storage.recovery_copies import _sync
from scripts.automation.stage_encrypted_recovery_handoff import _private_root
from scripts.db.canonical_cold_activate import _read_authority, _valid_digest
from scripts.db.canonical_cold_commit_outcome import prepare_activation_transaction
from scripts.db.canonical_cold_database_fence import close_database_fence, native_transaction_mode, reopen_database_fence
from scripts.db.canonical_cold_finalize_rollback import finalize_rollback_layout
from scripts.db.canonical_cold_identity_rebuild import PREVIOUS, SOURCE, _oid
from scripts.db.canonical_cold_paused_donor import PausedRollbackDonor, open_paused_donor
from scripts.db.canonical_cold_private_activate import _certificate_metadata, _pinned_bytes, _validate_private_execution
from scripts.db.canonical_cold_private_activation_capacity import SharedHddActivationLimits, supervise_private_rollback
from scripts.db.canonical_cold_private_placement import assert_private_placement, load_private_placement
from scripts.db.canonical_cold_private_seal import load_private_clone
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from scripts.db.canonical_cold_restore_initial_dated import capture_initial_dated_donor, restore_initial_dated_day
from scripts.db.canonical_cold_restore_legacy import measure_legacy_donor, restore_legacy_headers
from scripts.db.canonical_cold_restore_legacy_payload import capture_original_payload_day, restore_legacy_payload_day
from scripts.db.canonical_cold_restore_reclaimed_day import restore_reclaimed_header_day
from scripts.db.canonical_cold_restore_references import _require_disposable_fence, capture_original_reference_bindings
from scripts.db.canonical_cold_reverse_allocator import capture_allocator
from scripts.db.canonical_cold_reverse_projection import restore_candidate_only_pages
from scripts.db.canonical_cold_rollback_stream import capture_rollback_stream, verify_rollback_stream
from scripts.db.canonical_cold_witness import _validate as validate_witness_cases, measure_witnesses
from scripts.db.fact_header_v2_capture import _bounded_step


@dataclass(frozen=True)
class RollbackLimits:
    legacy_copy_bytes: int
    dated_copy_bytes: int
    payload_rows: int
    payload_bytes: int
    direct_pages: int
    direct_archive_bytes: int
    stream_rows: int
    archive_objects: int
    archive_bytes: int
    archive_pages: int
    witness_archive_bytes: int
    raw_index_copy_bytes: int
    custody_copy_bytes: int

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if type(value) is not int or not 1 <= value < 2**63:
                raise ValueError(f"private_rollback_invalid_limit: {field.name}")
        if (max(self.legacy_copy_bytes, self.dated_copy_bytes, self.payload_bytes, self.archive_bytes) > 1024**4
                or self.payload_rows > 50_000_000 or self.direct_pages > 16_384
                or self.direct_archive_bytes > 128 * 1024**3 or self.stream_rows > 10_000_000
                or self.archive_pages > 100_000 or self.witness_archive_bytes > 8 * 1024**3):
            raise ValueError("private_rollback_limits_outside_native_bounds")


def _seconds(fence, cap=None):
    remaining = int(fence.deadline - monotonic())
    if remaining < 1:
        raise TimeoutError("private_rollback_deadline")
    return remaining if cap is None else min(cap, remaining)


def rollback_under_fence(conn, donor, *, fence, donor_identity, initial_commit, latest_commit, execution_hash,
                         audit, receipts, seals, recurring, stream_ids, cases, store, donor_store, limits,
                         paused_donor=None):
    """Reconstruct every old cohort in the caller's transaction and verify rollback."""
    _require_disposable_fence(conn, fence)
    if (not isinstance(limits, RollbackLimits) or conn is donor or store.root == donor_store.root
            or (paused_donor is not None and (paused_donor.connection is not donor
                                             or donor_store.root != paused_donor.archive_objects))):
        raise ValueError("private_rollback_invalid_bound_copies")
    initial_days = [row for row in audit["prefix"]["days"] if audit["days"][row["day"]]["source_retention_state"] is not None]
    dated = {row["day"] for row in initial_days if row["child"] != "fact_versions_legacy"}
    if not set(seals) <= dated:
        raise ValueError("private_rollback_seal_outside_audited_dates")
    prior = conn.execute(text("""
        SELECT storage_day::text,manifest_set_hash FROM market.cold_fact_retired_days
        WHERE handoff_kind='recurring' ORDER BY storage_day LIMIT 4097
    """)).tuples().all()
    if len(prior) > 4096 or dict(prior) != recurring:
        raise RuntimeError("private_rollback_recurring_inventory_changed")
    allocator = capture_allocator(conn, fence=fence, layout=COLD_HISTORY_LAYOUT_VERSION)
    if allocator.visible_commit != latest_commit:
        raise RuntimeError("private_rollback_latest_commit_changed")
    original = capture_original_reference_bindings(donor)
    if original["database"] != donor_identity:
        raise RuntimeError("private_rollback_donor_changed")

    def seconds(cap=None):
        return _seconds(fence, cap)

    object_limits = ArchiveVerificationLimits(
        max_objects=limits.archive_objects, max_bytes=limits.archive_bytes, max_pages=limits.archive_pages,
    )
    stream_args = {"fence": fence, "store": store, "max_rows": limits.stream_rows, "object_limits": object_limits}
    with archive_namespace(store.root, exclusive=True):
        witness = measure_witnesses(conn, cases, store=store, timeout_seconds=seconds(600),
                                    max_archive_bytes=limits.witness_archive_bytes)
        streams = [capture_rollback_stream(conn, definition_id=item, timeout_seconds=seconds(120), **stream_args)
                   for item in stream_ids]
    projection = restore_candidate_only_pages(
        conn, fence=fence, expected_database=allocator.database, expected_execution_hash=execution_hash,
        expected_source_commit=latest_commit, reader=ColdHistoryReader(store), max_pages=limits.direct_pages,
        max_bytes=limits.direct_archive_bytes, timeout_seconds=seconds(),
    )
    by_day = defaultdict(list)
    for receipt in receipts:
        if "day_batch" in receipt:
            by_day[receipt["day_batch"]["storage_day"]].append(receipt)
    base = {"fence": fence, "audit": audit, "store": store, "donor_store": donor_store, "paused_donor": paused_donor}
    legacy = None
    donor_scope = {"donor_identity": donor_identity, "audit": audit, "source_commit": initial_commit,
                   "paused_donor": paused_donor}
    if audit["prefix"]["legacy_end_day"] is not None:
        measured = measure_legacy_donor(donor, **donor_scope)
        legacy_forward = [receipt for day, batches in by_day.items() if day < audit["prefix"]["legacy_end_day"]
                          for receipt in batches]
        legacy = restore_legacy_headers(conn, donor, **base, forward_receipts=legacy_forward,
            donor_scope=measured, expected_donor_hash=measured["scope_sha256"], max_copy_bytes=limits.legacy_copy_bytes,
            max_archive_bytes=limits.archive_bytes, max_pages=limits.archive_pages, max_seconds=seconds())

    payloads, restored_dated = [], []
    for row in initial_days:
        key = row["day"]
        day = date.fromisoformat(key)
        if row["child"] == "fact_versions_legacy":
            measured = capture_original_payload_day(
                donor, donor_identity=donor_identity, day=day, expected_header_oid=row["child_oid"],
                expected_source_commit_seq=initial_commit, max_rows=limits.payload_rows, max_bytes=limits.payload_bytes,
                timeout_seconds=seconds(), paused_donor=paused_donor,
            )
            payloads.append(restore_legacy_payload_day(
                conn, donor, fence=fence, original=measured, expected_original_hash=measured["receipt_sha256"],
                legacy_header_receipt=legacy, expected_legacy_header_receipt_hash=legacy["receipt_sha256"],
                max_rows=limits.payload_rows, max_bytes=limits.payload_bytes, store=store, object_limits=object_limits,
                timeout_seconds=seconds(), paused_donor=paused_donor,
            ))
        else:
            seal = seals.get(key)
            seal_args = {"private_seal": seal, "expected_private_seal_hash": archive_evidence_hash(seal)} if seal else {}
            measured = capture_initial_dated_donor(donor, day=day, **donor_scope, **seal_args)
            restored_dated.append(restore_initial_dated_day(
                conn, donor, **base, original=measured, expected_original_hash=measured["receipt_sha256"],
                forward_receipts=by_day[key], max_copy_bytes=limits.dated_copy_bytes, max_archive_bytes=limits.archive_bytes,
                max_pages=min(4096, limits.archive_pages), max_inventory_pages=limits.archive_pages,
                max_seconds=seconds(), **seal_args,
            ))
    restored_recurring = [restore_reclaimed_header_day(
        conn, fence=fence, day=date.fromisoformat(day), expected_manifest_set_hash=digest,
        store=store, limits=FactArchiveLimits(), object_limits=object_limits, timeout_seconds=seconds(),
    ) for day, digest in sorted(recurring.items())]
    result = finalize_rollback_layout(
        conn, donor, fence=fence, allocator=allocator, original=original, original_hash=archive_evidence_hash(original),
        legacy=legacy, payloads=payloads, dated=restored_dated, recurring=restored_recurring,
        direct_cold=projection, witness=witness, witness_hash=archive_evidence_hash(witness), store=store,
        raw_index_max_copy_bytes=limits.raw_index_copy_bytes, custody_max_copy_bytes=limits.custody_copy_bytes,
        timeout_seconds=seconds(), paused_donor=paused_donor,
    )
    with archive_namespace(store.root, exclusive=True):
        for item in streams:
            verify_rollback_stream(conn, evidence=item, expected_hash=item["receipt_sha256"],
                                   timeout_seconds=seconds(120), **stream_args)
    fence.assert_quiet(conn)
    return {"layout": result, "allocator_sha256": allocator.sha256, "post_cutover_pages": projection, "streams": streams}


def _active_candidate(conn, execution, execution_hash, latest):
    layout, evidence, commit = _read_authority(conn, execution["target_database"])
    expected = _certificate_metadata(execution) | {key: execution[key] for key in (
        "source_commit_seq", "cold_before_day", "audit_set_hash", "loaded_inventory_hash",
    )} | {"execution_hash": execution_hash}
    registry = execution["identity_registry"]
    if (layout != COLD_HISTORY_LAYOUT_VERSION or commit != latest or not isinstance(evidence, dict)
            or any(evidence.get(key) != value for key, value in expected.items())
            or (_oid(conn, SOURCE), _oid(conn, PREVIOUS)) != (registry["new_oid"], registry["old_oid"])):
        raise RuntimeError("private_rollback_candidate_boundary_changed")
    return "committed"


def run_private_rollback(conn, control, *, clone, placement, donor, execution, execution_hash, receipts,
                         settings, seals, store, donor_store, limits, timeout_seconds, record, supervise_capacity):
    """Hold one clone fence across inverse DDL, read acceptance and commit."""
    _validate_private_execution(execution, execution_hash)
    target = execution["target_database"]
    original = execution["source_commit_seq"]
    latest, recurring, stream_ids, cases = (settings[key] for key in ("latest_commit", "recurring", "stream_ids", "witness_cases"))
    if ({name for _, name in socket.if_nameindex()} != {"lo"} or type(donor) is not PausedRollbackDonor
            or conn is control or conn.engine.url != clone.url or control.engine.url != clone.url.set(database="postgres")
            or any(not native_transaction_mode(db) or db.in_transaction() for db in (conn, control))
            or conn is donor.connection or donor.accepted_commit_seq != original
            or placement["clone_proof_sha256"] != execution["clone_proof_sha256"]
            or clone.proof["accepted_commit_seq"] != original or clone.url.database != target["database"]
            or f"{target['system_identifier']}/{target['database_oid']}" != clone.proof["database_identity"]
            or Path(placement["archive_root"]) / "objects" != store.root
            or not isinstance(limits, RollbackLimits) or type(timeout_seconds) is not int
            or not 1 <= timeout_seconds <= MAX_MIGRATION_SECONDS
            or not callable(record) or not callable(supervise_capacity)
            or type(latest) is not int or not original < latest < 2**63
            or type(recurring) is not dict or len(recurring) > 4096
            or any(date.fromisoformat(day).isoformat() != day or not _valid_digest(digest) for day, digest in recurring.items())
            or not isinstance(stream_ids, list) or not 1 <= len(stream_ids) <= 32
            or not all(isinstance(item, str) and 1 <= len(item) <= 64 for item in stream_ids)
            or len(set(stream_ids)) != len(stream_ids)):
        raise ValueError("private_rollback_invalid_candidate_or_boundaries")
    validate_witness_cases(cases)
    audit = validate_audit_receipts(receipts, target_identity=target, source_identity=target)
    if (audit["audit_set_hash"], audit["loaded_inventory_hash"]) != (execution["audit_set_hash"], execution["loaded_inventory_hash"]):
        raise ValueError("private_rollback_audit_scope_changed")
    deadline = monotonic() + timeout_seconds

    def remaining():
        seconds = int(deadline - monotonic())
        if seconds < 1:
            raise TimeoutError("private_rollback_deadline")
        return seconds

    cold = partial(_active_candidate, execution=execution, execution_hash=execution_hash, latest=latest)
    with conn.begin(), _bounded_step(conn, remaining()):
        assert_private_placement(conn, clone=clone, placement=placement, require_writable=True, require_original_layout=False)
        if conn.scalar(text("SHOW fsync")) != "on":
            raise RuntimeError("private_rollback_requires_fsync")
        cold(conn)
        epoch = clone.proof["postmaster_epoch"] if conn.scalar(text("SHOW timescaledb.max_background_workers")) == "0" else None
    record("private_rollback_scope", {"execution_hash": execution_hash, "original_commit": original, "latest_commit": latest})
    fence = close_database_fence(conn, control, expected_database=target, timeout_seconds=remaining(), record=record,
                                 private_workerless_epoch=epoch)
    try:
        with conn.begin(), _bounded_step(conn, remaining()):
            fence.assert_quiet(conn)
            cold(conn)
            transaction = prepare_activation_transaction(conn, fence)
            record("private_rollback_writing", {"execution_hash": execution_hash, "transaction": transaction})
            with supervise_capacity(conn, remaining) as capacity:
                result = rollback_under_fence(conn, donor.connection, fence=fence, donor_identity=donor.identity,
                    initial_commit=original, latest_commit=latest, execution_hash=execution_hash, audit=audit,
                    receipts=receipts, seals=seals, recurring=recurring, stream_ids=stream_ids, cases=cases,
                    store=store, donor_store=donor_store, limits=limits, paused_donor=donor)
            fence.assert_quiet(conn)
            observed = conn.execute(text("""
                SELECT pg_current_xact_id_if_assigned()::text AS xid, current_setting('fsync') AS fsync,
                       current_setting('synchronous_commit') AS synchronous_commit, pg_current_wal_insert_lsn()::text AS wal
            """)).mappings().one()
            if any(observed[key] != transaction[key] for key in ("xid", "fsync", "synchronous_commit")):
                raise RuntimeError("private_rollback_transaction_changed")
            result = result | {"transaction": transaction, "physical_supervision": capacity}
            precommit = {"layout_receipt": result["layout"]["receipt_sha256"], "xid": transaction["xid"],
                         "wal_before_commit": observed["wal"], "capacity": capacity,
                         "stream_receipts": [item["receipt_sha256"] for item in result["streams"]]}
            record("private_rollback_precommit", precommit)
            remaining()
    except Exception as error:
        try:
            reopen_database_fence(fence, control, expected_outcome="committed", read_outcome=cold, record=record)
            record("private_rollback_aborted", {"execution_hash": execution_hash})
        except Exception as check_error:
            error.add_note(f"Private rollback fence needs reconciliation ({type(check_error).__name__}).")
        raise

    def restored(db):
        layout, evidence, commit = _read_authority(db, target)
        if (layout != FACT_STORAGE_LAYOUT_VERSION or commit != latest
                or archive_evidence_hash(evidence) != result["layout"]["receipt_sha256"]):
            raise RuntimeError("private_rollback_restored_outcome_changed")
        return "original"

    reopen_database_fence(fence, control, expected_outcome="original", read_outcome=restored, record=record)
    record("private_rollback_verified", precommit)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("proof", "placement", "scope", "audit-receipts", "seal-receipts", "request", "copy", "suffix"):
        parser.add_argument(f"--{name}", type=Path, required=True)
        parser.add_argument(f"--{name}-sha256", required=True)
    for name in ("donor-mount", "donor-archive", "pg-controldata", "journal"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--donor-tablespace", required=True)
    parser.add_argument("--donor-port", type=int, default=5432)
    parser.add_argument("--max-audit-bytes", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        if ({name for _, name in socket.if_nameindex()} != {"lo"} or not 1 <= args.max_audit_bytes <= 512 * 1024**2
                or not 10 <= args.timeout_seconds <= MAX_MIGRATION_SECONDS):
            raise ValueError("private_rollback_requires_network_disabled_worker_and_budget")

        def reviewed(name, budget):
            return json.loads(_pinned_bytes(getattr(args, name), getattr(args, f"{name}_sha256"), budget),
                              object_pairs_hook=_unique_fields)

        reviewed("proof", 65536)
        clone = load_private_clone(args.proof, args.proof_sha256, os.environ.get("PG_DSN"))
        placement = load_private_placement(args.placement, args.placement_sha256, clone, clone_proof_sha256=args.proof_sha256)
        execution = reviewed("scope", 65536)
        if (execution["clone_proof_sha256"], execution["placement_sha256"]) != (args.proof_sha256, args.placement_sha256):
            raise ValueError("private_rollback_wrong_scope_artifacts")
        receipts = reviewed("audit_receipts", args.max_audit_bytes)
        seal_rows = reviewed("seal_receipts", 4 * 1024**2)
        if not isinstance(receipts, list) or not isinstance(seal_rows, list) or len(seal_rows) > 4096:
            raise ValueError("private_rollback_invalid_receipt_arrays")
        seals = {item["storage_day"]: item for item in seal_rows}
        if len(seals) != len(seal_rows):
            raise ValueError("private_rollback_duplicate_seal")
        request = reviewed("request", 256 * 1024)
        if set(request) != {"schema", "limits", "capacity", "relation_bytes", "latest_commit", "recurring", "stream_ids",
                            "witness_cases"} or request.pop("schema") != "qt.private_canonical_cold_rollback_request.v1":
            raise ValueError("private_rollback_invalid_request")
        limits = RollbackLimits(**request.pop("limits"))
        capacity = partial(supervise_private_rollback, clone=clone, placement=placement, pg_controldata=args.pg_controldata,
                           limits=SharedHddActivationLimits(**request.pop("capacity")), relation_bytes=request.pop("relation_bytes"))
        archive = Path(placement["archive_root"])
        journal = args.journal
        root = clone.socket.parents[2]
        if (journal.resolve() != journal or any(journal.is_relative_to(path) for path in
                (archive, args.donor_archive, root, args.donor_mount)) or not os.statvfs(archive).f_flag & os.ST_RDONLY):
            raise ValueError("private_rollback_requires_separate_journal_and_readonly_archive")
        _, (device, _) = _private_root(journal.parent)
        if (f"{os.major(device)}:{os.minor(device)}" != clone.proof["device_id"]
                or os.statvfs(journal.parent).f_flag & os.ST_RDONLY):
            raise ValueError("private_rollback_journal_and_copy_scratch_must_use_private_hdd")
        # Native COPY can spill up to one original payload leaf. Keep that
        # allocation in the same physical budget measured by the watcher.
        tempfile.tempdir = str(journal.parent)
        engines = [create_engine(clone.url.set(database=name), poolclass=NullPool, hide_parameters=True,
                    isolation_level="READ COMMITTED", connect_args={"connect_timeout": 5, "passfile": "/dev/null",
                    "application_name": "qt_private_rollback", "options": "-c default_transaction_read_only=off"})
                    for name in (clone.url.database, "postgres")]
        try:
            with open_paused_donor(clone_proof=args.proof, clone_proof_sha256=args.proof_sha256,
                    copy_receipt=args.copy, copy_receipt_sha256=args.copy_sha256, suffix_receipt=args.suffix,
                    suffix_receipt_sha256=args.suffix_sha256, archive_root=args.donor_archive,
                    tablespace_name=args.donor_tablespace, port=args.donor_port, timeout_seconds=args.timeout_seconds,
                    readonly_mount_alias=args.donor_mount) as donor:
                fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as log, engines[0].connect() as conn, engines[1].connect() as control:
                    _sync(journal.parent)

                    def record(phase, data):
                        log.write(json.dumps({"phase": phase, "evidence": data}, sort_keys=True, allow_nan=False) + "\n")
                        log.flush()
                        os.fsync(log.fileno())

                    result = run_private_rollback(conn, control, clone=clone, placement=placement, donor=donor,
                        execution=execution, execution_hash=archive_evidence_hash(execution), receipts=receipts,
                        settings=request, seals=seals, store=FilesystemRawArchiveObjectStore(archive / "objects", writable=False),
                        donor_store=FilesystemRawArchiveObjectStore(donor.archive_objects, writable=False),
                        limits=limits, timeout_seconds=args.timeout_seconds, record=record, supervise_capacity=capacity)
        finally:
            for engine in engines:
                engine.dispose()
        print(json.dumps(result, sort_keys=True, allow_nan=False))
    except SQLAlchemyError as error:
        parser.exit(1, f"private_rollback_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, TypeError, ValueError) as error:
        parser.exit(1, f"private_rollback_refused: {error}\n")


if __name__ == "__main__":
    main()
