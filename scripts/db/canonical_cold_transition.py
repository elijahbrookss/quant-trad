"""Run the fenced SQL portion of one approved canonical storage outage."""

from __future__ import annotations

from datetime import date
from functools import partial
import logging
from time import monotonic

from sqlalchemy import text

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive_namespace import archive_namespace
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from scripts.automation.canonical_cold_package import bind_execution
from scripts.db.canonical_cold_activate import (
    activate_cold_storage, assert_original_identity_registry, raw_index_placement_scope, read_activation_outcome,
    read_original_watermark, verify_mounted_candidate,
)
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_commit_outcome import prepare_activation_transaction
from scripts.db.canonical_cold_identity_rebuild import finalize_identity_suffix
from scripts.db.canonical_cold_raw_index import assert_raw_mapping_index_placement
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from scripts.db.canonical_cold_witness import check_witnesses, load_approved_witnesses
from scripts.db.fact_header_v2_capture import _bounded_step


logger = logging.getLogger(__name__)


def run_fenced_transition(
    conn, control, *, package, approved_package_hash, receipts, store, limits, timeout_seconds,
    assert_host_held, host_remaining, verify_provider_boundary, verify_recovery_boundary, record,
    verify_candidate, candidate_witness, verified_objects=None,
):
    """Check the candidate inside the transaction before making DDL durable.

    An unresolved stop, SQL action, COMMIT or acceptance leaves the caller's
    durable host hold intact. The caller owns old-release restart, final service
    activation, and the independently qualified restore path.

    An owner can checksum the immutable candidate before stopping collectors.
    The handoff still rechecks the scope and each file's identity under lock.
    Supply the host-reviewed package. The provider callback runs on the held
    connection. The recovery callback can use the control database and must
    return a verified, separately retained receipt for that same watermark.
    It receives the existing archive ownership check for suffix capture.
    """
    if (type(timeout_seconds) is not int or not 1 <= timeout_seconds <= MAX_MIGRATION_SECONDS
            or not isinstance(limits, ArchiveVerificationLimits)
            or not all(callable(callback) for callback in (
                assert_host_held, host_remaining, verify_provider_boundary, verify_recovery_boundary, record, verify_candidate,
            ))):
        raise ValueError("canonical_cold_transition_invalid_boundaries")
    deadline = monotonic() + min(timeout_seconds, host_remaining())
    witness = load_approved_witnesses(
        candidate_witness, expected_sha256=package["artifacts"]["candidate_transaction_witness"]["sha256"],
    )
    scope = package["scope"]
    raw_index_scope = raw_index_placement_scope(scope)
    expected_database = scope["target_database"]
    audit = validate_audit_receipts(
        receipts, target_identity=expected_database, source_identity=scope["source_database"],
    )
    if (audit["audit_set_hash"] != scope["audit_set_hash"]
            or audit["loaded_inventory_hash"] != scope["loaded_inventory_hash"]):
        raise RuntimeError("canonical_cold_transition_audit_changed")
    current_original_watermark = partial(read_original_watermark, expected=expected_database,
                                         expected_inventory=scope["loaded_inventory_hash"])

    # An interrupted evidence capture can leave no execution record. Recovery
    # can still prove that the original relations and exact watermark survived.
    source_commit = None
    execution_hash = None

    def original_outcome(connection):
        commit = current_original_watermark(connection)
        assert_original_identity_registry(connection, scope=scope["identity_registry"])
        assert_raw_mapping_index_placement(connection, **raw_index_scope, on_history=False)
        if source_commit is not None and commit != source_commit:
            raise RuntimeError("canonical_cold_transition_original_boundary_changed")
        return "original"

    def remaining_seconds():
        left = int(min(deadline - monotonic(), host_remaining()))
        if left < 1:
            raise TimeoutError("canonical_cold_transition_deadline")
        return left

    assert_host_held()
    with archive_namespace(store.root, exclusive=True) as check_archive:
        check_archive()
        with conn.begin():
            with _bounded_step(conn, remaining_seconds()):
                if current_original_watermark(conn) < package["source_commit_floor"]:
                    raise RuntimeError("canonical_cold_transition_source_behind_approved_floor")
                assert_original_identity_registry(conn, scope=scope["identity_registry"])
                assert_raw_mapping_index_placement(conn, **raw_index_scope, on_history=False)
                if verified_objects is None:
                    verified_objects = verify_mounted_candidate(
                        conn, audit=audit, store=store, limits=limits, check_budget=remaining_seconds,
                    )
                elif (not isinstance(verified_objects, ArchiveVerificationBatch)
                        or verified_objects.object_store is not store or verified_objects.limits != limits):
                    raise RuntimeError("canonical_cold_transition_archive_verification_scope_changed")
        check_archive()
        assert_host_held()
        fence = close_database_fence(
            conn, control, expected_database=expected_database, timeout_seconds=remaining_seconds(), record=record,
        )
        assert_host_held()
        try:
            with conn.begin():
                with _bounded_step(conn, remaining_seconds()):
                    fence.assert_quiet(conn)
                    source_commit = current_original_watermark(conn)
                    provider_boundary = verify_provider_boundary(conn, source_commit)
                    fence.assert_quiet(conn)
            recovery_boundary = verify_recovery_boundary(
                fence, control, source_commit, provider_boundary, remaining_seconds, check_archive,
            )
            assert_host_held()
            check_archive()
            if conn.in_transaction() or control.in_transaction():
                raise RuntimeError("canonical_cold_transition_recovery_connection_left_in_transaction")
            execution, execution_hash = bind_execution(
                package, approved_package_hash=approved_package_hash, source_commit_seq=source_commit,
                provider_boundary=provider_boundary, independent_recovery=recovery_boundary,
            )
            outcome = partial(read_activation_outcome, execution=execution, expected_execution_hash=execution_hash)
            retired_outcome = partial(outcome, require_identity_retired=True)
            with conn.begin():
                with _bounded_step(conn, remaining_seconds()):
                    transaction = prepare_activation_transaction(conn, fence)
                    record("activation_committing", {"execution_hash": execution_hash, "source_commit_seq": source_commit,
                                                     "provider_boundary": provider_boundary,
                                                     "independent_recovery": recovery_boundary, "transaction": transaction})
                    result = activate_cold_storage(
                        conn, execution=execution, expected_execution_hash=execution_hash,
                        receipts=receipts, verified_objects=verified_objects, database_fence=fence,
                        statement_timeout_ms=min(remaining_seconds(), MAX_MIGRATION_SECONDS) * 1000,
                    )
                    if outcome(conn) != "committed":
                        raise RuntimeError("canonical_cold_transition_candidate_certificate_unconfirmed")
                    witness_hash = check_witnesses(conn, witness, store=store, timeout_seconds=min(600, remaining_seconds()))
                    verify_candidate(conn, result)
                    if outcome(conn) != "committed":
                        raise RuntimeError("canonical_cold_transition_candidate_changed_during_acceptance")
                    identity = result["identity"]
                    result["identity_retirement"] = finalize_identity_suffix(
                        conn, expected_database=identity["database_identity"], cutoff=date.fromisoformat(result["cold_before_day"]),
                        old_oid=identity["old_oid"], new_oid=identity["new_oid"], verified_recovery_digest=recovery_boundary,
                        statement_timeout_ms=remaining_seconds() * 1000,
                    )
                    if retired_outcome(conn) != "committed":
                        raise RuntimeError("canonical_cold_transition_candidate_changed_after_identity_retirement")
                    fence.assert_quiet(conn)
                    verified_objects.assert_unchanged()
                    check_archive()
                    assert_host_held()
                    remaining_seconds()
                    raw_index = result["raw_mapping_index"]
                    final_tx = conn.execute(text("""
                        SELECT pg_current_xact_id_if_assigned()::text AS xid, current_setting('fsync') AS fsync,
                               current_setting('synchronous_commit') AS synchronous_commit, pg_current_wal_insert_lsn()::text AS wal
                    """)).mappings().one()
                    if any(final_tx[key] != transaction[key] for key in ("xid", "fsync", "synchronous_commit")):
                        raise RuntimeError("canonical_cold_transition_activation_transaction_changed")
                    precommit = {"execution_hash": execution_hash, "xid": transaction["xid"], "witness_hash": witness_hash,
                                 "raw_mapping_index_receipt_hash": raw_index and raw_index["receipt_hash"],
                                 "identity_retirement": result["identity_retirement"],
                                 "wal_before_commit": final_tx["wal"]}
                    record("activation_precommit_verified", precommit)
        except Exception as error:
            # Prove the original outcome on the same live connection. A
            # disconnection, different result or expired budget leaves the hold
            # for its operator, with no reconnect or guessed rollback.
            if conn.closed or conn.invalidated:
                error.add_note("The retained SQL connection was lost. Inspect the held activation from the control database.")
                raise
            try:
                assert_host_held()
                with conn.begin():
                    with _bounded_step(conn, remaining_seconds()):
                        fence.assert_quiet(conn)
                        original_outcome(conn)
                record("activation_rolled_back", {"execution_hash": execution_hash, "source_commit_seq": source_commit})
                reopen_database_fence(fence, control, expected_outcome="original", read_outcome=original_outcome, record=record)
            except Exception as recovery_error:
                error.add_note(f"Original SQL restart needs reconciliation ({type(recovery_error).__name__}).")
                logger.error("canonical_cold_transition_recovery_unresolved | error=%s", type(recovery_error).__name__)
            raise
        assert_host_held()
        with conn.begin():
            with _bounded_step(conn, remaining_seconds()):
                fence.assert_quiet(conn)
                if retired_outcome(conn) != "committed":
                    raise RuntimeError("canonical_cold_transition_commit_unconfirmed")
        record("activation_verified", precommit)
        check_archive()
        assert_host_held()
        reopen_database_fence(fence, control, expected_outcome="committed", read_outcome=retired_outcome, record=record)
        assert_host_held()
        return result
