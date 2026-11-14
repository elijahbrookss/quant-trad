"""Run the fenced SQL portion of one approved canonical storage outage."""

from __future__ import annotations

from functools import partial
import logging
from time import monotonic

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive_namespace import archive_namespace
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from scripts.automation.canonical_cold_package import bind_execution
from scripts.db.canonical_cold_activate import (
    activate_cold_storage, read_activation_outcome, read_original_watermark, verify_mounted_candidate,
)
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
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
            recovery_boundary = verify_recovery_boundary(fence, control, source_commit, provider_boundary, remaining_seconds)
            assert_host_held()
            check_archive()
            if conn.in_transaction() or control.in_transaction():
                raise RuntimeError("canonical_cold_transition_recovery_connection_left_in_transaction")
            execution, execution_hash = bind_execution(
                package, approved_package_hash=approved_package_hash, source_commit_seq=source_commit,
                provider_boundary=provider_boundary, independent_recovery=recovery_boundary,
            )
            outcome = partial(read_activation_outcome, execution=execution, expected_execution_hash=execution_hash)
            record("activation_committing", {"execution_hash": execution_hash, "source_commit_seq": source_commit,
                                             "provider_boundary": provider_boundary, "independent_recovery": recovery_boundary})
            with conn.begin():
                with _bounded_step(conn, remaining_seconds()):
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
                    fence.assert_quiet(conn)
                    verified_objects.assert_unchanged()
                    check_archive()
                    assert_host_held()
                    remaining_seconds()
        except Exception as error:
            # Prove the original outcome on the same live connection. A
            # disconnection, different result or expired budget leaves the hold
            # for its operator, with no reconnect or guessed rollback.
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
                if outcome(conn) != "committed":
                    raise RuntimeError("canonical_cold_transition_commit_unconfirmed")
        record("activation_verified", {"execution_hash": execution_hash, "witness_hash": witness_hash})
        check_archive()
        assert_host_held()
        reopen_database_fence(fence, control, expected_outcome="committed", read_outcome=outcome, record=record)
        assert_host_held()
        return result
