"""Run the host and database owners for a single approved cold-storage cutover."""

from functools import partial
from time import monotonic, time

from sqlalchemy import text

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive_verification import ArchiveVerificationLimits
from scripts.automation.canonical_cold_collector_checks import bind_approved_collector_checks
from scripts.automation.canonical_cold_host_hold import hold_cold_cutover
from scripts.automation.canonical_cold_package import load_approved_package, read_approved_candidate_witness
from scripts.automation.canonical_cold_recovery_checks import bind_approved_recovery_checks
from scripts.db.canonical_cold_activate import (
    assert_original_identity_registry, raw_index_placement_scope, read_original_watermark, verify_mounted_candidate,
)
from scripts.db.canonical_cold_database_fence import _database
from scripts.db.canonical_cold_raw_index import assert_raw_mapping_index_placement
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from scripts.db.canonical_cold_transition import run_fenced_transition
from scripts.db.fact_header_v2_capture import _bounded_step


def _read_open_original(conn, control, *, package, timeout_seconds):
    scope = package["scope"]
    watermark = read_original_watermark(
        conn, expected=scope["target_database"], expected_inventory=scope["loaded_inventory_hash"],
    )
    if watermark < package["source_commit_floor"]:
        raise RuntimeError("canonical_cold_execute_source_behind_approved_floor")
    target = _database(conn, scope["target_database"]["database"])
    with control.begin():
        control.exec_driver_sql("SET TRANSACTION READ ONLY")
        with _bounded_step(control, timeout_seconds):
            if (control.scalar(text("SELECT current_database()")) != "postgres" or not target["allow_connections"]
                    or _database(control, target["database"]) != target):
                raise RuntimeError("canonical_cold_execute_control_endpoint_changed")
    assert_original_identity_registry(conn, scope=scope["identity_registry"])
    assert_raw_mapping_index_placement(conn, **raw_index_placement_scope(scope), on_history=False)
    return {"database": target, "source_commit_seq": watermark}


def execute_approved_cutover(
    conn, control, *, state_root, inventory, package_path, approved_package_hash, receipts, store, limits,
    preflight_seconds, outage_seconds, provider_receipt_root, verify_recovery_boundary,
    verify_candidate, verify_candidate_prestart, verify_candidate_ready, verify_original_ready,
):
    """Check exact assets before taking the host hold and dispatching any stops.

    The site owner supplies off-host recovery and application checks.
    Capture the provider tail while collection is still running under host
    exclusion and give that same observation to the final drained-boundary
    check. Only a journaled original SQL outcome can restart the old fleet.
    """
    callbacks = (
        verify_recovery_boundary, verify_candidate,
        verify_candidate_prestart, verify_candidate_ready, verify_original_ready,
    )
    if (not all(callable(callback) for callback in callbacks) or not isinstance(limits, ArchiveVerificationLimits)
            or any(type(seconds) is not int or not 1 <= seconds <= MAX_MIGRATION_SECONDS
                   for seconds in (preflight_seconds, outage_seconds))
            or conn is control or any(session.closed or session.invalidated or session.in_transaction()
                                      or session.get_execution_options().get("isolation_level") == "AUTOCOMMIT"
                                      for session in (conn, control))):
        raise ValueError("canonical_cold_execute_invalid_owner")

    package = load_approved_package(package_path, approved_package_hash=approved_package_hash, inventory=inventory)
    if outage_seconds > package["max_outage_seconds"]:
        raise ValueError("canonical_cold_execute_outage_exceeds_approved_budget")
    witness = read_approved_candidate_witness(package_path, package)
    capture_provider_tail, verify_provider_boundary, verify_provider_restart = bind_approved_collector_checks(
        conn, package_path=package_path, package=package, store=store, receipt_root=provider_receipt_root,
    )
    recovery_check = bind_approved_recovery_checks(
        package_path=package_path, package=package, receipt_root=provider_receipt_root,
        verify_site_restore=verify_recovery_boundary,
    )
    scope = package["scope"]
    audit = validate_audit_receipts(
        receipts, target_identity=scope["target_database"], source_identity=scope["source_database"],
    )
    if (audit["audit_set_hash"] != scope["audit_set_hash"]
            or audit["loaded_inventory_hash"] != scope["loaded_inventory_hash"]):
        raise RuntimeError("canonical_cold_execute_audit_changed")

    deadline = monotonic() + preflight_seconds

    def check_preflight():
        if monotonic() >= deadline:
            raise TimeoutError("canonical_cold_execute_preflight_deadline")

    with conn.begin():
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        with _bounded_step(conn, preflight_seconds):
            original = _read_open_original(conn, control, package=package, timeout_seconds=preflight_seconds)
            verified = verify_mounted_candidate(conn, audit=audit, store=store, limits=limits, check_budget=check_preflight)
    check_preflight()

    provider_tail = None

    def before_pause(check_deadline):
        nonlocal provider_tail
        verified.check_budget = check_deadline
        verified.assert_unchanged()
        provider_tail = capture_provider_tail(check_deadline)
        if provider_tail is None:
            raise RuntimeError("canonical_cold_execute_provider_tail_missing")
        if conn.in_transaction() or control.in_transaction():
            raise RuntimeError("canonical_cold_execute_provider_capture_connection_left_in_transaction")

    with hold_cold_cutover(
        state_root, inventory=inventory, package_path=package_path, approved_package_hash=approved_package_hash,
        outage_deadline=time() + outage_seconds, before_pause=before_pause,
    ) as (approved, assert_held, record, remaining, resume):
        database_phase = None
        database_outcome = None

        def record_phase(phase, evidence):
            nonlocal database_phase, database_outcome
            # Any attempted fence phase makes a pre-fence restart ineligible.
            database_phase = phase
            record(phase, evidence)
            if phase in ("database_reopened", "database_unchanged"):
                database_outcome = evidence["outcome"]

        def verify_drained(target, watermark):
            return verify_provider_boundary(target, watermark, provider_tail)

        try:
            activation = run_fenced_transition(
                conn, control, package=approved, approved_package_hash=approved_package_hash, receipts=receipts,
                store=store, limits=limits, timeout_seconds=outage_seconds, assert_host_held=assert_held,
                host_remaining=remaining, verify_provider_boundary=verify_drained,
                verify_recovery_boundary=recovery_check, record=record_phase,
                verify_candidate=verify_candidate, candidate_witness=witness, verified_objects=verified,
            )
        except Exception as error:
            if database_phase is None:
                try:
                    assert_held()
                    if any(session.closed or session.invalidated or session.in_transaction()
                           for session in (conn, control)):
                        raise RuntimeError("canonical_cold_execute_original_connection_lost")
                    seconds = min(10, int(remaining()))
                    with conn.begin():
                        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                        with _bounded_step(conn, seconds):
                            untouched = _read_open_original(conn, control, package=approved, timeout_seconds=seconds)
                    if (untouched["database"] != original["database"]
                            or untouched["source_commit_seq"] < original["source_commit_seq"]):
                        raise RuntimeError("canonical_cold_execute_original_changed")
                    record_phase("database_unchanged", {"outcome": "original", **untouched})
                except Exception as proof_error:
                    error.add_note(f"The original runtime remains held ({type(proof_error).__name__}).")
            if database_outcome == "original":
                try:
                    resume(verify_original_ready)
                except Exception as restart_error:
                    raise restart_error from error
                error.add_note("The original runtime passed restart checks.")
            raise

        if database_outcome != "committed":
            raise RuntimeError("canonical_cold_execute_committed_outcome_unverified")

        def verify_started(services):
            application = verify_candidate_ready(activation, services)
            if not isinstance(application, dict) or application.get("ready") is not True:
                return application
            return {**application, "collector_restart_sha256": verify_provider_restart(services)}

        runtime = resume(
            verify_started,
            verify_before_start=partial(verify_candidate_prestart, activation),
        )
    return {"activation": activation, "runtime": runtime}
