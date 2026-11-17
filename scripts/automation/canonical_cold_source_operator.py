"""Stage and review an independent receiver's recovery point while the source serves."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import signal
from time import monotonic

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.incremental_recovery import _binary, _private_bytes
from scripts.automation.canonical_cold_named_recovery import _publish, verify_receiver_preflight, verify_signed_recovery
from scripts.automation.canonical_cold_package import _hash_file, _pin
from scripts.automation.canonical_cold_receiver_operator import (
    OFFER_VERSION, REPLAY_SELECTION_VERSION, _fields, _load_site, _private_binary, _read_json,
)
from scripts.automation.canonical_cold_receiver_ready import verify_replayed_checkpoint_baseline
from scripts.automation.canonical_cold_recovery_checks import REPLAYED_PLAN_VERSION
from scripts.automation.canonical_cold_source_prefix import stage_source_wal_prefixes
from scripts.automation.canonical_cold_source_target import qualify_source_wal_target
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits, _identity, _private_root
from scripts.db.canonical_cold_recovery_suffix import prepare_replayed_archive_baseline
from scripts.db.canonical_cold_recovery_wal_transfer import _count_restart_segments, check_source_wal_capacity


PLAN_VERSION = "qt.canonical_cold_source_operator.v1"
_MAX_SELECTION_BYTES = 8 * 1024**2


def _deadline(timeout_seconds):
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= MAX_MIGRATION_SECONDS:
        raise ValueError("canonical_source_operator_time_budget_invalid")
    deadline = monotonic() + timeout_seconds

    def remaining():
        left = int(deadline - monotonic())
        if left < 1:
            raise TimeoutError("canonical_source_operator_deadline")
        return left

    return remaining


def _verified_preflight(policy, signed):
    measured = verify_receiver_preflight(signed, policy.receiver_public_key)
    if measured["site_policy"] != policy.preflight_scope():
        raise ValueError("canonical_source_operator_receiver_policy_changed")
    return measured


def _local_state(path, *, source, receiving):
    state, identity = _private_root(Path(path))
    if any(state.is_relative_to(other) or other.is_relative_to(state) for other in (Path(source), Path(receiving))):
        raise ValueError("canonical_source_operator_state_overlap")
    return state, identity


def _save_evidence(state, prefix, body, *, check):
    path = state / f"{prefix}-{archive_evidence_hash(body)}.json"
    check()
    try:
        _publish(path, body)
    except FileExistsError:
        if _read_json(path, max_bytes=_MAX_SELECTION_BYTES) != body:
            raise RuntimeError("canonical_source_operator_published_evidence_changed") from None
    check()
    digest, _ = _hash_file(path, json_limit=_MAX_SELECTION_BYTES)
    return path, digest


def stage_recovery_selection(*, control, expected_database, source, policy, receiver_preflight, target, limits,
                             state_directory, pg_waldump, pgbackrest, pgdata, database_key, max_batches, timeout_seconds):
    """Resume closed WAL staging and publish a natively verified receiver input."""
    remaining = _deadline(timeout_seconds)
    measured = _verified_preflight(policy, receiver_preflight)
    _fields(target, "segment target_lsn replay_lsn record_kind restore_name", "canonical_source_operator_invalid_target")
    if (not isinstance(limits, TransferLimits) or any(getattr(limits, key) > getattr(policy.wal, key)
            for key in ("seconds", "bytes", "files")) or limits.reserve_bytes < policy.wal.reserve_bytes):
        raise ValueError("canonical_source_operator_prefix_outside_site_budget")
    state, state_id = _local_state(state_directory, source=source, receiving=policy.receiving)
    receiving, receiving_id = _private_root(policy.receiving)
    baseline = policy.baseline_verification

    def check():
        if _identity(state.lstat()) != state_id or _identity(receiving.lstat()) != receiving_id:
            raise RuntimeError("canonical_source_operator_mount_changed")
        return remaining()

    prefix = stage_source_wal_prefixes(control=control, expected_database=expected_database, source=source,
        receiving=receiving, base_transfer_sha256=policy.base_transfer_sha256, baseline=baseline,
        target_through=target["segment"], limits=limits, progress_path=state / "wal-prefix-progress.json",
        max_batches=max_batches, timeout_seconds=check())
    terminal = prefix["terminal"]
    native = qualify_source_wal_target(pg_waldump=pg_waldump, pgbackrest=pgbackrest, pgdata=pgdata, repository=receiving / "database",
        database_key=database_key, workspace=state, archive_id=baseline["database_archive_id"], segment=target["segment"],
        expected_wal_sha1=terminal["wal_checksum"], wal_segment_bytes=baseline["wal_segment_bytes"],
        target_lsn=target["target_lsn"], replay_lsn=target["replay_lsn"], record_kind=target["record_kind"],
        restore_name=target["restore_name"], timeout_seconds=min(limits.seconds, check()),
        expected_system_identifier=baseline["database_identity"].split("/")[0])
    if native["segment"] != prefix["target_through"] or native["segment"] != terminal["segment"]:
        raise RuntimeError("canonical_source_operator_verified_terminal_changed")
    selection = {"schema": REPLAY_SELECTION_VERSION, "baseline_verification_sha256": baseline["verification_sha256"],
                 "receiver_preflight_sha256": measured["measurement_sha256"],
                 **{field: native[field] for field in ("timeline", "target_lsn", "replay_lsn")}, "prefixes": prefix["prefixes"]}
    target_path, target_sha256 = _save_evidence(state, "qualified-wal-target", native, check=check)
    selection_path, selection_sha256 = _save_evidence(state, "replay-selection", selection, check=check)
    return {"selection": selection, "selection_path": selection_path, "selection_sha256": selection_sha256,
            "target_receipt": native, "target_path": target_path, "target_sha256": target_sha256, "prefix_report": prefix}


def review_replayed_offer(*, control, expected_database, policy, receiver_preflight, receiver_plan_sha256,
                          selection, offer_path, pending_spool_scope_sha256, max_restart_segments, timeout_seconds):
    """Validate signed receiver custody and restarts before drafting recovery evidence."""
    remaining = _deadline(timeout_seconds)
    measured = _verified_preflight(policy, receiver_preflight)
    baseline = policy.baseline_verification
    receiving, receiving_id = _private_root(policy.receiving)
    offer_path = Path(offer_path)
    if (offer_path.parent != receiving or not _pin(receiver_plan_sha256)
            or type(max_restart_segments) is not int or not 1 <= max_restart_segments <= 2**44
            or (pending_spool_scope_sha256 is not None) != measured["pending_expected"]
            or (pending_spool_scope_sha256 is not None and not _pin(pending_spool_scope_sha256))):
        raise ValueError("canonical_source_operator_review_scope_invalid")
    _fields(selection, "schema baseline_verification_sha256 receiver_preflight_sha256 timeline target_lsn replay_lsn prefixes",
            "canonical_source_operator_selection_invalid")
    if (selection["schema"] != REPLAY_SELECTION_VERSION or selection["baseline_verification_sha256"] != baseline["verification_sha256"]
            or selection["receiver_preflight_sha256"] != measured["measurement_sha256"]
            or not isinstance(selection["prefixes"], list) or not selection["prefixes"]):
        raise ValueError("canonical_source_operator_selection_scope_changed")
    for batch in selection["prefixes"]:
        _fields(batch, "after through prefix_sha256 previous_prefix_sha256", "canonical_source_operator_selection_invalid")

    def check():
        if _identity(receiving.lstat()) != receiving_id:
            raise RuntimeError("canonical_source_operator_mount_changed")
        return remaining()

    envelope = _read_json(offer_path, max_bytes=1024**2)
    offer = verify_signed_recovery(envelope, policy.receiver_public_key)
    _fields(offer, "schema checkpoint site_plan_sha256 receiver_preflight_sha256 catalog",
            "canonical_source_operator_offer_invalid")
    if (offer["schema"] != OFFER_VERSION or offer["site_plan_sha256"] != receiver_plan_sha256
            or offer["receiver_preflight_sha256"] != measured["measurement_sha256"]
            or not isinstance(offer["checkpoint"], dict)):
        raise ValueError("canonical_source_operator_offer_scope_changed")
    checkpoint = offer["checkpoint"]
    pin = checkpoint.get("verification_sha256")
    through = verify_replayed_checkpoint_baseline(checkpoint, baseline_verification=baseline, expected_checkpoint_sha256=pin)
    if (offer_path.name != f"replayed-candidate-{pin}.json" or through != selection["prefixes"][-1]["through"]
            or any(checkpoint[key] != selection[key] for key in ("timeline", "target_lsn", "replay_lsn"))):
        raise ValueError("canonical_source_operator_offer_replay_changed")
    retained = _count_restart_segments(checkpoint["redo_wal_segment"], through, baseline["wal_segment_bytes"],
                                       max_restart_segments)
    if retained >= max_restart_segments:
        raise ValueError("canonical_source_operator_restart_budget_has_no_final_wal_room")
    catalog = offer["catalog"]
    _fields(catalog, "filename sha256 bytes", "canonical_source_operator_catalog_binding_invalid")
    if (catalog["sha256"] != checkpoint["catalog_inventory_sha256"]
            or catalog["filename"] != f"replayed-catalog-{catalog['sha256']}.jsonl"
            or type(catalog["bytes"]) is not int or catalog["bytes"] < 0):
        raise ValueError("canonical_source_operator_catalog_binding_invalid")
    manifest = receiving / catalog["filename"]
    if manifest.lstat().st_size != catalog["bytes"]:
        raise ValueError("canonical_source_operator_catalog_size_changed")
    inventory = prepare_replayed_archive_baseline(manifest, checkpoint=checkpoint, max_objects=policy.max_archive_objects, check=check)
    current_files = check_source_wal_capacity(control, expected_database=expected_database, backup_stop=through,
        segment_bytes=baseline["wal_segment_bytes"], max_segments=policy.wal.files, timeout_seconds=check(),
        restart_from=checkpoint["redo_wal_segment"], max_restart_segments=max_restart_segments)
    if _read_json(offer_path, max_bytes=1024**2) != envelope:
        raise RuntimeError("canonical_source_operator_offer_changed")
    check()
    scope = policy.preflight_scope()
    independent = {"schema": REPLAYED_PLAN_VERSION,
        **{key: scope[key] for key in ("base_transfer_sha256", "baseline_verification_sha256", "pair_receipt_sha256", "receiver_public_key")},
        "pending_spool_scope_sha256": pending_spool_scope_sha256, "receiver_preflight": receiver_preflight,
        "replayed_checkpoint_sha256": pin, "max_restart_segments": max_restart_segments}
    return {"independent_recovery": independent, "replayed_inventory": manifest, "checkpoint": checkpoint,
            "archive_objects": len(inventory.objects), "source_wal_files_now": current_files}


@contextmanager
def _source_control(credential):
    try:
        url = make_url(_private_bytes(credential, limit=8192).decode("utf-8").strip())
    except Exception:
        raise ValueError("canonical_source_operator_invalid_control_credential") from None
    if url.get_backend_name() != "postgresql" or url.database != "postgres":
        raise ValueError("canonical_source_operator_control_database_invalid")
    engine = create_engine(url, poolclass=NullPool, hide_parameters=True,
        connect_args={"connect_timeout": 5, "options": "-c default_transaction_read_only=on"})
    try:
        with engine.connect() as control:
            yield control
    finally:
        engine.dispose()


def _main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--plan-sha256", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("stage", help="Resume closed WAL staging and qualify the chosen replay point.")
    review = commands.add_parser("review", help="Verify a signed offer and write reviewed independent recovery evidence.")
    review.add_argument("--selection", required=True, type=Path)
    review.add_argument("--selection-sha256", required=True)
    review.add_argument("--offer", required=True, type=Path)
    review.add_argument("--receiver-plan-sha256", required=True)
    review.add_argument("--max-restart-segments", required=True, type=int)
    review.add_argument("--pending-spool-scope-sha256")
    args = parser.parse_args(argv)
    if not _pin(args.plan_sha256) or (args.command == "review" and not _pin(args.selection_sha256)):
        parser.error("plan and selection pins require 64 lowercase hexadecimal characters")
    plan = _read_json(args.plan, max_bytes=_MAX_SELECTION_BYTES, expected_hash=args.plan_sha256)
    _fields(plan, "schema site receiver_preflight source state_directory control_credential expected_database "
            "tools pgdata database_key target max_batches timeout_seconds", "canonical_source_operator_invalid_plan")
    if plan["schema"] != PLAN_VERSION:
        raise ValueError("canonical_source_operator_invalid_plan")
    _fields(plan["tools"], "pg_waldump pg_waldump_sha256 pgbackrest", "canonical_source_operator_invalid_tools")
    policy, _ = _load_site(plan)
    source, _ = _private_root(Path(plan["source"]))
    state, state_id = _local_state(plan["state_directory"], source=source, receiving=policy.receiving)
    _fields(plan["expected_database"], "database database_oid system_identifier postmaster_started_at allow_connections",
            "canonical_source_operator_invalid_database_pin")
    common = {"expected_database": plan["expected_database"], "policy": policy,
              "receiver_preflight": plan["receiver_preflight"], "timeout_seconds": plan["timeout_seconds"]}
    with _source_control(plan["control_credential"]) as control:
        if args.command == "stage":
            result = stage_recovery_selection(control=control, source=source, target=plan["target"], limits=policy.wal,
                state_directory=state, pg_waldump=_private_binary(plan["tools"]["pg_waldump"], plan["tools"]["pg_waldump_sha256"]),
                pgbackrest=_binary(plan["tools"]["pgbackrest"]), pgdata=Path(plan["pgdata"]), database_key=plan["database_key"],
                max_batches=plan["max_batches"],
                **common)
            print(json.dumps({"phase": "selection_staged", "selection": str(result["selection_path"]),
                              "selection_sha256": result["selection_sha256"], "target_receipt": str(result["target_path"]),
                              "target_receipt_sha256": result["target_sha256"]}, sort_keys=True))
        else:
            selection = _read_json(args.selection, max_bytes=_MAX_SELECTION_BYTES, expected_hash=args.selection_sha256)
            result = review_replayed_offer(control=control, receiver_plan_sha256=args.receiver_plan_sha256, selection=selection,
                offer_path=args.offer, pending_spool_scope_sha256=args.pending_spool_scope_sha256,
                max_restart_segments=args.max_restart_segments, **common)

            def check_state():
                if _identity(state.lstat()) != state_id:
                    raise RuntimeError("canonical_source_operator_mount_changed")

            path, digest = _save_evidence(state, "independent-recovery", result["independent_recovery"], check=check_state)
            print(json.dumps({"phase": "receiver_offer_reviewed", "independent_recovery": str(path),
                              "independent_recovery_sha256": digest, "replayed_inventory": str(result["replayed_inventory"]),
                              "checkpoint_sha256": result["checkpoint"]["verification_sha256"],
                              "archive_objects": result["archive_objects"],
                              "source_wal_files_now": result["source_wal_files_now"]}, sort_keys=True))


def main(argv=None):
    def terminate(signum, _frame):
        raise SystemExit(128 + signum)

    previous = signal.signal(signal.SIGTERM, terminate)
    try:
        _main(argv)
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == "__main__":
    main()
