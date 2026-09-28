"""Bounded online copy passes for the fixed preserving v1-to-v2 migration.

Internal preparation only. The source remains authoritative and writable.
No host pause, schema switch, archive-root activation or readiness certificate.
Preparation/relocation and the eventual short switch have separate admission.
"""
from __future__ import annotations

import logging
import math
from pathlib import Path
from time import monotonic

from sqlalchemy import text

from portal.backend.service.storage.header_resource_claims import _limits
from scripts.db import archive_reference_v2_placement as retained
from scripts.db import fact_header_v2_copy as headers, raw_mapping_v2_copy as raw
from scripts.db import archive_root_v2_online as archive_online
from scripts.db import archive_root_v2_copy as archives
from scripts.db import fact_header_v2_online_proof as protection
from scripts.db import fact_header_v2_references as references
from scripts.db.fact_header_v2_capture import (
    DEFAULT_ATTEMPT_SECONDS, capture_remaining_seconds, inspect_capture)
from core.storage_move_budget import MAX_MIGRATION_SECONDS
from scripts.db.fact_header_v2_handoff import _staging_transaction
from scripts.db.fact_header_v2_placement import CopyPlacement

logger = logging.getLogger(__name__)



def prepare_attempt(engine, *, placement, policy, resource_limits, source_root,
                    destination_root, attempt_seconds=DEFAULT_ATTEMPT_SECONDS,
                    max_duration_seconds=30, cancelled=None):
    """Atomically install fixed online captures and exact empty-shadow guards.

    The caller already admits host identity, source runtime and mounts. This
    short transaction may briefly fence writers, using NOWAIT; it does not stop
    clients, move tables or copy a baseline. Failure rolls back every new capture.
    Retry verifies the existing binding/protection and retains its original
    start and duration, even when a different duration is requested.
    """
    limits = _limits(resource_limits, migration=True)
    if (not isinstance(placement, CopyPlacement)
            or type(attempt_seconds) is not int
            or not 1 <= attempt_seconds <= MAX_MIGRATION_SECONDS
            or type(max_duration_seconds) is not int or not 1 <= max_duration_seconds <= 60
            or (cancelled is not None and not callable(cancelled))):
        raise ValueError("fact_header_online_preparation_inputs_invalid")
    if (policy.archives != policy.history or policy.backups != policy.history
            or not policy.movement_enabled or not policy.backup_enabled):
        raise ValueError("fact_header_online_fixed_automatic_policy_required")
    retained._fixed_inputs(policy, limits, (placement.recent, placement.history))
    if cancelled is not None and cancelled():
        raise RuntimeError("storage_move_cancelled")
    seconds = min(max_duration_seconds, limits["movement_timeout_seconds"])
    started = monotonic()
    with _staging_transaction(engine, placement=placement, policy=policy,
            limits={**limits, "movement_timeout_seconds": seconds},
            deadline=started+seconds, cancelled=cancelled) as (conn, saved):
        cutoff = conn.scalar(text(
            "SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date - :days"),
            {"days": policy.recent_days})
        if placement.history_before > cutoff:
            raise RuntimeError("fact_header_online_recent_window_on_history")
        archives._root(source_root, saved["recent_device"])
        destination, _ = archives._root(destination_root, saved["history_device"])
        if not destination.is_relative_to(Path(placement.history.root).resolve(strict=True)):
            raise RuntimeError("fact_header_online_archive_outside_fixed_target")
        headers.prepare_copy(conn, placement=placement, timeout_seconds=seconds,
                             attempt_seconds=attempt_seconds, identity_on_history=True)
        # The raw manifest FK must precede archive trigger binding. Integrity
        # guards must precede the first copied row, including on a retry.
        raw.prepare_copy(conn, timeout_seconds=seconds)
        protection.prepare(conn, timeout_seconds=seconds)
        archive_online.prepare(conn, source_root=source_root,
                               destination_root=destination_root, timeout_seconds=seconds)
        attempt = inspect_capture(conn)
        capture_remaining_seconds(conn)
    result = {"schema_version": "qt.fact_header_online_preparation.v1",
              "started_at": attempt["started_at"],
              "elapsed_seconds": monotonic()-started,
              "source_authoritative": True, "migration_ready": False,
              "final_switch_authorized": False}
    logger.info("fact_header_online_preparation_completed | attempt=%s elapsed_seconds=%.3f",
                result["started_at"], result["elapsed_seconds"])
    return result


_PREPARATION_STEPS = {
    "identity_history": headers.place_identity_on_history,
    "raw_history": raw.place_on_history,
    "identity_capture": headers.enable_identity_capture,
    "reference_prepare": references.prepare_reference,
    "reference_validate": references.validate_reference,
    "reference_adopt": references.adopt_payload_references,
}


def preparation_step(engine, *, step, placement, policy, resource_limits,
                     expected_started_at, relation=None, page_rows=128,
                     max_duration_seconds=60, cancelled=None):
    """Execute one explicitly admitted private-move/reference transaction.

    Large relocations and FK validations are never hidden in a copy-page or
    status request. Their caller supplies measured per-step resource/time
    allowances. Earlier committed steps survive failure; retry re-inspects
    actual placement and constraint identities under the original capture clock.
    This internal boundary has no host pause, runtime or final-switch authority.
    """
    limits = _limits(resource_limits, migration=True)
    relation_step = isinstance(step, str) and step in {"reference_prepare", "reference_validate"}
    if (not isinstance(step, str) or step not in _PREPARATION_STEPS
            or not isinstance(placement, CopyPlacement)
            or not isinstance(expected_started_at, str) or not expected_started_at
            or (relation_step and (not isinstance(relation, str) or not relation))
            or (not relation_step and relation is not None)
            or type(page_rows) is not int or not 1 <= page_rows <= 4096
            or type(max_duration_seconds) is not int
            or not 1 <= max_duration_seconds <= MAX_MIGRATION_SECONDS
            or (cancelled is not None and not callable(cancelled))):
        raise ValueError("fact_header_online_preparation_step_inputs_invalid")
    if (policy.archives != policy.history or policy.backups != policy.history
            or not policy.movement_enabled or not policy.backup_enabled):
        raise ValueError("fact_header_online_fixed_automatic_policy_required")
    retained._fixed_inputs(policy, limits, (placement.recent, placement.history))
    if cancelled is not None and cancelled():
        raise RuntimeError("storage_move_cancelled")
    seconds = min(max_duration_seconds, limits["movement_timeout_seconds"])
    started = monotonic()
    with _staging_transaction(engine, placement=placement, policy=policy,
            limits={**limits, "movement_timeout_seconds": seconds},
            deadline=started+seconds, cancelled=cancelled) as (conn, saved):
        if inspect_capture(conn)["started_at"] != expected_started_at:
            raise RuntimeError("fact_header_online_attempt_binding_changed")
        header, lookup = headers._inspect_progress(conn), raw._inspect(conn)
        if header["placement"] != saved:
            raise RuntimeError("fact_header_online_placement_changed")
        protection.inspect_protection(conn)
        cutoff = conn.scalar(text(
            "SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date - :days"),
            {"days": policy.recent_days})
        if placement.history_before > cutoff:
            raise RuntimeError("fact_header_online_recent_window_on_history")
        if conn.scalar(text("SELECT to_regclass(:relation)"),
                       {"relation": retained.RETAINED_LEGACY}) is not None:
            if retained.inspect_reference_catalog(
                    conn, relation=retained.RETAINED_LEGACY)["placement"] != "history":
                raise RuntimeError("fact_header_online_retained_relocation_required")
        if step == "raw_history":
            # A fresh attempt retires raw staging before allocating headers.
            # Already-started header copies keep their original allocation order.
            allowed = lookup["baseline_complete"] and (
                lookup["history_ready"] or header["after_day"] is None
                or (header["baseline_complete"] and header["identity_history_ready"]))
        else:
            allowed = (header["baseline_complete"]
                and (step == "identity_history" or header["identity_history_ready"])
                and (step == "identity_history" or lookup["history_ready"]))
        if not allowed:
            raise RuntimeError("fact_header_online_preparation_order_required")
        args = {"timeout_seconds": seconds}
        if relation_step:
            # The existing reference helper admits and quotes only incoming
            # source FK relations. This is not an arbitrary SQL/table mover.
            args["relation"] = relation
        if step == "identity_capture":
            args["page_rows"] = page_rows
        report = _PREPARATION_STEPS[step](conn, **args)
        capture_remaining_seconds(conn)
    result = {"schema_version": "qt.fact_header_online_preparation_step.v1",
              "step": step, "relation": relation,
              "started_at": expected_started_at,
              "elapsed_seconds": monotonic()-started, "committed": True,
              "source_authoritative": True, "migration_ready": False,
              "final_switch_authorized": False}
    # No serialized placement/resource report can become further authority.
    for key in ("reused", "validated", "references_complete"):
        if key in report:
            result[key] = report[key]
    logger.info("fact_header_online_preparation_step_completed | step=%s relation=%s elapsed_seconds=%.3f",
                step, relation, result["elapsed_seconds"])
    return result

def _phase(header, lookup):
    # Finish finite raw staging and retire it before new headers grow on SSD.
    # The durable header cursor preserves the old order for an already-started
    # attempt; changing its order could add raw staging to existing headers/IDs.
    if (not header["baseline_complete"] and header["after_day"] is None
            and not lookup["history_ready"]):
        return "raw_relocation_required" if lookup["baseline_complete"] else "raw_baseline"
    if not header["baseline_complete"]:
        return "header_baseline"
    if not header["identity_history_ready"]:
        return "identity_relocation_required"
    if not lookup["baseline_complete"]:
        return "raw_baseline"
    if not lookup["history_ready"]:
        return "raw_relocation_required"
    return "catch_up"


def copy_pass(engine, *, placement, policy, resource_limits, max_pages=32,
              page_rows=128, max_duration_seconds=60, cancelled=None,
              tail_only=False, deadline=None, connection=None):
    """Advance existing cursors with live writers and return after bounded work.

    Each successful page commits separately under the existing disk/WAL/temp
    watch. Retry reads the original capture deadline; this local pass allowance
    cannot extend or revive it. An elapsed pass ends between pages, while an
    expired attempt, resource failure, cancellation or interrupted page fails
    loud. A single page can consume only the remaining pass allowance.

    The caller must explicitly prepare the fixed shadow and relocate the known
    retained rollback table before this entrypoint. Large private-table moves
    are reported as phase boundaries, never hidden inside a small copy pass.
    """
    limits = _limits(resource_limits, migration=True)
    if (not isinstance(placement, CopyPlacement)
            or type(max_pages) is not int or not 2 <= max_pages <= 4096
            or type(page_rows) is not int or not 1 <= page_rows <= 4096
            or type(max_duration_seconds) is not int or not 1 <= max_duration_seconds <= 3600
            or (cancelled is not None and not callable(cancelled))):
        raise ValueError("fact_header_online_pass_inputs_invalid")
    if (policy.archives != policy.history or policy.backups != policy.history
            or not policy.movement_enabled or not policy.backup_enabled):
        raise ValueError("fact_header_online_fixed_automatic_policy_required")
    retained._fixed_inputs(policy, limits, (placement.recent, placement.history))
    started = monotonic()
    if (type(tail_only) is not bool or (deadline is not None and
            (type(deadline) not in (int, float) or not math.isfinite(deadline) or deadline <= started))):
        raise ValueError("fact_header_online_tail_deadline_invalid")
    deadline = min(deadline if deadline is not None else float("inf"), started + max_duration_seconds)
    pages = {"headers": 0, "raw": 0}
    rows = {"headers": 0, "raw": 0}
    # One pass probes both tails. Re-entering always starts with headers, but
    # a one-page pass is not permitted for catch-up: that would starve raw.
    tail_seen = set()
    next_tail = "headers"
    outcome = "page_budget_reached"
    phase = None

    while sum(pages.values()) < max_pages:
        if cancelled is not None and cancelled():
            raise RuntimeError("storage_move_cancelled")
        seconds = min(limits["movement_timeout_seconds"], int(deadline-monotonic()))
        if seconds < 1:
            outcome = "pass_time_budget_reached"
            break
        with _staging_transaction(engine, placement=placement, policy=policy,
                limits={**limits, "movement_timeout_seconds": seconds},
                deadline=deadline, cancelled=cancelled, connection=connection) as (conn, saved):
            cutoff = conn.scalar(text(
                "SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date - :days"),
                {"days": policy.recent_days})
            if placement.history_before > cutoff:
                raise RuntimeError("fact_header_online_recent_window_on_history")
            header = headers._inspect_progress(conn)
            lookup = raw._inspect(conn)
            if header["placement"] != saved:
                raise RuntimeError("fact_header_online_placement_changed")
            # Never infer SSD release merely from the presence of a move plan.
            if conn.scalar(text("SELECT to_regclass(:relation)"),
                           {"relation": retained.RETAINED_LEGACY}) is not None:
                if retained.inspect_reference_catalog(
                        conn, relation=retained.RETAINED_LEGACY)["placement"] != "history":
                    raise RuntimeError("fact_header_online_retained_relocation_required")
            remaining = capture_remaining_seconds(conn)
            phase = _phase(header, lookup)
            if tail_only and phase != "catch_up":
                raise RuntimeError("fact_header_online_tail_requires_completed_preparation")
            if phase.endswith("_required"):
                outcome = phase
                break
            if phase == "catch_up":
                # Alternate, even if one source never becomes quiet. A finite
                # pass must not chase the header tail forever before touching raw.
                family = next_tail
                next_tail = "raw" if family == "headers" else "headers"
            else:
                family = "headers" if phase == "header_baseline" else "raw"
            copier = headers if family == "headers" else raw
            report = copier.copy_page(conn, page_rows=page_rows,
                                      timeout_seconds=min(seconds, max(1, int(remaining))))
        pages[family] += 1
        rows[family] += report["verified_page_rows"]
        if phase == "catch_up":
            if report["caught_up_at_observation"]:
                tail_seen.add(family)
            else:
                tail_seen.discard(family)
            if len(tail_seen) == 2:
                outcome = "both_tails_observed_empty"
                break

    result = {"schema_version": "qt.fact_header_online_copy_pass.v1",
              "outcome": outcome, "phase": phase, "committed_pages": pages,
              "verified_page_rows": rows, "elapsed_seconds": monotonic()-started,
              "source_authoritative": True, "migration_ready": False,
              "final_switch_authorized": False}
    logger.info("fact_header_online_copy_pass_completed | outcome=%s pages=%s rows=%s",
                outcome, pages, rows)
    return result
