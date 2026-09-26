"""Bounded online copy passes for the fixed preserving v1-to-v2 migration.

Internal preparation only. The source remains authoritative and writable.
No host pause, schema switch, archive-root activation or readiness certificate.
Preparation/relocation and the eventual short switch have separate admission.
"""
from __future__ import annotations

import logging
from time import monotonic

from sqlalchemy import text

from portal.backend.service.storage.header_resource_claims import _limits
from scripts.db import archive_reference_v2_placement as retained
from scripts.db import fact_header_v2_copy as headers, raw_mapping_v2_copy as raw
from scripts.db.fact_header_v2_capture import capture_remaining_seconds
from scripts.db.fact_header_v2_handoff import _staging_transaction
from scripts.db.fact_header_v2_placement import CopyPlacement

logger = logging.getLogger(__name__)


def _phase(header, lookup):
    # Finish each finite baseline before following an unbounded live tail.
    # Keep the proven SSD allocation order: identities retire before raw grows.
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
              page_rows=128, max_duration_seconds=60, cancelled=None):
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
    deadline = started + max_duration_seconds
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
                deadline=deadline, cancelled=cancelled) as (conn, saved):
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
