"""Transactional intent and capacity ownership for historical header movement.

Internal repository boundary, not an API or executor. The caller owns the
transaction: returned receipts are provisional until it commits. A reservation
never authorizes DDL. Prepared tablespace identity is bound into the review; fresh locked physical
checks and execution still belong to the future worker.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date
from uuid import uuid4

from sqlalchemy import select, text

from core.storage_targets import StoragePolicy
from portal.backend.db.storage_target_models import (
    StorageHeaderBatchRecord, StorageHeaderMoveRecord, StorageHeaderTablespaceRecord,
    StoragePlanRecord, StoragePolicyRecord, StorageTargetRecord,
)
from portal.backend.service.storage_management import StorageConflict, _target
from .header_filesystem import VerifiedHeaderPlacement
from .header_resource_claims import _release_resource_claims
from .header_admission import lock_header_storage as _lock, fresh_header_database, registered_header_targets
from .header_destinations import review_header_moves, stable_header_destination

logger = logging.getLogger(__name__)
_ACTIVE = ("reserved", "running", "blocked")


def _rows(session, batch):
    rows = list(session.scalars(select(StorageHeaderMoveRecord).where(
        StorageHeaderMoveRecord.plan_id == batch.plan_id
    ).order_by(StorageHeaderMoveRecord.storage_day).limit(4097).execution_options(populate_existing=True)))
    if len(rows) != batch.group_count or len(rows) > 4096:
        raise StorageConflict("storage_journal_incomplete")
    if batch.cancelled_at is not None and any(row.state != "cancelled" for row in rows):
        raise StorageConflict("storage_journal_incomplete")
    return rows


def _receipt(batch, rows, *, reused):
    return {
        "plan_id": batch.plan_id, "review_hash": batch.review_hash,
        "reused": reused, "group_count": batch.group_count,
        "cancelled": batch.cancelled_at is not None,
        "moves": [{"id": row.id, "storage_day": row.storage_day.isoformat(),
                   "target_id": row.target_id, "state": row.state,
                   "reserved_bytes": row.reserved_bytes if row.state in _ACTIVE else 0}
                  for row in rows],
        "execution_available": False, "activation_ready": False,
    }


def reserve_header_batch(session, *, plan_id, review_hash, verified, max_moves=None):
    """Save a reviewed proposal and all copy reservations atomically.

    Only a worker with an already queued/running plan can call this boundary.
    The current public queue endpoint remains disabled. The verified argument
    is internal filesystem-adapter evidence, never deserialized user input.
    Replays return the durable receipt, even after its evidence expires; they
    do not reserve again or imply the old physical evidence is still current.
    """
    if not isinstance(review_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", review_hash):
        raise ValueError("storage_journal_invalid_review_hash")
    if not isinstance(verified, VerifiedHeaderPlacement):
        raise ValueError("storage_journal_verified_inventory_required")
    _lock(session)
    batch = session.get(StorageHeaderBatchRecord, plan_id, populate_existing=True)
    if batch is not None:
        if batch.review_hash != review_hash:
            raise StorageConflict("storage_journal_review_conflict")
        return _receipt(batch, _rows(session, batch), reused=True)

    plan = session.get(StoragePlanRecord, plan_id, populate_existing=True)
    if plan is None or plan.state not in ("queued", "running"):
        raise StorageConflict("storage_journal_plan_not_queued")
    config = session.get(StoragePolicyRecord, 1, populate_existing=True)
    if config is None or config.revision != plan.base_revision:
        raise StorageConflict("storage_policy_changed")
    policy = StoragePolicy.from_dict(plan.policy)
    if plan.policy_hash != policy.fingerprint or not policy.movement_enabled:
        raise StorageConflict("storage_journal_policy_not_admitted")

    identity = fresh_header_database(session, verified)
    targets = registered_header_targets(session)
    target_values = [_target(row) for row in targets]
    try:
        proposal = review_header_moves(
            verified=verified, policy=policy, targets=target_values,
            reserved_bytes={row.id: row.reserved_bytes + row.auxiliary_reserved_bytes for row in targets},
            max_moves=max_moves,
        )
    except ValueError as exc:
        raise StorageConflict("storage_journal_review_invalid: " + str(exc)) from exc
    if not proposal["planning_complete"]:
        raise StorageConflict("storage_journal_placement_blocked")
    if proposal["plan_hash"] != review_hash:
        raise StorageConflict("storage_journal_review_changed")
    target_by_id = {target.target_id: target for target in target_values}
    for target_id, destination in proposal["destination_evidence"].items():
        registration = session.get(StorageHeaderTablespaceRecord, (identity, target_id), populate_existing=True)
        if (registration is None or registration.tablespace_oid != destination["tablespace_oid"]
                or registration.binding != stable_header_destination(destination, target_by_id[target_id])):
            raise StorageConflict("storage_journal_destination_not_registered")
    oids = [move["heap"]["oid"] for move in proposal["moves"]]
    if oids and session.scalar(select(StorageHeaderMoveRecord.id).where(
        StorageHeaderMoveRecord.database_identity == identity,
        StorageHeaderMoveRecord.heap_oid.in_(oids),
        StorageHeaderMoveRecord.state.in_(_ACTIVE),
    ).limit(1)):
        raise StorageConflict("storage_journal_group_already_owned")

    # Validate the entire bounded batch before mutating tracked ORM objects.
    for move in proposal["moves"]:
        if len(json.dumps(move, ensure_ascii=False).encode()) > 65536:
            raise StorageConflict("storage_journal_group_evidence_budget")
    additions = proposal["additional_copy_reservations"]
    for target in targets:
        if target.reserved_bytes + target.auxiliary_reserved_bytes + additions.get(target.id, 0) > 2**63 - 1:
            raise StorageConflict("storage_journal_reservation_overflow")
    batch = StorageHeaderBatchRecord(
        plan_id=plan_id, review_hash=review_hash, policy_hash=policy.fingerprint,
        base_revision=plan.base_revision, database_identity=identity,
        group_count=len(oids), captured_at=verified.snapshot.captured_at,
        verified_at=verified.verified_at,
    )
    session.add(batch)
    session.flush()
    rows = []
    for move in proposal["moves"]:
        row = StorageHeaderMoveRecord(
            id="header_" + uuid4().hex, plan_id=plan_id,
            database_identity=identity, storage_day=date.fromisoformat(move["storage_day"]),
            heap_oid=move["heap"]["oid"], target_id=move["destination_target_id"],
            filesystem_uuid=move["destination_filesystem_uuid"], source_group=move,
            destination_binding=dict(proposal["destination_evidence"][move["destination_target_id"]]),
            reserved_bytes=move["reserve_copy_bytes"], state="reserved",
        )
        session.add(row)
        rows.append(row)
    for target in targets:
        target.reserved_bytes += additions.get(target.id, 0)
    session.flush()
    logger.info("storage_header_reservation_staged | plan_id=%s review_hash=%s groups=%s copy_bytes=%s",
                plan_id, review_hash, len(rows), sum(additions.values()))
    return _receipt(batch, rows, reused=False)


def cancel_unstarted_header_batch(session, *, plan_id, review_hash):
    """Release only a wholly unstarted reservation, in the caller transaction.

    No timeout handler calls this automatically. Running, blocked, completed or
    mixed batches require worker reconciliation, which is not implemented here.
    """
    _lock(session)
    batch = session.get(StorageHeaderBatchRecord, plan_id, populate_existing=True)
    if batch is None or batch.review_hash != review_hash:
        raise StorageConflict("storage_journal_review_conflict")
    rows = _rows(session, batch)
    if batch.cancelled_at is not None:
        if any(row.state != "cancelled" for row in rows):
            raise StorageConflict("storage_journal_incomplete")
        return _receipt(batch, rows, reused=True)
    if any(row.state != "reserved" for row in rows):
        raise StorageConflict("storage_journal_reconciliation_required")
    release = {}
    for row in rows:
        release[row.target_id] = release.get(row.target_id, 0) + row.reserved_bytes
    targets = {}
    for target_id, amount in release.items():
        target = session.get(StorageTargetRecord, target_id, populate_existing=True)
        if target is None or target.reserved_bytes < amount:
            raise StorageConflict("storage_journal_reservation_inconsistent")
        targets[target_id] = target
    now = session.scalar(text("SELECT clock_timestamp()"))
    _release_resource_claims(session, rows)
    batch.cancelled_at = now
    for row in rows:
        row.state, row.updated_at = "cancelled", now
    for target_id, amount in release.items():
        targets[target_id].reserved_bytes -= amount
    session.flush()
    logger.info("storage_header_cancellation_staged | plan_id=%s review_hash=%s groups=%s release_bytes=%s",
                plan_id, review_hash, len(rows), sum(release.values()))
    return _receipt(batch, rows, reused=False)
