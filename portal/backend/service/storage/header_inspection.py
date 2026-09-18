"""Inspect a reserved history move under the caller's execution locks.

This boundary does not execute DDL, mark completion or release reservations.
Copy-only capacity is insufficient for activation: WAL/temp/growth and recovery
budgets, physical execution and reconciliation remain separate worker work.
"""
from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from time import monotonic
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from sqlalchemy import text

from core.storage_header_placement import RelationPlacement
from core.storage_move_budget import assess_header_move_resources
from core.storage_targets import StoragePolicy, allocate_target
from portal.backend.db.storage_target_models import (
    StorageHeaderBatchRecord, StorageHeaderMoveRecord, StorageHeaderTablespaceRecord,
    StoragePlanRecord, StoragePolicyRecord,
)
from portal.backend.service.storage_management import StorageConflict, _target
from .header_admission import lock_header_storage, fresh_header_database, registered_header_targets
from .header_catalog import read_locked_header_group
from .header_destinations import checked_header_destinations, stable_header_destination
from .header_filesystem import VerifiedHeaderPlacement, verify_header_filesystem
from .header_journal import _rows
from .header_resources import VerifiedHeaderResources, observe_header_resources

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HeaderMoveInspection:
    move_id: str
    plan_id: str
    target_id: str
    tablespace_oid: int
    tablespace_name: str
    moving_relations: tuple[RelationPlacement, ...]
    copy_bytes: int
    reserved_copy_bytes: int
    verified: VerifiedHeaderPlacement
    execution_available: bool = False
    uncovered: tuple[str, ...] = (
        "wal_temporary_and_growth_headroom", "physical_execution_and_reconciliation",
        "recovery_copies_and_performance_acceptance",
    )


def _compare_reserved_group(move, verified):
    """Compare immutable relation identity and membership, allowing bounded growth."""
    if (not isinstance(verified, VerifiedHeaderPlacement)
            or verified.snapshot.inventory_complete
            or len(verified.snapshot.partitions) != 1):
        raise StorageConflict("storage_move_requires_partial_group")
    group, = verified.snapshot.partitions
    if (group.storage_day != move.storage_day or group.heap.oid != move.heap_oid
            or not group.index_inventory_complete or not group.toast_colocated):
        raise StorageConflict("storage_move_group_mismatch")
    source = move.source_group
    try:
        if not isinstance(source, dict) or not isinstance(source["indexes"], list) or len(source["indexes"]) > 64:
            raise ValueError("source group")
        heap = RelationPlacement(**source["heap"])
        indexes = tuple(RelationPlacement(**item) for item in source["indexes"])
        originals = (heap, *indexes)
        expected = {item.oid: item for item in originals}
        original_moving = [item for item in originals if item.target_id != move.target_id]
        original_bytes = sum(item.byte_count for item in original_moving)
        if (len(expected) != len(originals) or not original_moving
                or heap.oid != move.heap_oid or source["storage_day"] != move.storage_day.isoformat()
                or source["destination_target_id"] != move.target_id
                or source["destination_filesystem_uuid"] != move.filesystem_uuid
                or type(source["copy_bytes"]) is not int or source["copy_bytes"] != original_bytes
                or type(source["reserve_copy_bytes"]) is not int
                or source["reserve_copy_bytes"] != max(1, original_bytes)
                or source["reserve_copy_bytes"] != move.reserved_bytes
                or type(source["source_space_credited_bytes"]) is not int
                or source["source_space_credited_bytes"] != 0
                or source["requires_atomic_table_and_index_move"] is not True
                or source["requires_catalog_mount_capacity_and_policy_recheck"] is not True):
            raise ValueError("inconsistent intent")
    except (KeyError, TypeError, ValueError) as exc:
        raise StorageConflict("storage_move_intent_invalid") from exc
    current = {item.oid: item for item in group.relations}
    if len(current) != len(group.relations) or current.keys() != expected.keys():
        raise StorageConflict("storage_move_index_membership_changed")
    if any(replace(item, byte_count=expected[oid].byte_count) != expected[oid]
           for oid, item in current.items()):
        raise StorageConflict("storage_move_source_identity_changed")
    moving = tuple(item for item in group.relations if item.target_id != move.target_id)
    copy_bytes = sum(item.byte_count for item in moving)
    if max(1, copy_bytes) > move.reserved_bytes:
        raise StorageConflict("storage_move_copy_reservation_exceeded")
    return moving, copy_bytes


def inspect_reserved_header_move(session, *, move_id, review_hash, pg_controldata,
                                 timeout_seconds=30):
    """Hold ownership and selected-group locks while checking current intent.

    Caller owns this READ COMMITTED transaction and must roll back on any error.
    No caller-supplied filesystem proof is accepted; adapters read current
    catalog/files on this connection. Returned evidence is valid only for this
    inspection, not an independent authorization to execute or activate policy.
    """
    if not isinstance(move_id, str) or not re.fullmatch(r"header_[0-9a-f]{32}", move_id):
        raise ValueError("storage_move_invalid_id")
    if not isinstance(review_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", review_hash):
        raise ValueError("storage_move_invalid_review_hash")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise ValueError("storage_move_invalid_time_budget")
    lock_header_storage(session)
    move = session.get(StorageHeaderMoveRecord, move_id, populate_existing=True)
    if move is None:
        raise StorageConflict("storage_move_not_found")
    batch = session.get(StorageHeaderBatchRecord, move.plan_id, populate_existing=True)
    if batch is None or batch.review_hash != review_hash or batch.cancelled_at is not None:
        raise StorageConflict("storage_move_review_conflict")
    batch_rows = _rows(session, batch)  # Require complete bounded durable batch accounting.
    if move.state != "reserved":
        raise StorageConflict("storage_move_reconciliation_required")
    plan = session.get(StoragePlanRecord, move.plan_id, populate_existing=True)
    config = session.get(StoragePolicyRecord, 1, populate_existing=True)
    if plan is None or plan.state not in ("queued", "running"):
        raise StorageConflict("storage_move_plan_not_queued")
    if config is None or config.revision != plan.base_revision or batch.base_revision != plan.base_revision:
        raise StorageConflict("storage_policy_changed")
    policy = StoragePolicy.from_dict(plan.policy)
    if (not policy.movement_enabled or policy.fingerprint != plan.policy_hash
            or policy.fingerprint != batch.policy_hash):
        raise StorageConflict("storage_move_policy_not_admitted")
    target_rows = registered_header_targets(session)
    targets = tuple(_target(row) for row in target_rows)
    policy.validate_targets(targets)
    by_id = {target.target_id: target for target in targets}
    reserved = {row.id: row.reserved_bytes for row in target_rows}
    if move.target_id not in policy.history or move.target_id not in by_id:
        raise StorageConflict("storage_move_history_target_changed")
    claims = {}
    for row in batch_rows:
        if row.state in ("reserved", "running", "blocked"):
            claims[row.target_id] = claims.get(row.target_id, 0) + row.reserved_bytes
    if any(reserved.get(target_id, 0) < amount for target_id, amount in claims.items()):
        raise StorageConflict("storage_journal_reservation_inconsistent")
    registration = session.get(StorageHeaderTablespaceRecord,
        (move.database_identity, move.target_id), populate_existing=True)
    if registration is None or move.database_identity != batch.database_identity:
        raise StorageConflict("storage_move_registration_missing")
    verified = _verified_move_group(session, move, targets, registration,
                                    pg_controldata, timeout_seconds)
    if move.storage_day >= verified.snapshot.database_day - timedelta(days=policy.recent_days):
        raise StorageConflict("storage_move_no_longer_historical")
    destination = checked_header_destinations(verified, targets)[move.target_id]
    moving, copying = _compare_reserved_group(move, verified)
    # Existing aggregate includes this move. Subtract its own reservation once,
    # then require actual free space for its current copy plus every other claim.
    other = {**reserved, move.target_id: reserved[move.target_id] - move.reserved_bytes}
    selected = allocate_target(policy=policy, role="history", targets=targets,
        capacity={move.target_id: verified.capacity[move.target_id]},
        required_bytes=max(1, copying), reserved_bytes=other)
    if selected.target_id != move.target_id:
        raise StorageConflict("storage_move_destination_changed")
    logger.info("storage_header_move_inspected | plan_id=%s move_id=%s target_id=%s "
                "copy_bytes=%s reserved_copy_bytes=%s capacity_scope=copy_only",
                move.plan_id, move.id, move.target_id, copying, move.reserved_bytes)
    return HeaderMoveInspection(move.id, move.plan_id, move.target_id,
        destination["tablespace_oid"], destination["tablespace_name"], moving,
        copying, move.reserved_bytes, verified)


def _verified_move_group(session, move, targets, registration, pg_controldata, timeout_seconds):
    """Shared current registered-file observation; caller already owns storage."""
    catalog = read_locked_header_group(session.connection(), storage_day=move.storage_day,
        heap_oid=move.heap_oid, timeout_seconds=timeout_seconds,
        destination_tablespace_oids=(registration.tablespace_oid,))
    verified = verify_header_filesystem(catalog, targets, pg_controldata=pg_controldata,
        timeout_seconds=timeout_seconds,
        destination_assignments={move.target_id: registration.tablespace_oid})
    if fresh_header_database(session, verified) != move.database_identity:
        raise StorageConflict("storage_move_database_changed")
    destination = checked_header_destinations(verified, targets).get(move.target_id)
    if (destination is None or destination != move.destination_binding
            or stable_header_destination(destination, next(target for target in targets if target.target_id == move.target_id)) != registration.binding
            or destination["filesystem_uuid"] != move.filesystem_uuid):
        raise StorageConflict("storage_move_destination_changed")
    return verified


@dataclass(frozen=True)
class HeaderMoveResourceInspection:
    move: HeaderMoveInspection
    resources: VerifiedHeaderResources
    budget: dict
    review_hash: str
    policy_revision: int
    temporary_target_ids: tuple[str, ...]
    execution_available: bool = False


def _bound_resource_targets(resources, targets, initial_capacity):
    """Check that both observations bind the same enrolled physical inventory."""
    if not isinstance(resources, VerifiedHeaderResources):
        raise StorageConflict("storage_move_resource_observation_required")
    ids = {item.target_id for item in targets}
    if set(resources.capacity) != ids or set(initial_capacity) != ids:
        raise StorageConflict("storage_move_resource_inventory_changed")
    for target in targets:
        before, after = initial_capacity[target.target_id], resources.capacity[target.target_id]
        if (after.filesystem_uuid != target.filesystem_uuid or after.read_only
                or (before.filesystem_uuid, before.device_id, before.path)
                != (after.filesystem_uuid, after.device_id, after.path)):
            raise StorageConflict("storage_move_resource_mount_changed")
    roles = {"database": [], "wal": [], "database_default": [],
             "temporary_files": [], "temporary_relations": []}
    if not isinstance(resources.bindings, tuple) or not 5 <= len(resources.bindings) <= 69:
        raise StorageConflict("storage_move_resource_bindings_invalid")
    for binding in resources.bindings:
        if (not isinstance(binding, dict) or binding.get("role") not in roles
                or binding.get("target_id") not in ids):
            raise StorageConflict("storage_move_resource_bindings_invalid")
        evidence = resources.capacity[binding["target_id"]]
        if (binding.get("filesystem_uuid") != evidence.filesystem_uuid
                or binding.get("device_id") != evidence.device_id):
            raise StorageConflict("storage_move_resource_mount_changed")
        roles[binding["role"]].append(binding)
    if any(len(roles[key]) != 1 for key in ("database", "wal", "database_default")):
        raise StorageConflict("storage_move_resource_bindings_invalid")
    file_spaces = {item.get("tablespace_oid") for item in roles["temporary_files"]}
    relation_spaces = {item.get("tablespace_oid") for item in roles["temporary_relations"]}
    if (not file_spaces or file_spaces != relation_spaces
            or any(type(oid) is not int or not 1 <= oid <= 2**32 - 1 for oid in file_spaces)
            or len(file_spaces) != len(roles["temporary_files"])
            or len(relation_spaces) != len(roles["temporary_relations"])
            or roles["database_default"][0].get("tablespace_oid") not in file_spaces):
        raise StorageConflict("storage_move_resource_bindings_invalid")
    temporary = tuple(sorted({item["target_id"]
        for key in ("temporary_files", "temporary_relations") for item in roles[key]}))
    return roles["wal"][0]["target_id"], temporary


def inspect_reserved_header_move_resources(
    session, *, move_id, review_hash, pg_controldata, wal_bytes, temporary_bytes,
    growth_bytes_per_second, maintenance_bytes, movement_timeout_seconds,
    cancellation_grace_seconds, timeout_seconds=30,
):
    """Compose current intent, real resource paths and declared capacity limits.

    Holds the caller's storage/group locks; no DDL, journal writes, commit or
    reservation changes. Returned sufficiency is conditional, not execution
    admission: other producers, enforced limits, auxiliary claims and worker
    supervision remain unqualified. Caller must roll back on failure.
    """
    if not session.in_transaction():
        raise StorageConflict("storage_move_caller_transaction_required")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise ValueError("storage_move_invalid_time_budget")
    # Freeze the caller's mutable mappings before any database or file probes.
    maps = (temporary_bytes, growth_bytes_per_second, maintenance_bytes)
    if any(not isinstance(value, Mapping) or len(value) > 32 for value in maps):
        raise ValueError("storage_move_resource_limits_invalid")
    temporary_bytes, growth_bytes_per_second, maintenance_bytes = map(dict, maps)
    deadline = monotonic() + timeout_seconds
    conn = session.connection()
    previous = conn.execute(text("""
        SELECT current_setting('statement_timeout') AS original, setting::bigint AS milliseconds
        FROM pg_settings WHERE name='statement_timeout'
    """)).mappings().one()
    if previous["milliseconds"]:
        deadline = min(deadline, monotonic() + previous["milliseconds"] / 1000)

    def remaining():
        milliseconds = int((deadline - monotonic()) * 1000)
        if milliseconds <= 0:
            raise RuntimeError("storage_move_resource_time_budget_exceeded")
        return milliseconds

    def phase():
        milliseconds = remaining()
        conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"),
                     {"timeout": str(milliseconds)})
        return min(60, (milliseconds + 999) // 1000)

    inspection = inspect_reserved_header_move(session, move_id=move_id,
        review_hash=review_hash, pg_controldata=pg_controldata, timeout_seconds=phase())
    phase()
    target_rows = registered_header_targets(session)
    targets = tuple(_target(row) for row in target_rows)
    resources = observe_header_resources(conn, targets, pg_controldata=pg_controldata,
                                         timeout_seconds=phase())
    wal_target, temporary_targets = _bound_resource_targets(resources, targets, inspection.verified.capacity)
    phase()
    context = conn.execute(text("SELECT clock_timestamp() AS now, pg_backend_pid() AS pid")).mappings().one()
    if (resources.database_identity != inspection.verified.snapshot.database_identity
            or type(resources.backend_pid) is not int or resources.backend_pid != context["pid"]):
        raise StorageConflict("storage_move_resource_database_or_backend_changed")
    for stamp in (resources.observed_at, resources.verified_at):
        if (not isinstance(stamp, datetime) or stamp.tzinfo is None or stamp.utcoffset() is None
                or not 0 <= (context["now"] - stamp).total_seconds() <= 30):
            raise StorageConflict("storage_move_resource_evidence_expired")
    if (resources.observed_at > resources.verified_at
            or resources.observed_at < inspection.verified.snapshot.captured_at):
        raise StorageConflict("storage_move_resource_observation_order")
    phase()
    plan = session.get(StoragePlanRecord, inspection.plan_id, populate_existing=True)
    phase()
    config = session.get(StoragePolicyRecord, 1, populate_existing=True)
    phase()
    batch = session.get(StorageHeaderBatchRecord, inspection.plan_id, populate_existing=True)
    if (plan is None or config is None or batch is None
            or config.revision != plan.base_revision or batch.base_revision != plan.base_revision
            or batch.review_hash != review_hash or batch.cancelled_at is not None
            or plan.state not in ("queued", "running")):
        raise StorageConflict("storage_move_resource_plan_changed")
    policy = StoragePolicy.from_dict(plan.policy)
    if policy.fingerprint != plan.policy_hash or policy.fingerprint != batch.policy_hash:
        raise StorageConflict("storage_move_policy_not_admitted")
    budget = assess_header_move_resources(
        targets=targets, capacity=resources.capacity, policy=policy,
        observed_at=resources.observed_at, now=context["now"],
        copy_target_id=inspection.target_id, copy_bytes=inspection.copy_bytes,
        own_reserved_bytes=inspection.reserved_copy_bytes,
        reserved_bytes={row.id: row.reserved_bytes for row in target_rows},
        wal_target_id=wal_target, wal_bytes=wal_bytes, temporary_bytes=temporary_bytes,
        growth_bytes_per_second=growth_bytes_per_second, maintenance_bytes=maintenance_bytes,
        timeout_seconds=movement_timeout_seconds, cancellation_grace_seconds=cancellation_grace_seconds)
    budget["uncovered"] = [
        "other_producer_placement_and_existing_temporary_objects",
        "qualified_and_enforced_resource_limits",
        "durable_auxiliary_reservations_and_worker_supervision",
    ]
    remaining()
    conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"),
                 {"timeout": previous["original"]})
    remaining()
    logger.info("storage_header_move_resources_inspected | plan_id=%s move_id=%s wal_target_id=%s "
                "temporary_target_count=%s declared_capacity_sufficient=%s",
                inspection.plan_id, move_id, wal_target, len(temporary_targets),
                budget["capacity_sufficient_for_declared_limits"])
    return HeaderMoveResourceInspection(inspection, resources, budget, review_hash,
                                        config.revision, temporary_targets)
