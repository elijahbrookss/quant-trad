"""Pure, bounded proposals for moving dated header tables and their indexes.

This module reads no filesystem or database and executes no DDL. A proposal is
not a reservation, a migration certificate, or authority to activate policy.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Mapping, Sequence

from .storage_mounts import FilesystemEvidence, StorageMountError
from .storage_targets import StorageLocation, StoragePolicy, StorageTarget, allocate_target


def _integer(value, name, *, minimum=0, maximum=2**63 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"header_placement_invalid: {name}")
    return value


@dataclass(frozen=True)
class RelationPlacement:
    oid: int
    relfilenode: int
    schema: str
    name: str
    target_id: str | None
    byte_count: int

    def __post_init__(self):
        _integer(self.oid, "relation oid", minimum=1, maximum=2**32 - 1)
        _integer(self.relfilenode, "physical relfilenode", minimum=1, maximum=2**32 - 1)
        _integer(self.byte_count, "relation bytes")
        for value in (self.schema, self.name):
            if not isinstance(value, str) or not value or "\x00" in value or len(value.encode("utf-8")) > 63:
                raise ValueError("header_placement_invalid: relation label")
        if self.target_id is not None:
            StorageLocation(self.target_id, "postgres")


@dataclass(frozen=True)
class HeaderPartitionPlacement:
    storage_day: date
    heap: RelationPlacement
    indexes: tuple[RelationPlacement, ...]
    index_inventory_complete: bool
    toast_colocated: bool

    def __post_init__(self):
        if type(self.storage_day) is not date or not isinstance(self.heap, RelationPlacement):
            raise ValueError("header_placement_invalid: header partition")
        if (not isinstance(self.indexes, tuple) or len(self.indexes) > 64
                or any(not isinstance(item, RelationPlacement) for item in self.indexes)):
            raise ValueError("header_placement_invalid: index inventory")
        if type(self.index_inventory_complete) is not bool or type(self.toast_colocated) is not bool:
            raise ValueError("header_placement_invalid: inventory admission flags")

    @property
    def relations(self):
        # heap.byte_count includes TOAST and its internal indexes. indexes are
        # only the header table's ordinary indexes, preventing double counting.
        return (self.heap, *self.indexes)


@dataclass(frozen=True)
class HeaderPlacementSnapshot:
    database_identity: str
    database_day: date
    captured_at: datetime
    partitions: tuple[HeaderPartitionPlacement, ...]
    inventory_complete: bool

    def __post_init__(self):
        if (not isinstance(self.database_identity, str) or not self.database_identity
                or "\x00" in self.database_identity or len(self.database_identity) > 128):
            raise ValueError("header_placement_invalid: database identity")
        if (type(self.database_day) is not date or not isinstance(self.captured_at, datetime)
                or self.captured_at.tzinfo is None or self.captured_at.utcoffset() is None
                or self.captured_at.astimezone(UTC).date() != self.database_day):
            raise ValueError("header_placement_invalid: database clock")
        if (not isinstance(self.partitions, tuple) or len(self.partitions) > 4096
                or any(not isinstance(item, HeaderPartitionPlacement) for item in self.partitions)
                or type(self.inventory_complete) is not bool):
            raise ValueError("header_placement_invalid: snapshot inventory")


def _capacity(targets, observed):
    unknown = set(observed) - set(targets)
    if unknown:
        raise ValueError("header_placement_invalid: capacity for unknown target")
    verified = {}
    devices = set()
    for target_id, evidence in observed.items():
        if not isinstance(evidence, FilesystemEvidence):
            raise ValueError("header_placement_invalid: filesystem evidence")
        _integer(evidence.total_bytes, "filesystem total", minimum=1)
        _integer(evidence.used_bytes, "filesystem used")
        _integer(evidence.available_bytes, "filesystem available")
        if evidence.used_bytes + evidence.available_bytes > evidence.total_bytes:
            raise ValueError("header_placement_invalid: inconsistent filesystem capacity")
        if type(evidence.read_only) is not bool:
            raise ValueError("header_placement_invalid: filesystem writability")
        if (evidence.read_only or evidence.filesystem_uuid != targets[target_id].filesystem_uuid):
            continue
        if not isinstance(evidence.device_id, str) or not evidence.device_id:
            raise ValueError("header_placement_invalid: filesystem device identity")
        if evidence.device_id in devices:
            raise ValueError("header_placement_invalid: duplicate filesystem device")
        devices.add(evidence.device_id)
        verified[target_id] = evidence
    return verified


def plan_header_placement(
    *, snapshot: HeaderPlacementSnapshot, policy: StoragePolicy,
    targets: Sequence[StorageTarget], capacity: Mapping[str, FilesystemEvidence],
    reserved_bytes: Mapping[str, int] | None = None, max_partitions: int = 4096,
) -> dict:
    """Plan historical header groups without crediting uncommitted source frees.

    The eventual inventory adapter must verify attachment, partition bounds,
    complete ordinary indexes, TOAST colocation and OID-to-target bindings.
    The executor must repeat those checks and reserve capacity transactionally.
    """
    _integer(max_partitions, "partition budget", minimum=1, maximum=4096)
    if not isinstance(snapshot, HeaderPlacementSnapshot) or not isinstance(policy, StoragePolicy):
        raise ValueError("header_placement_invalid: typed snapshot and policy required")
    if len(snapshot.partitions) > max_partitions:
        raise ValueError("header_placement_inventory_budget_exceeded")
    if not 1 <= len(targets) <= 32 or len(capacity) > 32:
        raise ValueError("header_placement_invalid: target inventory budget")
    policy.validate_targets(targets)
    by_id = {target.target_id: target for target in targets}
    verified = _capacity(by_id, capacity)
    reservations = dict(reserved_bytes or {})
    if set(reservations) - set(by_id):
        raise ValueError("header_placement_invalid: reservation for unknown target")
    for value in reservations.values():
        _integer(value, "reservation")
    original_reservations = dict(reservations)
    ordered = sorted(snapshot.partitions, key=lambda item: item.storage_day)
    days, relation_ids = set(), set()
    for partition in ordered:
        if partition.storage_day in days:
            raise ValueError("header_placement_invalid: duplicate storage day")
        days.add(partition.storage_day)
        for relation in partition.relations:
            if relation.oid in relation_ids:
                raise ValueError("header_placement_invalid: duplicate physical relation")
            relation_ids.add(relation.oid)
    cutoff = snapshot.database_day - timedelta(days=policy.recent_days)
    blockers, retained, moves = [], [], []
    if not snapshot.inventory_complete:
        blockers.append({"code": "header_inventory_incomplete"})
    else:
        for partition in ordered:
            day = partition.storage_day.isoformat()
            if not partition.index_inventory_complete or not partition.toast_colocated:
                blockers.append({"code": "header_group_inventory_unproven", "storage_day": day})
                continue
            unverified = sorted({item.target_id or "<unregistered>" for item in partition.relations
                                 if item.target_id not in verified})
            if unverified:
                blockers.append({"code": "header_source_unverified", "storage_day": day,
                                 "targets": unverified})
                continue
            if partition.storage_day >= cutoff:
                if all(item.target_id == policy.recent[0] for item in partition.relations):
                    retained.append({"storage_day": day, "target_id": policy.recent[0], "role": "recent"})
                else:
                    blockers.append({"code": "recent_header_placement_requires_separate_cutover",
                                     "storage_day": day})
                continue

            candidates = []
            for target_id in policy.history:
                evidence = verified.get(target_id)
                if evidence is None:
                    continue
                copying = sum(item.byte_count for item in partition.relations if item.target_id != target_id)
                relocating = any(item.target_id != target_id for item in partition.relations)
                required = max(1, copying) if relocating else 0
                if required:
                    try:
                        allocate_target(policy=policy, role="history", targets=targets,
                                        capacity={target_id: evidence}, required_bytes=required,
                                        reserved_bytes=reservations)
                    except StorageMountError:
                        continue
                headroom = (evidence.available_bytes
                            - (evidence.total_bytes * policy.reserve_percent + 99) // 100
                            - reservations.get(target_id, 0))
                # Retain an eligible existing heap location before redistributing
                # to a drive with more free space. Adding a drive must not churn history.
                candidates.append((target_id != partition.heap.target_id, -headroom,
                                   target_id, copying, required))
            if not candidates:
                blockers.append({"code": "header_destination_capacity_unavailable", "storage_day": day})
                continue
            _, _, target_id, copying, required = min(candidates)
            if not required:
                retained.append({"storage_day": day, "target_id": target_id, "role": "history"})
                continue
            reservations[target_id] = reservations.get(target_id, 0) + required
            moves.append({
                "storage_day": day, "destination_target_id": target_id,
                "destination_filesystem_uuid": by_id[target_id].filesystem_uuid,
                "heap": asdict(partition.heap),
                "indexes": [asdict(item) for item in sorted(partition.indexes, key=lambda item: item.oid)],
                "copy_bytes": copying, "reserve_copy_bytes": required,
                "source_space_credited_bytes": 0,
                "requires_atomic_table_and_index_move": True,
                "requires_catalog_mount_capacity_and_policy_recheck": True,
            })

    # A blocked preview is not a partial executable/reservation batch.
    if blockers:
        moves = []
        reservations = original_reservations
    payload = {
        "schema_version": "qt.header_placement_plan.v1",
        "scope": "dated_header_partitions_only",
        "database_identity": snapshot.database_identity,
        "database_day": snapshot.database_day.isoformat(),
        "captured_at": snapshot.captured_at.astimezone(UTC).isoformat(),
        "policy_hash": policy.fingerprint, "history_before": cutoff.isoformat(),
        "planning_complete": not blockers, "blockers": blockers,
        "moves": moves, "retained": retained,
        "additional_copy_reservations": {
            key: value - original_reservations.get(key, 0)
            for key, value in sorted(reservations.items())
            if value > original_reservations.get(key, 0)
        },
        "execution_available": False, "activation_ready": False,
        "uncovered": [
            "growing_identity_and_raw_mapping_placement", "recent_header_cutover",
            "payload_and_archive_placement", "wal_temporary_and_growth_headroom",
            "recovery_copies_and_restore", "physical_executor_and_performance_acceptance",
        ],
        "review_evidence": {
            "inventory_complete": snapshot.inventory_complete,
            "max_partitions": max_partitions,
            "targets": [asdict(by_id[key]) for key in sorted(by_id)],
            "capacity": {key: asdict(capacity[key]) for key in sorted(capacity)},
            "existing_reservations": dict(sorted(original_reservations.items())),
            "partitions": [
                {"storage_day": item.storage_day.isoformat(), "heap": asdict(item.heap),
                 "indexes": [asdict(index) for index in sorted(item.indexes, key=lambda index: index.oid)],
                 "index_inventory_complete": item.index_inventory_complete,
                 "toast_colocated": item.toast_colocated}
                for item in ordered
            ],
        },
    }
    payload["plan_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return payload
