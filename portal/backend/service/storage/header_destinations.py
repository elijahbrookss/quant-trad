"""Immutable prepared tablespace registration and destination-bound reviews.

Only internal verified observations are accepted. This module creates no
tablespace, directory or physical data. The caller owns registration commits.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict
from pathlib import PurePosixPath

from sqlalchemy import select

from core.storage_header_placement import plan_header_placement
from core.storage_targets import StorageTarget
from portal.backend.db.storage_target_models import StorageHeaderTablespaceRecord
from portal.backend.service.storage_management import StorageConflict, _target
from .header_admission import fresh_header_database, lock_header_storage, registered_header_targets
from .header_filesystem import VerifiedHeaderPlacement, VerifiedTablespaceDestination

logger = logging.getLogger(__name__)


def checked_header_destinations(verified, targets):
    """Validate bounded typed evidence without probing a filesystem or database."""
    if not isinstance(verified, VerifiedHeaderPlacement):
        raise ValueError("header_destination_verified_inventory_required")
    items = verified.destinations
    if (not isinstance(items, tuple) or len(items) > 32
            or any(not isinstance(item, VerifiedTablespaceDestination) for item in items)):
        raise ValueError("header_destination_inventory_invalid")
    if (not 1 <= len(targets) <= 32 or len(verified.capacity) > 32
            or any(not isinstance(target, StorageTarget) for target in targets)):
        raise ValueError("header_destination_target_budget")
    by_id = {target.target_id: target for target in targets}
    if len(by_id) != len(targets):
        raise ValueError("header_destination_duplicate_target")
    result, oids = {}, set()
    for item in items:
        target = by_id.get(item.target_id)
        capacity = verified.capacity.get(item.target_id)
        if (target is None or capacity is None or target.state != "active" or "history" not in target.roles
                or item.database_identity != verified.snapshot.database_identity
                or item.filesystem_uuid != target.filesystem_uuid or item.target_root != target.root
                or item.filesystem_uuid != capacity.filesystem_uuid
                or item.device_id != capacity.device_id or capacity.read_only):
            raise ValueError("header_destination_identity_mismatch")
        if (type(item.tablespace_oid) is not int or not 1 <= item.tablespace_oid <= 2**32 - 1
                or item.tablespace_oid == 1664 or item.tablespace_oid in oids
                or item.target_id in result
                or type(item.directory_inode) is not int or not 1 <= item.directory_inode <= 2**64 - 1
                or type(item.catalog_version) is not int or not 1 <= item.catalog_version <= 2**31 - 1):
            raise ValueError("header_destination_identity_invalid")
        if (not isinstance(item.tablespace_name, str) or not item.tablespace_name
                or "\x00" in item.tablespace_name or len(item.tablespace_name.encode()) > 63):
            raise ValueError("header_destination_name_invalid")
        for value in (item.directory, item.server_directory, capacity.path):
            if (not isinstance(value, str) or "\x00" in value or len(value.encode()) > 4096
                    or not PurePosixPath(value).is_absolute() or ".." in PurePosixPath(value).parts):
                raise ValueError("header_destination_path_invalid")
        if not PurePosixPath(item.directory).is_relative_to(PurePosixPath(capacity.path)):
            raise ValueError("header_destination_path_outside_target")
        if item.tablespace_oid == 1663:
            if (item.tablespace_name != "pg_default" or item.tablespace_location != ""
                    or PurePosixPath(item.server_directory).parts[-2:]
                    != ("base", item.database_identity.split("/")[-1])):
                raise ValueError("header_destination_default_invalid")
        else:
            location = item.tablespace_location
            if (not isinstance(location, str) or "\x00" in location or len(location.encode()) > 4096
                    or not PurePosixPath(location).is_absolute() or ".." in PurePosixPath(location).parts
                    or PurePosixPath(item.server_directory)
                    != PurePosixPath(location) / f"PG_15_{item.catalog_version}"):
                raise ValueError("header_destination_location_invalid")
        payload = asdict(item)
        if len(json.dumps(payload, ensure_ascii=False).encode()) > 16384:
            raise ValueError("header_destination_evidence_budget")
        result[item.target_id] = payload
        oids.add(item.tablespace_oid)
    return result


def stable_header_destination(destination, target):
    # Remounting or a verified physical restore can change these observations.
    # They are bound into the current move review, not immutable registration.
    return {**{key: value for key, value in destination.items()
               if key not in ("device_id", "directory_inode")}, "target_root": target.root}


def review_header_moves(*, verified, policy, targets, reserved_bytes=None):
    """Bind the pure placement proposal to its observed PostgreSQL destinations."""
    destinations = checked_header_destinations(verified, targets)
    proposal = plan_header_placement(snapshot=verified.snapshot, policy=policy, targets=targets,
        capacity=verified.capacity, reserved_bytes=reserved_bytes)
    payload = {**proposal, "schema_version": "qt.header_move_review.v1",
               "placement_plan_hash": proposal["plan_hash"]}
    payload.pop("plan_hash")
    used = {move["destination_target_id"] for move in proposal["moves"]}
    missing = sorted(used - destinations.keys())
    if missing:
        payload["planning_complete"] = False
        payload["blockers"] = [*proposal["blockers"], *(
            {"code": "header_destination_unverified", "target_id": target_id} for target_id in missing)]
        payload["moves"] = []
        payload["additional_copy_reservations"] = {}
    payload["destination_evidence"] = {key: destinations[key] for key in sorted(used & destinations.keys())}
    payload["plan_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()
    return payload


def register_header_tablespaces(session, *, verified):
    """Stage immutable destination registration under the shared storage lock."""
    lock_header_storage(session)
    identity = fresh_header_database(session, verified)
    targets = [_target(row) for row in registered_header_targets(session)]
    by_id = {target.target_id: target for target in targets}
    destinations = checked_header_destinations(verified, targets)
    if not destinations:
        raise ValueError("header_tablespace_observation_required")
    records = list(session.scalars(select(StorageHeaderTablespaceRecord).where(
        StorageHeaderTablespaceRecord.database_identity == identity
    ).limit(33).execution_options(populate_existing=True)))
    if len(records) > 32:
        raise StorageConflict("header_tablespace_registry_budget")
    existing = {row.target_id: row for row in records}
    owners = {row.tablespace_oid: row.target_id for row in records}
    pending, receipts = [], []
    for target_id, destination in sorted(destinations.items()):
        binding = stable_header_destination(destination, by_id[target_id])
        row = existing.get(target_id)
        if row is not None:
            if row.tablespace_oid != destination["tablespace_oid"] or row.binding != binding:
                raise StorageConflict("header_tablespace_identity_changed")
        else:
            if destination["tablespace_oid"] in owners:
                raise StorageConflict("header_tablespace_already_registered")
            pending.append(StorageHeaderTablespaceRecord(database_identity=identity, target_id=target_id,
                tablespace_oid=destination["tablespace_oid"], binding=binding))
        receipts.append({"target_id": target_id, "tablespace_oid": destination["tablespace_oid"],
                         "reused": row is not None})
    if len(existing) + len(pending) > 32:
        raise StorageConflict("header_tablespace_registry_budget")
    session.add_all(pending)
    session.flush()
    logger.info("storage_header_tablespaces_staged | database_identity=%s added=%s reused=%s",
                identity, len(pending), len(receipts)-len(pending))
    return receipts
