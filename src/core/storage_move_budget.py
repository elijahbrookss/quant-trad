"""Pure per-filesystem headroom calculation for a bounded historical move.

Inputs are explicit estimates, not proof that WAL, temp or ingestion is bounded.
No filesystem/SQL access, reservation, worker activation or implicit defaults.
The runtime boundary must bind resource paths, qualify/enforce these allowances,
and recheck capacity while holding movement ownership before using this result.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from math import ceil
import re

from .storage_mounts import FilesystemEvidence
from .storage_targets import StoragePolicy, StorageTarget

_MAX = 2**63 - 1


def _integer(value, name, minimum=0, maximum=_MAX):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("storage_move_budget_invalid: " + name)
    return value


def _complete_map(values, ids, name):
    if not isinstance(values, Mapping) or set(values) != ids:
        raise ValueError("storage_move_budget_invalid: complete " + name + " required")
    return {key: _integer(value, name) for key, value in values.items()}


def assess_header_move_resources(
    *, targets: Sequence[StorageTarget], capacity: Mapping[str, FilesystemEvidence],
    policy: StoragePolicy, observed_at: datetime, now: datetime,
    copy_target_id: str, copy_bytes: int, own_reserved_bytes: int,
    reserved_bytes: Mapping[str, int], wal_target_id: str, wal_bytes: int,
    temporary_bytes: Mapping[str, int], growth_bytes_per_second: Mapping[str, int],
    maintenance_bytes: Mapping[str, int], timeout_seconds: int,
    cancellation_grace_seconds: int,
) -> dict:
    """Count all declared demands once against each physical filesystem.

    reserved_bytes includes this move's existing COPY reservation. Replace that
    one claim with its current copy requirement; retain all competing claims.
    WAL/temp/maintenance are additional bytes from observation through movement
    and cancellation. Growth also covers elapsed observation age and other
    writers, including archives and recovery-copy creation where applicable,
    excluding separately counted WAL,
    temporary and maintenance demand. Existing occupied bytes are already
    reflected in available_bytes; neither those bytes nor future source frees
    are added to available capacity. Zero allowances must be explicit.
    """
    if (not isinstance(targets, Sequence) or not 1 <= len(targets) <= 32
            or any(not isinstance(item, StorageTarget) for item in targets)
            or not isinstance(policy, StoragePolicy)):
        raise ValueError("storage_move_budget_invalid: typed bounded inventory")
    by_id = {item.target_id: item for item in targets}
    if (len(by_id) != len(targets)
            or len({item.filesystem_uuid for item in targets}) != len(targets)):
        raise ValueError("storage_move_budget_invalid: duplicate physical target")
    policy.validate_targets(targets)
    ids = set(by_id)
    if not isinstance(capacity, Mapping) or set(capacity) != ids:
        raise ValueError("storage_move_budget_invalid: complete capacity required")
    for stamp in (observed_at, now):
        if (not isinstance(stamp, datetime) or stamp.tzinfo is None
                or stamp.utcoffset() is None):
            raise ValueError("storage_move_budget_invalid: aware observation clock")
    age = (now - observed_at).total_seconds()
    if not 0 <= age <= 30:
        raise ValueError("storage_move_budget_invalid: stale or future observation")
    _integer(timeout_seconds, "movement timeout", 1, 3600)
    _integer(cancellation_grace_seconds, "cancellation grace", 1, 60)
    observation_age = ceil(age)
    window = observation_age + timeout_seconds + cancellation_grace_seconds
    _integer(copy_bytes, "copy bytes")
    _integer(own_reserved_bytes, "own copy reservation", 1)
    _integer(wal_bytes, "additional WAL allowance", 1)
    if (not isinstance(copy_target_id, str) or not isinstance(wal_target_id, str)
            or copy_target_id not in ids or copy_target_id not in policy.history
            or wal_target_id not in ids):
        raise ValueError("storage_move_budget_invalid: copy or WAL target")
    if own_reserved_bytes < max(1, copy_bytes):
        raise ValueError("storage_move_budget_invalid: copy outgrew reservation")
    reservations = _complete_map(reserved_bytes, ids, "copy reservations")
    temporary = _complete_map(temporary_bytes, ids, "temporary allowances")
    growth = _complete_map(growth_bytes_per_second, ids, "growth rates")
    maintenance = _complete_map(maintenance_bytes, ids, "maintenance allowances")
    if reservations[copy_target_id] < own_reserved_bytes:
        raise ValueError("storage_move_budget_invalid: aggregate below own reservation")

    filesystems, blockers, devices = [], [], set()
    for target_id in sorted(ids):
        target, evidence = by_id[target_id], capacity[target_id]
        if not isinstance(evidence, FilesystemEvidence):
            raise ValueError("storage_move_budget_invalid: filesystem evidence")
        _integer(evidence.total_bytes, "filesystem total", 1)
        _integer(evidence.used_bytes, "filesystem used")
        _integer(evidence.available_bytes, "filesystem available")
        if (evidence.used_bytes + evidence.available_bytes > evidence.total_bytes
                or evidence.filesystem_uuid != target.filesystem_uuid
                or type(evidence.read_only) is not bool
                or not isinstance(evidence.device_id, str)
                or not re.fullmatch(r"(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*)", evidence.device_id)):
            raise ValueError("storage_move_budget_invalid: filesystem identity or capacity")
        if evidence.device_id in devices:
            raise ValueError("storage_move_budget_invalid: duplicate filesystem device")
        devices.add(evidence.device_id)
        own = own_reserved_bytes if target_id == copy_target_id else 0
        claims = reservations[target_id] - own
        demands = {
            "copy_bytes": max(1, copy_bytes) if target_id == copy_target_id else 0,
            "other_reserved_copy_bytes": claims,
            "additional_wal_bytes": wal_bytes if target_id == wal_target_id else 0,
            "temporary_bytes": temporary[target_id],
            "ingestion_and_other_growth_bytes": growth[target_id] * window,
            "maintenance_bytes": maintenance[target_id],
            "policy_reserve_bytes": (evidence.total_bytes * policy.reserve_percent + 99) // 100,
        }
        required = _integer(sum(demands.values()), "combined filesystem demand")
        shortfall = max(0, required - evidence.available_bytes)
        if evidence.read_only or target.state != "active":
            blockers.append({"code": "filesystem_not_writable_active", "target_id": target_id})
        if shortfall:
            blockers.append({"code": "filesystem_headroom_insufficient",
                             "target_id": target_id, "shortfall_bytes": shortfall})
        filesystems.append({
            "target_id": target_id, "filesystem_uuid": target.filesystem_uuid,
            "device_id": evidence.device_id, "available_bytes": evidence.available_bytes,
            **demands, "required_bytes": required, "shortfall_bytes": shortfall,
            "remaining_headroom_bytes": max(0, evidence.available_bytes - required),
        })
    return {
        "schema_version": "qt.header_move_resource_budget.v1",
        "capacity_sufficient_for_declared_limits": not blockers,
        "observed_at": observed_at.isoformat(), "evaluated_at": now.isoformat(),
        "policy_hash": policy.fingerprint, "movement_timeout_seconds": timeout_seconds,
        "cancellation_grace_seconds": cancellation_grace_seconds,
        "observation_age_seconds": observation_age,
        "growth_window_seconds": window, "filesystems": filesystems,
        "blockers": blockers, "source_space_credited_bytes": 0,
        "execution_available": False, "activation_ready": False,
        "uncovered": ["verified_wal_and_temporary_path_bindings",
                      "qualified_and_enforced_resource_limits",
                      "durable_auxiliary_reservations_and_worker_supervision"],
    }
