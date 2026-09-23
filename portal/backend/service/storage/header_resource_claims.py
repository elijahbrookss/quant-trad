"""Durable auxiliary capacity ownership for a reserved historical move.

Reservations are estimates, not enforced limits or authority to execute.
No timeout/age-based release exists. Completion and unstarted cancellation
release them in the same transaction as the existing copy ledger.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from time import monotonic
from collections.abc import Mapping

from sqlalchemy import text

from portal.backend.db.storage_target_models import StorageHeaderMoveRecord, StorageHeaderBatchRecord
from portal.backend.service.storage_management import StorageConflict
from .header_admission import lock_header_storage, registered_header_targets

logger = logging.getLogger(__name__)
_SCHEMA = "qt.header_resource_claim.v1"
_MAX = 2**63 - 1
_MAPS = ("temporary_bytes", "growth_bytes_per_second", "maintenance_bytes")


def _limits(values, *, migration=False):
    if type(migration) is not bool:
        raise ValueError("storage_resource_claim_limits_invalid")
    if not isinstance(values, Mapping) or set(values) != {
        *_MAPS, "wal_bytes", "movement_timeout_seconds", "cancellation_grace_seconds"
    }:
        raise ValueError("storage_resource_claim_limits_invalid")
    result = {}
    for key in _MAPS:
        value = values[key]
        if (not isinstance(value, Mapping) or not 1 <= len(value) <= 32
                or any(not isinstance(k, str) or not 1 <= len(k) <= 48
                       or type(v) is not int or not 0 <= v <= _MAX for k, v in value.items())):
            raise ValueError("storage_resource_claim_limits_invalid")
        result[key] = dict(value)
    if any(set(result[key]) != set(result[_MAPS[0]]) for key in _MAPS):
        raise ValueError("storage_resource_claim_limits_invalid")
    for key, maximum in (("wal_bytes", _MAX), ("movement_timeout_seconds", 86400 if migration else 3600),
                         ("cancellation_grace_seconds", 60)):
        value = values[key]
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError("storage_resource_claim_limits_invalid")
        result[key] = value
    return result


def _hash(limits):
    return hashlib.sha256(json.dumps(limits, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def claim_allocations(move, targets):
    """Validate saved intent; terminal moves retain evidence but own no capacity."""
    result = {target.id: 0 for target in targets}
    claim = move.resource_claim
    if claim is None:
        return result
    try:
        if (not isinstance(claim, dict) or claim.get("schema_version") != _SCHEMA
                or len(json.dumps(claim).encode()) > 32768):
            raise ValueError("claim shape")
        limits = _limits(claim["limits"])
        allocations = claim["allocations"]
        ids = set(limits["temporary_bytes"])
        window = claim["growth_window_seconds"]
        minimum = limits["movement_timeout_seconds"] + limits["cancellation_grace_seconds"]
        if (claim["limits_hash"] != _hash(limits)
                or not isinstance(claim["review_hash"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", claim["review_hash"])
                or not isinstance(allocations, dict) or set(allocations) != ids
                or not ids <= result.keys() or claim["wal_target_id"] not in ids
                or type(window) is not int or not minimum <= window <= minimum + 30):
            raise ValueError("claim identity")
        registered = {target.id: target for target in targets}
        for target_id, allocation in allocations.items():
            amount = (limits["temporary_bytes"][target_id] + limits["maintenance_bytes"][target_id]
                      + limits["growth_bytes_per_second"][target_id] * window
                      + (limits["wal_bytes"] if target_id == claim["wal_target_id"] else 0))
            if (not isinstance(allocation, dict) or type(allocation.get("bytes")) is not int
                    or allocation["bytes"] != amount or not 0 <= amount <= _MAX
                    or allocation.get("filesystem_uuid") != registered[target_id].filesystem_uuid):
                raise ValueError("claim allocation")
            if move.state in ("reserved", "running", "blocked"):
                result[target_id] = amount
    except (KeyError, TypeError, ValueError) as exc:
        raise StorageConflict("storage_resource_claim_invalid") from exc
    return result


def _receipt(move, allocations, *, reused):
    return {"move_id": move.id, "state": move.state, "reused": reused,
            "auxiliary_reserved_bytes": allocations,
            "commit_required": True, "execution_available": False, "activation_ready": False}


def reserve_header_resources(session, *, move_id, review_hash, pg_controldata,
                             timeout_seconds=30, **declared_limits):
    """Stage one immutable claim after fresh inspection; replay never adds twice.

    Caller owns commit. A replay only acknowledges the saved claim; execution
    must freshly inspect resources and enforce limits. Savepoint rollback
    prevents partial claims even when the caller catches a Python exception.
    """
    if not session.in_transaction():
        raise StorageConflict("storage_move_caller_transaction_required")
    if not isinstance(move_id, str) or not re.fullmatch(r"header_[0-9a-f]{32}", move_id):
        raise ValueError("storage_move_invalid_id")
    if not isinstance(review_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", review_hash):
        raise ValueError("storage_move_invalid_review_hash")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise ValueError("storage_move_invalid_time_budget")
    limits = _limits(declared_limits)
    deadline = monotonic() + timeout_seconds
    with session.begin_nested():
        conn = session.connection()
        previous = conn.execute(text("""
            SELECT current_setting('statement_timeout') AS original, setting::bigint AS milliseconds
            FROM pg_settings WHERE name='statement_timeout'
        """)).mappings().one()
        if previous["milliseconds"]:
            deadline = min(deadline, monotonic() + previous["milliseconds"] / 1000)

        def phase():
            remaining = int((deadline - monotonic()) * 1000)
            if remaining <= 0:
                raise RuntimeError("storage_resource_claim_time_budget_exceeded")
            conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"),
                         {"timeout": str(remaining)})
            return min(60, (remaining + 999) // 1000)

        def finish(receipt):
            phase()
            conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"),
                         {"timeout": previous["original"]})
            if monotonic() >= deadline:
                raise RuntimeError("storage_resource_claim_time_budget_exceeded")
            return receipt

        phase()
        lock_header_storage(session)
        move = session.get(StorageHeaderMoveRecord, move_id, populate_existing=True)
        if move is None:
            raise StorageConflict("storage_move_not_found")
        batch = session.get(StorageHeaderBatchRecord, move.plan_id, populate_existing=True)
        if batch is None or batch.review_hash != review_hash:
            raise StorageConflict("storage_move_review_conflict")
        targets = registered_header_targets(session)
        if move.resource_claim is not None:
            allocations = claim_allocations(move, targets)
            if (move.resource_claim["limits_hash"] != _hash(limits)
                    or move.resource_claim["review_hash"] != review_hash):
                raise StorageConflict("storage_resource_claim_request_conflict")
            if any(target.auxiliary_reserved_bytes < allocations[target.id] for target in targets):
                raise StorageConflict("storage_resource_claim_aggregate_inconsistent")
            return finish(_receipt(move, allocations, reused=True))

        # Local import avoids a cycle: inspection reads existing claims, while
        # acquisition invokes that same real inspection instead of trusting input proof.
        from .header_inspection import inspect_reserved_header_move_resources
        inspected = inspect_reserved_header_move_resources(session, move_id=move_id,
            review_hash=review_hash, pg_controldata=pg_controldata,
            timeout_seconds=phase(), **limits)
        if not inspected.budget["capacity_sufficient_for_declared_limits"]:
            raise StorageConflict("storage_resource_claim_capacity_blocked")
        registered = {target.id: target for target in targets}
        allocations = {}
        for row in inspected.budget["filesystems"]:
            amount = sum(row[key] for key in ("additional_wal_bytes", "temporary_bytes",
                         "ingestion_and_other_growth_bytes", "maintenance_bytes"))
            target = registered[row["target_id"]]
            if target.reserved_bytes + target.auxiliary_reserved_bytes + amount > _MAX:
                raise StorageConflict("storage_resource_claim_overflow")
            allocations[target.id] = {"filesystem_uuid": target.filesystem_uuid, "bytes": amount}
        wal_target = next(item["target_id"] for item in inspected.resources.bindings if item["role"] == "wal")
        move.resource_claim = {"schema_version": _SCHEMA, "review_hash": review_hash,
            "limits": limits, "limits_hash": _hash(limits), "wal_target_id": wal_target,
            "growth_window_seconds": inspected.budget["growth_window_seconds"],
            "observed_at": inspected.budget["observed_at"], "allocations": allocations}
        amounts = claim_allocations(move, targets)
        for target in targets:
            target.auxiliary_reserved_bytes += amounts[target.id]
        phase()
        move.updated_at = session.scalar(text("SELECT clock_timestamp()"))
        session.flush()
        logger.info("storage_header_auxiliary_claim_staged | plan_id=%s move_id=%s bytes=%s",
                    move.plan_id, move.id, sum(amounts.values()))
        return finish(_receipt(move, amounts, reused=False))


def _release_resource_claims(session, moves):
    """Internal only: caller proved completion or owns wholly unstarted cancellation."""
    targets = registered_header_targets(session)
    release = {target.id: 0 for target in targets}
    for move in moves:
        if move.state not in ("reserved", "running", "blocked"):
            raise StorageConflict("storage_resource_claim_release_requires_active_move")
        for target_id, amount in claim_allocations(move, targets).items():
            release[target_id] += amount
    if any(target.auxiliary_reserved_bytes < release[target.id] for target in targets):
        raise StorageConflict("storage_resource_claim_aggregate_inconsistent")
    for target in targets:
        target.auxiliary_reserved_bytes -= release[target.id]
    if any(release.values()):
        logger.info("storage_header_auxiliary_release_staged | moves=%s bytes=%s",
                    len(moves), sum(release.values()))
