"""Internal atomic PostgreSQL primitive for a future budgeted storage worker.

No API/CLI/scheduler calls this module. It qualifies physical transaction and
retry behavior on disposable databases. The production worker must additionally
admit WAL/temp/ingest-growth budgets and performance before calling it; copy-only
capacity and a queued intent are not a whole-system execution certificate.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import re
from dataclasses import asdict
from time import monotonic

from sqlalchemy import text

from portal.backend.db.storage_target_models import (
    StorageHeaderBatchRecord, StorageHeaderMoveRecord, StorageHeaderTablespaceRecord,
    StorageTargetRecord,
)
from portal.backend.service.storage_management import StorageConflict, _target
from .header_admission import lock_header_storage, registered_header_targets
from .header_inspection import inspect_reserved_header_move, _verified_move_group
from .header_journal import _rows
from .header_resource_claims import _release_resource_claims

logger = logging.getLogger(__name__)
_SCHEMA = "qt.header_move_completion.v1"


def _physical_identity(verified):
    """Compact exact file identity; byte growth alone does not undo a completion."""
    if verified.snapshot.inventory_complete or len(verified.snapshot.partitions) != 1:
        raise StorageConflict("storage_move_completion_group_unproven")
    group, = verified.snapshot.partitions
    if not group.index_inventory_complete or not group.toast_colocated:
        raise StorageConflict("storage_move_completion_group_unproven")
    bindings = {item["relation_oid"]: item for item in verified.bindings}
    if (len(bindings) != len(verified.bindings)
            or set(bindings) != {item.oid for item in group.relations}):
        raise StorageConflict("storage_move_completion_bindings_incomplete")
    members = []
    for relation in sorted(group.relations, key=lambda item: item.oid):
        bound = bindings[relation.oid]
        item = asdict(relation)
        item.pop("byte_count")
        item.update({key: bound[key] for key in ("filesystem_uuid", "device_id", "inode", "tablespace_oid")})
        item["server_path_sha256"] = hashlib.sha256(bound["server_path"].encode("utf-8")).hexdigest()
        members.append(item)
    return {"database_identity": verified.snapshot.database_identity,
            "storage_day": group.storage_day.isoformat(), "heap_oid": group.heap.oid,
            "members": members}


def _verify_transition(before, after, moving_oids, destination_oid, target_id):
    old = _physical_identity(before)
    new = _physical_identity(after)
    if any(old[key] != new[key] for key in ("database_identity", "storage_day", "heap_oid")):
        raise StorageConflict("storage_move_postcopy_group_changed")
    prior = {item["oid"]: item for item in old["members"]}
    current = {item["oid"]: item for item in new["members"]}
    if prior.keys() != current.keys():
        raise StorageConflict("storage_move_postcopy_membership_changed")
    for oid, item in current.items():
        if any(item[key] != prior[oid][key] for key in ("oid", "schema", "name")):
            raise StorageConflict("storage_move_postcopy_identity_changed")
        if oid in moving_oids:
            if item["target_id"] != target_id or item["tablespace_oid"] != destination_oid:
                raise StorageConflict("storage_move_postcopy_destination_changed")
        elif item != prior[oid]:
            raise StorageConflict("storage_move_retained_member_changed")
    return new


def _receipt(move, *, reused):
    return {"move_id": move.id, "plan_id": move.plan_id, "target_id": move.target_id,
            "state": "completed", "reused": reused, "reserved_bytes": 0,
            "physical_verified": True, "commit_required": True,
            "automatic_execution_available": False, "activation_ready": False}


def stage_header_move(session, *, move_id, review_hash, pg_controldata, timeout_seconds=300):
    """Stage DDL, post-verification, completion and release in one caller transaction.

    A savepoint rolls this operation back on ANY exception, including a Python
    failure after successful DDL. The outer caller still owns commit/rollback.
    A completed retry re-verifies durable completion against current registered
    files and performs no ALTER or second release. A busy prior transaction
    refuses immediately; it is never inferred to have failed from client time.
    This internal primitive is not a runtime worker or activation boundary.
    """
    if not session.in_transaction():
        raise StorageConflict("storage_move_caller_transaction_required")
    if not isinstance(move_id, str) or not re.fullmatch(r"header_[0-9a-f]{32}", move_id):
        raise ValueError("storage_move_invalid_id")
    if not isinstance(review_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", review_hash):
        raise ValueError("storage_move_invalid_review_hash")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600:
        raise ValueError("storage_move_invalid_time_budget")
    deadline = monotonic() + timeout_seconds
    with session.begin_nested():
        lock_header_storage(session)
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
                raise RuntimeError("storage_move_time_budget_exceeded")
            return milliseconds

        def probe_budget():
            return min(60, max(1, math.ceil(remaining() / 1000)))

        move = session.get(StorageHeaderMoveRecord, move_id, populate_existing=True)
        if move is None:
            raise StorageConflict("storage_move_not_found")
        batch = session.get(StorageHeaderBatchRecord, move.plan_id, populate_existing=True)
        if (batch is None or batch.review_hash != review_hash or batch.cancelled_at is not None
                or batch.database_identity != move.database_identity):
            raise StorageConflict("storage_move_review_conflict")
        _rows(session, batch)
        if move.state == "completed":
            registration = session.get(StorageHeaderTablespaceRecord,
                (move.database_identity, move.target_id), populate_existing=True)
            if registration is None:
                raise StorageConflict("storage_move_registration_missing")
            targets = tuple(_target(row) for row in registered_header_targets(session))
            current = _verified_move_group(session, move, targets, registration,
                                           pg_controldata, probe_budget())
            completion = move.completion_evidence
            if (not isinstance(completion, dict) or completion.get("schema_version") != _SCHEMA
                    or completion.get("physical") != _physical_identity(current)):
                raise StorageConflict("storage_move_completed_files_changed")
            remaining()
            logger.info("storage_header_completion_observed | plan_id=%s move_id=%s target_id=%s",
                        move.plan_id, move.id, move.target_id)
            return _receipt(move, reused=True)

        inspection = inspect_reserved_header_move(session, move_id=move_id,
            review_hash=review_hash, pg_controldata=pg_controldata, timeout_seconds=probe_budget())
        quote = conn.dialect.identifier_preparer.quote_identifier
        destination = quote(inspection.tablespace_name)
        heap_oid = inspection.verified.snapshot.partitions[0].heap.oid
        moving = {item.oid for item in inspection.moving_relations}
        for relation in inspection.moving_relations:
            conn.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                         {"timeout": str(remaining())})
            qualified = quote(relation.schema) + "." + quote(relation.name)
            kind = "TABLE ONLY" if relation.oid == heap_oid else "INDEX"
            # Identifiers are catalog-verified and dialect-quoted. Driver SQL
            # avoids interpreting a colon inside an identifier as a bind parameter.
            conn.exec_driver_sql(f"ALTER {kind} {qualified} SET TABLESPACE {destination}")
            logger.info("storage_header_member_copy_staged | plan_id=%s move_id=%s relation_oid=%s",
                        move.plan_id, move.id, relation.oid)
        # Our short DDL timeout must not replace the caller's original budget.
        conn.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                     {"timeout": previous["original"]})
        registration = session.get(StorageHeaderTablespaceRecord,
            (move.database_identity, move.target_id), populate_existing=True)
        targets = tuple(_target(row) for row in registered_header_targets(session))
        verified_after = _verified_move_group(session, move, targets, registration,
                                             pg_controldata, probe_budget())
        physical = _verify_transition(inspection.verified, verified_after, moving,
                                      inspection.tablespace_oid, inspection.target_id)
        completion = {"schema_version": _SCHEMA, "physical": physical,
                      "verified_at": verified_after.verified_at.isoformat(),
                      "copy_bytes": inspection.copy_bytes}
        if len(json.dumps(completion, ensure_ascii=False).encode("utf-8")) > 65536:
            raise StorageConflict("storage_move_completion_evidence_budget")
        target = session.get(StorageTargetRecord, move.target_id, populate_existing=True)
        if target is None or target.reserved_bytes < move.reserved_bytes:
            raise StorageConflict("storage_journal_reservation_inconsistent")
        conn.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                     {"timeout": str(remaining())})
        updated_at = conn.scalar(text("SELECT clock_timestamp()"))
        _release_resource_claims(session, [move])
        move.state = "completed"
        move.completion_evidence = completion
        move.updated_at = updated_at
        target.reserved_bytes -= move.reserved_bytes
        session.flush()
        remaining()
        conn.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                     {"timeout": previous["original"]})
        logger.info("storage_header_completion_staged | plan_id=%s move_id=%s target_id=%s released_bytes=%s",
                    move.plan_id, move.id, move.target_id, move.reserved_bytes)
        return _receipt(move, reused=False)
