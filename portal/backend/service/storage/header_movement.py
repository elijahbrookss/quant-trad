"""Atomic historical movement and supervised execution of one reserved move.

No API/CLI/scheduler calls this module. The supervised boundary requires saved
resource claims, checks fresh paths/capacity, and cancels on shutdown, deadline
or observed space loss. Physical performance and measured limits remain required
before automatic policy activation.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import re
from dataclasses import asdict
from time import monotonic
import threading

from sqlalchemy import text
from sqlalchemy.orm import Session

from portal.backend.db.storage_target_models import (
    StorageHeaderBatchRecord, StorageHeaderMoveRecord, StorageHeaderTablespaceRecord,
    StorageTargetRecord,
)
from portal.backend.service.storage_management import StorageConflict, _target
from .header_admission import lock_header_storage, registered_header_targets
from .header_inspection import (
    inspect_reserved_header_move, inspect_reserved_header_move_resources, _verified_move_group,
)
from .header_journal import _rows
from .header_resource_claims import _release_resource_claims, claim_allocations

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


class _MoveWatch:
    """Watch only this checked-out backend and its already admitted filesystems."""

    def __init__(self, *, driver, targets, capacity, floors, deadline, cancelled, grace):
        self.driver = driver
        self.targets = targets
        self.capacity = capacity
        self.floors = floors
        self.deadline = deadline
        self.cancelled = cancelled
        self.grace = grace
        self.failure = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="qt-history-move-watch", daemon=True)

    def _observe(self):
        if self.cancelled is not None and self.cancelled():
            raise RuntimeError("storage_move_cancelled")
        if monotonic() >= self.deadline:
            raise RuntimeError("storage_move_time_budget_exceeded")
        for target in self.targets:
            current = target.inspect(require_writable=True)
            original = self.capacity[target.target_id]
            if ((current.filesystem_uuid, current.device_id, current.path)
                    != (original.filesystem_uuid, original.device_id, original.path)):
                raise RuntimeError("storage_move_filesystem_changed")
            if current.available_bytes < self.floors[target.target_id]:
                raise RuntimeError("storage_move_space_budget_exceeded")

    def check(self):
        if self.failure is not None:
            raise RuntimeError(self.failure)
        self._observe()

    def start(self):
        self.check()
        self._thread.start()

    def _run(self):
        while not self._stop.wait(.1):
            try:
                self._observe()
            except Exception as exc:
                self.failure = str(exc) or type(exc).__name__
                try:
                    # psycopg2 permits cancel from another thread. No SQL or
                    # transaction/session state is shared with this watcher.
                    self.driver.cancel()
                except Exception:
                    logger.exception("storage_header_cancel_delivery_failed")
                return

    def stop(self, connection):
        self._stop.set()
        if self._thread.ident is not None:
            self._thread.join(self.grace)
            if self._thread.is_alive():
                # Never let a late cancellation target a pooled connection
                # already borrowed by unrelated work.
                connection.invalidate()
                raise RuntimeError("storage_move_watcher_did_not_stop")


def execute_reserved_header_move(database, *, move_id, review_hash, pg_controldata,
                                 cancelled=None):
    """Commit one existing move with fresh budgets and in-flight supervision.

    No scheduling, policy activation, new plan or reservation is created here.
    The caller must have saved qualified resource limits in the existing claim.
    Polling bounds net filesystem consumption; it is not attribution of each
    backend's WAL/temp writes or an instantaneous disk-allocation limit.
    """
    if cancelled is not None and not callable(cancelled):
        raise ValueError("storage_move_cancellation_callback_invalid")
    if cancelled is not None and cancelled():
        raise RuntimeError("storage_move_cancelled")
    if not database.ensure_schema():
        raise RuntimeError("storage_move_database_unavailable")
    # Pin the physical connection until the watcher has stopped, including
    # commit, rollback and Session.close. It cannot cancel a later pool borrower.
    with database._engine.connect() as connection:
        watch = None
        started = monotonic()
        try:
            with Session(bind=connection) as session, session.begin():
                lock_header_storage(session)
                move = session.get(StorageHeaderMoveRecord, move_id, populate_existing=True)
                if move is None:
                    raise StorageConflict("storage_move_not_found")
                if move.state == "completed":
                    receipt = stage_header_move(session, move_id=move_id, review_hash=review_hash,
                        pg_controldata=pg_controldata, timeout_seconds=30)
                else:
                    records = registered_header_targets(session)
                    own = claim_allocations(move, records)
                    if move.resource_claim is None:
                        raise StorageConflict("storage_move_resource_claim_required")
                    limits = move.resource_claim["limits"]
                    deadline = started + limits["movement_timeout_seconds"]
                    original_ms = connection.scalar(text(
                        "SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
                    if original_ms:
                        deadline = min(deadline, started + original_ms / 1000)

                    def remaining():
                        milliseconds = int((deadline - monotonic()) * 1000)
                        if milliseconds <= 0:
                            raise RuntimeError("storage_move_time_budget_exceeded")
                        return milliseconds

                    inspected = inspect_reserved_header_move_resources(session,
                        move_id=move_id, review_hash=review_hash, pg_controldata=pg_controldata,
                        timeout_seconds=min(60, math.ceil(remaining()/1000)), **limits)
                    if not inspected.budget["capacity_sufficient_for_declared_limits"]:
                        raise StorageConflict("storage_move_resource_capacity_blocked")
                    targets = tuple(_target(record) for record in records)
                    floors = {}
                    for row in inspected.budget["filesystems"]:
                        key = row["target_id"]
                        copy_allowance = move.reserved_bytes if key == move.target_id else 0
                        # Protect policy reserve and all other jobs. Also retain
                        # unallocated free space instead of spending beyond this
                        # move's saved copy + auxiliary allowance.
                        floors[key] = max(
                            row["policy_reserve_bytes"] + row["other_reserved_copy_bytes"]
                                + row["other_reserved_auxiliary_bytes"],
                            row["available_bytes"] - copy_allowance - own[key])
                    driver = connection.connection.driver_connection
                    watch = _MoveWatch(driver=driver, targets=targets,
                        capacity=inspected.resources.capacity, floors=floors,
                        deadline=deadline, cancelled=cancelled,
                        grace=limits["cancellation_grace_seconds"])
                    watch.start()
                    receipt = stage_header_move(session, move_id=move_id, review_hash=review_hash,
                        pg_controldata=pg_controldata, timeout_seconds=math.ceil(remaining()/1000))
                    watch.check()
                # Completion and release remain atomic in this transaction.
            if watch is not None:
                watch.check()
            return {**receipt, "commit_required": False, "supervised_execution": True}
        except Exception as exc:
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            if watch is not None:
                watch.stop(connection)
