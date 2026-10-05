"""Explicit, atomic relocation of three retained lookup indexes to recent SSD.

This extends the fixed v2 physical owner, not runtime maintenance. The retired
adoption and original copy records remain unchanged. One separate durable intent
owns the clock and completion; normal reads never call the mover.
"""
from __future__ import annotations

from copy import deepcopy
import json
import logging
import math
import re
from time import monotonic

from sqlalchemy import text

from portal.backend.service.storage.header_movement import _MoveWatch
from portal.backend.service.storage.header_resource_claims import _limits
from portal.backend.service.storage.header_resources import observe_header_resources
from scripts.db import archive_reference_v2_placement as resources_owner
from scripts.db import fact_header_forward_adoption as adoption
from scripts.db import fact_header_forward_keys as keys
from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_placement as physical
from scripts.db.fact_header_v2_deadline import CONTROLLER_LOCK

STATE = keys.SCHEMA + ".lookup_placement"
RELATIONS = (adoption.IDENTITY, adoption.raw.TARGET)
logger = logging.getLogger(__name__)


def _row(conn):
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name": STATE}) is None:
        return None
    return adoption._json_row(conn, STATE)


def _indexes(conn, saved):
    result = []
    for relation in RELATIONS:
        names = physical.recent_lookup_index_names(relation, saved)
        rows = conn.execute(text("""
            SELECT c.oid::bigint,c.relname,c.relfilenode::bigint,c.reltablespace::bigint,
                   pg_relation_size(c.oid) AS bytes,i.indisvalid,i.indisready,i.indislive
            FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
            WHERE i.indrelid=to_regclass(:relation) AND c.relname=ANY(:names)
            ORDER BY c.oid
        """), {"relation": relation, "names": sorted(names)}).mappings().all()
        if (not names or {row["relname"] for row in rows} != names
                or any(not all(row[key] for key in ("indisvalid", "indisready", "indislive")) for row in rows)):
            raise RuntimeError("fact_header_lookup_index_inventory_changed")
        result.extend(dict(row) for row in rows)
    if len(result) != 3:
        raise RuntimeError("fact_header_lookup_index_inventory_changed")
    return result


def _files(indexes):
    return [{key: row[key] for key in ("oid", "relname", "relfilenode", "reltablespace")} for row in indexes]


def _translate(before, old, new):
    """Allow only the three admitted indexes' file/tablespace fields to change."""
    if (len(old) != 3 or len(new) != 3
            or [(r["oid"], r["relname"]) for r in old] != [(r["oid"], r["relname"]) for r in new]):
        raise RuntimeError("fact_header_lookup_transition_changed")
    translated = deepcopy(before)
    for prior, current in zip(old, new):
        if set(prior) != set(current) or set(prior) != {"oid", "relname", "relfilenode", "reltablespace"}:
            raise RuntimeError("fact_header_lookup_transition_changed")
        found = [i for i, row in enumerate(translated["files"]) if row["oid"] == prior["oid"]]
        if len(found) != 1 or translated["files"][found[0]] != prior:
            raise RuntimeError("fact_header_lookup_retired_files_changed")
        translated["files"][found[0]] = dict(current)
    return translated


def retired_binding(conn, state):
    """Reconcile a retired receipt through this exact committed physical change.

    The original receipt itself is never rewritten. All other files, logical
    definitions, permissions, queues and ownership still require exact equality.
    """
    before = state["terminal"]["binding"]
    row = _row(conn)
    if row is None or row["binding"]["predecessor_operation_sha256"] != state["operation_sha256"]:
        return before
    binding = row["binding"]
    if (binding["predecessor_terminal_sha256"] != adoption._digest(state["terminal"])
            or binding["predecessor_state_sha256"] != adoption._digest(
                adoption._json_row(conn, adoption.state_relation(conn, state["operation_sha256"])))):
        raise RuntimeError("fact_header_lookup_predecessor_changed")
    completion = row["completion"]
    if completion is None:
        return before
    if completion["before_files"] != binding["before_files"]:
        raise RuntimeError("fact_header_lookup_completion_changed")
    return _translate(before, completion["before_files"], completion["after_files"])


def _completed(conn, operation_sha256):
    """Catalog identity for adoption snapshots, including read-only retirement."""
    row = _row(conn)
    if row is None or row["operation_sha256"] != operation_sha256 or row["completion"] is None:
        raise RuntimeError("fact_header_lookup_committed_placement_required")
    if (_files(_indexes(conn, row["binding"]["placement"])) != row["completion"]["after_files"]
            or row["completion"]["before_files"] != row["binding"]["before_files"]):
        raise RuntimeError("fact_header_lookup_completed_files_changed")
    return row


def completed_receipt(conn, operation_sha256):
    """Read and verify committed placement, including actual namespace/files."""
    row = _completed(conn, operation_sha256)
    saved = row["binding"]["placement"]
    pid = physical.verify(conn, saved)
    for relation in RELATIONS:
        physical.verify_group(conn, relation, history=True, saved=saved, pid=pid)
    return row


def move_lookup_indexes(engine, *, operation_sha256, predecessor_operation_sha256,
                        predecessor_terminal_sha256, placement, policy, resource_limits,
                        cancelled=None, deadline=None):
    """Own one bounded all-or-nothing copy and its commit/reconciliation.

    Requires already retired mirrors. Serving source inserts continue, while
    only the private targets are fenced. The existing resource watcher covers
    SSD copy, WAL, temporary space and concurrent growth. A lost commit reply
    reconciles the durable completion without another ALTER or clock reset.
    """
    for value in (operation_sha256, predecessor_operation_sha256, predecessor_terminal_sha256):
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("fact_header_lookup_intent_invalid")
    if operation_sha256 == predecessor_operation_sha256:
        raise ValueError("fact_header_lookup_distinct_intent_required")
    if not isinstance(placement, physical.CopyPlacement) or not placement.recent_lookup_indexes:
        raise ValueError("fact_header_lookup_recent_placement_required")
    limits = _limits(resource_limits, migration=True)
    if limits["movement_timeout_seconds"] > 3600:
        raise ValueError("fact_header_lookup_one_hour_bound_required")
    if cancelled is not None and not callable(cancelled):
        raise ValueError("fact_header_lookup_cancellation_invalid")
    if cancelled is not None and cancelled():
        raise RuntimeError("storage_move_cancelled")
    started = monotonic()
    if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline) or deadline <= started):
        raise ValueError("fact_header_lookup_deadline_invalid")
    deadline = min(deadline if deadline is not None else float("inf"), started + limits["movement_timeout_seconds"])
    targets = (placement.recent, placement.history)
    resources_owner._fixed_inputs(policy, limits, targets)
    with engine.connect() as conn:
        watch = None
        try:
            # Session ownership spans durable intent, relocation, commit and
            # response reconciliation. Discard this connection on every exit.
            with conn.begin():
                previous_ms = conn.scalar(text("SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
                if previous_ms:
                    deadline = min(deadline, started + previous_ms/1000)
            with conn.begin(), capture._bounded_step(conn, 30):
                for name in (CONTROLLER_LOCK, "qt.storage.management.v1"):
                    if not conn.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"), {"name": name}):
                        raise RuntimeError("fact_header_lookup_owner_busy")
                # _bounded_step's own statement ceiling is restored at exit;
                # the outer absolute bound and persisted deadline remain owners.
                saved, pid = physical.observe(conn, placement, deadline=deadline)
                terminal = adoption.inspect_retirement(conn, operation_sha256=predecessor_operation_sha256)
                if terminal is None or adoption._digest(terminal) != predecessor_terminal_sha256:
                    raise RuntimeError("fact_header_lookup_retirement_required")
                predecessor = adoption._state(conn, predecessor_operation_sha256)
                prior = predecessor["binding"]["old_headers"]["placement"]
                expected = deepcopy(saved)
                expected["plan"].pop("recent_lookup_indexes")
                if expected != prior:
                    raise RuntimeError("fact_header_lookup_original_placement_changed")
                request = dict(predecessor_operation_sha256=predecessor_operation_sha256,
                    predecessor_terminal_sha256=predecessor_terminal_sha256,
                    predecessor_state_sha256=adoption._digest(adoption._json_row(conn,
                        adoption.state_relation(conn, predecessor_operation_sha256))),
                    placement=saved, policy=policy.to_dict(), resource_limits=limits)
                row = _row(conn)
                if row is None:
                    for relation in RELATIONS:
                        physical.verify_group(conn, relation, history=True, saved=prior, pid=pid)
                    request["before_files"] = _files(_indexes(conn, saved))
                    conn.exec_driver_sql("CREATE TABLE " + STATE + "(id integer PRIMARY KEY CHECK(id=1),"
                        "operation_sha256 text NOT NULL,binding jsonb NOT NULL,started_at timestamptz NOT NULL,"
                        "expires_at timestamptz NOT NULL,duration_seconds integer NOT NULL CHECK(duration_seconds BETWEEN 1 AND 3600),"
                        "completion jsonb,CHECK(expires_at=started_at+duration_seconds*interval '1 second'))")
                    conn.exec_driver_sql("REVOKE ALL ON " + STATE + " FROM PUBLIC")
                    conn.execute(text("INSERT INTO " + STATE + " SELECT 1,:operation,CAST(:binding AS jsonb),"
                        "stamp,stamp+:seconds*interval '1 second',:seconds,NULL FROM (SELECT clock_timestamp() stamp) s"),
                        {"operation": operation_sha256, "binding": json.dumps(request),
                         "seconds": limits["movement_timeout_seconds"]})
                else:
                    request["before_files"] = row["binding"]["before_files"]
                    if row["operation_sha256"] != operation_sha256 or row["binding"] != request:
                        raise RuntimeError("fact_header_lookup_intent_changed")
                    if row["completion"] is not None:
                        return dict(completed_receipt(conn, operation_sha256), reused=True, migration_ready=False)
            with conn.begin(), capture._bounded_step(conn, limits["movement_timeout_seconds"]) as limit:
                remaining = conn.scalar(text("SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM " + STATE + " WHERE id=1"))
                deadline = min(deadline, monotonic() + float(remaining))
                if deadline <= monotonic():
                    raise RuntimeError("fact_header_lookup_expired")
                limit(deadline - monotonic())
                # ACCESS SHARE permits ordinary source writes. Private target
                # fences do not block collection once retirement is verified.
                conn.exec_driver_sql("LOCK TABLE " + adoption.headers.SOURCE + "," + adoption.raw.SOURCE + " IN ACCESS SHARE MODE NOWAIT")
                conn.exec_driver_sql("LOCK TABLE " + ",".join(RELATIONS) + " IN ACCESS EXCLUSIVE MODE NOWAIT")
                terminal = adoption.inspect_retirement(conn, operation_sha256=predecessor_operation_sha256)
                if adoption._digest(terminal) != predecessor_terminal_sha256:
                    raise RuntimeError("fact_header_lookup_retirement_changed")
                indexes = _indexes(conn, saved)
                if _files(indexes) != request["before_files"]:
                    raise RuntimeError("fact_header_lookup_source_files_changed")
                physical.verify(conn, prior)
                for relation in RELATIONS:
                    physical.verify_group(conn, relation, history=True, saved=prior, pid=pid)
                resources = observe_header_resources(conn, targets, pg_controldata=placement.pg_controldata,
                    timeout_seconds=min(30, limits["movement_timeout_seconds"]))
                copy_bytes = sum(row["bytes"] for row in indexes)
                budget, floors = resources_owner._budget(conn,
                    observed={"bytes": copy_bytes, "_binding": saved}, policy=policy,
                    limits={**limits, "movement_timeout_seconds": max(1, math.ceil(deadline-monotonic()))},
                    targets=targets, resources=resources, copy_target_id=placement.recent.target_id)
                watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets,
                    capacity=resources.capacity, floors=floors, deadline=deadline,
                    cancelled=cancelled, grace=limits["cancellation_grace_seconds"])
                watch.start()
                quote = conn.dialect.identifier_preparer.quote
                for index in indexes:
                    limit(deadline-monotonic())
                    conn.exec_driver_sql("ALTER INDEX " + quote(capture.SCHEMA) + "." + quote(index["relname"]) + " SET TABLESPACE pg_default")
                    watch.check()
                after = _files(_indexes(conn, saved))
                if _translate(terminal["binding"], request["before_files"], after) != adoption._snapshot(conn, predecessor_operation_sha256):
                    raise RuntimeError("fact_header_lookup_unexpected_transition")
                pid = physical.verify(conn, saved)
                for relation in RELATIONS:
                    physical.verify_group(conn, relation, history=True, saved=saved, pid=pid)
                completion = dict(before_files=request["before_files"], after_files=after,
                    copy_bytes=copy_bytes, resource_budget=budget,
                    completed_at=conn.scalar(text("SELECT clock_timestamp()")).isoformat())
                conn.execute(text("UPDATE " + STATE + " SET completion=CAST(:value AS jsonb) WHERE id=1"), {"value": json.dumps(completion)})
                watch.check()
            watch.check()
            with conn.begin(), capture._bounded_step(conn, 30):
                result = completed_receipt(conn, operation_sha256)
            logger.info("fact_header_lookup_placement_complete | operation=%s bytes=%s duration_seconds=%s",
                operation_sha256, copy_bytes, monotonic()-started)
            return dict(result, reused=False, migration_ready=False)
        except Exception as exc:
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            try:
                if watch is not None:
                    watch.stop(conn)
            finally:
                conn.invalidate()
