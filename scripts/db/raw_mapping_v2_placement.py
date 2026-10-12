"""Explicit post-cutover placement of the unchanged authoritative raw mapping.

This fixed operator reuses storage plans, resource admission and the native
physical owner. It is not runtime maintenance or another migration controller.
Its caller must qualify the serving layout, paired recovery and collection/spool
budget before dispatch. This module never stops services or launches recovery.
"""
from __future__ import annotations

import hashlib
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
from scripts.db import fact_header_v2_placement as physical
from scripts.db import raw_mapping_v2_copy as raw
from scripts.db.fact_header_v2_capture import LOCK, _bounded_step

logger = logging.getLogger(__name__)
OPERATION = "qt.retained_raw_history.v1"
LAYOUT = "market.fact_storage_tiers.v2"
POINTER = "retained_raw_history"
PLANS = "public.portal_storage_plans"


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _plan_id(request_id):
    if not isinstance(request_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,80}", request_id):
        raise ValueError("raw_history_request_id_invalid")
    return "raw-history-" + hashlib.sha256(request_id.encode()).hexdigest()[:32]


def _plan(conn, plan_id):
    return conn.scalar(text("SELECT to_jsonb(p) FROM " + PLANS + " p WHERE id=:id"), {"id": plan_id})


def _state(conn):
    row = conn.execute(text("SELECT state,evidence FROM market.fact_storage_state "
                            "WHERE layout_version=:layout"), {"layout": LAYOUT}).mappings().one_or_none()
    if row is None or row["state"] != "ready" or not isinstance(row["evidence"], dict):
        raise RuntimeError("raw_history_committed_handoff_required")
    return row["evidence"]


def _handoff(evidence):
    from scripts.db.fact_header_v2_handoff import RECEIPT_VERSION
    receipt = evidence.get("handoff")
    forward = receipt.get("forward_header") if isinstance(receipt, dict) else None
    if (evidence.get("source_retained") is not True or not isinstance(forward, dict)
            or receipt.get("schema_version") != RECEIPT_VERSION
            or forward.get("raw_mapping_mode") != "retain_source"
            or forward.get("raw_history_placement_pending") is not True
            or forward.get("canonical_raw_oid") != receipt.get("active_relation_oids", {}).get(raw.NAME)
            or receipt.get("binding", {}).get("plan", {}).get("recent_lookup_indexes") is not True):
        raise RuntimeError("raw_history_retained_source_handoff_required")
    return receipt


def _files(conn):
    rows = conn.execute(text("""
        WITH heap AS (SELECT oid,reltoastrelid FROM pg_class WHERE oid=to_regclass(:relation)),
        heaps AS (SELECT oid FROM heap UNION ALL SELECT reltoastrelid FROM heap WHERE reltoastrelid<>0),
        members AS (SELECT oid FROM heaps UNION ALL SELECT indexrelid FROM pg_index WHERE indrelid IN(SELECT oid FROM heaps))
        SELECT c.oid::bigint,c.relname,c.relkind,c.relfilenode::bigint,
               COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)::bigint AS tablespace_oid
        FROM members JOIN pg_class c ON c.oid=members.oid
        CROSS JOIN pg_database d WHERE d.datname=current_database() ORDER BY c.oid LIMIT 129
    """), {"relation": raw.SOURCE}).mappings().all()
    if not rows or len(rows) > 128:
        raise RuntimeError("raw_history_file_inventory_invalid")
    return [dict(row) for row in rows]


def inspect_completed_raw_history(conn, receipt, *, pid):
    """Verify a separate committed placement without rewriting its old handoff.

    Absence means the original SSD bridge still applies. A pointer or receipt is
    never sufficient without the exact relation identities and native placement.
    This inspection does not move data, renew clocks or certify recovery.
    """
    evidence = _state(conn)
    if _handoff(evidence) != receipt:
        raise RuntimeError("raw_history_handoff_changed")
    pointer = evidence.get(POINTER)
    if pointer is None:
        return None
    if (not isinstance(pointer, dict) or set(pointer) != {"plan_id", "completion_sha256"}
            or not isinstance(pointer["plan_id"], str)):
        raise RuntimeError("raw_history_completion_pointer_invalid")
    plan = _plan(conn, pointer["plan_id"])
    progress = plan.get("progress", {}) if plan else {}
    completion = progress.get("completion")
    if (plan is None or plan["state"] != "completed" or progress.get("operation") != OPERATION
            or progress.get("handoff_sha256") != _digest(receipt)
            or not isinstance(completion, dict) or _digest(completion) != pointer["completion_sha256"]
            or completion.get("before_files") != plan["impact"].get("before_files")
            or completion.get("after_files") != _files(conn)
            or plan["impact"].get("definition_sha256") != _digest(resources_owner._definition(conn, raw.SOURCE))):
        raise RuntimeError("raw_history_completion_changed")
    physical.verify_group(conn, raw.SOURCE, history=True, saved=receipt["binding"], pid=pid)
    return {"plan_id": plan["id"], "completion": completion, "reused": True,
            "raw_history_placement_pending": False, "recovery_verified": False}


def _owner(conn):
    # Session ownership survives intent commit and is discarded on every exit.
    for name in (LOCK, "qt.storage.management.v1"):
        if not conn.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"), {"name": name}):
            raise RuntimeError("raw_history_storage_busy")


def _current_policy(conn, policy):
    row = conn.execute(text("SELECT revision,policy FROM public.portal_storage_policy WHERE id=1")).mappings().one_or_none()
    if row is None or row["policy"] != policy.to_dict():
        raise RuntimeError("raw_history_saved_policy_changed")
    return row["revision"]


def _copy_bytes(conn, saved):
    recent = sorted(physical.recent_lookup_index_names(raw.SOURCE, saved))
    return int(conn.scalar(text("SELECT pg_total_relation_size(:relation)-COALESCE(("
        "SELECT sum(pg_total_relation_size(indexrelid)) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
        "WHERE i.indrelid=to_regclass(:relation) AND c.relname=ANY(:recent)),0)"),
        {"relation": raw.SOURCE, "recent": recent}))


def inspect_retained_raw_history(engine, *, request_id, handoff_sha256, policy, resource_limits):
    """Observe native placement and this exact intent without starting its clock.

    This bounded, read-only transaction does not reserve space or authorize a
    move. A completion is verified even after its original deadline; its recovery
    status remains separate. No row or archive-content scan is performed.
    """
    plan_id = _plan_id(request_id)
    if not isinstance(handoff_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", handoff_sha256):
        raise ValueError("raw_history_handoff_hash_invalid")
    limits = _limits(resource_limits)
    with engine.connect() as conn:
        try:
            with conn.begin():
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                with _bounded_step(conn, 30):
                    _owner(conn)
                    receipt = _handoff(_state(conn))
                    if _digest(receipt) != handoff_sha256:
                        raise RuntimeError("raw_history_handoff_changed")
                    saved = receipt["binding"]
                    placement = physical._restore(saved["plan"])
                    resources_owner._fixed_inputs(policy, limits, (placement.recent, placement.history))
                    pid = physical.verify(conn, saved)
                    completed = inspect_completed_raw_history(conn, receipt, pid=pid)
                    plan = _plan(conn, plan_id)
                    if plan is not None and (plan["request_id"] != request_id
                            or plan["policy"] != policy.to_dict()
                            or plan["progress"].get("operation") != OPERATION
                            or plan["progress"].get("handoff_sha256") != handoff_sha256
                            or plan["progress"].get("resource_limits") != limits):
                        raise RuntimeError("raw_history_intent_changed")
                    if completed is not None:
                        if completed["plan_id"] != plan_id:
                            raise RuntimeError("raw_history_already_completed_by_other_request")
                        return {**completed, "state": "completed", "inspection_only": True}
                    from scripts.db.fact_header_v2_handoff import _verify_handoff_relations
                    _verify_handoff_relations(conn, receipt, pid=pid)
                    revision = _current_policy(conn, policy)
                    now = conn.scalar(text("SELECT clock_timestamp()"))
                    progress = plan["progress"] if plan else {}
                    return {"plan_id": plan_id, "state": plan["state"] if plan else "not_started",
                        "policy_revision": revision, "observed_at": now.isoformat(),
                        "started_at": progress.get("started_at"), "expires_at": progress.get("expires_at"),
                        "copy_bytes": _copy_bytes(conn, saved),
                        "retained_ssd_indexes": sorted(physical.recent_lookup_index_names(raw.SOURCE, saved)),
                        "raw_history_placement_pending": True, "recovery_verified": False,
                        "inspection_only": True, "execution_admitted": False}
        finally:
            conn.invalidate()


def move_retained_raw_history(engine, *, request_id, handoff_sha256, policy, resource_limits,
                              cancelled=None):
    """One atomic native move; repeated calls retain the original intent deadline.

    Heap/TOAST and secondary indexes move to HDD; the bound raw primary index
    stays on SSD. ACCESS EXCLUSIVE can block raw ingestion and provenance reads.
    A lost commit reply must be inspected using this same request, not treated as
    failed work. Retained copies, archives and the original handoff are untouched.
    """
    plan_id = _plan_id(request_id)
    if not isinstance(handoff_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", handoff_sha256):
        raise ValueError("raw_history_handoff_hash_invalid")
    limits = _limits(resource_limits)
    if cancelled is not None and not callable(cancelled):
        raise ValueError("raw_history_cancellation_invalid")
    if cancelled is not None and cancelled():
        raise RuntimeError("storage_move_cancelled")
    started = monotonic()
    deadline = started + limits["movement_timeout_seconds"]
    with engine.connect() as conn:
        watch = None
        try:
            with conn.begin():
                previous = conn.scalar(text("SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
                if previous:
                    deadline = min(deadline, started + previous / 1000)
            with conn.begin(), _bounded_step(conn, min(30, limits["movement_timeout_seconds"])) as limit:
                limit(deadline - monotonic())
                _owner(conn)
                evidence = _state(conn)
                receipt = _handoff(evidence)
                if _digest(receipt) != handoff_sha256:
                    raise RuntimeError("raw_history_handoff_changed")
                saved = receipt["binding"]
                placement = physical._restore(saved["plan"])
                targets = (placement.recent, placement.history)
                resources_owner._fixed_inputs(policy, limits, targets)
                pid = physical.verify(conn, saved, deadline=deadline)
                completed = inspect_completed_raw_history(conn, receipt, pid=pid)
                if completed is not None:
                    if completed["plan_id"] != plan_id:
                        raise RuntimeError("raw_history_already_completed_by_other_request")
                    prior = _plan(conn, plan_id)
                    if (prior["request_id"] != request_id or prior["policy"] != policy.to_dict()
                            or prior["progress"].get("resource_limits") != limits):
                        raise RuntimeError("raw_history_intent_changed")
                    return completed
                from scripts.db.fact_header_v2_handoff import _verify_handoff_relations
                _verify_handoff_relations(conn, receipt, pid=pid)
                revision = _current_policy(conn, policy)
                if conn.scalar(text("SELECT EXISTS(SELECT 1 FROM " + PLANS + " WHERE "
                        "state IN ('queued','running','blocked') AND id<>:id)"), {"id": plan_id}):
                    raise RuntimeError("raw_history_other_storage_plan_active")
                plan = _plan(conn, plan_id)
                if plan is None:
                    # Fence definitions briefly while recording the intent.
                    conn.exec_driver_sql("LOCK TABLE " + raw.SOURCE + " IN ACCESS SHARE MODE NOWAIT")
                    impact = {"before_files": _files(conn),
                              "definition_sha256": _digest(resources_owner._definition(conn, raw.SOURCE))}
                    progress = {"operation": OPERATION, "handoff_sha256": handoff_sha256,
                                "resource_limits": limits, "completion": None}
                    conn.execute(text("INSERT INTO " + PLANS + "(id,request_id,base_revision,policy,policy_hash,state,impact,progress) "
                        "SELECT :id,:request,:revision,CAST(:policy AS jsonb),:hash,'running',CAST(:impact AS jsonb),"
                        "CAST(:progress AS jsonb)||jsonb_build_object('started_at',stamp,'expires_at',"
                        "stamp+:seconds*interval '1 second') FROM (SELECT clock_timestamp() stamp)s"),
                        {"id": plan_id, "request": request_id, "revision": revision, "policy": json.dumps(policy.to_dict()),
                         "hash": policy.fingerprint, "impact": json.dumps(impact), "progress": json.dumps(progress),
                         "seconds": limits["movement_timeout_seconds"]})
                    plan = _plan(conn, plan_id)
                if (plan["request_id"] != request_id or plan["state"] != "running"
                        or plan["base_revision"] != revision or plan["policy"] != policy.to_dict()
                        or plan["policy_hash"] != policy.fingerprint
                        or plan["progress"].get("operation") != OPERATION
                        or plan["progress"].get("handoff_sha256") != handoff_sha256
                        or plan["progress"].get("resource_limits") != limits
                        or plan["progress"].get("completion") is not None):
                    raise RuntimeError("raw_history_intent_changed")
            logger.info("retained_raw_history_intent | plan_id=%s expires_at=%s",
                        plan_id, plan["progress"]["expires_at"])
            with conn.begin(), _bounded_step(conn, limits["movement_timeout_seconds"]) as limit:
                clock = conn.execute(text("SELECT extract(epoch FROM expires-started) AS duration,"
                    "extract(epoch FROM clock_timestamp()-started) AS age FROM (SELECT "
                    "CAST(progress->>'expires_at' AS timestamptz) expires,"
                    "CAST(progress->>'started_at' AS timestamptz) started FROM " + PLANS + " WHERE id=:id)s"),
                    {"id": plan_id}).mappings().one()
                if clock["duration"] != limits["movement_timeout_seconds"] or clock["age"] < 0:
                    raise RuntimeError("raw_history_intent_clock_changed")
                deadline = min(deadline, monotonic() + float(clock["duration"] - clock["age"]))
                limit(deadline - monotonic())
                if cancelled is not None and cancelled():
                    raise RuntimeError("storage_move_cancelled")
                conn.exec_driver_sql("LOCK TABLE " + raw.SOURCE + " IN ACCESS EXCLUSIVE MODE NOWAIT")
                if _state(conn) != evidence or _plan(conn, plan_id) != plan:
                    raise RuntimeError("raw_history_intent_changed")
                if (_files(conn) != plan["impact"]["before_files"]
                        or _digest(resources_owner._definition(conn, raw.SOURCE)) != plan["impact"]["definition_sha256"]):
                    raise RuntimeError("raw_history_source_changed")
                physical.verify_group(conn, raw.SOURCE, history=False, saved=saved,
                                      pid=physical.verify(conn, saved, deadline=deadline))
                if _current_policy(conn, policy) != plan["base_revision"]:
                    raise RuntimeError("raw_history_saved_policy_changed")
                indexes = conn.execute(text("SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                    "WHERE i.indrelid=to_regclass(:relation) ORDER BY c.oid LIMIT 65"), {"relation": raw.SOURCE}).scalars().all()
                recent = physical.recent_lookup_index_names(raw.SOURCE, saved)
                copy_bytes = _copy_bytes(conn, saved)
                resources = observe_header_resources(conn, targets, pg_controldata=placement.pg_controldata,
                    timeout_seconds=min(30, max(1, math.ceil(deadline - monotonic()))))
                budget, floors = resources_owner._budget(conn, observed={"bytes": int(copy_bytes), "_binding": saved},
                    policy=policy, limits={**limits, "movement_timeout_seconds": max(1, math.ceil(deadline-monotonic()))},
                    targets=targets, resources=resources)
                watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets,
                    capacity=resources.capacity, floors=floors, deadline=deadline,
                    cancelled=cancelled, grace=limits["cancellation_grace_seconds"])
                watch.start()
                logger.info("retained_raw_history_moving | plan_id=%s copy_bytes=%s", plan_id, copy_bytes)
                quote = conn.dialect.identifier_preparer.quote
                destination = quote(saved["history_name"])
                conn.exec_driver_sql("ALTER TABLE " + raw.SOURCE + " SET TABLESPACE " + destination)
                watch.check()
                for name in indexes:
                    if name not in recent:
                        conn.exec_driver_sql("ALTER INDEX market." + quote(name) + " SET TABLESPACE " + destination)
                        watch.check()
                if _digest(resources_owner._definition(conn, raw.SOURCE)) != plan["impact"]["definition_sha256"]:
                    raise RuntimeError("raw_history_definition_changed")
                physical.verify_group(conn, raw.SOURCE, history=True, saved=saved,
                                      pid=physical.verify(conn, saved, deadline=deadline))
                completion = {"before_files": plan["impact"]["before_files"], "after_files": _files(conn),
                    "copy_bytes": int(copy_bytes), "resource_budget": budget,
                    "completed_at": conn.scalar(text("SELECT clock_timestamp()")).isoformat()}
                conn.execute(text("UPDATE " + PLANS + " SET state='completed',updated_at=clock_timestamp(),"
                    "progress=progress||jsonb_build_object('completion',CAST(:completion AS jsonb)) WHERE id=:id"),
                    {"id": plan_id, "completion": json.dumps(completion)})
                pointer = {"plan_id": plan_id, "completion_sha256": _digest(completion)}
                conn.execute(text("UPDATE market.fact_storage_state SET evidence=evidence||"
                    "jsonb_build_object(CAST(:key AS text),CAST(:pointer AS jsonb)) WHERE layout_version=:layout"),
                    {"key": POINTER, "pointer": json.dumps(pointer), "layout": LAYOUT})
                watch.check()
            watch.check()
            with conn.begin(), _bounded_step(conn, 30):
                result = inspect_completed_raw_history(conn, receipt, pid=physical.verify(conn, saved))
            logger.info("retained_raw_history_completed | plan_id=%s copy_bytes=%s duration_seconds=%s",
                        plan_id, copy_bytes, monotonic()-started)
            return {**result, "reused": False}
        except Exception as exc:
            logger.error("retained_raw_history_stopped | plan_id=%s error_type=%s inspect_before_retry=true",
                         plan_id, type(exc).__name__)
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            try:
                if watch is not None:
                    watch.stop(conn)
            finally:
                conn.invalidate()


def cancel_retained_raw_history(engine, *, request_id, handoff_sha256):
    """Close one unmoved intent, even after expiry; never reverse a committed move.

    The same storage/physical owner proves rollback before releasing the plan.
    An uncertain connection failure leaves the intent intact for inspection.
    A later separately authorized attempt must use a distinct request identity.
    """
    plan_id = _plan_id(request_id)
    if not isinstance(handoff_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", handoff_sha256):
        raise ValueError("raw_history_handoff_hash_invalid")
    with engine.connect() as conn:
        try:
            with conn.begin(), _bounded_step(conn, 30):
                _owner(conn)
                receipt = _handoff(_state(conn))
                if _digest(receipt) != handoff_sha256:
                    raise RuntimeError("raw_history_handoff_changed")
                plan = _plan(conn, plan_id)
                if (plan is None or plan["request_id"] != request_id
                        or plan["progress"].get("operation") != OPERATION
                        or plan["progress"].get("handoff_sha256") != handoff_sha256):
                    raise RuntimeError("raw_history_intent_changed")
                pid = physical.verify(conn, receipt["binding"])
                completed = inspect_completed_raw_history(conn, receipt, pid=pid)
                if completed is not None:
                    if completed["plan_id"] != plan_id:
                        raise RuntimeError("raw_history_already_completed_by_other_request")
                    return completed
                if plan["state"] not in {"running", "cancelled"} or plan["progress"].get("completion") is not None:
                    raise RuntimeError("raw_history_intent_changed")
                conn.exec_driver_sql("LOCK TABLE " + raw.SOURCE + " IN ACCESS SHARE MODE NOWAIT")
                if (_files(conn) != plan["impact"]["before_files"]
                        or _digest(resources_owner._definition(conn, raw.SOURCE)) != plan["impact"]["definition_sha256"]):
                    raise RuntimeError("raw_history_rollback_unconfirmed")
                physical.verify_group(conn, raw.SOURCE, history=False, saved=receipt["binding"], pid=pid)
                if plan["state"] != "cancelled":
                    conn.execute(text("UPDATE " + PLANS + " SET state='cancelled',updated_at=clock_timestamp(),"
                        "progress=progress||jsonb_build_object('cancelled_at',clock_timestamp()) WHERE id=:id"),
                        {"id": plan_id})
                return {"plan_id": plan_id, "state": "cancelled", "source_preserved": True,
                        "reused": plan["state"] == "cancelled"}
        finally:
            conn.invalidate()
