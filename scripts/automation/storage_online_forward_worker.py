"""Fixed key and initialization phases for the confined forward worker.

No host launch, source stop or capture renewal. The published request binds the
canceled attempt; actual database receipts own the separate preparation clocks.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import json
import logging
import math
import re
from time import monotonic

LOG = logging.getLogger(__name__)
INITIAL = "qt_fact_header_forward_v2.initialization"


def request_binding(request):
    if "forward" not in request:
        return None
    value = request["forward"]
    fields = {"schema_version", "original_plan_sha256", "cancellation_intent_sha256",
        "original_capture", "end_day", "candidate_revision", "candidate_source_hash", "operation_sha256"}
    if (not isinstance(value, dict) or set(value) != fields
            or value["schema_version"] != "qt.storage_online_forward_intent.v1"
            or "capture_preparation" in request
            or not isinstance(value["original_capture"], dict) or not value["original_capture"]
            or not isinstance(value["end_day"], str)
            or any(not isinstance(value[k], str) or not re.fullmatch(pattern, value[k]) for k, pattern in (
                ("original_plan_sha256", r"[0-9a-f]{64}"), ("cancellation_intent_sha256", r"[0-9a-f]{64}"),
                ("operation_sha256", r"[0-9a-f]{64}"), ("candidate_revision", r"[0-9a-f]{40}"),
                ("candidate_source_hash", r"[0-9a-f]{64}")))
            or value["candidate_revision"] != request.get("source_revision")
            or value["candidate_source_hash"] != request.get("source_tree_hash")
            or value["operation_sha256"] == value["cancellation_intent_sha256"]
            or value["operation_sha256"] != _digest({k:v for k,v in value.items() if k != "operation_sha256"})):
        raise ValueError("storage_forward_worker_request_invalid")
    if date.fromisoformat(value["end_day"]).isoformat() != value["end_day"]:
        raise ValueError("storage_forward_worker_end_day_invalid")
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def capture_observation(state):
    """The immutable adoption identity shared by live and terminal observers."""
    return {**{name: state[name] for name in
        ("operation_sha256", "started_at", "expires_at", "attempt_seconds")},
        "cancellation": state["binding"]["terminal"],
        "placement": state["binding"]["old_headers"]["placement"]}


def capture_binding(value):
    """Compact exact proof identity for the bounded host/worker control pipe.

    Full cancellation and placement metadata stay in SQL and the bounded host
    launch journal. Hash the complete observation; never truncate its proof.
    """
    fields = {"operation_sha256", "started_at", "expires_at", "attempt_seconds", "cancellation", "placement"}
    if (not isinstance(value, dict) or set(value) != fields
            or not isinstance(value["operation_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", value["operation_sha256"])
            or type(value["attempt_seconds"]) is not int
            or not 30 <= value["attempt_seconds"] <= 345600
            or any(not isinstance(value[k], dict) or not value[k] for k in ("cancellation", "placement"))):
        raise ValueError("storage_forward_capture_binding_invalid")
    normalized = dict(value)
    for key in ("started_at", "expires_at"):
        stamp = value[key] if isinstance(value[key], datetime) else datetime.fromisoformat(value[key])
        if stamp.utcoffset() != timedelta(0):
            raise ValueError("storage_forward_capture_binding_clock_invalid")
        normalized[key] = stamp.isoformat()
    return dict(schema_version="qt.storage_online_forward_capture.v1",
        operation_sha256=value["operation_sha256"], started_at=normalized["started_at"],
        expires_at=normalized["expires_at"], attempt_seconds=value["attempt_seconds"],
        proof_sha256=_digest(normalized))


def _initial(conn):
    from sqlalchemy import text
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name":INITIAL}) is None:
        return None
    rows = conn.execute(text("SELECT id,binding,started_at,expires_at,duration_seconds,complete FROM "+INITIAL+" LIMIT 2")).mappings().all()
    if (len(rows) != 1 or rows[0]["id"] != 1
            or rows[0]["expires_at"] != rows[0]["started_at"]+timedelta(seconds=rows[0]["duration_seconds"])):
        raise RuntimeError("storage_forward_initialization_receipt_changed")
    return dict(rows[0])


def prepare_forward(engine, request, *, targets, policy, limits, source, destination,
                    key_seconds=3600, initial_seconds=600):
    """Prepare keys, then persist ONE short initialization clock before adoption.

    A completed initializer only inspects its original adoption/archive state.
    An interrupted initializer uses its original receipt, never a fresh timeout.
    Both initialization writes commit together; lost COMMIT replies reconcile
    the same complete record. The old capture is never activated or amended.
    """
    from sqlalchemy import text
    from scripts.db import fact_header_forward_keys as keys
    from scripts.db import fact_header_forward_adoption as adoption
    from scripts.db import fact_header_v2_capture as capture
    from scripts.db import fact_header_v2_copy as headers
    from scripts.db import fact_header_v2_placement as physical
    from scripts.db import archive_root_v2_online as archives
    from scripts.db import archive_reference_v2_placement as reference_move
    from portal.backend.service.storage.header_resource_claims import _limits
    from portal.backend.service.storage.header_resources import observe_header_resources
    from portal.backend.service.storage.header_movement import _MoveWatch

    intent = request_binding(request)
    if (intent is None or type(key_seconds) is not int or not 30 <= key_seconds <= 3600
            or type(initial_seconds) is not int or not 30 <= initial_seconds <= 600):
        raise ValueError("storage_forward_worker_phase_bounds_invalid")
    seconds = intent["original_capture"].get("attempt_seconds")
    if type(seconds) is not int or not 30 <= seconds <= 345600:
        raise ValueError("storage_forward_worker_adoption_bound_invalid")
    limits = _limits(limits, migration=True)
    binding = dict(request_sha256=_digest(request), operation_sha256=intent["operation_sha256"],
        cancellation_intent_sha256=intent["cancellation_intent_sha256"], key_seconds=key_seconds,
        initial_seconds=initial_seconds, attempt_seconds=seconds)
    with engine.begin() as conn, capture._bounded_step(conn, 10):
        prior = _initial(conn)
        saved = conn.scalar(text("SELECT placement FROM "+headers.STATE+" WHERE id=1"))
        if not isinstance(saved, dict) or not isinstance(saved.get("plan"), dict):
            raise RuntimeError("storage_forward_retained_placement_required")
        placement = physical._restore(saved["plan"])
        if {t.target_id:t for t in targets} != {t.target_id:t for t in (placement.recent,placement.history)}:
            raise RuntimeError("storage_forward_worker_inventory_changed")
    if prior is None:
        keys.prepare_keys_supervised(engine, expected_capture=intent["original_capture"],
            intent_sha256=intent["cancellation_intent_sha256"], placement=placement,
            policy=policy, resource_limits=limits, max_duration_seconds=key_seconds)
    with engine.connect() as conn:
        acquired = []
        watch = None
        try:
            with conn.begin(), capture._bounded_step(conn, 10):
                for name in ("qt.storage.management.v1", keys.CONTROLLER_LOCK):
                    if not conn.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"), {"name":name}):
                        raise RuntimeError("storage_forward_initialization_owner_busy")
                    acquired.append(name)
                phase = _initial(conn)
                prepared = keys._read_state(conn)
                if (prepared is None or not prepared["complete"]
                        or prepared["duration_seconds"] != key_seconds
                        or prepared["index_oids"] != keys.inspect_keys(conn)):
                    raise RuntimeError("storage_forward_initialization_keys_changed")
                if phase is None:
                    if adoption._state(conn) is not None:
                        raise RuntimeError("storage_forward_unowned_adoption")
                    actual = keys._source_binding(conn, intent["original_capture"], intent["cancellation_intent_sha256"])
                    if actual != prepared["binding"]:
                        raise RuntimeError("storage_forward_initialization_source_changed")
                    conn.exec_driver_sql("CREATE TABLE "+INITIAL+"(id integer PRIMARY KEY CHECK(id=1),"
                        "binding jsonb NOT NULL,started_at timestamptz NOT NULL,expires_at timestamptz NOT NULL,"
                        "duration_seconds integer NOT NULL CHECK(duration_seconds BETWEEN 30 AND 600),"
                        "complete boolean NOT NULL DEFAULT false,"
                        "CHECK(expires_at=started_at+duration_seconds*interval '1 second'))")
                    conn.execute(text("INSERT INTO "+INITIAL+" SELECT 1,CAST(:binding AS jsonb),"
                        "stamp,stamp+:seconds*interval '1 second',:seconds,false FROM (SELECT clock_timestamp() stamp) s"),
                        {"binding":json.dumps(binding),"seconds":initial_seconds})
                    phase = _initial(conn)
                if phase["binding"] != binding or phase["duration_seconds"] != initial_seconds:
                    raise RuntimeError("storage_forward_initialization_binding_changed")
            # Durable initialization intent is committed before adoption/archive
            # preparation. Session locks still belong to this actual connection.
            if not phase["complete"]:
                with conn.begin(), capture._bounded_step(conn, 10):
                    remaining = float(conn.scalar(text("SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM "+INITIAL+" WHERE id=1")))
                    if remaining <= 1:
                        raise RuntimeError("storage_forward_initialization_expired")
                    deadline = monotonic()+min(remaining, limits["movement_timeout_seconds"])
                    actual, _ = physical.observe(conn, placement)
                    if actual != saved:
                        raise RuntimeError("storage_forward_initialization_placement_changed")
                    reference_move._fixed_inputs(policy, limits, targets)
                    resources = observe_header_resources(conn, targets, pg_controldata=placement.pg_controldata, timeout_seconds=10)
                    remaining = deadline-monotonic()
                    if remaining <= 0:
                        raise RuntimeError("storage_forward_initialization_expired")
                    _, floors = reference_move._budget(conn, observed={"bytes":0,"_binding":saved},
                        policy=policy, limits={**limits, "movement_timeout_seconds":math.ceil(remaining)},
                        targets=targets, resources=resources)
                    watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets,
                        capacity=resources.capacity, floors=floors, deadline=deadline,
                        cancelled=None, grace=limits["cancellation_grace_seconds"])
                    watch.start()
                with conn.begin():
                    allowance = int(min(30, deadline-monotonic()))
                    if allowance < 1: raise RuntimeError("storage_forward_initialization_expired")
                    adoption.prepare_adoption(conn, expected_capture=intent["original_capture"],
                        cancellation_intent_sha256=intent["cancellation_intent_sha256"],
                        operation_sha256=intent["operation_sha256"], attempt_seconds=seconds, timeout_seconds=allowance)
                    allowance = int(min(30, deadline-monotonic()))
                    if allowance < 1: raise RuntimeError("storage_forward_initialization_expired")
                    archives.prepare(conn, source_root=source, destination_root=destination,
                        forward_operation_sha256=intent["operation_sha256"], timeout_seconds=allowance)
                    watch.check()
                    conn.exec_driver_sql("UPDATE "+INITIAL+" SET complete=true WHERE id=1")
                watch.check()
                LOG.info("storage_forward_initialized operation_sha256=%s", intent["operation_sha256"])
            with conn.begin(), capture._bounded_step(conn, 10) as limit:
                state = adoption._inspect(conn, intent["operation_sha256"], limit)
                if (state["attempt_seconds"] != seconds
                        or state["binding"]["terminal"]["receipt"]["capture"] != intent["original_capture"]
                        or state["binding"]["terminal"]["receipt"]["intent_sha256"] != intent["cancellation_intent_sha256"]):
                    raise RuntimeError("storage_forward_adoption_binding_changed")
                archives._inspect(conn, source, destination, forward_operation_sha256=intent["operation_sha256"], saved=saved)
                return placement, state["started_at"].isoformat()
        except Exception as exc:
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            try:
                if watch is not None: watch.stop(conn)
            finally:
                if not conn.closed and not conn.invalidated:
                    conn.rollback()
                    for name in reversed(acquired):
                        conn.execute(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"), {"name":name})
                    conn.commit()
