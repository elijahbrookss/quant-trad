"""Explicit online key preparation for the retained-header forward handoff.

Internal database primitive only. No capture installation, target adoption,
range constraint, table attachment, runtime activation or cutover authority.
The host still owns source/fleet/physical/resource admission and cancellation.
"""
from __future__ import annotations

from contextlib import nullcontext
import math
import json
import logging
from time import monotonic

from sqlalchemy import text

from scripts.db import fact_header_v2_cancel as cancellation
from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_copy as headers
from scripts.db import fact_header_v2_admission as admission
from scripts.db.fact_header_v2_deadline import CONTROLLER_LOCK

SCHEMA = "qt_fact_header_forward_v2"
STATE = SCHEMA + ".key_preparation"
KEYS = {
    "qt_header_forward_day_pk": ("id", "storage_day"),
    "qt_header_forward_revision_day": ("series_id", "observation_key", "revision", "storage_day"),
}
logger = logging.getLogger(__name__)


def inspect_keys(conn):
    """Admit only the two exact ordinary, immediate, default-btree keys."""
    result = {}
    for name, columns in KEYS.items():
        row = conn.execute(text("""
            SELECT c.oid::bigint AS oid,c.relkind,c.relpersistence,c.relacl,c.reloptions,
                   c.relowner=s.relowner AS same_owner,
                   i.indrelid=s.oid AS same_source,i.indisvalid,i.indisready,
                   i.indislive,i.indisunique,i.indimmediate,i.indisprimary,
                   pg_get_indexdef(c.oid) AS definition,
                   EXISTS(SELECT 1 FROM pg_constraint WHERE conindid=c.oid) AS constrained
            FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid
            CROSS JOIN pg_class s WHERE c.oid=to_regclass(:name)
              AND s.oid='market.fact_versions'::regclass
        """), {"name":"market."+name}).mappings().one_or_none()
        if row is None:
            result[name] = None
            continue
        expected = ("CREATE UNIQUE INDEX "+name+" ON market.fact_versions USING btree ("
                    +", ".join(columns)+")")
        if (row["relkind"] != "i" or row["relpersistence"] != "p"
                or row["relacl"] is not None or row["reloptions"] is not None
                or not all(row[key] for key in ("same_owner", "same_source", "indisvalid",
                           "indisready", "indislive", "indisunique", "indimmediate"))
                or row["indisprimary"] or row["constrained"] or row["definition"] != expected):
            raise RuntimeError("fact_header_forward_key_changed_or_incomplete: "+name)
        result[name] = row["oid"]
    return result


def _source_binding(conn, expected_capture, intent_sha256):
    receipt = cancellation.inspect_cancellation(conn, expected_capture=expected_capture,
        intent_sha256=intent_sha256, timeout_seconds=30)
    if receipt is None:
        raise RuntimeError("fact_header_forward_committed_cancellation_required")
    # The old progress and targets are evidence, never modified or made active.
    state = dict(conn.execute(text("SELECT * FROM "+headers.STATE+" WHERE id=1")).mappings().one())
    if state["targets"] != {name:headers._shape(conn,name) for name in headers.TABLE_NAMES}:
        raise RuntimeError("fact_header_forward_retained_target_changed")
    headers._source_columns(conn)
    admission._assert_v1_source_layout(conn, context=expected_capture,
        capture_active=False, forward_keys=True)
    files = [dict(row) for row in conn.execute(text("""
        SELECT c.oid::bigint AS oid,c.relfilenode::bigint AS file,c.relname
        FROM pg_class c WHERE c.oid='market.fact_versions'::regclass
           OR c.oid IN (SELECT indexrelid FROM pg_index
              WHERE indrelid='market.fact_versions'::regclass)
        ORDER BY c.oid
    """)).mappings() if row["relname"] not in KEYS]
    return dict(capture=expected_capture, cancellation_intent_sha256=intent_sha256,
                source_files=files, retained_targets=state["targets"])


def _read_state(conn):
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name":STATE}) is None:
        return None
    rows = conn.execute(text("SELECT id,binding,started_at,expires_at,duration_seconds,"
        "index_oids,complete FROM "+STATE+" LIMIT 2")).mappings().all()
    if len(rows) != 1 or rows[0]["id"] != 1:
        raise RuntimeError("fact_header_forward_preparation_state_changed")
    return dict(rows[0])


def prepare_keys(engine, *, expected_capture, intent_sha256, max_duration_seconds=600,
                 connection=None, deadline=None):
    """Build the two composite keys without copying the heap or search indexes.

    One durable database deadline covers both concurrent builds and all retries.
    A failed/invalid index is retained and refused, never silently dropped or
    rebuilt. Production callers use prepare_keys_supervised for the same-session
    resource watch. A supplied connection/deadline can only narrow ownership.
    It does not make the old captured targets complete or authorize attachment.
    """
    cancellation._validate_intent(expected_capture, intent_sha256)
    if type(max_duration_seconds) is not int or not 30 <= max_duration_seconds <= 3600:
        raise ValueError("fact_header_forward_preparation_bound_invalid")
    if connection is not None and (connection.engine is not engine or connection.closed
            or connection.invalidated or connection.in_transaction()):
        raise ValueError("fact_header_forward_preparation_connection_invalid")
    if deadline is not None and (type(deadline) not in (int, float)
            or not math.isfinite(deadline) or deadline <= monotonic()):
        raise ValueError("fact_header_forward_preparation_deadline_invalid")
    deadline = min(deadline, monotonic()+max_duration_seconds) if deadline is not None else monotonic()+max_duration_seconds
    with (nullcontext(connection) if connection is not None else engine.connect()) as conn:
        locked = conn.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"),
                             {"name":CONTROLLER_LOCK})
        conn.commit()
        if not locked:
            raise RuntimeError("fact_header_forward_controller_active")
        try:
            with conn.begin(), capture._bounded_step(conn, 30):
                binding = _source_binding(conn, expected_capture, intent_sha256)
                state = _read_state(conn)
                keys = inspect_keys(conn)
                if state is None:
                    if any(value is not None for value in keys.values()):
                        raise RuntimeError("fact_header_forward_unowned_preparation_key")
                    if conn.scalar(text("SELECT to_regnamespace(:name)"), {"name":SCHEMA}) is not None:
                        raise RuntimeError("fact_header_forward_unowned_preparation_schema")
                    conn.exec_driver_sql("CREATE SCHEMA "+SCHEMA)
                    conn.exec_driver_sql("CREATE TABLE "+STATE+"(id integer PRIMARY KEY CHECK(id=1),"
                        "binding jsonb NOT NULL,started_at timestamptz NOT NULL,expires_at timestamptz NOT NULL,"
                        "duration_seconds integer NOT NULL CHECK(duration_seconds BETWEEN 30 AND 3600),"
                        "index_oids jsonb NOT NULL,complete boolean NOT NULL DEFAULT false,"
                        "CHECK(expires_at=started_at+duration_seconds*interval '1 second'))")
                    conn.execute(text("INSERT INTO "+STATE+" SELECT 1,CAST(:binding AS jsonb),"
                        "stamp,stamp+:seconds*interval '1 second',:seconds,'{}'::jsonb,false "
                        "FROM (SELECT clock_timestamp() AS stamp) start"),
                        {"binding":json.dumps(binding), "seconds":max_duration_seconds})
                    state = _read_state(conn)
                if state["binding"] != binding or state["duration_seconds"] != max_duration_seconds:
                    raise RuntimeError("fact_header_forward_preparation_binding_changed")
                if any(keys.get(name) != oid for name,oid in state["index_oids"].items()):
                    raise RuntimeError("fact_header_forward_preparation_index_replaced")
                if state["complete"]:
                    if set(state["index_oids"]) != set(KEYS):
                        raise RuntimeError("fact_header_forward_preparation_state_changed")
                    return dict(keys_prepared=True, reused=True, migration_ready=False,
                                final_switch_authorized=False, source_retained=True)
            for name, columns in KEYS.items():
                with conn.begin(), capture._bounded_step(conn, 30):
                    binding_now = _source_binding(conn, expected_capture, intent_sha256)
                    state = _read_state(conn)
                    if state["binding"] != binding_now or binding_now != binding:
                        raise RuntimeError("fact_header_forward_preparation_binding_changed")
                    seconds = min(deadline-monotonic(), conn.scalar(text(
                        "SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM "+STATE+" WHERE id=1")))
                    if seconds <= 5:
                        raise RuntimeError("fact_header_forward_preparation_expired")
                    current = inspect_keys(conn)[name]
                if current is None:
                    # Concurrent builds require a separate autocommit statement;
                    # the same session retains controller exclusion throughout.
                    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                    settings = dict(conn.execute(text("SELECT name,setting FROM pg_settings WHERE name IN "
                        "('statement_timeout','lock_timeout','default_tablespace')")).all())
                    statement_ms = max(1,int((seconds-5)*1000))
                    if int(settings["statement_timeout"]):
                        statement_ms = min(statement_ms,int(settings["statement_timeout"]))
                    lock_ms = min(1000,int(settings["lock_timeout"]) or 1000)
                    try:
                        conn.exec_driver_sql("SET statement_timeout="+str(statement_ms))
                        conn.exec_driver_sql("SET lock_timeout="+str(lock_ms))
                        conn.exec_driver_sql("SET default_tablespace=''")
                        conn.exec_driver_sql("CREATE UNIQUE INDEX CONCURRENTLY "+name+
                            " ON market.fact_versions ("+", ".join(columns)+")")
                    finally:
                        conn.rollback()
                        if conn.closed or conn.invalidated:
                            raise RuntimeError("fact_header_forward_preparation_session_lost")
                        for setting,value in settings.items():
                            conn.execute(text("SELECT set_config(:setting,:value,false)"),
                                         {"setting":setting,"value":value})
                        conn.rollback()
                        conn = conn.execution_options(isolation_level="READ COMMITTED")
                with conn.begin(), capture._bounded_step(conn, 30):
                    if _source_binding(conn, expected_capture, intent_sha256) != binding:
                        raise RuntimeError("fact_header_forward_preparation_binding_changed")
                    keys = inspect_keys(conn)
                    conn.execute(text("UPDATE "+STATE+" SET index_oids=index_oids||CAST(:value AS jsonb) WHERE id=1"),
                                 {"value":json.dumps({name:keys[name]})})
                logger.info("fact_header_forward_key_prepared | index=%s oid=%s", name, keys[name])
            with conn.begin(), capture._bounded_step(conn, 30):
                if _source_binding(conn, expected_capture, intent_sha256) != binding:
                    raise RuntimeError("fact_header_forward_preparation_binding_changed")
                state = _read_state(conn)
                if state["index_oids"] != inspect_keys(conn) or set(state["index_oids"]) != set(KEYS):
                    raise RuntimeError("fact_header_forward_preparation_index_replaced")
                if (monotonic() >= deadline or conn.scalar(text(
                        "SELECT clock_timestamp()>=expires_at FROM "+STATE+" WHERE id=1"))):
                    raise RuntimeError("fact_header_forward_preparation_expired")
                conn.exec_driver_sql("UPDATE "+STATE+" SET complete=true WHERE id=1")
            return dict(keys_prepared=True, reused=False, migration_ready=False,
                        final_switch_authorized=False, source_retained=True)
        finally:
            conn.rollback()
            if conn.closed or conn.invalidated:
                raise RuntimeError("fact_header_forward_preparation_session_lost")
            conn.execute(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"),
                         {"name":CONTROLLER_LOCK})
            conn.commit()


def prepare_keys_supervised(engine, *, expected_capture, intent_sha256, placement,
                            policy, resource_limits, max_duration_seconds=600,
                            deadline=None, cancelled=None):
    """Admit fixed-drive resources and watch the actual concurrent-build session.

    Explicit maintenance allowances cover the two indexes; observed net space,
    temp/WAL/growth reserves and the original deadline remain enforced through
    each AUTOCOMMIT build and its durable progress write. This does not predict
    production index size, duration or collection impact.
    """
    from scripts.db import fact_header_v2_placement as physical
    from scripts.db import archive_reference_v2_placement as reference_move
    from portal.backend.service.storage.header_resource_claims import _limits
    from portal.backend.service.storage.header_resources import observe_header_resources
    from portal.backend.service.storage.header_movement import _MoveWatch

    cancellation._validate_intent(expected_capture, intent_sha256)
    limits = _limits(resource_limits, migration=True)
    if (not isinstance(placement, physical.CopyPlacement)
            or type(max_duration_seconds) is not int or not 30 <= max_duration_seconds <= 3600
            or (cancelled is not None and not callable(cancelled))
            or (deadline is not None and (type(deadline) not in (int, float)
                or not math.isfinite(deadline) or deadline <= monotonic()))):
        raise ValueError("fact_header_forward_supervision_invalid")
    targets = (placement.recent, placement.history)
    reference_move._fixed_inputs(policy, limits, targets)
    bound = monotonic()+min(max_duration_seconds, limits["movement_timeout_seconds"])
    deadline = min(deadline, bound) if deadline is not None else bound
    if cancelled is not None and cancelled():
        raise RuntimeError("storage_move_cancelled")
    with engine.connect() as conn:
        watch = None
        locked = False
        try:
            with conn.begin():
                previous = conn.scalar(text("SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
                if previous:
                    deadline = min(deadline, monotonic()+previous/1000)
                with capture._bounded_step(conn, min(30, limits["movement_timeout_seconds"])):
                    locked = conn.scalar(text("SELECT pg_try_advisory_lock("
                        "hashtextextended('qt.storage.management.v1',0))"))
                    if not locked:
                        raise RuntimeError("fact_header_forward_storage_busy")
                    _source_binding(conn, expected_capture, intent_sha256)
                    saved, _ = physical.observe(conn, placement)
                    retained = conn.scalar(text("SELECT placement FROM "+headers.STATE+" WHERE id=1"))
                    if saved != retained:
                        raise RuntimeError("fact_header_forward_preparation_placement_changed")
                    state = _read_state(conn)
                    if state is not None and not state["complete"]:
                        remaining = conn.scalar(text("SELECT extract(epoch FROM expires_at-clock_timestamp()) "
                            "FROM "+STATE+" WHERE id=1"))
                        deadline = min(deadline, monotonic()+float(remaining))
                    resources = observe_header_resources(conn, targets, pg_controldata=placement.pg_controldata,
                        timeout_seconds=min(30, limits["movement_timeout_seconds"]))
                    budget, floors = reference_move._budget(conn, observed={"bytes":0, "_binding":saved},
                        policy=policy, limits=limits, targets=targets, resources=resources)
                    watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets,
                        capacity=resources.capacity, floors=floors, deadline=deadline,
                        cancelled=cancelled, grace=limits["cancellation_grace_seconds"])
                    watch.start()
            result = prepare_keys(engine, expected_capture=expected_capture, intent_sha256=intent_sha256,
                max_duration_seconds=max_duration_seconds, connection=conn, deadline=deadline)
            watch.check()
            return {**result, "resource_budget":budget}
        except Exception as exc:
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            # Join before returning the physical connection to its pool; a late
            # cancel must never reach another borrower's SQL.
            try:
                if watch is not None:
                    watch.stop(conn)
            finally:
                conn.rollback()
                if locked and not conn.closed and not conn.invalidated:
                    conn.execute(text("SELECT pg_advisory_unlock("
                        "hashtextextended('qt.storage.management.v1',0))"))
                    conn.commit()
