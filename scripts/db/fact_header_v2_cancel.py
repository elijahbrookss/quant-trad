"""Terminal preserving cancellation of an admitted online v1 migration attempt.

No CLI, data deletion, deadline renewal or source/runtime switch. The caller
commits one short transaction; rollback restores all capture and staged FKs.
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import text

from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_copy as headers
from scripts.db import fact_header_v2_references as references
from scripts.db import raw_mapping_v2_copy as raw
from scripts.db import archive_root_v2_online as archives, archive_root_v2_copy as archive_files
from scripts.db.fact_header_v2_admission import assert_v1_source_admission, _incoming_references

logger = logging.getLogger(__name__)


def _exists(conn, name):
    return conn.scalar(text("SELECT to_regclass(:name)"), {"name": name}) is not None


def _triggers(conn, relations):
    return [dict(row) for row in conn.execute(text("""
        SELECT t.tgrelid::regclass::text AS relation,t.tgname,
               t.oid::bigint AS oid,pg_get_triggerdef(t.oid) AS definition,t.tgenabled
        FROM pg_trigger t WHERE t.tgrelid IN
          (SELECT to_regclass(name) FROM unnest(CAST(:names AS text[])) AS name)
          AND NOT t.tgisinternal ORDER BY relation,t.tgname
    """), {"names": relations}).mappings()]


def cancel_attempt(conn, *, expected_started_at, source_root=None,
                   destination_root=None, timeout_seconds=30):
    """Detach only admitted temporary write dependencies, preserving every row.

    Expiry forbids normal work but must not make dead queues grow forever.
    This separate terminal path has a cumulative maximum 30-second transaction
    budget, nonwaiting writer fences and the normal migration advisory lock.
    It cannot resume or replace the attempt. A lost reply requires inspection of
    the committed cancellation receipt; it is never permission to start copying.
    """
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 30:
        raise ValueError("fact_header_cancel_timeout_out_of_bounds")
    if not isinstance(expected_started_at, str) or not expected_started_at:
        raise ValueError("fact_header_cancel_original_start_required")
    with capture._bounded_step(conn, timeout_seconds):
        capture.require_not_cancelled(conn)
        # Exact old source identity refuses a committed or uncertain v2 switch.
        # NOWAIT leaves a busy serving transaction alone, rather than draining it.
        conn.exec_driver_sql(f"LOCK TABLE {capture.SOURCE} IN ACCESS EXCLUSIVE MODE NOWAIT")
        captured = capture.inspect_capture(conn)
        if captured["started_at"] != expected_started_at:
            raise RuntimeError("fact_header_cancel_attempt_binding_changed")
        saved = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
        header_state = headers._inspect_progress(conn) if _exists(conn, headers.STATE) else None
        if header_state is None and any(_exists(conn, capture.SCHEMA+"."+name)
                                        for name in headers.TABLE_NAMES):
            raise RuntimeError("fact_header_cancel_unregistered_shadow")
        identity = bool(header_state and header_state["identity_capture"])
        if header_state is not None:
            assert_v1_source_admission(conn, identity_capture=identity)
        slots = references._inventory(conn) if identity else {}
        if not identity and conn.scalar(text("""
            SELECT EXISTS(SELECT 1 FROM pg_constraint
                WHERE contype='f' AND confrelid=to_regclass(:target)
                  AND connamespace='market'::regnamespace)
        """), {"target": references.TARGET}):
            raise RuntimeError("fact_header_cancel_unregistered_references")
        # Parent locking also fences new payload leaves. All constraints are
        # re-inspected after locks, before removing any mirror or source capture.
        for relation in sorted(slots):
            conn.exec_driver_sql(f"LOCK TABLE {references._qualified(conn, relation)} "
                                 "IN ACCESS EXCLUSIVE MODE NOWAIT")
        if slots and references._inventory(conn) != slots:
            raise RuntimeError("fact_header_cancel_reference_inventory_changed")
        staged = references._states(conn, slots) if slots else {}
        original_references = _incoming_references(conn) if header_state is not None else None
        raw_state = None
        if _exists(conn, raw.STATE):
            if header_state is None:
                raise RuntimeError("fact_header_cancel_raw_without_headers")
            conn.exec_driver_sql(f"LOCK TABLE {raw.SOURCE} IN ACCESS EXCLUSIVE MODE NOWAIT")
            raw_state = raw._inspect(conn)
        elif any(_exists(conn, name) for name in (raw.TARGET, raw.QUEUE)):
            raise RuntimeError("fact_header_cancel_unregistered_raw")
        archive_state = None
        if _exists(conn, archives.STATE):
            if raw_state is None or source_root is None or destination_root is None:
                raise RuntimeError("fact_header_cancel_archive_roots_and_raw_required")
            conn.exec_driver_sql("LOCK TABLE "+",".join(
                "market."+name for name in archive_files.FAMILIES)+
                " IN ACCESS EXCLUSIVE MODE NOWAIT")
            archive_state = dict(archives._inspect(conn, source_root, destination_root))
        elif any(_exists(conn, name) for name in (archives.QUEUE, archives.PROGRESS, archives.CLOSED)):
            raise RuntimeError("fact_header_cancel_unregistered_archive")
        removed = {(capture.SOURCE, "trg_qt_header_v2_capture"),
                   (capture.SOURCE, "trg_qt_header_v2_reject_change")}
        if identity:
            removed.add((capture.SOURCE, "trg_qt_header_v2_capture_identity"))
        if raw_state is not None:
            removed |= {(raw.SOURCE, "trg_qt_raw_mapping_v2_capture"),
                        (raw.SOURCE, "trg_qt_raw_mapping_v2_reject_change")}
        if archive_state is not None:
            removed |= {("market."+name, trigger)
                        for name in archive_files.FAMILIES
                        for trigger in (archives.TRIGGER, archives.GUARD)}
        relations = sorted({relation for relation, _ in removed})
        original_triggers = _triggers(conn, relations)
        if not removed <= {(row["relation"], row["tgname"]) for row in original_triggers}:
            raise RuntimeError("fact_header_cancel_capture_trigger_missing")
        # Drop inherited parent first; PostgreSQL removes only its inherited
        # staged children. Original source references stay enforced throughout.
        roots = [name for name, state in staged.items() if state and not state["parent_oid"]]
        for name in sorted(roots, key=lambda name: (name != references.PARENT, name)):
            conn.exec_driver_sql(f"ALTER TABLE {references._qualified(conn, name)} "
                                 f"DROP CONSTRAINT {references.STAGED}")
        if any(references._existing(conn, slot) is not None for slot in slots.values()):
            raise RuntimeError("fact_header_cancel_staged_reference_remains")
        for relation, trigger in sorted(removed):
            conn.exec_driver_sql(f"DROP TRIGGER {trigger} ON {relation}")
        remaining = [row for row in original_triggers
                     if (row["relation"], row["tgname"]) not in removed]
        if _triggers(conn, relations) != remaining:
            raise RuntimeError("fact_header_cancel_original_trigger_changed")
        if original_references is not None and _incoming_references(conn) != original_references:
            raise RuntimeError("fact_header_cancel_original_reference_changed")
        receipt = {
            "schema_version": "qt.fact_header_cancel.v1",
            "capture": json.loads(json.dumps(saved, default=str)),
            "archive_capture": archive_state,
            "removed_triggers": [list(item) for item in sorted(removed)],
            "removed_reference_roots": roots,
            "original_triggers": remaining,
            "original_references": original_references,
            "source_retained": True, "partial_copies_retained": True,
            "migration_ready": False, "final_switch_authorized": False,
        }
        conn.exec_driver_sql(f"CREATE TABLE {capture.CANCELLED}("
                             "id integer PRIMARY KEY CHECK(id=1),"
                             "cancelled_at timestamptz NOT NULL DEFAULT clock_timestamp(),"
                             "receipt jsonb NOT NULL)")
        conn.execute(text(f"INSERT INTO {capture.CANCELLED}(id,receipt) "
                          "VALUES(1,CAST(:receipt AS jsonb))"),
                     {"receipt": json.dumps(receipt, default=str)})
        logger.info("fact_header_attempt_cancellation_staged | source_oid=%s started_at=%s "
                    "temporary_triggers=%s reference_roots=%s",
                    captured["source_oid"], expected_started_at, len(removed), len(roots))
        return receipt
