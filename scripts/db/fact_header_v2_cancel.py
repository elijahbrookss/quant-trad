"""Terminal preserving cancellation of an admitted online v1 migration attempt.

No CLI, data deletion, deadline renewal or source/runtime switch. The caller
commits one short transaction; rollback restores all capture and staged FKs.
"""
from __future__ import annotations

import json
import logging
import re

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
                   destination_root=None, timeout_seconds=30,
                   expected_capture=None, intent_sha256=None, read_only_namespace=False):
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
    if type(read_only_namespace) is not bool:
        raise ValueError("fact_header_cancel_namespace_mode_invalid")
    placement_options = {"read_only_namespace":True} if read_only_namespace else {}
    bound = expected_capture is not None or intent_sha256 is not None
    if bound:
        _validate_intent(expected_capture, intent_sha256)
    with capture._bounded_step(conn, timeout_seconds):
        _require_retired_controller(conn)
        capture.require_not_cancelled(conn)
        # Exact old source identity refuses a committed or uncertain v2 switch.
        # NOWAIT leaves a busy serving transaction alone, rather than draining it.
        conn.exec_driver_sql(f"LOCK TABLE {capture.SOURCE} IN ACCESS EXCLUSIVE MODE NOWAIT")
        captured = capture.inspect_capture(conn)
        if captured["started_at"] != expected_started_at:
            raise RuntimeError("fact_header_cancel_attempt_binding_changed")
        saved = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
        if bound and _capture_binding(conn) != expected_capture:
            raise RuntimeError("fact_header_cancel_capture_preimage_changed")
        header_state = headers._inspect_progress(conn, **placement_options) if _exists(conn, headers.STATE) else None
        if header_state is None and any(_exists(conn, capture.SCHEMA+"."+name)
                                        for name in headers.TABLE_NAMES):
            raise RuntimeError("fact_header_cancel_unregistered_shadow")
        identity = bool(header_state and header_state["identity_capture"])
        if header_state is not None:
            assert_v1_source_admission(conn, identity_capture=identity)
        slots = references._inventory(conn, **placement_options) if identity else {}
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
        if slots and references._inventory(conn, **placement_options) != slots:
            raise RuntimeError("fact_header_cancel_reference_inventory_changed")
        staged = references._states(conn, slots) if slots else {}
        original_references = _incoming_references(conn) if header_state is not None else None
        raw_state = None
        if _exists(conn, raw.STATE):
            if header_state is None:
                raise RuntimeError("fact_header_cancel_raw_without_headers")
            conn.exec_driver_sql(f"LOCK TABLE {raw.SOURCE} IN ACCESS EXCLUSIVE MODE NOWAIT")
            raw_state = raw._inspect(conn, **placement_options)
        elif any(_exists(conn, name) for name in (raw.TARGET, raw.QUEUE)):
            raise RuntimeError("fact_header_cancel_unregistered_raw")
        archive_state = None
        if _exists(conn, archives.STATE):
            if raw_state is None or source_root is None or destination_root is None:
                raise RuntimeError("fact_header_cancel_archive_roots_and_raw_required")
            conn.exec_driver_sql("LOCK TABLE "+",".join(
                "market."+name for name in archive_files.FAMILIES)+
                " IN ACCESS EXCLUSIVE MODE NOWAIT")
            archive_state = dict(archives._inspect(conn, source_root, destination_root, **placement_options))
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
        if bound:
            receipt.update(schema_version="qt.fact_header_cancel.v2",
                           capture=expected_capture, intent_sha256=intent_sha256)
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


def _validate_intent(expected_capture, intent_sha256):
    if (not isinstance(expected_capture, dict) or not expected_capture
            or not isinstance(intent_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", intent_sha256) is None):
        raise ValueError("fact_header_cancel_exact_intent_required")


def _require_retired_controller(conn):
    # The transaction lock excludes another migration step. The controller's
    # session lock also excludes an idle foreign controller between commands.
    # A controller cancelling itself uses its retained owning SQL session.
    from scripts.db.fact_header_v2_deadline import CONTROLLER_LOCK
    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                       {"name": CONTROLLER_LOCK}):
        raise RuntimeError("fact_header_cancel_controller_active")


def _capture_binding(conn):
    value = conn.execute(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c")).scalar_one()
    # PostgreSQL's JSON encoder emits oid values as strings; the native source
    # context uses bigint. One canonical preimage is used before and after COMMIT.
    return {**value, **{name: int(value[name]) for name in
                       ("source_oid", "database_oid", "queue_oid")}}


def inspect_cancellation(conn, *, expected_capture, intent_sha256, timeout_seconds=10):
    """Read a committed terminal outcome; never replay a cancellation or copy.

    The host must supply its durable intent and original capture preimage. None
    means no committed marker was observed, NOT permission to redispatch after
    an uncertain commit. No normal capture inspector is used after cancellation:
    its temporary triggers intentionally no longer exist.
    """
    _validate_intent(expected_capture, intent_sha256)
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 30:
        raise ValueError("fact_header_cancel_timeout_out_of_bounds")
    with capture._bounded_step(conn, timeout_seconds):
        _require_retired_controller(conn)
        conn.exec_driver_sql(f"LOCK TABLE {capture.SOURCE} IN ACCESS SHARE MODE NOWAIT")
        context = capture._context(conn)
        if (any(expected_capture.get(k) != v for k, v in context.items())
                or _capture_binding(conn) != expected_capture
                or conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                               {"name": capture.QUEUE}) != expected_capture.get("queue_oid")):
            raise RuntimeError("fact_header_cancel_capture_preimage_changed")
        if not _exists(conn, capture.CANCELLED):
            return None
        # One actual durable marker, not an arbitrary view or multirow result.
        kind = conn.execute(text("SELECT relkind,relpersistence FROM pg_class "
                                 "WHERE oid=to_regclass(:name)"),
                            {"name": capture.CANCELLED}).one()
        if tuple(kind) != ("r", "p"):
            raise RuntimeError("fact_header_cancel_receipt_layout_changed")
        rows = conn.execute(text(f"SELECT id,cancelled_at,receipt FROM {capture.CANCELLED} LIMIT 2")).mappings().all()
        if len(rows) != 1 or rows[0]["id"] != 1 or rows[0]["cancelled_at"] is None:
            raise RuntimeError("fact_header_cancel_receipt_layout_changed")
        receipt = rows[0]["receipt"]
        if (not isinstance(receipt, dict)
                or receipt.get("schema_version") != "qt.fact_header_cancel.v2"
                or receipt.get("intent_sha256") != intent_sha256
                or receipt.get("capture") != expected_capture
                or receipt.get("source_retained") is not True
                or receipt.get("partial_copies_retained") is not True
                or receipt.get("migration_ready") is not False
                or receipt.get("final_switch_authorized") is not False):
            raise RuntimeError("fact_header_cancel_receipt_binding_changed")
        relations = sorted({relation for relation, _ in receipt["removed_triggers"]})
        if _triggers(conn, relations) != receipt["original_triggers"]:
            raise RuntimeError("fact_header_cancel_original_trigger_changed")
        if conn.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_constraint "
                            "WHERE contype='f' AND confrelid=to_regclass(:target) "
                            "AND connamespace='market'::regnamespace)"),
                       {"target": references.TARGET}):
            raise RuntimeError("fact_header_cancel_staged_reference_remains")
        if receipt["original_references"] is not None:
            # New source payload days may appear while collection continues.
            # Validate their native references, and retain every original OID.
            actual = _incoming_references(conn)
            if any(row not in actual for row in receipt["original_references"]):
                raise RuntimeError("fact_header_cancel_original_reference_changed")
        return receipt
