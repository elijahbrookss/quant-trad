"""Explicit pre-expiry capture amendment after the online controller retires.

Internal transaction primitive. The host owns authorization, capacity admission,
worker retirement and rebinding. Ordinary retries never call this function.
The audit and duration change commit together; neither changes the start clock.
"""
from __future__ import annotations

from copy import deepcopy
import re

from sqlalchemy import text

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from scripts.db import fact_header_v2_capture as capture

AUDIT = capture.SCHEMA + ".deadline_amendment"
CONTROLLER_LOCK = "qt.storage.online.controller.v1"


def inspect_amendment(conn):
    """Read a committed amendment, without granting replay or worker authority."""
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name": AUDIT}) is None:
        return None
    rows = conn.execute(text(f"SELECT id,receipt FROM {AUDIT}")).mappings().all()
    if len(rows) != 1 or rows[0]["id"] != 1 or not isinstance(rows[0]["receipt"], dict):
        raise RuntimeError("fact_header_deadline_audit_invalid")
    return rows[0]["receipt"]


def amend_capture_deadline(conn, *, expected_capture, attempt_seconds, intent_sha256):
    """Amend once in the caller transaction, bounded by the OLD live deadline.

    A saved audit is reconciled read-only by the host, never by replaying this
    operation. Lock exclusion covers both the controller's session ownership
    and all page/capture transactions. Source writes continue through capture.
    """
    if (not isinstance(expected_capture, dict)
            or type(expected_capture.get("attempt_seconds")) is not int
            or type(attempt_seconds) is not int
            or not 1 <= expected_capture["attempt_seconds"] < attempt_seconds <= MAX_MIGRATION_SECONDS
            or not isinstance(intent_sha256, str)
            or not re.fullmatch(r"[0-9a-f]{64}", intent_sha256)):
        raise ValueError("fact_header_deadline_amendment_invalid")
    with capture.migration_step(conn, 30):
        if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                           {"name": CONTROLLER_LOCK}):
            raise RuntimeError("fact_header_deadline_controller_active")
        capture.inspect_capture(conn)
        actual = conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c WHERE id=1 FOR UPDATE NOWAIT"))
        if actual != expected_capture:
            raise RuntimeError("fact_header_deadline_capture_changed")
        if conn.scalar(text("SELECT to_regclass(:name)"), {"name": AUDIT}) is not None:
            raise RuntimeError("fact_header_deadline_amendment_requires_reconciliation")
        amended = deepcopy(actual)
        amended["attempt_seconds"] = attempt_seconds
        receipt = dict(schema_version="qt.fact_header_deadline_amendment.v1",
                       intent_sha256=intent_sha256, before=actual, after=amended)
        conn.exec_driver_sql(f"CREATE TABLE {AUDIT}(id integer PRIMARY KEY CHECK(id=1),receipt jsonb NOT NULL)")
        conn.exec_driver_sql(f"REVOKE ALL ON {AUDIT} FROM PUBLIC")
        import json
        conn.execute(text(f"INSERT INTO {AUDIT} VALUES(1,CAST(:receipt AS jsonb))"),
                     {"receipt": json.dumps(receipt, sort_keys=True)})
        conn.execute(text(f"UPDATE {capture.STATE} SET attempt_seconds=:seconds WHERE id=1"),
                     {"seconds": attempt_seconds})
        # No duration extension of this transaction: migration_step retained
        # its original monotonic ceiling before the mutation above.
        capture.inspect_capture(conn)
        return receipt
