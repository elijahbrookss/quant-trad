"""Write a named pre-switch WAL point under the existing database fence."""

from __future__ import annotations

from secrets import token_hex

from sqlalchemy import text

from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from portal.backend.service.storage.recovery_copies import _snapshot_layout
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.db.canonical_cold_database_fence import _database


def create_original_recovery_anchor(fence, control, *, expected_source_commit_seq):
    """Return a WAL endpoint to verify with the independent archive inventory.

    The caller holds host/archive exclusion and the live source database fence.
    The result has no implication about archive availability or restore success.
    """
    conn = fence.connection
    if (control is conn or control.in_transaction() or type(expected_source_commit_seq) is not int
            or expected_source_commit_seq < 0):
        raise ValueError("canonical_cold_recovery_anchor_invalid_boundary")
    fence.assert_quiet(conn)
    layout = _snapshot_layout(conn)
    if (layout["layout_version"] != FACT_STORAGE_LAYOUT_VERSION
            or PostgresMarketDataRepository._current_commit_seq_with_session(conn) != expected_source_commit_seq):
        raise RuntimeError("canonical_cold_recovery_anchor_original_authority_changed")
    restore_name = "qt_original_" + token_hex(16)
    with control.begin():
        control.exec_driver_sql("SET LOCAL statement_timeout='5s'")
        if (control.scalar(text("SELECT current_database()")) != "postgres"
                or _database(control, fence.database["database"]) != fence.database
                or control.scalar(text("SHOW archive_mode")) != "on"):
            raise RuntimeError("canonical_cold_recovery_anchor_control_mismatch")
        fence.check_clock()
        anchor_lsn = control.scalar(text("SELECT pg_create_restore_point(:name)::text"),
                                    {"name": restore_name})
        segment = control.scalar(text("SELECT pg_walfile_name(CAST(:lsn AS pg_lsn))"),
                                 {"lsn": anchor_lsn})
        control.execute(text("SELECT pg_switch_wal()"))
        if not control.scalar(text("SELECT pg_current_wal_flush_lsn() >= CAST(:lsn AS pg_lsn)"),
                              {"lsn": anchor_lsn}):
            raise RuntimeError("canonical_cold_recovery_anchor_not_flushed")
    fence.assert_quiet(conn)
    return {
        "database": dict(fence.database), "storage_layout": layout, "source_commit_seq": expected_source_commit_seq,
        "restore_name": restore_name, "anchor_lsn": anchor_lsn, "wal_segment": segment,
        "timeline": int(segment[:8], 16),
    }
