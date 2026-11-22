"""Restore the original archive FK enforcement on a disposable rollback clone."""

from __future__ import annotations

from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import (
    ARCHIVE_FACT_REFERENCE_TABLES,
    _FUNCTION,
    _TRIGGER,
    _fact_reference_foreign_keys,
    assert_cold_dependency_bindings,
)
from portal.backend.db.fact_identity_schema import assert_fact_identity_references
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db.canonical_cold_database_fence import DatabaseFence
from scripts.db.canonical_cold_migration import PostgresMigrationSource
from scripts.db.fact_header_v2_capture import _bounded_step


VERSION = "qt.canonical_cold_restore_references.v1"


def _original_names(conn):
    constraints = {}
    for table in ARCHIVE_FACT_REFERENCE_TABLES:
        rows = _fact_reference_foreign_keys(conn, table)
        if len(rows) != 1 or tuple(rows[0][1:]) != (True, False, False, 0, "r", "a", "s", True, True):
            raise RuntimeError(f"canonical_cold_recovery_original_fk_invalid: {table}")
        constraints[table] = rows[0][0]
    return constraints


def _ready(conn, layout):
    state = conn.execute(text("""
        SELECT layout_version,state,completed_at IS NOT NULL FROM market.fact_storage_state
        ORDER BY layout_version LIMIT 2
    """)).all()
    if state != [(layout, "ready", True)]:
        raise RuntimeError("canonical_cold_recovery_reference_layout_changed")


def _database_identity(conn):
    system = conn.scalar(text("SELECT system_identifier::text FROM pg_control_system()"))
    return PostgresMigrationSource(conn.engine, expected_system_identifier=system).identity(conn)


def capture_original_reference_bindings(conn):
    """Measure original validated FK names in the supplied stable snapshot."""
    if not conn.in_transaction() or conn.get_isolation_level() != "REPEATABLE READ":
        raise ValueError("canonical_cold_recovery_reference_snapshot_required")
    _ready(conn, FACT_STORAGE_LAYOUT_VERSION)
    return {"version": VERSION, "database": _database_identity(conn), "constraints": _original_names(conn)}


def _require_disposable_fence(conn, fence):
    url = conn.engine.url
    if (not isinstance(fence, DatabaseFence) or not conn.in_transaction()
            or not str(url.database).startswith(("qt_bench_", "qt_migration_"))
            or url.host not in {None, "timescaledb", "127.0.0.1", "::1"}
            or (url.query.get("host") is not None and not str(url.query["host"]).startswith("/"))):
        raise ValueError("canonical_cold_recovery_disposable_fence_required")


def restore_original_reference_bindings(conn, *, fence, original, expected_hash, donor=None, timeout_seconds=120):
    """Restore all four FK roots after exact header and identity recovery.

    The restored cold clone and old layout are from the same physical cluster
    lineage. The caller still owns the full reverse conversion and startup
    check. This step preserves the archive rows and layout certificate.
    """
    _require_disposable_fence(conn, fence)
    if (not isinstance(original, dict) or original.keys() != {"version", "database", "constraints"}
            or original["version"] != VERSION or archive_evidence_hash(original) != expected_hash
            or not isinstance(original["constraints"], dict)
            or set(original["constraints"]) != set(ARCHIVE_FACT_REFERENCE_TABLES)
            or any(not isinstance(name, str) or not 1 <= len(name.encode()) <= 63
                   or "\x00" in name for name in original["constraints"].values())):
        raise ValueError("canonical_cold_recovery_reference_original_evidence_changed")

    with _bounded_step(conn, timeout_seconds):
        fence.assert_quiet(conn)
        target = _database_identity(conn)
        source = original["database"]
        if donor is not None and (donor is conn or capture_original_reference_bindings(donor) != original):
            raise RuntimeError("canonical_cold_recovery_reference_donor_changed")
        keys = ("system_identifier", "header_oid") if donor is not None else tuple(target)
        if any(target[key] != source[key] for key in keys):
            raise RuntimeError("canonical_cold_recovery_reference_database_changed")
        if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                           {"name": _LIFECYCLE_LOCK_NAME}):
            raise RuntimeError("canonical_cold_recovery_reference_lifecycle_busy")
        conn.exec_driver_sql("LOCK TABLE ONLY market.fact_identities IN SHARE MODE NOWAIT")
        conn.exec_driver_sql("LOCK TABLE market.fact_storage_state IN SHARE MODE NOWAIT")
        _ready(conn, COLD_HISTORY_LAYOUT_VERSION)
        for table in sorted(ARCHIVE_FACT_REFERENCE_TABLES):
            conn.exec_driver_sql(f"LOCK TABLE market.{table} IN ACCESS EXCLUSIVE MODE NOWAIT")
        quote = conn.dialect.identifier_preparer.quote_identifier
        # A previous committed step can be resumed before the certificate is
        # switched, but every constraint must still match the old shape/name.
        already_restored = conn.scalar(text("SELECT to_regprocedure(:proc) IS NULL"), {"proc": _FUNCTION + "()"})
        if not already_restored:
            assert_cold_dependency_bindings(conn)
            for table, constraint in original["constraints"].items():
                name = quote(constraint)
                conn.exec_driver_sql(f"ALTER TABLE market.{table} ADD CONSTRAINT {name} "
                                     "FOREIGN KEY (fact_version_id) REFERENCES market.fact_identities(id) "
                                     "ON DELETE RESTRICT NOT VALID")
                conn.exec_driver_sql(f"ALTER TABLE market.{table} VALIDATE CONSTRAINT {name}")
                conn.exec_driver_sql(f"DROP TRIGGER {_TRIGGER} ON market.{table}")
            conn.exec_driver_sql(f"DROP FUNCTION {_FUNCTION}() RESTRICT")
        if _original_names(conn) != original["constraints"]:
            raise RuntimeError("canonical_cold_recovery_reference_fk_names_changed")
        assert_fact_identity_references(conn)
        fence.assert_quiet(conn)
    return {"version": VERSION, "database": target, "original_database": source, "original_hash": expected_hash,
            "constraints": dict(original["constraints"]), "already_restored": already_restored}
