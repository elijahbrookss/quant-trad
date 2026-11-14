"""Identify the activation transaction and observe it with the login gate closed."""

import re

from sqlalchemy import text

from scripts.db.canonical_cold_database_fence import _database


_LSN = re.compile(r"[0-9A-F]{1,8}/[0-9A-F]{1,8}")
_XID = re.compile(r"[1-9][0-9]{0,19}")
_SYNC_MODES = {"local", "remote_write", "remote_apply", "on"}


def _lsn_number(value):
    if not isinstance(value, str) or _LSN.fullmatch(value) is None:
        raise ValueError("canonical_cold_activation_invalid_wal_position")
    upper, lower = (int(part, 16) for part in value.split("/"))
    return (upper << 32) | lower


def prepare_activation_transaction(conn, fence):
    """Give the held top-level SQL transition a native transaction identifier."""
    fence.assert_quiet(conn)
    if conn.scalar(text("SHOW synchronous_commit")) == "off":
        conn.exec_driver_sql("SET LOCAL synchronous_commit='local'")
    row = conn.execute(text("""
        SELECT pg_current_xact_id()::text AS xid, pg_current_wal_insert_lsn()::text AS wal_before,
               current_setting('fsync') AS fsync, current_setting('synchronous_commit') AS synchronous_commit
    """)).mappings().one()
    if row["synchronous_commit"] not in _SYNC_MODES:
        raise RuntimeError("canonical_cold_activation_transaction_requires_synchronous_commit")
    return dict(row)


def observe_activation_transaction(control, *, database, backend_pid, transaction, precommit=None, timeout_seconds=10):
    """Read a recent source XID on the control database without admitting target logins."""
    if (control.closed or control.invalidated or control.in_transaction()
            or control.get_execution_options().get("isolation_level") == "AUTOCOMMIT"
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60
            or not isinstance(database, dict) or set(database) != {
                "system_identifier", "database_oid", "database", "allow_connections", "postmaster_started_at",
            } or database["allow_connections"] is not False or database["database"] in ("postgres", "template0", "template1")
            or type(backend_pid) is not int or backend_pid <= 0
            or not isinstance(transaction, dict) or set(transaction) != {"xid", "wal_before", "fsync", "synchronous_commit"}
            or not isinstance(transaction["xid"], str) or _XID.fullmatch(transaction["xid"]) is None
            or int(transaction["xid"]) >= 2**64 or transaction["fsync"] not in ("on", "off")
            or transaction["synchronous_commit"] not in _SYNC_MODES):
        raise ValueError("canonical_cold_activation_invalid_transaction_observation")
    before = _lsn_number(transaction["wal_before"])
    if precommit is not None and _lsn_number(precommit) < before:
        raise ValueError("canonical_cold_activation_precommit_wal_regressed")

    with control.begin():
        control.exec_driver_sql("SET TRANSACTION READ ONLY")
        control.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": str(timeout_seconds * 1000)})
        control.exec_driver_sql("SET LOCAL lock_timeout='500ms'")

        def assert_source():
            if (control.scalar(text("SELECT current_database()")) != "postgres"
                    or control.scalar(text("SELECT pg_is_in_recovery()"))
                    or _database(control, database["database"]) != database
                    or control.scalar(text("SHOW fsync")) != transaction["fsync"]):
                raise RuntimeError("canonical_cold_activation_transaction_source_changed")

        assert_source()
        status = control.scalar(text("SELECT pg_xact_status(CAST(:xid AS xid8))"), {"xid": transaction["xid"]})
        if status not in ("in progress", "aborted", "committed", None):
            raise RuntimeError("canonical_cold_activation_transaction_status_invalid")
        if status == "committed" and precommit is None:
            raise RuntimeError("canonical_cold_activation_missing_precommit_checks")
        # This statement runs after the status read. Its insert point therefore
        # includes the commit record when PostgreSQL reported a committed XID.
        insert = control.scalar(text("SELECT pg_current_wal_insert_lsn()::text"))
        flush = control.scalar(text("SELECT pg_current_wal_flush_lsn()::text"))
        if _lsn_number(insert) < (_lsn_number(precommit) if precommit is not None else before):
            raise RuntimeError("canonical_cold_activation_transaction_wal_changed")
        control.exec_driver_sql("SELECT pg_stat_clear_snapshot()")
        backends = control.execute(text("""
            SELECT pid FROM pg_stat_activity
            WHERE datid=:oid AND backend_type IS DISTINCT FROM 'autovacuum worker'
            ORDER BY pid LIMIT 2
        """), {"oid": database["database_oid"]}).scalars().all()
        prepared = control.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_prepared_xacts WHERE database=:db)"),
                                  {"db": database["database"]})
        if prepared or any(pid != backend_pid for pid in backends):
            raise RuntimeError("canonical_cold_activation_transaction_publishers_changed")
        assert_source()
    return {"xid": transaction["xid"], "transaction_status": status or "unknown",
            "source_backend_present": bool(backends), "observed_insert_lsn": insert, "observed_flush_lsn": flush,
            "commit_flushed": status == "committed" and transaction["fsync"] == "on"
            and _lsn_number(flush) >= _lsn_number(insert)}
