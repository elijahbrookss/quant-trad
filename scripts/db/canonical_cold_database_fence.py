"""Close the canonical database to other writers during a cutover transaction."""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic, sleep

from sqlalchemy import text

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from scripts.automation.storage_online_controller import OnlineController
from scripts.db.fact_header_v2_capture import _bounded_step


_DATABASE = """
    SELECT control.system_identifier::text AS system_identifier, d.oid::bigint AS database_oid,
           d.datname AS database, d.datallowconn AS allow_connections,
           extract(epoch FROM pg_postmaster_start_time())::text AS postmaster_started_at
    FROM pg_database AS d CROSS JOIN pg_control_system() AS control
    WHERE d.datname=:name
"""


def _database(connection, name):
    return dict(connection.execute(text(_DATABASE), {"name": name}).mappings().one())


def native_transaction_mode(conn):
    """Check the driver because engine-level autocommit can omit execution options."""
    return not conn.closed and not conn.invalidated and conn.connection.driver_connection.autocommit is False


def _other_publishers(conn, reader=None):
    conn.exec_driver_sql("SELECT pg_stat_clear_snapshot()")
    # Native autovacuum can enter a nonconnectable database. It maintains
    # physical storage and statistics, and can't publish QT facts. DDL still
    # has to acquire its usual relation locks within the cutover budget.
    return conn.execute(text("""
        WITH clients AS (
            SELECT pid,backend_start,backend_type FROM pg_stat_activity
            WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database())
              AND pid<>pg_backend_pid() AND backend_type IS DISTINCT FROM 'autovacuum worker'
        ), retained AS (
            SELECT pid FROM clients WHERE pid=CAST(:reader_pid AS integer)
              AND backend_start=CAST(:reader_start AS timestamptz) AND backend_type='client backend'
        ), other AS (
            SELECT backend_type,count(*) AS count FROM clients WHERE pid NOT IN (SELECT pid FROM retained)
            GROUP BY backend_type
        )
        SELECT (SELECT coalesce(sum(count),0) FROM other) AS other_backends,
               (SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()) AS prepared_transactions,
               (SELECT coalesce(jsonb_object_agg(coalesce(backend_type,'unknown'),count),'{}') FROM other) AS backend_types,
               EXISTS(SELECT 1 FROM retained) AS reader_present
    """), {"reader_pid": None if reader is None else reader.backend_pid,
           "reader_start": None if reader is None else reader.backend_start}).one()


def _same_database(observed, expected):
    return all(observed[key] == expected[key] for key in ("system_identifier", "database_oid", "database"))


@dataclass(frozen=True)
class RetainedReadSession:
    connection: object
    transaction: object
    backend_pid: int
    backend_start: object
    role: tuple[str, str]

    def assert_held(self, database):
        conn = self.connection
        if (not native_transaction_mode(conn) or conn.get_transaction() is not self.transaction
                or not self.transaction.is_active or _database(conn, database["database"]) != database):
            raise RuntimeError("canonical_cold_database_fence_reader_changed")
        state = conn.execute(text("""
            SELECT pg_backend_pid(),backend_start,current_setting('transaction_read_only'),
                   current_setting('transaction_isolation'),pg_current_xact_id_if_assigned() IS NULL,
                   current_user,session_user
            FROM pg_stat_activity WHERE pid=pg_backend_pid()
        """)).one()
        if tuple(state) != (self.backend_pid, self.backend_start, "on", "read committed", True, *self.role):
            raise RuntimeError("canonical_cold_database_fence_reader_changed")


def retain_read_session(conn, *, writer, control, expected_database, stack):
    """Reserve a server-enforced read-only transaction before closing new logins."""
    if conn is writer or conn is control or not native_transaction_mode(conn) or conn.in_transaction():
        raise ValueError("canonical_cold_database_fence_invalid_reader")
    with writer.begin():
        writer.exec_driver_sql("SET LOCAL statement_timeout='5s'")
        role = tuple(writer.exec_driver_sql("SELECT current_user,session_user").one())
    transaction = conn.begin()
    stack.callback(transaction.close)
    conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY")
    conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
    conn.exec_driver_sql("SET LOCAL lock_timeout='500ms'")
    pid, started, current, session, database_name = conn.execute(text("""
        SELECT pid,backend_start,current_user,session_user,current_database()
        FROM pg_stat_activity WHERE pid=pg_backend_pid()
    """)).one()
    if (current, session) != role:
        raise RuntimeError("canonical_cold_database_fence_reader_role_changed")
    if (database_name != expected_database["database"]
            or not _same_database(_database(conn, database_name), expected_database)):
        raise RuntimeError("canonical_cold_database_fence_reader_database_changed")
    return RetainedReadSession(conn, transaction, pid, started, role)


def _set_login_gate(control, *, database, allow_connections, check):
    """Only change the pinned database in the pinned running postmaster."""
    if not native_transaction_mode(control) or control.in_transaction():
        raise RuntimeError("canonical_cold_database_fence_control_transaction_changed")
    with control.begin():
        control.exec_driver_sql("SET LOCAL statement_timeout='5s'")
        control.exec_driver_sql("SET LOCAL lock_timeout='500ms'")
        if control.scalar(text("SELECT current_database()")) != "postgres":
            raise RuntimeError("canonical_cold_database_fence_control_endpoint_invalid")
        observed = _database(control, database["database"])
        if observed != database:
            raise RuntimeError("canonical_cold_database_fence_control_identity_changed")
        quoted_name = control.dialect.identifier_preparer.quote_identifier(observed["database"])
        flag = "true" if allow_connections else "false"
        check()
        control.exec_driver_sql(f"ALTER DATABASE {quoted_name} ALLOW_CONNECTIONS {flag}")
        if _database(control, database["database"]) != {**database, "allow_connections": allow_connections}:
            raise RuntimeError("canonical_cold_database_fence_login_result_unconfirmed")


@dataclass(frozen=True)
class DatabaseFence:
    """Live retained session. Serialized observations carry no fence authority."""

    connection: object
    database: dict
    backend_pid: int
    jobs: tuple
    deadline: float
    reader: RetainedReadSession | None = None

    def check_clock(self):
        if monotonic() >= self.deadline:
            raise TimeoutError("canonical_cold_database_fence_deadline")

    def assert_quiet(self, conn):
        self.check_clock()
        if self.connection is not conn or not native_transaction_mode(conn) or not conn.in_transaction():
            raise RuntimeError("canonical_cold_database_fence_session_lost")
        if conn.scalar(text("SELECT pg_backend_pid()")) != self.backend_pid:
            raise RuntimeError("canonical_cold_database_fence_backend_changed")
        if _database(conn, self.database["database"]) != self.database:
            raise RuntimeError("canonical_cold_database_fence_target_changed")
        OnlineController._require_job_environment(conn)
        if OnlineController._supported_builtin_catalog(conn) != self.jobs:
            raise RuntimeError("canonical_cold_database_fence_job_catalog_changed")
        if self.reader is not None:
            self.reader.assert_held(self.database)
        activity = _other_publishers(conn, self.reader)
        if self.reader is not None and not activity.reader_present:
            raise RuntimeError("canonical_cold_database_fence_reader_missing")
        if activity.other_backends or activity.prepared_transactions:
            raise RuntimeError("canonical_cold_database_fence_publishers_not_drained: "
                               f"backend_types={activity.backend_types} prepared={activity.prepared_transactions}")


def close_database_fence(conn, control, *, expected_database, timeout_seconds, record, reader=None):
    """Close new logins and stop qualified background jobs on one retained connection.

    Caller owns the host/publisher hold and a durable record callback. Existing
    clients are allowed to drain. A refusal leaves whatever was already recorded
    and applied, for explicit reconciliation by the owner.
    """
    if (conn is control or any(not native_transaction_mode(session) or session.in_transaction() for session in (conn, control))
            or not callable(record) or type(timeout_seconds) is not int
            or not 1 <= timeout_seconds <= MAX_MIGRATION_SECONDS
            or expected_database.get("database") in (None, "postgres", "template0", "template1")):
        raise ValueError("canonical_cold_database_fence_invalid_control")
    deadline = monotonic() + timeout_seconds
    with conn.begin():
        with _bounded_step(conn, timeout_seconds):
            if conn.scalar(text("SELECT current_database()")) != expected_database["database"]:
                raise RuntimeError("canonical_cold_database_fence_target_database_changed")
            observed = _database(conn, expected_database["database"])
            if not _same_database(observed, expected_database) or not observed["allow_connections"]:
                raise RuntimeError("canonical_cold_database_fence_target_database_changed")
            OnlineController._require_job_environment(conn, allow_connections=True)
            jobs = OnlineController._supported_builtin_catalog(conn)
            pid = conn.scalar(text("SELECT pg_backend_pid()"))
            if reader is not None:
                reader.assert_held(observed)
            if _other_publishers(conn, reader).prepared_transactions:
                raise RuntimeError("canonical_cold_database_fence_prepared_transactions")
    fence = DatabaseFence(conn, {**observed, "allow_connections": False}, pid, jobs, deadline, reader=reader)
    receipt = {"database": observed, "backend_pid": pid, "jobs": list(jobs)}
    if reader is not None:
        receipt["retained_reader"] = {"backend_pid": reader.backend_pid, "backend_start": reader.backend_start.isoformat()}
    fence.check_clock()
    record("login_closing", receipt)
    fence.check_clock()
    _set_login_gate(control, database=observed, allow_connections=False, check=fence.check_clock)
    with conn.begin():
        with _bounded_step(conn, timeout_seconds) as limit:
            limit(deadline - monotonic())
            if (_database(conn, observed["database"]) != fence.database
                    or OnlineController._supported_builtin_catalog(conn) != jobs
                    or conn.scalar(text("SELECT pg_backend_pid()")) != pid):
                raise RuntimeError("canonical_cold_database_fence_target_changed")
            record("jobs_stopping", receipt)
            fence.check_clock()
            if conn.scalar(text("SELECT _timescaledb_functions.stop_background_workers()")) is not True:
                raise RuntimeError("canonical_cold_database_fence_job_stop_unconfirmed")

    while True:
        fence.check_clock()
        with conn.begin():
            with _bounded_step(conn, timeout_seconds) as limit:
                limit(deadline - monotonic())
                activity = _other_publishers(conn, reader)
                if activity.prepared_transactions:
                    raise RuntimeError("canonical_cold_database_fence_prepared_transactions")
                if not activity.other_backends:
                    fence.assert_quiet(conn)
                    break
        sleep(min(0.1, max(0, deadline - monotonic())))
    record("database_fenced", receipt)
    fence.check_clock()
    return fence


def reopen_database_fence(fence, control, *, expected_outcome, read_outcome, record):
    """Release SQL exclusion only after a caller-validated committed or original outcome."""
    if expected_outcome not in {"original", "committed"} or not callable(read_outcome) or not callable(record):
        raise ValueError("canonical_cold_database_fence_invalid_release_outcome")
    conn = fence.connection
    with conn.begin():
        conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
        fence.assert_quiet(conn)
        if read_outcome(conn) != expected_outcome:
            raise RuntimeError("canonical_cold_database_fence_release_outcome_changed")
    # TimescaleDB requires an open login gate when restarting the scheduler.
    # The enclosing host hold excludes application processes through both steps.
    receipt = {"database": fence.database, "backend_pid": fence.backend_pid, "outcome": expected_outcome}
    record("login_opening", receipt)
    fence.check_clock()
    _set_login_gate(control, database=fence.database, allow_connections=True, check=fence.check_clock)
    with conn.begin():
        conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
        fence.check_clock()
        if (_database(conn, fence.database["database"]) != {**fence.database, "allow_connections": True}
                or conn.scalar(text("SELECT pg_backend_pid()")) != fence.backend_pid):
            raise RuntimeError("canonical_cold_database_fence_target_changed")
        OnlineController._require_job_environment(conn, allow_connections=True)
        if OnlineController._supported_builtin_catalog(conn) != fence.jobs or read_outcome(conn) != expected_outcome:
            raise RuntimeError("canonical_cold_database_fence_release_outcome_changed")
        record("jobs_restarting", receipt)
        fence.check_clock()
        if conn.scalar(text("SELECT _timescaledb_functions.start_background_workers()")) is not True:
            raise RuntimeError("canonical_cold_database_fence_job_restart_unconfirmed")
    record("database_reopened", receipt)
