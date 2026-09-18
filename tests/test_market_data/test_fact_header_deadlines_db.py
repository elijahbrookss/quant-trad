"""Native PostgreSQL timing and rollback for the fixed migration attempt."""
from contextlib import ExitStack

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from scripts.db.fact_header_v2_capture import (
    STATE, SOURCE, QUEUE, install_capture, inspect_capture, migration_step,
)
from tests.test_market_data.test_fact_header_capture_db import capture_source, insert

pytestmark = pytest.mark.db


def test_attempt_clock_survives_reconnect_and_expiry_preserves_collection(capture_source):
    engine = capture_source
    with engine.begin() as conn:
        first = install_capture(conn)
        assert install_capture(conn)["started_at"] == first["started_at"]
        conn.exec_driver_sql(f"UPDATE {STATE} SET prepared_at=clock_timestamp()-interval '25 hours'")
        expired = inspect_capture(conn)
    engine.dispose()  # A new connection must not grant this attempt another day.
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="attempt_expired"):
            install_capture(conn)
        assert inspect_capture(conn)["started_at"] == expired["started_at"]
        insert(conn, "collection-after-deadline")
        assert conn.scalar(text(f"SELECT count(*) FROM {QUEUE}")) == 1
        assert conn.scalar(text(f"SELECT count(*) FROM {SOURCE}")) == 2
        assert inspect_capture(conn)["migration_ready"] is False


def test_step_budget_is_cumulative_and_timeout_rolls_back_only_the_step(capture_source):
    engine = capture_source
    with engine.begin() as conn:
        install_capture(conn)
        insert(conn, "outer-preserved")
        original = conn.scalar(text("SHOW statement_timeout"))
        with pytest.raises((DBAPIError, RuntimeError), match="statement timeout|step_timeout"):
            with migration_step(conn, timeout_seconds=1):
                insert(conn, "timed-out")
                conn.exec_driver_sql("SELECT pg_sleep(0.65)")
                conn.exec_driver_sql("SELECT pg_sleep(0.65)")
        assert conn.scalar(text("SHOW statement_timeout")) == original
        assert conn.execute(text(f"SELECT id FROM {SOURCE} ORDER BY id")).scalars().all() == [
            "existing", "outer-preserved"]
        assert conn.execute(text(f"SELECT id FROM {QUEUE} ORDER BY id")).scalars().all() == ["outer-preserved"]
        # This query also proves the expired listener was removed before reuse.
        assert conn.scalar(text("SELECT 1")) == 1


def test_nested_step_cannot_extend_its_parent_budget(capture_source):
    with capture_source.begin() as conn:
        install_capture(conn)
        with pytest.raises((DBAPIError, RuntimeError), match="statement timeout|step_timeout"):
            with migration_step(conn, timeout_seconds=1):
                conn.exec_driver_sql("SELECT pg_sleep(0.65)")
                with migration_step(conn, timeout_seconds=10):
                    conn.exec_driver_sql("SELECT pg_sleep(0.65)")
        assert conn.scalar(text("SELECT 1")) == 1


def test_caller_timeout_is_preserved_on_success_and_failure(capture_source):
    with capture_source.begin() as conn:
        install_capture(conn)
        conn.exec_driver_sql("SET LOCAL statement_timeout='200ms'")
        conn.exec_driver_sql("SET LOCAL lock_timeout='75ms'")
        with migration_step(conn, timeout_seconds=10):
            assert conn.scalar(text("SELECT 1")) == 1
        assert conn.scalar(text("SHOW statement_timeout")) == "200ms"
        assert conn.scalar(text("SHOW lock_timeout")) == "75ms"
        with pytest.raises((DBAPIError, RuntimeError), match="statement timeout|step_timeout"):
            with migration_step(conn, timeout_seconds=10):
                conn.exec_driver_sql("SELECT pg_sleep(0.4)")
        assert conn.scalar(text("SHOW statement_timeout")) == "200ms"
        assert conn.scalar(text("SHOW lock_timeout")) == "75ms"


def test_remaining_attempt_time_caps_a_longer_requested_step(capture_source):
    with capture_source.begin() as conn:
        install_capture(conn)
        conn.exec_driver_sql(f"""
            UPDATE {STATE}
            SET prepared_at=clock_timestamp()-interval '24 hours'+interval '1 second'
        """)
        original = inspect_capture(conn)
        with pytest.raises((DBAPIError, RuntimeError), match="statement timeout|step_timeout|attempt_expired"):
            with migration_step(conn, timeout_seconds=10):
                conn.exec_driver_sql("SELECT pg_sleep(2)")
        assert inspect_capture(conn)["started_at"] == original["started_at"]


def test_future_capture_clock_refuses_work_without_rewriting_it(capture_source):
    with capture_source.begin() as conn:
        install_capture(conn)
        conn.exec_driver_sql(f"UPDATE {STATE} SET prepared_at=clock_timestamp()+interval '1 hour'")
        original = inspect_capture(conn)
        with pytest.raises(RuntimeError, match="start_time_invalid"):
            with migration_step(conn):
                pytest.fail("future-dated capture cannot admit migration work")
        assert inspect_capture(conn) == original



def test_disconnect_during_timeout_setup_invalidates_connection_and_preserves_source(capture_source):
    engine = capture_source
    with engine.begin() as conn:
        install_capture(conn)
        insert(conn, "committed-before-disconnect")
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            with pytest.raises(DBAPIError):
                with migration_step(conn):
                    insert(conn, "lost-copy-step")
                    pid = conn.scalar(text("SELECT pg_backend_pid()"))
                    with engine.begin() as killer:
                        assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"), {"pid": pid})
                    # Its timeout setup is the first call on the dead connection.
                    conn.exec_driver_sql("SELECT 1")
            assert conn.invalidated
        finally:
            transaction.rollback()
        assert conn.execute(text(f"SELECT id FROM {SOURCE} ORDER BY id")).scalars().all() == [
            "committed-before-disconnect", "existing"]
        assert conn.execute(text(f"SELECT id FROM {QUEUE} ORDER BY id")).scalars().all() == [
            "committed-before-disconnect"]


def test_deep_verification_does_not_amplify_timeout_sql(capture_source):
    with capture_source.begin() as conn:
        install_capture(conn)
        observed = []
        def record(connection, cursor, statement, parameters, context, executemany):
            observed.append(statement)
        event.listen(conn, "after_cursor_execute", record)
        try:
            with ExitStack() as steps:
                for _ in range(6):
                    steps.enter_context(migration_step(conn, timeout_seconds=30))
                observed.clear()
                assert conn.scalar(text("SELECT 42")) == 42
                # Nested verification must not turn a single read into hundreds
                # of timeout queries. Allow implementation-independent linear
                # overhead rather than depending on a particular listener count.
                assert len(observed) <= 12
            assert conn.scalar(text("SHOW statement_timeout")) == "0"
            assert conn.scalar(text("SELECT 1")) == 1
        finally:
            event.remove(conn, "after_cursor_execute", record)


def test_expired_child_releases_its_deadline_without_poisoning_parent(capture_source):
    with capture_source.begin() as conn:
        install_capture(conn)
        with migration_step(conn, timeout_seconds=10):
            insert(conn, "outer-survives-child")
            with pytest.raises((DBAPIError, RuntimeError), match="statement timeout|step_timeout"):
                with migration_step(conn, timeout_seconds=1):
                    insert(conn, "child-must-rollback")
                    conn.exec_driver_sql("SELECT pg_sleep(1.2)")
            assert conn.scalar(text("SELECT 1")) == 1
            assert conn.execute(text(f"SELECT id FROM {SOURCE} ORDER BY id")).scalars().all() == [
                "existing", "outer-survives-child"]
        assert conn.scalar(text("SHOW statement_timeout")) == "0"
