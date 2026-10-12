"""Real signals must interrupt the dedicated placement CLI's native SQL wait."""
import json
import os
import selectors
import signal
import subprocess
import sys
from time import monotonic, sleep

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


pytestmark = [pytest.mark.db, pytest.mark.skipif(
    os.name != "posix", reason="the maintenance CLI runs on Linux")]


@pytest.mark.parametrize("stop_signal", [signal.SIGINT, signal.SIGTERM])
def test_placement_signal_interrupts_blocked_native_query(stop_signal):
    dsn = os.getenv("PG_DSN")
    if not dsn:
        pytest.skip("requires the disposable database test stack")
    child_code = """
import json, os
from time import monotonic
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from cli.storage_placement import _cancellation
from portal.backend.service.storage.header_movement import _MoveWatch

engine = create_engine(os.environ['PG_DSN'], poolclass=NullPool,
    connect_args={'connect_timeout': 3, 'options': '-c statement_timeout=15000'})
try:
    with engine.connect() as conn, _cancellation() as cancelled:
        driver = conn.connection.driver_connection
        watch = _MoveWatch(driver=driver, targets=[], capacity={}, floors={},
            deadline=monotonic()+30, cancelled=cancelled, grace=2)
        watch.start()
        print(json.dumps({'backend_pid': driver.get_backend_pid()}), flush=True)
        started = monotonic()
        interrupted = False
        try:
            with driver.cursor() as cursor:
                cursor.execute('SELECT pg_sleep(30)')
        except psycopg2.errors.QueryCanceled:
            interrupted = True
        finally:
            elapsed = monotonic()-started
            watch.stop(conn)
            driver.rollback()
        assert interrupted and cancelled(), 'signal did not cancel the query'
        assert watch.failure == 'storage_move_cancelled', watch.failure
        assert elapsed < 8, 'query reached its statement timeout'
        with driver.cursor() as cursor:
            cursor.execute('SELECT 1')
            assert cursor.fetchone() == (1,)
        print(json.dumps({'cancelled': True, 'elapsed': elapsed}), flush=True)
finally:
    engine.dispose()
"""
    engine = create_engine(dsn, poolclass=NullPool, connect_args={
        "connect_timeout": 3, "options": "-c statement_timeout=2000"})
    # Pytest's configured source paths are not inherited by a fresh interpreter.
    child_env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    child = subprocess.Popen([sys.executable, "-c", child_code],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, env=child_env)
    try:
        with selectors.DefaultSelector() as ready:
            ready.register(child.stdout, selectors.EVENT_READ)
            assert ready.select(15), "placement child did not become ready"
        line = child.stdout.readline()
        if not line:
            _, errors = child.communicate(timeout=5)
            pytest.fail("placement child exited before its database connection "
                "was ready: " + errors)
        backend_pid = json.loads(line)["backend_pid"]
        with engine.connect() as observer:
            deadline = monotonic()+5
            while monotonic() < deadline:
                active = observer.scalar(text(
                    "SELECT state='active' AND query='SELECT pg_sleep(30)' "
                    "FROM pg_stat_activity WHERE pid=:pid"), {"pid": backend_pid})
                observer.rollback()  # Refresh PostgreSQL's statistics snapshot.
                if active:
                    break
                sleep(.02)
            else:
                pytest.fail("placement child never entered its native SQL wait")
        child.send_signal(stop_signal)
        output, errors = child.communicate(timeout=8)
        assert child.returncode == 0, errors
        assert json.loads(output)["cancelled"] is True
    finally:
        if child.poll() is None:
            child.kill()  # Only this test's disposable child, never a database PID.
            child.communicate(timeout=5)
        engine.dispose()
