"""Disposable database cleanup must terminate only its own remaining sessions."""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db


@pytest.mark.parametrize("install_extensions", [False, True])
def test_cleanup_drops_owned_database_with_open_session_and_preserves_other_database(install_extensions):
    engine = admin_engine = None
    leftover = admin = None
    try:
        with fresh_migration_database("cleanup", install_extensions=install_extensions) as dsn:
            url = make_url(dsn)
            engine = create_engine(url, isolation_level="AUTOCOMMIT")
            admin_engine = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
            leftover = engine.connect()
            admin = admin_engine.connect()
            target_pid = leftover.execute(text("SELECT pg_backend_pid()")).scalar_one()
            admin_pid = admin.execute(text("SELECT pg_backend_pid()")).scalar_one()
            assert target_pid != admin_pid
            assert leftover.execute(text("SELECT 1")).scalar_one() == 1
            if install_extensions:
                assert leftover.execute(text(
                    "SELECT current_setting('timescaledb.telemetry_level')"
                )).scalar_one() == "off"
            # Intentionally keep this session open across context cleanup.
        assert admin.execute(text("SELECT pg_backend_pid()")).scalar_one() == admin_pid
        assert admin.execute(text(
            "SELECT count(*) FROM pg_database WHERE datname=:name"
        ), {"name": url.database}).scalar_one() == 0
        with pytest.raises(DBAPIError):
            leftover.execute(text("SELECT 1"))
    finally:
        if leftover is not None:
            leftover.invalidate()
            leftover.close()
        if admin is not None:
            admin.close()
        if engine is not None:
            engine.dispose()
        if admin_engine is not None:
            admin_engine.dispose()
