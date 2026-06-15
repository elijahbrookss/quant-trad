"""Regression tests for lazy DB session wiring."""


from dataclasses import replace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.db import session as db_session

Database = db_session.Database


def test_database_does_not_require_pg_dsn_until_used(monkeypatch) -> None:
    """Constructing the DB helper should be pure and side-effect free for unit tests."""

    monkeypatch.delenv("PG_DSN", raising=False)
    monkeypatch.setattr(db_session, "_DB_SETTINGS", replace(db_session._DB_SETTINGS, dsn=None))

    database = Database()

    assert database.dsn is None
    assert database.ensure_schema() is False
    assert isinstance(database.last_error, RuntimeError)
    assert "PG_DSN is required" in str(database.last_error)
