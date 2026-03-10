"""Regression tests for lazy DB session wiring."""


import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.db.session import Database


def test_database_does_not_require_pg_dsn_until_used(monkeypatch) -> None:
    """Constructing the DB helper should be pure and side-effect free for unit tests."""

    monkeypatch.delenv("PG_DSN", raising=False)

    database = Database()

    assert database.dsn is None
    assert database.ensure_schema() is False
    assert isinstance(database.last_error, RuntimeError)
    assert "PG_DSN is required" in str(database.last_error)
