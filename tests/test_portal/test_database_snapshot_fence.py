from __future__ import annotations

from types import SimpleNamespace

import pytest

from portal.backend.db import session as module


class Connection:
    def __init__(self, events, failure=None):
        self.events = events
        self.failure = failure
        self.invalidated = False
        self.transaction = False

    def _step(self, name):
        self.events.append(name)
        if self.failure == name:
            raise RuntimeError(name)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.events.append("connection.close")

    def execution_options(self, *, isolation_level):
        name = isolation_level
        if isolation_level == "AUTOCOMMIT" and "lock" in self.events:
            name = "unlock.autocommit"
        self._step(name)
        return self

    def execute(self, sql, params):
        operation = "unlock" if "pg_advisory_unlock" in str(sql) else "lock"
        self._step(operation)
        self.transaction = True
        return SimpleNamespace(scalar_one=lambda: not (operation == "unlock" and self.failure == "unlock.false"))

    def commit(self):
        self._step("connection.commit")
        self.transaction = False

    def in_transaction(self):
        return self.transaction

    def rollback(self):
        self._step("connection.rollback")
        self.transaction = False

    def invalidate(self):
        self.events.append("invalidate")
        self.invalidated = True


def _database(monkeypatch, *, failure=None):
    events = []
    connection = Connection(events, failure)

    class Session:
        def __init__(self, **kwargs):
            assert kwargs["bind"] is connection
            events.append("session.open")

        def __enter__(self):
            return self

        def __exit__(self, *_):
            events.append("session.close")

        def commit(self):
            events.append("session.commit")

        def rollback(self):
            events.append("session.rollback")

    database = module.Database()
    database._engine = SimpleNamespace(connect=lambda: connection)
    monkeypatch.setattr(database, "ensure_schema", lambda: True)
    monkeypatch.setattr(module, "Session", Session)
    return database, connection, events


def test_snapshot_establishes_visibility_only_after_fence_and_releases_after_commit(monkeypatch):
    database, connection, events = _database(monkeypatch)
    with database.locked_snapshot_session(shared_lock_name="test"):
        events.append("read")
    assert events == ["AUTOCOMMIT", "lock", "connection.commit", "REPEATABLE READ",
                      "session.open", "read", "session.commit", "session.close",
                      "unlock.autocommit", "unlock", "connection.commit", "connection.close"]
    assert not connection.invalidated


@pytest.mark.parametrize("failure", ["lock", "REPEATABLE READ", "unlock.autocommit", "unlock", "unlock.false"])
def test_ambiguous_ownership_never_returns_a_locked_connection_to_the_pool(monkeypatch, failure):
    database, connection, events = _database(monkeypatch, failure=failure)
    with pytest.raises(RuntimeError):
        with database.locked_snapshot_session(shared_lock_name="test"):
            pass
    if failure == "REPEATABLE READ":
        # A failed isolation change still releases the already acquired fence.
        assert "unlock" in events
    else:
        assert connection.invalidated
        assert events.index("invalidate") < events.index("connection.close")


def test_base_exception_rolls_back_reader_and_releases_fence(monkeypatch):
    database, connection, events = _database(monkeypatch)
    with pytest.raises(KeyboardInterrupt):
        with database.locked_snapshot_session(shared_lock_name="test"):
            raise KeyboardInterrupt()
    assert events.index("session.rollback") < events.index("unlock") < events.index("connection.close")
    assert not connection.invalidated


def test_failed_cleanup_rollback_invalidates_connection(monkeypatch):
    database, connection, events = _database(monkeypatch, failure="connection.rollback")
    with pytest.raises(RuntimeError, match="connection.rollback"):
        with database.locked_snapshot_session(shared_lock_name="test"):
            connection.transaction = True
    assert connection.invalidated
    assert "unlock" not in events
