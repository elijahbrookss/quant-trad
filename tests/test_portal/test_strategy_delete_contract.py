from __future__ import annotations

from contextlib import contextmanager

import pytest
from portal.backend.service.storage.repos import strategies as strategy_repos


class _FakeExecuteResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)


class _BlockingSession:
    def __init__(self) -> None:
        self.deleted = []

    def execute(self, _stmt):
        return _FakeExecuteResult([("bot-1", "Momentum Bot"), ("bot-2", "Breakout Bot")])

    def get(self, _model, _strategy_id):
        raise AssertionError("strategy row lookup should not happen when dependent bots exist")

    def delete(self, record):
        self.deleted.append(record)

    def query(self, _model):
        raise AssertionError("dependent-row deletes should not happen when dependent bots exist")


class _FakeDb:
    available = True

    @contextmanager
    def session(self):
        yield _BlockingSession()


def test_delete_strategy_blocks_when_bots_still_reference_it(monkeypatch, caplog) -> None:
    caplog.set_level("ERROR")
    monkeypatch.setattr(strategy_repos, "db", _FakeDb())

    with pytest.raises(ValueError, match="dependent bots exist"):
        strategy_repos.delete_strategy("strategy-1")

    assert "strategy_delete_blocked_by_bots" in caplog.text
    assert "bot-1" in caplog.text
    assert "bot-2" in caplog.text
