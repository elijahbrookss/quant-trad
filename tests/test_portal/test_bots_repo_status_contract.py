from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.storage.repos import bots
from portal.backend.service.storage.repos import lifecycle as lifecycle_repo


class _FakeScalarResult:
    def __init__(self, value):
        self._value = value

    def scalars(self):
        return self

    def first(self):
        return self._value


class _FakeSession:
    def __init__(self, *, bot_row, latest_lifecycle):
        self.bot_row = bot_row
        self.latest_lifecycle = latest_lifecycle

    def get(self, model, key):
        if model is bots.BotRecord and key == "bot-1":
            return self.bot_row
        return None

    def execute(self, _stmt):
        return _FakeScalarResult(self.latest_lifecycle)


class _FakeDb:
    available = True

    def __init__(self, *, bot_row, latest_lifecycle):
        self.session_handle = _FakeSession(bot_row=bot_row, latest_lifecycle=latest_lifecycle)

    @contextmanager
    def session(self):
        yield self.session_handle


def test_mark_bot_crashed_skips_terminal_completed_run(monkeypatch: pytest.MonkeyPatch) -> None:
    bot_row = SimpleNamespace(
        runner_id="runner-1",
        heartbeat_at="2026-04-17T13:44:00Z",
        updated_at=None,
    )
    fake_db = _FakeDb(
        bot_row=bot_row,
        latest_lifecycle=SimpleNamespace(
            run_id="run-1",
            phase="completed",
            status="completed",
            checkpoint_at="2026-04-17T13:45:00Z",
            updated_at="2026-04-17T13:45:00Z",
        ),
    )
    recorded = []

    monkeypatch.setattr(bots, "db", fake_db)
    monkeypatch.setattr(lifecycle_repo, "record_bot_run_lifecycle_checkpoint", lambda payload: recorded.append(payload))

    result = bots.mark_bot_crashed("bot-1", "container_not_running:quant-trad-bots-bot-1")

    assert result is False
    assert bot_row.runner_id == "runner-1"
    assert bot_row.heartbeat_at == "2026-04-17T13:44:00Z"
    assert recorded == []


def test_mark_bot_crashed_rejects_missing_run_context(monkeypatch: pytest.MonkeyPatch) -> None:
    bot_row = SimpleNamespace(
        runner_id="runner-1",
        heartbeat_at="2026-04-17T13:44:00Z",
        updated_at=None,
    )
    fake_db = _FakeDb(
        bot_row=bot_row,
        latest_lifecycle=None,
    )
    recorded = []

    monkeypatch.setattr(bots, "db", fake_db)
    monkeypatch.setattr(lifecycle_repo, "record_bot_run_lifecycle_checkpoint", lambda payload: recorded.append(payload))

    result = bots.mark_bot_crashed("bot-1", "container_not_running:quant-trad-bots-bot-1")

    assert result is False
    assert bot_row.runner_id == "runner-1"
    assert bot_row.heartbeat_at == "2026-04-17T13:44:00Z"
    assert recorded == []
