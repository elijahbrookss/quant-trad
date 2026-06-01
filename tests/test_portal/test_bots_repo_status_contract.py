from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.storage.repos import bots
from portal.backend.service.storage.repos import lifecycle as lifecycle_repo
from portal.backend.service.storage.repos import run_leases as run_lease_repo


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


class _FakeLoadResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _FakeLoadSession:
    def __init__(self, rows):
        self.rows = list(rows)

    def execute(self, _stmt):
        return _FakeLoadResult(self.rows)


class _FakeLoadDb:
    available = True

    def __init__(self, rows):
        self.session_handle = _FakeLoadSession(rows)

    @contextmanager
    def session(self):
        yield self.session_handle


def test_load_bots_returns_definition_only_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_db = _FakeLoadDb(
        [
            {
                "id": "bot-1",
                "name": "Bot 1",
                "strategy_id": "strategy-1",
                "status": "running",
                "risk": {"execution_mode": "full"},
                "last_run_artifact": {"large": "payload"},
            }
        ]
    )
    monkeypatch.setattr(bots, "db", fake_db)

    result = bots.load_bots()

    assert result[0]["id"] == "bot-1"
    assert result[0]["strategy_ids"] == ["strategy-1"]
    assert result[0]["execution_mode"] == "full"
    assert "status" not in result[0]
    assert "last_run_artifact" not in result[0]
    assert "runner_id" not in result[0]


def test_upsert_bot_rejects_runtime_fields() -> None:
    with pytest.raises(ValueError, match="portal_bots is definition-only"):
        bots.upsert_bot({"id": "bot-1", "name": "Bot 1", "last_run_artifact": {"large": "payload"}})


def test_mark_bot_crashed_skips_terminal_completed_run(monkeypatch: pytest.MonkeyPatch) -> None:
    bot_row = SimpleNamespace(updated_at=None)
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
    monkeypatch.setattr(run_lease_repo, "get_bot_run_lease", lambda _run_id: None)
    recorded = []

    monkeypatch.setattr(bots, "db", fake_db)
    monkeypatch.setattr(
        lifecycle_repo,
        "get_latest_bot_run_lifecycle",
        lambda bot_id: fake_db.session_handle.latest_lifecycle,
    )
    monkeypatch.setattr(lifecycle_repo, "record_bot_run_lifecycle_checkpoint", lambda payload: recorded.append(payload))

    result = bots.mark_bot_crashed("bot-1", "container_not_running:quant-trad-bots-bot-1")

    assert result is False
    assert recorded == []


def test_mark_bot_crashed_rejects_missing_run_context(monkeypatch: pytest.MonkeyPatch) -> None:
    bot_row = SimpleNamespace(updated_at=None)
    fake_db = _FakeDb(
        bot_row=bot_row,
        latest_lifecycle=None,
    )
    monkeypatch.setattr(run_lease_repo, "get_bot_run_lease", lambda _run_id: None)
    recorded = []

    monkeypatch.setattr(bots, "db", fake_db)
    monkeypatch.setattr(
        lifecycle_repo,
        "get_latest_bot_run_lifecycle",
        lambda bot_id: fake_db.session_handle.latest_lifecycle,
    )
    monkeypatch.setattr(lifecycle_repo, "record_bot_run_lifecycle_checkpoint", lambda payload: recorded.append(payload))

    result = bots.mark_bot_crashed("bot-1", "container_not_running:quant-trad-bots-bot-1")

    assert result is False
    assert recorded == []


def test_mark_bot_crashed_classifies_stale_run_lease_as_recoverable_lifecycle_degradation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot_row = SimpleNamespace(updated_at=None)
    fake_db = _FakeDb(
        bot_row=bot_row,
        latest_lifecycle=SimpleNamespace(
            run_id="run-1",
            phase="live",
            status="running",
            checkpoint_at="2026-04-17T13:43:00Z",
            updated_at="2026-04-17T13:43:00Z",
        ),
    )
    monkeypatch.setattr(
        run_lease_repo,
        "get_bot_run_lease",
        lambda _run_id: {"runner_id": "backend.quanttrad"},
    )
    recorded = []

    monkeypatch.setattr(bots, "db", fake_db)
    monkeypatch.setattr(
        lifecycle_repo,
        "get_latest_bot_run_lifecycle",
        lambda bot_id: fake_db.session_handle.latest_lifecycle,
    )
    monkeypatch.setattr(lifecycle_repo, "record_bot_run_lifecycle_checkpoint", lambda payload: recorded.append(payload))

    result = bots.mark_bot_crashed(
        "bot-1",
        "stale_run_lease:prev=backend.quanttrad",
        diagnostics={
            "lease_expired_age_seconds": 125.0,
            "runner_clock_gap": {"gap_seconds": 3672.0, "detected_at": "2026-05-19T07:57:54Z"},
        },
    )

    assert result is True
    assert len(recorded) == 1
    assert recorded[0]["phase"] == "degraded"
    assert recorded[0]["status"] == "degraded"
    assert recorded[0]["metadata"]["watchdog_classification"] == "recoverable"
    assert recorded[0]["metadata"]["watchdog_diagnostics"]["lease_expired_age_seconds"] == 125.0
    assert recorded[0]["metadata"]["watchdog_diagnostics"]["runner_clock_gap"]["gap_seconds"] == 3672.0
    assert recorded[0]["failure"]["reason_code"] == "stale_run_lease"
    assert recorded[0]["failure"]["recoverable"] is True
