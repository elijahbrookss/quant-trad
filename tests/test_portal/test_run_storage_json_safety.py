from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

import pytest

from portal.backend.service.storage.repos import runs


class _FakeSession:
    def __init__(self) -> None:
        self.record = None

    def get(self, _model, _run_id):
        return self.record

    def add(self, record) -> None:
        self.record = record


class _FakeDb:
    available = True

    def __init__(self) -> None:
        self.session_instance = _FakeSession()

    @contextmanager
    def session(self) -> Iterator[_FakeSession]:
        yield self.session_instance


def test_upsert_bot_run_json_sanitizes_nested_runtime_snapshot(monkeypatch) -> None:
    fake_db = _FakeDb()
    monkeypatch.setattr(runs, "db", fake_db)

    result = runs.upsert_bot_run(
        {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "bot_name": "Bot 1",
            "strategy_id": "strategy-1",
            "run_type": "backtest",
            "symbols": ["BTCUSD"],
            "config_snapshot": {
                "started_at": datetime(2026, 5, 17, 7, 45, tzinfo=timezone.utc),
                "bot": {"updated_at": datetime(2026, 5, 17, 7, 46)},
            },
        }
    )

    assert result["config_snapshot"]["started_at"] == "2026-05-17T07:45:00Z"
    assert result["config_snapshot"]["bot"]["updated_at"] == "2026-05-17T07:46:00Z"
    assert result["config_hash"]
    assert result["material_config_hash"]
    assert "decision_ledger" not in result


def test_upsert_bot_run_rejects_lifecycle_owned_fields(monkeypatch) -> None:
    monkeypatch.setattr(runs, "db", _FakeDb())

    with pytest.raises(ValueError, match="lifecycle fields are ledger-owned"):
        runs.upsert_bot_run(
            {
                "run_id": "run-1",
                "status": "completed",
                "started_at": "2026-05-17T07:45:00Z",
                "ended_at": "2026-05-17T07:46:00Z",
            }
        )
