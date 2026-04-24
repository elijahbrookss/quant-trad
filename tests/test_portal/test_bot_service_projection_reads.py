from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import bot_service


class _FakeConfigService:
    def __init__(self) -> None:
        self._bots = [
            {
                "id": "bot-1",
                "name": "Bot 1",
                "strategy_id": "strategy-1",
                "wallet_config": {"balances": {"USDC": 100.0}},
                "snapshot_interval_ms": 1000,
                "run_type": "backtest",
                "status": "running",
                "runner_id": "runner-1",
                "heartbeat_at": "2026-04-09T04:21:43Z",
            }
        ]

    def list_bots(self):
        return [dict(bot) for bot in self._bots]

    def get_bot(self, bot_id: str):
        for bot in self._bots:
            if bot["id"] == bot_id:
                return dict(bot)
        raise KeyError(bot_id)


class _FakeStorage:
    def __init__(self) -> None:
        self.run = {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "status": "running",
            "started_at": "2026-04-09T04:21:37Z",
            "summary": {"total_trades": 4},
        }
        self.lifecycle = {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "phase": "live",
            "status": "running",
            "owner": "runtime",
            "message": "live",
            "metadata": {},
            "failure": {},
            "checkpoint_at": "2026-04-09T04:21:43Z",
            "updated_at": "2026-04-09T04:21:43Z",
        }

    def get_latest_bot_run_lifecycle(self, bot_id: str):
        return dict(self.lifecycle) if str(bot_id) == "bot-1" else None

    def get_latest_bot_runtime_run_id(self, bot_id: str):
        return "run-1" if str(bot_id) == "bot-1" else None

    def get_bot_run(self, run_id: str):
        return dict(self.run) if str(run_id) == "run-1" else None

    def list_bot_runs(self, bot_id: str):
        return [dict(self.run)] if str(bot_id) == "bot-1" else []


class _FakeTelemetryHub:
    def __init__(self, snapshots: dict[str, object] | None = None) -> None:
        self._snapshots = dict(snapshots or {})

    def get_run_snapshot(self, *, run_id: str):
        return self._snapshots.get(str(run_id))


class _FakeComposition:
    def __init__(self, *, config_service, storage) -> None:
        self.config_service = config_service
        self.storage = storage
        self.stream_manager = SimpleNamespace(broadcast=lambda *args, **kwargs: None)
        self.runtime_control_service = SimpleNamespace(
            start_bot=lambda bot_id: {"id": bot_id},
            stop_bot=lambda bot_id, preserve_container=False: {"id": bot_id, "preserve_container": preserve_container},
            bots_stream=lambda: None,
            watchdog_status=lambda: {},
        )
        self.watchdog = SimpleNamespace(set_orphan_callback=lambda callback: None)


def test_list_bot_runs_for_bot_reports_snapshot_unavailable_without_replay(monkeypatch):
    def _unexpected_replay(*args, **kwargs):
        _ = args, kwargs
        raise AssertionError("run list projection must not trigger replay")

    composition = _FakeComposition(config_service=_FakeConfigService(), storage=_FakeStorage())
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(bot_service, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(
        "portal.backend.service.bots.botlens_event_replay.rebuild_run_projection_snapshot",
        _unexpected_replay,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.bot_service.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: {
            "name": "quant-trad-bots-bot-1",
            "status": "running",
            "running": True,
            "id": "container-1",
            "started_at": "2026-04-09T04:21:37Z",
            "finished_at": None,
            "exit_code": None,
            "error": None,
        },
    )

    result = bot_service.list_bot_runs_for_bot("bot-1")

    assert result["active_run_id"] == "run-1"
    assert result["runs"][0]["botlens_available"] is False
    assert result["runs"][0]["botlens_reason"] == "snapshot_unavailable"
    assert result["runs"][0]["seq"] is None


def test_runtime_capacity_marks_estimate_incomplete_when_snapshot_missing(monkeypatch):
    composition = _FakeComposition(config_service=_FakeConfigService(), storage=_FakeStorage())
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(bot_service, "_telemetry_hub", lambda: _FakeTelemetryHub())

    result = bot_service.runtime_capacity()

    assert result["running_bots"] == 1
    assert result["workers_in_use"] == 0
    assert result["workers_requested"] == 0
    assert result["telemetry_unavailable_bots"] == 1
    assert result["estimate_incomplete"] is True
