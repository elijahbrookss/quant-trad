from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots.runtime_control_service import BotRuntimeControlService


@pytest.fixture(autouse=True)
def _stub_lifecycle_bridge(monkeypatch):
    monkeypatch.setattr(
        "portal.backend.service.bots.startup_service.emit_lifecycle_event",
        lambda payload: None,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.emit_lifecycle_event",
        lambda payload: None,
    )


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
                "status": "idle",
            }
        ]

    def list_bots(self):
        return [dict(bot) for bot in self._bots]

    def get_bot(self, bot_id: str):
        for bot in self._bots:
            if bot["id"] == bot_id:
                return dict(bot)
        raise KeyError(bot_id)

    def prepare_startup_artifacts(self, bot):
        _ = bot
        return {
            "strategy_id": "strategy-1",
            "wallet_config": {"balances": {"USDC": 100.0}},
            "strategy": SimpleNamespace(
                id="strategy-1",
                name="Strategy 1",
                timeframe="1m",
                datasource="demo",
                exchange="paper",
            ),
            "runtime_readiness": {
                "symbols": ["BTCUSDT"],
                "profiles": [{"symbol": "BTCUSDT"}],
            },
        }


class _FakeStreamManager:
    def __init__(self) -> None:
        self.messages = []
        self.initial = None

    def broadcast(self, event, payload):
        self.messages.append((event, payload))

    def subscribe_all(self, snapshot_fn):
        self.initial = {"type": "snapshot", "bots": snapshot_fn()}
        return (lambda: None), None, self.initial


class _FakeStorage:
    def __init__(self) -> None:
        self.bots = []
        self.runs = {}
        self.lifecycle = {}

    def upsert_bot(self, payload):
        self.bots.append(dict(payload))

    def upsert_bot_run(self, payload):
        row = dict(payload)
        self.runs[str(row["run_id"])] = row
        return row

    def get_bot_run(self, run_id):
        return dict(self.runs.get(str(run_id), {})) or None

    def get_latest_bot_runtime_run_id(self, bot_id):
        for run_id, row in reversed(list(self.runs.items())):
            if str(row.get("bot_id")) == str(bot_id):
                return run_id
        return None

    def get_latest_bot_run_lifecycle(self, bot_id):
        for row in reversed(self.lifecycle.get(str(bot_id), [])):
            return dict(row)
        return None

    def record_bot_run_lifecycle_checkpoint(self, payload):
        row = dict(payload)
        self.lifecycle.setdefault(str(row["bot_id"]), []).append(row)
        return row

    def update_bot_runtime_status(self, *, bot_id, run_id, status, telemetry_degraded=False):
        _ = telemetry_degraded
        if str(run_id) in self.runs:
            self.runs[str(run_id)]["status"] = status


class _FakeWatchdog:
    runner_id = "runner-test"

    def register_bot(self, _bot_id: str):
        return None

    def unregister_bot(self, _bot_id: str):
        return None

    def scan_stale_heartbeats(self):
        return []

    def verify_container_ownership(self):
        return []

    def status(self):
        return {"runner_id": self.runner_id}


class _FakeTelemetryHub:
    def __init__(self, snapshots: dict[str, object] | None = None) -> None:
        self._snapshots = dict(snapshots or {})

    def get_run_snapshot(self, *, run_id: str):
        return self._snapshots.get(str(run_id))


def test_start_bot_persists_startup_failed_when_runner_fails():
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()

    class _FailingRunner:
        def start_bot(self, *, bot, run_id):
            _ = bot, run_id
            raise RuntimeError("container boot failed")

        def stop_bot(self, *, bot_id):
            _ = bot_id
            return None

    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: _FailingRunner(),
    )

    with pytest.raises(RuntimeError, match="container boot failed"):
        service.start_bot("bot-1")

    assert storage.runs, "expected run row to exist before launch failure"
    run = next(iter(storage.runs.values()))
    assert run["status"] == "startup_failed"
    assert storage.bots[-1]["status"] == "startup_failed"
    assert storage.bots[-1]["runner_id"] is None
    assert storage.lifecycle["bot-1"][-1]["phase"] == "startup_failed"
    assert "container boot failed" in storage.lifecycle["bot-1"][-1]["message"]
    assert stream.messages[-1][0] == "bot"


def test_start_bot_rejects_unknown_runtime_target(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    service = BotRuntimeControlService(config, stream, storage=_FakeStorage(), watchdog=_FakeWatchdog())
    monkeypatch.setattr(BotRuntimeControlService, "_runner_target", staticmethod(lambda: "vps"))

    with pytest.raises(RuntimeError, match="Unsupported bot runtime target"):
        service.start_bot("bot-1")


def test_stop_bot_can_preserve_container_for_debugging(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    storage.runs["run-1"] = {
        "run_id": "run-1",
        "bot_id": "bot-1",
        "status": "running",
        "started_at": "2026-04-09T04:21:37Z",
    }
    storage.lifecycle["bot-1"] = [
        {
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
    ]
    stop_calls: list[dict[str, object]] = []

    class _Runner:
        def start_bot(self, *, bot, run_id):
            _ = bot, run_id
            return "container-1"

        def stop_bot(self, *, bot_id, preserve_container=False):
            stop_calls.append({"bot_id": bot_id, "preserve_container": preserve_container})
            return None

    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: {
            "name": "quant-trad-bots-bot-1",
            "status": "exited",
            "running": False,
            "id": "container-1",
            "started_at": "2026-04-09T04:21:37Z",
            "finished_at": "2026-04-09T04:22:00Z",
            "exit_code": 137,
            "oom_killed": False,
            "error": None,
        },
    )

    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: _Runner(),
    )

    projected = service.stop_bot("bot-1", preserve_container=True)

    assert stop_calls == [{"bot_id": "bot-1", "preserve_container": True}]
    assert storage.lifecycle["bot-1"][-1]["metadata"]["terminal_actor"] == "platform_stop"
    assert storage.lifecycle["bot-1"][-1]["metadata"]["preserve_container"] is True
    assert projected["lifecycle"]["container"]["status"] == "exited"


def test_bots_stream_snapshot_uses_projected_bot_payload():
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    storage.runs["run-1"] = {
        "run_id": "run-1",
        "bot_id": "bot-1",
        "status": "startup_failed",
        "started_at": "2026-04-09T04:21:37Z",
        "ended_at": "2026-04-09T04:21:43Z",
    }
    storage.lifecycle["bot-1"] = [
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "phase": "startup_failed",
            "status": "startup_failed",
            "owner": "runtime",
            "message": "Worker worker-1 exited with code 1",
            "metadata": {},
            "failure": {"message": "Worker worker-1 exited with code 1"},
            "checkpoint_at": "2026-04-09T04:21:43Z",
            "updated_at": "2026-04-09T04:21:43Z",
        }
    ]
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: None,
    )

    _release, _channel, initial = service.bots_stream()

    assert initial["type"] == "snapshot"
    assert initial["bots"][0]["status"] == "startup_failed"
    assert initial["bots"][0]["active_run_id"] == "run-1"
    assert initial["bots"][0]["lifecycle"]["status"] == "startup_failed"
    assert initial["bots"][0]["controls"]["can_start"] is True


def test_bots_stream_snapshot_marks_telemetry_unavailable_without_replay(monkeypatch):
    def _unexpected_replay(*args, **kwargs):
        _ = args, kwargs
        raise AssertionError("bots_stream must not trigger replay")

    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    storage.runs["run-1"] = {
        "run_id": "run-1",
        "bot_id": "bot-1",
        "status": "running",
        "started_at": "2026-04-09T04:21:37Z",
    }
    storage.lifecycle["bot-1"] = [
        {
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
    ]
    monkeypatch.setattr(
        BotRuntimeControlService,
        "_telemetry_hub",
        staticmethod(lambda: _FakeTelemetryHub()),
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.botlens_event_replay.rebuild_run_projection_snapshot",
        _unexpected_replay,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.inspect_bot_container",
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
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: None,
    )

    _release, _channel, initial = service.bots_stream()

    bot = initial["bots"][0]
    assert bot["status"] == "running"
    assert bot["runtime"]["phase"] == "live"
    assert bot["lifecycle"]["telemetry"]["available"] is False
    assert bot["lifecycle"]["telemetry"]["reason"] == "snapshot_unavailable"
    assert bot["lifecycle"]["telemetry"]["worker_count"] is None
    assert bot["lifecycle"]["telemetry"]["seq"] is None


def test_bots_stream_snapshot_tolerates_null_recent_transitions(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    storage.runs["run-1"] = {
        "run_id": "run-1",
        "bot_id": "bot-1",
        "status": "running",
        "started_at": "2026-04-09T04:21:37Z",
    }
    storage.lifecycle["bot-1"] = [
        {
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
    ]
    monkeypatch.setattr(
        BotRuntimeControlService,
        "_telemetry_hub",
        staticmethod(
            lambda: _FakeTelemetryHub(
                {
                    "run-1": SimpleNamespace(
                        seq=7,
                        health=SimpleNamespace(
                            to_dict=lambda: {
                                "status": "running",
                                "runtime_state": "live",
                                "recent_transitions": None,
                                "warning_count": 0,
                                "warnings": [],
                                "worker_count": 2,
                                "active_workers": 2,
                                "last_event_at": "2026-04-09T04:21:43Z",
                            }
                        ),
                        symbol_catalog=SimpleNamespace(entries={}),
                        open_trades=SimpleNamespace(entries={}),
                    )
                }
            )
        ),
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.inspect_bot_container",
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
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: None,
    )

    _release, _channel, initial = service.bots_stream()

    assert initial["type"] == "snapshot"
    assert initial["bots"][0]["active_run_id"] == "run-1"
    assert initial["bots"][0]["runtime"]["status"] == "running"
    assert initial["bots"][0]["lifecycle"]["telemetry"]["available"] is True
