from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import time

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
        self.leases = {}
        self.released_leases = []

    def upsert_bot(self, payload):
        self.bots.append(dict(payload))

    def upsert_bot_run(self, payload):
        row = dict(payload)
        self.runs[str(row["run_id"])] = row
        return row

    def get_bot_run(self, run_id):
        return dict(self.runs.get(str(run_id), {})) or None

    def list_bot_runs(self, *, bot_id=None):
        rows = list(self.runs.values())
        if bot_id:
            rows = [row for row in rows if str(row.get("bot_id")) == str(bot_id)]
        return [dict(row) for row in rows]

    def get_latest_bot_runtime_run_id(self, bot_id):
        for run_id, row in reversed(list(self.runs.items())):
            if str(row.get("bot_id")) == str(bot_id):
                return run_id
        return None

    def get_bot_run_lifecycle(self, run_id):
        for rows in self.lifecycle.values():
            for row in reversed(rows):
                if str(row.get("run_id")) == str(run_id):
                    return dict(row)
        return None

    def get_bot_run_lease(self, run_id):
        row = self.leases.get(str(run_id))
        return dict(row) if row else None

    def acquire_bot_run_lease(self, **kwargs):
        row = {
            "run_id": kwargs["run_id"],
            "bot_id": kwargs["bot_id"],
            "runner_id": kwargs["runner_id"],
            "lease_token_hash": "lease-token-hash",
            "status": "active",
            "generation": 1,
            "expires_at": "2026-05-19T00:02:00Z",
        }
        self.leases[str(kwargs["run_id"])] = {**dict(kwargs), **row}
        return row

    def release_bot_run_lease(self, **kwargs):
        self.released_leases.append(dict(kwargs))
        run_id = str(kwargs["run_id"])
        if run_id in self.leases:
            self.leases[run_id]["status"] = kwargs.get("status") or "released"
        return dict(kwargs)

    def get_latest_bot_run_lifecycle(self, bot_id):
        for row in reversed(self.lifecycle.get(str(bot_id), [])):
            return dict(row)
        return None

    def record_bot_run_lifecycle_checkpoint(self, payload):
        row = dict(payload)
        assert str(row["run_id"]) in self.runs, "lifecycle checkpoints require an existing run row"
        self.lifecycle.setdefault(str(row["bot_id"]), []).append(row)
        return row

    def update_bot_runtime_status(self, *, bot_id, run_id, status, telemetry_degraded=False):
        _ = telemetry_degraded
        if str(run_id) in self.runs:
            self.runs[str(run_id)]["status"] = status
        for bot in self.bots:
            if str(bot.get("id")) == str(bot_id):
                bot["status"] = status


class _RecordingRunner:
    def __init__(self, *, delay: float = 0.0) -> None:
        self.delay = delay
        self.starts: list[dict[str, object]] = []
        self.stops: list[dict[str, object]] = []

    def start_bot(self, *, bot, run_id):
        if self.delay:
            time.sleep(self.delay)
        self.starts.append({"bot": dict(bot), "run_id": run_id})
        return f"container-{run_id}"

    def stop_bot(self, *, bot_id, preserve_container=False, run_id=None):
        self.stops.append({"bot_id": bot_id, "preserve_container": preserve_container, "run_id": run_id})
        return None


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


def test_start_bot_same_request_id_returns_existing_run(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    runner = _RecordingRunner()
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: runner,
    )

    first = service.start_bot("bot-1", request_id="req-start-1")
    run_id = first["run_id"]
    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: {
            "status": "running",
            "running": True,
            "runtime_run_id": run_id,
            "exit_code": None,
        },
    )

    second = service.start_bot("bot-1", request_id="req-start-1")

    assert first["status"] == "started"
    assert second["status"] == "already_started"
    assert second["run_id"] == run_id
    assert second["request_id"] == "req-start-1"
    assert len(runner.starts) == 1
    assert storage.runs[run_id]["config_snapshot"]["start_request"]["request_id"] == "req-start-1"
    assert storage.lifecycle["bot-1"][0]["metadata"]["request_id"] == "req-start-1"


def test_start_observe_only_paper_run_uses_docker_runner_with_effective_snapshot():
    config = _FakeConfigService()
    config._bots[0]["status"] = "failed"
    config._bots[0]["runner_id"] = "stale-runner"
    config._bots[0]["last_run_artifact"] = {"runtime_event_stream": ["stale-heavy-artifact"]}
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    runner = _RecordingRunner()

    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: runner,
    )

    response = service.start_bot(
        "bot-1",
        request_id="req-observe-1",
        start_overrides={
            "run_type": "paper",
            "execution_behavior": "observe-only",
            "duration_seconds": 5,
            "market_data_stream_policy": {
                "reconnect_enabled": True,
                "initial_backoff_seconds": 0.5,
                "max_backoff_seconds": 30,
                "continuous_disconnect_budget_seconds": 120,
                "heartbeat_stale_seconds": 10,
            },
        },
    )

    run_id = response["run_id"]
    run = storage.runs[run_id]
    assert response["status"] == "started"
    assert len(runner.starts) == 1
    assert runner.starts[0]["run_id"] == run_id
    assert runner.starts[0]["bot"]["run_type"] == "paper"
    assert runner.starts[0]["bot"]["execution_behavior"] == "observe-only"
    assert runner.starts[0]["bot"]["duration_seconds"] == 5.0
    assert runner.starts[0]["bot"]["market_data_stream_policy"]["continuous_disconnect_budget_seconds"] == 120.0

    assert run["run_type"] == "paper"
    assert run["config_snapshot"]["execution_behavior"] == "observe-only"
    assert run["config_snapshot"]["bot"]["execution_behavior"] == "observe-only"
    assert run["config_snapshot"]["bot"]["market_data_stream_policy"]["heartbeat_stale_seconds"] == 10.0
    assert "status" not in run["config_snapshot"]["bot"]
    assert "runner_id" not in run["config_snapshot"]["bot"]
    assert "last_run_artifact" not in run["config_snapshot"]["bot"]
    assert run["config_snapshot"]["start_request"]["overrides"]["duration_seconds"] == 5.0
    assert (
        run["config_snapshot"]["start_request"]["overrides"]["market_data_stream_policy"][
            "continuous_disconnect_budget_seconds"
        ]
        == 120.0
    )
    assert storage.bots[-1]["runner_id"] == "runner-test"


def test_start_bot_passes_run_lease_to_runner():
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    runner = _RecordingRunner()
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: runner,
    )

    result = service.start_bot("bot-1", request_id="req-lease")
    run_id = str(result["run_id"])

    assert runner.starts[0]["run_id"] == run_id
    assert runner.starts[0]["bot"]["_runtime_runner_id"] == "runner-test"
    assert runner.starts[0]["bot"]["_runtime_run_lease_token"]
    assert storage.leases[run_id]["runner_id"] == "runner-test"


def test_stop_observe_only_paper_run_uses_docker_runner():
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    runner = _RecordingRunner()
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: runner,
    )
    start = service.start_bot(
        "bot-1",
        request_id="req-observe-1",
        start_overrides={"run_type": "paper", "execution_behavior": "observe-only"},
    )

    response = service.stop_bot("bot-1", run_id=start["run_id"], request_id="cancel-observe-1")

    assert response["status"] == "canceled"
    assert runner.stops == [{"bot_id": "bot-1", "preserve_container": False, "run_id": start["run_id"]}]
    phases = [entry["phase"] for entry in storage.lifecycle["bot-1"]]
    assert "cancel_requested" in phases
    assert "canceling" in phases


def test_start_bot_different_request_while_active_returns_conflict(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    runner = _RecordingRunner()
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: runner,
    )

    first = service.start_bot("bot-1", request_id="req-start-1")
    run_id = first["run_id"]
    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: {
            "status": "running",
            "running": True,
            "runtime_run_id": run_id,
            "exit_code": None,
        },
    )

    second = service.start_bot("bot-1", request_id="req-start-2")

    assert second["status"] == "conflict"
    assert second["active_run_id"] == run_id
    assert second["reason_code"] == "active_run_conflict"
    assert len(runner.starts) == 1
    assert runner.stops == []


def test_start_bot_after_terminal_run_starts_new_run(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    runner = _RecordingRunner()
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: runner,
    )

    first = service.start_bot("bot-1", request_id="req-start-1")
    container_state = {
        "status": "running",
        "running": True,
        "runtime_run_id": first["run_id"],
        "exit_code": None,
    }
    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: dict(container_state),
    )
    service.stop_bot("bot-1", run_id=first["run_id"], request_id="req-cancel-1")
    container_state.update({"status": "missing", "running": False, "runtime_run_id": None})

    second = service.start_bot("bot-1", request_id="req-start-2")

    assert second["status"] == "started"
    assert second["run_id"] != first["run_id"]
    assert len(runner.starts) == 2


def test_concurrent_start_attempts_create_one_active_run(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    runner = _RecordingRunner(delay=0.01)

    def _inspect(_bot_id):
        run_id = storage.get_latest_bot_runtime_run_id("bot-1")
        return {
            "status": "running" if run_id else "missing",
            "running": bool(run_id),
            "runtime_run_id": run_id,
            "exit_code": None,
        }

    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.inspect_bot_container",
        _inspect,
    )
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: runner,
    )

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: service.start_bot("bot-1", request_id="req-concurrent"), range(4)))

    assert [result["status"] for result in results].count("started") == 1
    assert [result["status"] for result in results].count("already_started") == 3
    assert len({result["run_id"] for result in results}) == 1
    assert len(runner.starts) == 1


def test_stale_active_run_reconciles_before_new_start(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    storage.runs["run-stale"] = {"run_id": "run-stale", "bot_id": "bot-1", "status": "running"}
    storage.lifecycle["bot-1"] = [
        {
            "bot_id": "bot-1",
            "run_id": "run-stale",
            "phase": "live",
            "status": "running",
            "owner": "runtime",
            "message": "live",
            "metadata": {},
            "failure": {},
        }
    ]
    runner = _RecordingRunner()
    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: {"status": "missing", "running": False, "runtime_run_id": None, "exit_code": None},
    )
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: runner,
    )

    response = service.start_bot("bot-1", request_id="req-after-stale")

    stale_events = [row for row in storage.lifecycle["bot-1"] if row["run_id"] == "run-stale"]
    assert stale_events[-1]["phase"] == "crashed"
    assert stale_events[-1]["metadata"]["reason_code"] == "container_missing"
    assert response["status"] == "started"
    assert response["run_id"] != "run-stale"


@pytest.mark.parametrize(
    ("container_state", "expected_phase", "expected_reason"),
    [
        ({"status": "exited", "running": False, "runtime_run_id": "run-stale", "exit_code": 0}, "completed", "container_exited_zero"),
        ({"status": "exited", "running": False, "runtime_run_id": "run-stale", "exit_code": 2}, "crashed", "container_exited_nonzero"),
    ],
)
def test_active_run_container_exit_reconciles_to_terminal(monkeypatch, container_state, expected_phase, expected_reason):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    storage.runs["run-stale"] = {"run_id": "run-stale", "bot_id": "bot-1", "status": "running"}
    storage.lifecycle["bot-1"] = [
        {
            "bot_id": "bot-1",
            "run_id": "run-stale",
            "phase": "live",
            "status": "running",
            "owner": "runtime",
            "message": "live",
            "metadata": {},
            "failure": {},
        }
    ]
    runner = _RecordingRunner()
    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: dict(container_state),
    )
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: runner,
    )

    service.start_bot("bot-1", request_id="req-reconcile")

    stale_events = [row for row in storage.lifecycle["bot-1"] if row["run_id"] == "run-stale"]
    assert stale_events[-1]["phase"] == expected_phase
    assert stale_events[-1]["metadata"]["reason_code"] == expected_reason


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

        def stop_bot(self, *, bot_id, preserve_container=False, run_id=None):
            stop_calls.append({"bot_id": bot_id, "preserve_container": preserve_container, "run_id": run_id})
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

    response = service.stop_bot("bot-1", preserve_container=True, request_id="cancel-1")
    projected = response["bot"]

    assert response["status"] == "canceled"
    assert response["request_id"] == "cancel-1"
    assert stop_calls == [{"bot_id": "bot-1", "preserve_container": True, "run_id": "run-1"}]
    assert [row["phase"] for row in storage.lifecycle["bot-1"][-3:]] == ["cancel_requested", "canceling", "canceled"]
    assert storage.lifecycle["bot-1"][-1]["phase"] == "canceled"
    assert storage.lifecycle["bot-1"][-1]["metadata"]["terminal_actor"] == "platform_cancel"
    assert storage.lifecycle["bot-1"][-1]["metadata"]["preserve_container"] is True
    assert projected["lifecycle"]["container"]["status"] == "exited"


def test_cancel_terminal_run_is_idempotent_and_preserves_history(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    storage = _FakeStorage()
    storage.runs["run-1"] = {
        "run_id": "run-1",
        "bot_id": "bot-1",
        "status": "completed",
        "summary": {"net_pnl": 1.0},
    }
    storage.lifecycle["bot-1"] = [
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "phase": "completed",
            "status": "completed",
            "owner": "container",
            "message": "completed",
            "metadata": {"artifact": "kept"},
            "failure": {},
        }
    ]
    runner = _RecordingRunner()
    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: {"status": "missing", "running": False, "runtime_run_id": None, "exit_code": None},
    )
    service = BotRuntimeControlService(
        config,
        stream,
        storage=storage,
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: runner,
    )

    response = service.stop_bot("bot-1", run_id="run-1", request_id="req-cancel-done")

    assert response["status"] == "already_terminal"
    assert response["run_id"] == "run-1"
    assert runner.stops == []
    assert storage.runs["run-1"]["summary"] == {"net_pnl": 1.0}
    assert storage.lifecycle["bot-1"] == [
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "phase": "completed",
            "status": "completed",
            "owner": "container",
            "message": "completed",
            "metadata": {"artifact": "kept"},
            "failure": {},
        }
    ]


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
    assert initial["bots"][0]["active_run_id"] is None
    assert initial["bots"][0]["latest_run_id"] == "run-1"
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
