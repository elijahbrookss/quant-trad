from __future__ import annotations

from types import SimpleNamespace

from portal.backend.service.bots.startup_lifecycle import BotLifecyclePhase
from portal.backend.service.bots.startup_service import BotStartupOrchestrator


class _FakeConfig:
    def __init__(self) -> None:
        self._bot = {
            "id": "bot-1",
            "name": "Bot 1",
            "strategy_id": "strategy-1",
            "wallet_config": {"balances": {"USDC": 100.0}},
            "snapshot_interval_ms": 1000,
            "run_type": "backtest",
            "status": "idle",
        }

    def list_bots(self):
        return [dict(self._bot)]

    def prepare_startup_artifacts(self, bot):
        assert bot["id"] == "bot-1"
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
                "symbols": ["BTCUSDT", "ETHUSDT"],
                "profiles": [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}],
            },
        }


class _FakeStorage:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.runs = []
        self.lifecycle = []
        self.bots = []

    def upsert_bot(self, payload):
        self.order.append("upsert_bot")
        self.bots.append(dict(payload))

    def upsert_bot_run(self, payload):
        self.order.append("upsert_bot_run")
        self.runs.append(dict(payload))
        return dict(payload)

    def record_bot_run_lifecycle_checkpoint(self, payload):
        self.order.append(f"phase:{payload['phase']}")
        self.lifecycle.append(dict(payload))
        return dict(payload)

    def update_bot_runtime_status(self, *, bot_id, run_id, status, telemetry_degraded=False):
        _ = bot_id, run_id, status, telemetry_degraded
        self.order.append(f"status:{status}")


class _FakeRunner:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.calls = []

    def start_bot(self, *, bot, run_id):
        self.order.append("runner.start_bot")
        self.calls.append({"bot": dict(bot), "run_id": run_id})
        return "container-123"


class _FakeWatchdog:
    runner_id = "runner-test"

    def __init__(self, order: list[str]) -> None:
        self.order = order

    def register_bot(self, bot_id: str):
        self.order.append(f"watchdog.register:{bot_id}")


def test_startup_orchestrator_creates_run_before_container_launch():
    order: list[str] = []
    storage = _FakeStorage(order)
    runner = _FakeRunner(order)
    orchestrator = BotStartupOrchestrator(
        config_service=_FakeConfig(),
        storage=storage,
        runner=runner,
        watchdog=_FakeWatchdog(order),
    )

    ctx = orchestrator.start_bot("bot-1")

    assert ctx.run_id
    assert runner.calls[0]["run_id"] == ctx.run_id
    assert order.index("upsert_bot_run") < order.index("runner.start_bot")
    assert order.index("upsert_bot_run") < order.index(f"phase:{BotLifecyclePhase.START_REQUESTED.value}")
    assert [row["phase"] for row in storage.lifecycle[:5]] == [
        BotLifecyclePhase.START_REQUESTED.value,
        BotLifecyclePhase.VALIDATING_CONFIGURATION.value,
        BotLifecyclePhase.RESOLVING_STRATEGY.value,
        BotLifecyclePhase.RESOLVING_RUNTIME_DEPENDENCIES.value,
        BotLifecyclePhase.PREPARING_RUN.value,
    ]
    assert storage.lifecycle[-1]["phase"] == BotLifecyclePhase.AWAITING_CONTAINER_BOOT.value
    assert storage.bots[-1]["status"] == "starting"
    assert storage.bots[-1]["last_run_artifact"]["startup"]["run_id"] == ctx.run_id


def test_startup_orchestrator_persists_startup_failed_phase():
    order: list[str] = []
    storage = _FakeStorage(order)

    class _FailingRunner(_FakeRunner):
        def start_bot(self, *, bot, run_id):
            self.order.append("runner.start_bot")
            self.calls.append({"bot": dict(bot), "run_id": run_id})
            raise RuntimeError("docker launch failed")

    orchestrator = BotStartupOrchestrator(
        config_service=_FakeConfig(),
        storage=storage,
        runner=_FailingRunner(order),
        watchdog=_FakeWatchdog(order),
    )

    try:
        orchestrator.start_bot("bot-1")
    except RuntimeError as exc:
        assert "docker launch failed" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected startup failure")

    assert storage.runs, "run row should be persisted before failure"
    assert storage.lifecycle[-1]["phase"] == BotLifecyclePhase.STARTUP_FAILED.value
    assert "docker launch failed" in storage.lifecycle[-1]["message"]
    assert storage.bots[-1]["status"] == "startup_failed"
