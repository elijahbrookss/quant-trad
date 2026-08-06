from __future__ import annotations

import pytest

from engines.bot_runtime.core.execution_assumptions import legacy_execution_assumptions
from engines.bot_runtime.core.execution_context import (
    build_execution_context_bundle,
    resolve_execution_context,
)
from engines.bot_runtime.core.execution_profile import compile_series_execution_profile
from engines.bot_runtime.strategy.models import Strategy
import portal.backend.service.bots.startup_service as startup_mod
from portal.backend.service.bots.startup_lifecycle import BotLifecyclePhase
from portal.backend.service.bots.startup_service import BotStartupOrchestrator


@pytest.fixture(autouse=True)
def _disable_lifecycle_bridge(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(startup_mod, "emit_lifecycle_event", lambda _payload: None)
    monkeypatch.setattr(
        startup_mod,
        "validate_backtest_dataset",
        lambda **_kwargs: {
            "dataset_id": "mds_test",
            "dataset_hash": "dataset-hash",
            "dataset_contract_version": "market_dataset.v1",
            "strategy_hash": "compiled-strategy-hash",
            "quality": {"status": "ready", "evidence_count": 0},
        },
    )


def _strategy() -> Strategy:
    return Strategy(
        id="strategy-1",
        name="Strategy 1",
        timeframe="1m",
        datasource="demo",
        exchange="paper",
        atm_template_id=None,
        atm_template={},
        risk_config={},
        indicator_links=[],
        instrument_links=[],
    )


def _resolved_execution_context_bundle() -> dict:
    instrument = {
        "id": "instrument-btc-usd",
        "symbol": "BTC-USD",
        "instrument_type": "spot",
        "datasource": "demo",
        "exchange": "paper",
        "tick_size": 0.01,
        "contract_size": 1.0,
        "tick_value": 0.01,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "min_order_size": 0.001,
        "qty_step": 0.001,
        "maker_fee_rate": 0.001,
        "taker_fee_rate": 0.002,
        "fee_source": "startup_test_fixture",
        "fee_schedule_version": "startup_test_fees.v1",
    }
    profile = compile_series_execution_profile(instrument, execution_semantics="spot")
    context = resolve_execution_context(
        profile,
        legacy_execution_assumptions(),
        instrument_payload=instrument,
        source="startup_test_fixture",
    )
    return build_execution_context_bundle([context]).to_dict()


class _FakeConfig:
    def __init__(self) -> None:
        self._bot = {
            "id": "bot-1",
            "dataset_id": "mds_test",
            "backtest_start": "2026-01-01T00:00:00Z",
            "backtest_end": "2026-01-02T00:00:00Z",
            "name": "Bot 1",
            "strategy_id": "strategy-1",
            "wallet_config": {"balances": {"USDC": 100.0}},
            "snapshot_interval_ms": 1000,
            "run_type": "backtest",
        }

    def get_bot(self, bot_id):
        if bot_id != self._bot["id"]:
            raise KeyError(bot_id)
        return dict(self._bot)

    def list_bots(self):
        return [dict(self._bot)]

    def prepare_startup_artifacts(self, bot):
        assert bot["id"] == "bot-1"
        context_bundle = _resolved_execution_context_bundle()
        return {
            "strategy_id": "strategy-1",
            "wallet_config": {"balances": {"USDC": 100.0}},
            "strategy": _strategy(),
            "runtime_readiness": {
                "symbols": ["BTCUSDT", "ETHUSDT"],
                "profiles": [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}],
                "resolved_execution_context_bundle": context_bundle,
            },
            "resolved_execution_context_bundle": context_bundle,
        }


class _FakeStorage:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.runs = []
        self.leases = []
        self.released_leases = []
        self.lifecycle = []
        self._next_lifecycle_seq = 1

    def acquire_bot_run_lease(self, **kwargs):
        self.order.append("acquire_bot_run_lease")
        lease = {
            "run_id": kwargs["run_id"],
            "bot_id": kwargs["bot_id"],
            "runner_id": kwargs["runner_id"],
            "lease_token_hash": "lease-token-hash",
            "status": "active",
            "generation": 1,
            "expires_at": "2026-05-19T00:02:00Z",
        }
        self.leases.append({**dict(kwargs), **lease})
        return lease

    def release_bot_run_lease(self, **kwargs):
        self.order.append("release_bot_run_lease")
        self.released_leases.append(dict(kwargs))
        return dict(kwargs)

    def upsert_bot_run(self, payload):
        self.order.append("upsert_bot_run")
        self.runs.append(dict(payload))
        return dict(payload)

    def record_bot_run_lifecycle_checkpoint(self, payload):
        self.order.append(f"phase:{payload['phase']}")
        persisted = dict(payload)
        persisted["seq"] = int(persisted.get("seq") or self._next_lifecycle_seq)
        self._next_lifecycle_seq = max(self._next_lifecycle_seq, persisted["seq"] + 1)
        self.lifecycle.append(dict(persisted))
        return persisted

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
    assert ctx.run_lease_token
    assert runner.calls[0]["run_id"] == ctx.run_id
    assert runner.calls[0]["bot"]["_runtime_run_lease_token"] == ctx.run_lease_token
    assert runner.calls[0]["bot"]["_runtime_runner_id"] == "runner-test"
    assert order.index("upsert_bot_run") < order.index("runner.start_bot")
    assert order.index("acquire_bot_run_lease") < order.index("runner.start_bot")
    assert order.index("upsert_bot_run") < order.index(f"phase:{BotLifecyclePhase.START_REQUESTED.value}")
    configured_run = next(row for row in reversed(storage.runs) if row.get("strategy_hash"))
    assert configured_run["strategy_hash"] == "compiled-strategy-hash"
    assert configured_run["config_snapshot"]["run_strategy_snapshot"]["strategy_hash"] == (
        "compiled-strategy-hash"
    )
    pinned_bundle = configured_run["config_snapshot"]["resolved_execution_context_bundle"]
    assert pinned_bundle["bundle_hash"]
    assert len(pinned_bundle["contexts"]) == 1
    assert runner.calls[0]["bot"]["resolved_execution_context_bundle"] == pinned_bundle
    dependency_phase = next(
        row
        for row in storage.lifecycle
        if row["phase"] == BotLifecyclePhase.RESOLVING_RUNTIME_DEPENDENCIES.value
    )
    assert dependency_phase["metadata"]["resolved_execution_context_bundle_hash"] == (
        pinned_bundle["bundle_hash"]
    )
    assert dependency_phase["metadata"]["resolved_execution_context_count"] == 1
    assert [row["phase"] for row in storage.lifecycle[:5]] == [
        BotLifecyclePhase.START_REQUESTED.value,
        BotLifecyclePhase.VALIDATING_CONFIGURATION.value,
        BotLifecyclePhase.RESOLVING_STRATEGY.value,
        BotLifecyclePhase.RESOLVING_RUNTIME_DEPENDENCIES.value,
        BotLifecyclePhase.PREPARING_RUN.value,
    ]
    assert storage.lifecycle[-1]["phase"] == BotLifecyclePhase.AWAITING_CONTAINER_BOOT.value
    assert storage.lifecycle[0]["metadata"]["run_lease"]["runner_id"] == "runner-test"
    assert {row["status"] for row in storage.lifecycle} == {"starting"}


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
    assert storage.released_leases[-1]["status"] == "released"
    assert storage.lifecycle[-1]["phase"] == BotLifecyclePhase.STARTUP_FAILED.value
    assert "docker launch failed" in storage.lifecycle[-1]["message"]
    assert storage.lifecycle[-1]["status"] == "startup_failed"


def test_startup_orchestrator_rejects_untyped_strategy_artifact() -> None:
    order: list[str] = []
    storage = _FakeStorage(order)

    class _UntypedConfig(_FakeConfig):
        def prepare_startup_artifacts(self, bot):
            artifacts = super().prepare_startup_artifacts(bot)
            artifacts["strategy"] = {"id": "strategy-1"}
            return artifacts

    orchestrator = BotStartupOrchestrator(
        config_service=_UntypedConfig(),
        storage=storage,
        runner=_FakeRunner(order),
        watchdog=_FakeWatchdog(order),
    )

    with pytest.raises(TypeError, match="typed Strategy"):
        orchestrator.start_bot("bot-1")

    assert storage.lifecycle[-1]["phase"] == BotLifecyclePhase.STARTUP_FAILED.value
