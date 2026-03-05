from __future__ import annotations

import sys
import time
import types

# Stub heavy runtime dependencies so we can import emitter in isolation.
db_session_mod = types.ModuleType("portal.backend.db.session")
db_session_mod.db = types.SimpleNamespace(reset_for_fork=lambda: None)
sys.modules["portal.backend.db.session"] = db_session_mod

storage_mod = types.ModuleType("portal.backend.service.storage.storage")
storage_mod.list_bot_runtime_events = lambda *args, **kwargs: []
storage_mod.load_bots = lambda *args, **kwargs: []
storage_mod.record_bot_run_step = lambda *args, **kwargs: None
storage_mod.update_bot_runtime_status = lambda *args, **kwargs: None
sys.modules["portal.backend.service.storage.storage"] = storage_mod

bot_runtime_mod = types.ModuleType("portal.backend.service.bots.bot_runtime")
bot_runtime_mod.BotRuntime = object
sys.modules["portal.backend.service.bots.bot_runtime"] = bot_runtime_mod

strategy_loader_mod = types.ModuleType("portal.backend.service.bots.bot_runtime.strategy.strategy_loader")
strategy_loader_mod.StrategyLoader = types.SimpleNamespace(fetch_strategy=lambda strategy_id: types.SimpleNamespace(instrument_links=[]))
sys.modules["portal.backend.service.bots.bot_runtime.strategy.strategy_loader"] = strategy_loader_mod

runtime_events_mod = types.ModuleType("engines.bot_runtime.core.runtime_events")
runtime_events_mod.RuntimeEventName = types.SimpleNamespace(WALLET_INITIALIZED="WALLET_INITIALIZED")
runtime_events_mod.build_correlation_id = lambda **kwargs: "corr"
runtime_events_mod.new_runtime_event = lambda **kwargs: types.SimpleNamespace(serialize=lambda: kwargs)
sys.modules["engines.bot_runtime.core.runtime_events"] = runtime_events_mod

from portal.backend.service.bots import container_runtime


def test_telemetry_emitter_send_message_is_non_blocking(monkeypatch):
    emitter = container_runtime._TelemetryEmitter("ws://example")

    def _slow_deliver(self, message: str) -> bool:  # noqa: ARG001
        time.sleep(0.2)
        return True

    monkeypatch.setattr(container_runtime._TelemetryEmitter, "_deliver_message", _slow_deliver)

    started = time.perf_counter()
    for _ in range(10):
        accepted = emitter.send_message('{"x":1}')
        assert accepted is True
    elapsed = time.perf_counter() - started
    emitter.close()

    assert elapsed < 0.05
