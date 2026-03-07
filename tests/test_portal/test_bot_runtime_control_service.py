from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots.runtime_control_service import BotRuntimeControlService


class _FakeConfigService:
    def __init__(self) -> None:
        self._bots = [
            {
                "id": "bot-1",
                "strategy_id": "strategy-1",
                "wallet_config": {"balances": {"USDC": 100.0}},
                "snapshot_interval_ms": 1000,
                "status": "idle",
            }
        ]

    def list_bots(self):
        return [dict(bot) for bot in self._bots]

    def validate_wallet_config(self, value):
        return value

    def validate_strategy_id(self, value):
        return str(value)

    def validate_backtest_window(self, _bot):
        return None

    def validate_strategy_existence(self, _bot):
        return None

    def validate_instrument_policy(self, _bot):
        return None

    def validate_runtime_readiness(self, _bot):
        return None


class _FakeStreamManager:
    def __init__(self) -> None:
        self.messages = []

    def broadcast(self, event, payload):
        self.messages.append((event, payload))


def test_start_bot_persists_error_and_broadcasts_runtime_when_runner_fails(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    service = BotRuntimeControlService(config, stream)
    persisted = []

    class _FailingRunner:
        def start_bot(self, *, bot):
            raise RuntimeError("container boot failed")

    monkeypatch.setenv("BOT_RUNTIME_TARGET", "docker")
    monkeypatch.setattr(
        "portal.backend.service.bots.runtime_control_service.DockerBotRunner.from_env",
        lambda: _FailingRunner(),
    )
    monkeypatch.setattr(
        "portal.backend.service.storage.storage.upsert_bot",
        lambda payload: persisted.append(dict(payload)),
    )

    with pytest.raises(RuntimeError, match="container boot failed"):
        service.start_bot("bot-1")

    assert persisted, "expected error state to be persisted"
    saved = persisted[-1]
    assert saved["status"] == "error"
    assert saved.get("runner_id") is None
    assert "container boot failed" in str(saved.get("last_run_artifact", {}).get("error", {}).get("message", ""))

    assert stream.messages, "expected bot stream broadcast"
    event, payload = stream.messages[-1]
    assert event == "bot"
    assert payload["bot"]["id"] == "bot-1"
    assert payload["bot"]["runtime"]["status"] == "error"
    assert "container boot failed" in payload["bot"]["runtime"]["error"]["message"]


def test_start_bot_rejects_unknown_runtime_target(monkeypatch):
    config = _FakeConfigService()
    stream = _FakeStreamManager()
    service = BotRuntimeControlService(config, stream)
    monkeypatch.setenv("BOT_RUNTIME_TARGET", "vps")

    with pytest.raises(RuntimeError, match="Unsupported bot runtime target"):
        service.start_bot("bot-1")
