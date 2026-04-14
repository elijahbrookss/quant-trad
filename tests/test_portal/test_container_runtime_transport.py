from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import pytest

pytest.importorskip("sqlalchemy")

import portal.backend.service.bots.container_runtime as runtime_mod
import portal.backend.service.bots.container_runtime_telemetry as telemetry_mod
from portal.backend.service.bots.startup_lifecycle import BotLifecyclePhase


def _wait_until(predicate, *, timeout_s: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("timed out waiting for background transport worker")


class _FakeSyncWebSocket:
    def __init__(self, *, fail_first_send: bool = False) -> None:
        self.sent: list[str] = []
        self.close_calls = 0
        self._fail_first_send = fail_first_send

    def send(self, message: str) -> None:
        if self._fail_first_send:
            self._fail_first_send = False
            raise RuntimeError("simulated transport failure")
        self.sent.append(str(message))

    def close(self) -> None:
        self.close_calls += 1


def test_telemetry_emitter_reuses_single_websocket_for_multiple_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    connections: list[_FakeSyncWebSocket] = []

    def _connect(_url: str, *, open_timeout: int, close_timeout: int) -> _FakeSyncWebSocket:
        assert open_timeout == 2
        assert close_timeout == 1
        ws = _FakeSyncWebSocket()
        connections.append(ws)
        return ws

    monkeypatch.setattr(telemetry_mod, "sync_connect", _connect)

    emitter = telemetry_mod.TelemetryEmitter(
        "ws://example.test/telemetry",
        queue_max=8,
        queue_timeout_ms=50,
        retry_ms=25,
    )
    try:
        assert emitter.send({"kind": "botlens_runtime_bootstrap_facts", "bot_id": "bot-1", "run_id": "run-1", "run_seq": 1})
        assert emitter.send({"kind": "botlens_runtime_facts", "bot_id": "bot-1", "run_id": "run-1", "run_seq": 2})

        _wait_until(lambda: len(connections) == 1 and len(connections[0].sent) == 2)

        assert len(connections) == 1
        assert [json.loads(message)["run_seq"] for message in connections[0].sent] == [1, 2]
    finally:
        emitter.close()

    assert connections[0].close_calls == 1


def test_telemetry_emitter_reconnects_after_send_failure_without_dropping_queued_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[_FakeSyncWebSocket] = []

    def _connect(_url: str, *, open_timeout: int, close_timeout: int) -> _FakeSyncWebSocket:
        assert open_timeout == 2
        assert close_timeout == 1
        ws = _FakeSyncWebSocket(fail_first_send=len(connections) == 0)
        connections.append(ws)
        return ws

    monkeypatch.setattr(telemetry_mod, "sync_connect", _connect)

    emitter = telemetry_mod.TelemetryEmitter(
        "ws://example.test/telemetry",
        queue_max=8,
        queue_timeout_ms=50,
        retry_ms=10,
    )
    try:
        assert emitter.send({"kind": "botlens_runtime_facts", "bot_id": "bot-1", "run_id": "run-1", "run_seq": 7})

        _wait_until(lambda: len(connections) == 2 and len(connections[1].sent) == 1)

        assert len(connections) == 2
        assert connections[0].close_calls == 1
        assert json.loads(connections[1].sent[0])["run_seq"] == 7
    finally:
        emitter.close()


def test_notify_backend_lifecycle_event_prefers_persistent_sender(monkeypatch: pytest.MonkeyPatch) -> None:
    sender = MagicMock()
    sender.send.return_value = True
    ephemeral = MagicMock(return_value=True)

    monkeypatch.setattr(runtime_mod, "emit_telemetry_ephemeral_message", ephemeral)

    delivered = runtime_mod._notify_backend_lifecycle_event(
        lifecycle_state={
            "bot_id": "bot-1",
            "run_id": "run-1",
            "seq": 3,
            "phase": "live",
            "status": "running",
            "checkpoint_at": "2026-01-01T00:00:00Z",
        },
        telemetry_sender=sender,
    )

    assert delivered is True
    sender.send.assert_called_once()
    ephemeral.assert_not_called()
    assert sender.send.call_args.args[0]["kind"] == "botlens_lifecycle_event"


def test_handle_runtime_facts_event_emits_live_lifecycle_only_once(monkeypatch: pytest.MonkeyPatch) -> None:
    sender = MagicMock()
    sender.send.return_value = True
    persisted: list[dict] = []

    monkeypatch.setattr(runtime_mod, "_next_run_event_seq", lambda _proxy: 1)
    monkeypatch.setattr(runtime_mod, "_persist_lifecycle_phase", lambda **kwargs: persisted.append(dict(kwargs)) or {"seq": len(persisted)})

    ctx = runtime_mod.ContainerStartupContext(
        bot_id="bot-1",
        run_id="run-1",
        bot={},
        runtime_bot_config={},
        strategy_id="strategy-1",
        symbols=["BTC"],
        symbol_shards=[["BTC"]],
        wallet_config={},
        manager=MagicMock(),
        shared_wallet_proxy={},
        telemetry_sender=sender,
        worker_symbols={"worker-1": ["BTC"]},
    )

    event = {
        "worker_id": "worker-1",
        "series_key": "instrument-btc|1m",
        "bridge_session_id": "session-1",
        "bridge_seq": 1,
        "known_at": "2026-01-01T00:00:00Z",
        "event_time": "2026-01-01T00:00:00Z",
        "facts": [{"fact_type": "candle_upserted"}],
    }

    runtime_mod._handle_runtime_facts_event(ctx, event, telemetry_sender=sender)
    runtime_mod._handle_runtime_facts_event(ctx, {**event, "bridge_seq": 2}, telemetry_sender=sender)

    live_calls = [call for call in persisted if call["phase"] == BotLifecyclePhase.LIVE.value]

    assert sender.send.call_count == 2
    assert len(live_calls) == 1
    assert ctx.startup_live_emitted is True
