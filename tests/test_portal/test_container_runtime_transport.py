from __future__ import annotations

import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("sqlalchemy")

import portal.backend.service.bots.container_runtime as runtime_mod
import portal.backend.service.bots.container_runtime_telemetry as telemetry_mod
from portal.backend.service.bots.botlens_runtime_state import BotLensRuntimeState
from portal.backend.service.bots.startup_lifecycle import BotLifecyclePhase


def _wait_until(predicate, *, timeout_s: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("timed out waiting for background transport worker")


def test_persist_lifecycle_phase_does_not_write_status_for_non_terminal_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        runtime_mod,
        "record_bot_run_lifecycle_checkpoint",
        lambda payload: {**dict(payload), "status": "running"},
    )
    monkeypatch.setattr(
        runtime_mod,
        "update_bot_runtime_status",
        lambda **kwargs: status_calls.append(dict(kwargs)),
    )
    monkeypatch.setattr(runtime_mod, "_notify_backend_lifecycle_event", lambda **kwargs: True)

    result = runtime_mod._persist_lifecycle_phase(
        bot_id="bot-1",
        run_id="run-1",
        phase=BotLifecyclePhase.LIVE.value,
        owner="runtime",
        message="runtime is live",
        status="running",
        metadata={},
    )

    assert result["status"] == "running"
    assert status_calls == []


def test_persist_lifecycle_phase_writes_status_for_terminal_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        runtime_mod,
        "record_bot_run_lifecycle_checkpoint",
        lambda payload: {**dict(payload), "status": "completed"},
    )
    monkeypatch.setattr(
        runtime_mod,
        "update_bot_runtime_status",
        lambda **kwargs: status_calls.append(dict(kwargs)),
    )
    monkeypatch.setattr(runtime_mod, "_notify_backend_lifecycle_event", lambda **kwargs: True)

    result = runtime_mod._persist_lifecycle_phase(
        bot_id="bot-1",
        run_id="run-1",
        phase=BotLifecyclePhase.COMPLETED.value,
        owner="runtime",
        message="runtime completed",
        status="completed",
        metadata={},
    )

    assert result["status"] == "completed"
    assert status_calls == [
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "status": "completed",
            "telemetry_degraded": False,
        }
    ]


def test_persist_lifecycle_phase_writes_status_for_startup_failed_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        runtime_mod,
        "record_bot_run_lifecycle_checkpoint",
        lambda payload: {**dict(payload), "status": "startup_failed"},
    )
    monkeypatch.setattr(
        runtime_mod,
        "update_bot_runtime_status",
        lambda **kwargs: status_calls.append(dict(kwargs)),
    )
    monkeypatch.setattr(runtime_mod, "_notify_backend_lifecycle_event", lambda **kwargs: True)

    result = runtime_mod._persist_lifecycle_phase(
        bot_id="bot-1",
        run_id="run-1",
        phase=BotLifecyclePhase.STARTUP_FAILED.value,
        owner="runtime",
        message="startup failed before live",
        status="startup_failed",
        metadata={},
    )

    assert result["status"] == "startup_failed"
    assert status_calls == [
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "status": "startup_failed",
            "telemetry_degraded": False,
        }
    ]


def test_handle_worker_terminal_event_records_explicit_terminal_statuses() -> None:
    ctx = runtime_mod.ContainerStartupContext(
        bot_id="bot-1",
        run_id="run-1",
        bot={},
        runtime_bot_config={},
        strategy_id="strategy-1",
        symbols=["BTC", "ETH"],
        symbol_shards=[["BTC"], ["ETH"]],
        wallet_config={},
        manager=MagicMock(),
        shared_wallet_proxy={},
        worker_symbols={"worker-1": ["BTC"]},
    )

    runtime_mod._handle_worker_terminal_event(
        ctx,
        {
            "worker_id": "worker-1",
            "symbols": ["BTC"],
            "status": "completed",
            "message": "Worker runtime exited with terminal status completed.",
        },
    )

    assert ctx.reported_worker_terminal_statuses == {"worker-1": "completed"}
    assert ctx.series_states["BTC"]["status"] == "completed"


def test_series_worker_reports_structured_startup_error_before_process_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    emitted_events: list[dict[str, object]] = []

    class _FakeEventQueue:
        def put(self, payload, timeout=None):  # noqa: ANN001
            del timeout
            emitted_events.append(dict(payload))

    class _FakeRuntime:
        def __init__(self, *, bot_id, config, deps):  # noqa: ANN001
            del bot_id, config, deps

        def reset_if_finished(self) -> None:
            return None

        def warm_up(self) -> None:
            raise ValueError("run context is required before canonical BotLens fact append")

        def snapshot(self) -> dict[str, object]:
            return {"status": "idle"}

    monkeypatch.setattr(runtime_mod.db, "reset_for_fork", lambda: None)
    monkeypatch.setattr(runtime_mod, "BotRuntime", _FakeRuntime)
    monkeypatch.setattr(runtime_mod, "build_bot_runtime_deps", lambda: object())

    runtime_mod._series_worker(
        run_id="run-1",
        bot_id="bot-1",
        worker_id="worker-1",
        strategy_id="strategy-1",
        symbols=["BIP-20DEC30-CDE"],
        bot_config={},
        shared_wallet_proxy={},
        event_queue=_FakeEventQueue(),
        control_queue=_FakeEventQueue(),
    )

    assert emitted_events[0]["kind"] == "worker_phase"
    assert emitted_events[0]["phase"] == BotLifecyclePhase.WARMING_UP_RUNTIME.value
    assert emitted_events[1]["kind"] == "worker_error"
    assert emitted_events[1]["error"] == "run context is required before canonical BotLens fact append"
    assert emitted_events[1]["exception_type"] == "ValueError"
    assert "run context is required before canonical BotLens fact append" in str(emitted_events[1]["traceback"])
    assert emitted_events[2] == {
        "kind": "worker_terminal",
        "worker_id": "worker-1",
        "symbols": ["BIP-20DEC30-CDE"],
        "status": "error",
        "message": "Worker runtime exited with terminal status error.",
        "event_time": emitted_events[2]["event_time"],
    }


def test_supervise_startup_and_runtime_shuts_down_manager_after_final_lifecycle_persist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call_order: list[str] = []
    manager = MagicMock()
    manager.shutdown.side_effect = lambda: call_order.append("shutdown")

    monkeypatch.setattr(
        runtime_mod,
        "terminal_status_after_supervision",
        lambda **kwargs: (BotLifecyclePhase.STOPPED.value, "stopped"),
    )
    monkeypatch.setattr(
        runtime_mod,
        "finalize_run_artifact_bundle_from_workers",
        lambda **kwargs: call_order.append("finalize"),
    )
    monkeypatch.setattr(
        runtime_mod,
        "_final_terminal_reason",
        lambda ctx, final_phase, final_status: ("container", "container_runtime", "container supervision stopped"),
    )
    monkeypatch.setattr(runtime_mod, "_transition_for_phase", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        runtime_mod,
        "_persist_lifecycle_phase",
        lambda **kwargs: call_order.append("persist") or {},
    )

    ctx = runtime_mod.ContainerStartupContext(
        bot_id="bot-1",
        run_id="run-1",
        bot={},
        runtime_bot_config={},
        strategy_id="strategy-1",
        symbols=[],
        symbol_shards=[],
        wallet_config={},
        manager=manager,
        shared_wallet_proxy={},
        telemetry_sender=MagicMock(),
        children={},
        worker_symbols={},
    )

    runtime_mod.supervise_startup_and_runtime(ctx)

    assert call_order == ["finalize", "persist", "shutdown"]


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


def _large_runtime_facts_payload(
    *,
    bridge_session_id: str = "session-1",
    progress_state: str = "progressing",
    last_useful_progress_at: str = "2026-01-01T00:00:00Z",
) -> dict:
    return {
        "kind": "botlens_runtime_facts",
        "bot_id": "bot-1",
        "run_id": "run-1",
        "series_key": "instrument-btc|1m",
        "bridge_session_id": bridge_session_id,
        "facts": [
            {
                "fact_type": "runtime_state_observed",
                "runtime": {
                    "status": "running",
                    "runtime_state": "live",
                    "progress_state": progress_state,
                    "last_useful_progress_at": last_useful_progress_at,
                },
            },
            {
                "fact_type": "diagnostic_recorded",
                "series_key": "instrument-btc|1m",
                "log": {
                    "id": "diag-1",
                    "level": "INFO",
                    "message": "x" * (70 * 1024),
                },
            },
        ],
    }


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


def test_telemetry_emitter_suppresses_identical_bootstrap_payloads_in_same_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    payload = {
        "kind": "botlens_runtime_bootstrap_facts",
        "bot_id": "bot-1",
        "run_id": "run-1",
        "series_key": "instrument-btc|1m",
        "bridge_session_id": "session-1",
        "run_seq": 1,
        "facts": [{"fact_type": "series_state_observed", "series_key": "instrument-btc|1m"}],
    }
    try:
        assert emitter.send(payload) is True
        assert emitter.send({**payload, "run_seq": 2, "bridge_session_id": "session-1"}) is True

        _wait_until(lambda: len(connections) == 1 and len(connections[0].sent) == 1)

        assert len(connections[0].sent) == 1
        assert json.loads(connections[0].sent[0])["run_seq"] == 1
    finally:
        emitter.close()


def test_telemetry_emitter_allows_identical_bootstrap_payloads_for_new_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    payload = {
        "kind": "botlens_runtime_bootstrap_facts",
        "bot_id": "bot-1",
        "run_id": "run-1",
        "series_key": "instrument-btc|1m",
        "run_seq": 1,
        "bridge_session_id": "session-1",
        "facts": [{"fact_type": "series_state_observed", "series_key": "instrument-btc|1m"}],
    }
    try:
        assert emitter.send(payload) is True
        assert emitter.send({**payload, "run_seq": 2, "bridge_session_id": "session-2"}) is True

        _wait_until(lambda: len(connections) == 1 and len(connections[0].sent) == 2)

        assert [json.loads(message)["bridge_session_id"] for message in connections[0].sent] == [
            "session-1",
            "session-2",
        ]
    finally:
        emitter.close()


def test_telemetry_emitter_suppresses_identical_large_facts_in_same_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[_FakeSyncWebSocket] = []

    def _connect(_url: str, *, open_timeout: int, close_timeout: int) -> _FakeSyncWebSocket:
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
    payload = _large_runtime_facts_payload()
    try:
        assert emitter.send(payload) is True
        assert emitter.send({**payload, "run_seq": 2}) is True

        _wait_until(lambda: len(connections) == 1 and len(connections[0].sent) == 1)

        assert len(connections[0].sent) == 1
    finally:
        emitter.close()


def test_telemetry_emitter_allows_large_facts_when_forward_progress_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[_FakeSyncWebSocket] = []

    def _connect(_url: str, *, open_timeout: int, close_timeout: int) -> _FakeSyncWebSocket:
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
        assert emitter.send(_large_runtime_facts_payload(last_useful_progress_at="2026-01-01T00:00:00Z")) is True
        assert emitter.send(_large_runtime_facts_payload(last_useful_progress_at="2026-01-01T00:00:05Z")) is True

        _wait_until(lambda: len(connections) == 1 and len(connections[0].sent) == 2)

        payloads = [json.loads(message) for message in connections[0].sent]
        assert [payload["facts"][0]["runtime"]["last_useful_progress_at"] for payload in payloads] == [
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:05Z",
        ]
    finally:
        emitter.close()


def test_telemetry_emitter_allows_large_facts_for_new_session_even_when_payload_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[_FakeSyncWebSocket] = []

    def _connect(_url: str, *, open_timeout: int, close_timeout: int) -> _FakeSyncWebSocket:
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
        assert emitter.send(_large_runtime_facts_payload(bridge_session_id="session-1")) is True
        assert emitter.send(_large_runtime_facts_payload(bridge_session_id="session-2")) is True

        _wait_until(lambda: len(connections) == 1 and len(connections[0].sent) == 2)

        assert [json.loads(message)["bridge_session_id"] for message in connections[0].sent] == [
            "session-1",
            "session-2",
        ]
    finally:
        emitter.close()


def test_telemetry_emitter_prioritizes_control_lane_over_general_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[_FakeSyncWebSocket] = []

    def _connect(_url: str, *, open_timeout: int, close_timeout: int) -> _FakeSyncWebSocket:
        ws = _FakeSyncWebSocket(fail_first_send=len(connections) == 0)
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
        assert emitter.send(_large_runtime_facts_payload()) is True
        _wait_until(lambda: len(connections) == 1 and connections[0].close_calls == 1)

        assert emitter.send(
            {
                "kind": "botlens_runtime_bootstrap_facts",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "series_key": "instrument-btc|1m",
                "bridge_session_id": "session-1",
                "run_seq": 1,
                "facts": [{"fact_type": "series_state_observed", "series_key": "instrument-btc|1m"}],
            }
        )

        _wait_until(lambda: len(connections) == 2 and len(connections[1].sent) == 2)

        payloads = [json.loads(message) for message in connections[1].sent]
        assert [payload["kind"] for payload in payloads] == [
            "botlens_runtime_bootstrap_facts",
            "botlens_runtime_facts",
        ]
    finally:
        emitter.close()


def test_telemetry_emitter_pressure_snapshot_exposes_control_and_general_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = {"open": False}

    def _connect(_url: str, *, open_timeout: int, close_timeout: int) -> _FakeSyncWebSocket:
        del open_timeout, close_timeout
        ws = _FakeSyncWebSocket()
        original_send = ws.send

        def _send(message: str) -> None:
            while not gate["open"]:
                time.sleep(0.01)
            original_send(message)

        ws.send = _send  # type: ignore[method-assign]
        return ws

    monkeypatch.setattr(telemetry_mod, "sync_connect", _connect)

    emitter = telemetry_mod.TelemetryEmitter(
        "ws://example.test/telemetry",
        queue_max=8,
        queue_timeout_ms=100,
        retry_ms=25,
    )
    try:
        assert emitter.send(_large_runtime_facts_payload()) is True
        assert emitter.send(
            {
                "kind": "botlens_runtime_bootstrap_facts",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "series_key": "instrument-btc|1m",
                "bridge_session_id": "session-1",
                "run_seq": 1,
                "facts": [{"fact_type": "series_state_observed", "series_key": "instrument-btc|1m"}],
            }
        )

        _wait_until(
            lambda: emitter.pressure_snapshot()["queue_depth"] >= 2,
            timeout_s=2.0,
        )
        snapshot = emitter.pressure_snapshot()
        assert snapshot["queue_depth"] == 2
        assert snapshot["control_queue_depth"] == 1
        assert snapshot["emit_queue_depth"] == 1
        assert snapshot["queue_capacity"] == snapshot["control_queue_capacity"] + snapshot["emit_queue_capacity"]
    finally:
        gate["open"] = True
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


def test_notify_backend_lifecycle_event_terminal_prefers_direct_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    sender = MagicMock()
    sender.send.return_value = True
    ephemeral = MagicMock(return_value=True)

    monkeypatch.setattr(runtime_mod, "emit_telemetry_ephemeral_message", ephemeral)

    delivered = runtime_mod._notify_backend_lifecycle_event(
        lifecycle_state={
            "bot_id": "bot-1",
            "run_id": "run-1",
            "seq": 4,
            "phase": "completed",
            "status": "completed",
            "checkpoint_at": "2026-01-01T00:00:00Z",
        },
        telemetry_sender=sender,
    )

    assert delivered is True
    ephemeral.assert_called_once()
    sender.send.assert_not_called()


def test_notify_backend_lifecycle_event_terminal_falls_back_to_sender_when_direct_send_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sender = MagicMock()
    sender.send.return_value = True
    ephemeral = MagicMock(return_value=False)

    monkeypatch.setattr(runtime_mod, "emit_telemetry_ephemeral_message", ephemeral)

    delivered = runtime_mod._notify_backend_lifecycle_event(
        lifecycle_state={
            "bot_id": "bot-1",
            "run_id": "run-1",
            "seq": 5,
            "phase": "completed",
            "status": "completed",
            "checkpoint_at": "2026-01-01T00:00:00Z",
        },
        telemetry_sender=sender,
    )

    assert delivered is True
    ephemeral.assert_called_once()
    sender.send.assert_called_once()


def test_handle_runtime_facts_event_emits_live_lifecycle_only_once(monkeypatch: pytest.MonkeyPatch) -> None:
    sender = MagicMock()
    sender.send.return_value = True
    persisted: list[dict] = []

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

    def _persist(**kwargs):
        if kwargs["phase"] == BotLifecyclePhase.LIVE.value:
            assert ctx.startup_live_emitted is False
        persisted.append(dict(kwargs))
        return {"seq": len(persisted)}

    monkeypatch.setattr(runtime_mod, "_persist_lifecycle_phase", _persist)

    event = {
        "worker_id": "worker-1",
        "run_seq": 7,
        "series_key": "instrument-btc|1m",
        "bridge_session_id": "session-1",
        "bridge_seq": 1,
        "known_at": "2026-01-01T00:00:00Z",
        "event_time": "2026-01-01T00:00:00Z",
        "facts": [{"fact_type": "candle_upserted"}],
    }

    first = runtime_mod._handle_runtime_facts_event(ctx, event, telemetry_sender=sender)
    second = runtime_mod._handle_runtime_facts_event(
        ctx,
        {**event, "bridge_seq": 2, "run_seq": 8},
        telemetry_sender=sender,
    )

    live_calls = [call for call in persisted if call["phase"] == BotLifecyclePhase.LIVE.value]

    assert first[0] == 7
    assert second[0] == 8
    assert sender.send.call_count == 2
    assert len(live_calls) == 1
    assert ctx.startup_live_emitted is True


def test_handle_runtime_facts_started_event_marks_startup_live_without_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted: list[dict] = []
    monkeypatch.setattr(
        runtime_mod,
        "_persist_lifecycle_phase",
        lambda **kwargs: persisted.append(dict(kwargs)) or {"seq": len(persisted)},
    )

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
        worker_symbols={"worker-1": ["BTC"]},
        runtime_state=BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value,
    )
    runtime_mod._set_series_state(
        ctx,
        symbol="BTC",
        status="bootstrapped",
        worker_id="worker-1",
        series_key="instrument-btc|1m",
        bootstrap_seq=10,
    )

    def _load_observations(ctx, *, pending_series_by_key):  # noqa: ANN001
        assert ctx.run_id == "run-1"
        assert dict(pending_series_by_key) == {"instrument-btc|1m": "BTC"}
        return {
            "instrument-btc|1m": {
                "first_seq": 11,
                "latest_seq": 11,
                "known_at": "2026-01-01T00:00:20Z",
            }
        }

    monkeypatch.setattr(runtime_mod, "_load_canonical_first_live_observations", _load_observations)

    runtime_mod._handle_runtime_facts_started_event(
        ctx,
        {
            "worker_id": "worker-1",
            "symbols": ["BTC"],
            "series_key": "instrument-btc|1m",
            "reason": "subscriber_backpressure",
            "trigger_event": "facts",
            "known_at": "2026-01-01T00:00:20Z",
            "event_time": "2026-01-01T00:00:20Z",
        },
    )

    assert ctx.startup_live_emitted is True
    assert ctx.runtime_state == BotLensRuntimeState.LIVE.value
    assert ctx.first_snapshot_series == {"BTC"}
    assert ctx.series_states["BTC"]["status"] == "live"
    assert ctx.latest_pressure_snapshot["trigger"] == "canonical_runtime_facts_observed"
    assert persisted[-1]["phase"] == BotLifecyclePhase.LIVE.value
    assert persisted[-1]["status"] == "running"
    assert persisted[-1]["message"] == "All planned series committed first canonical runtime facts; bot is live."


def test_handle_series_bootstrap_event_suppresses_resync_after_live(monkeypatch: pytest.MonkeyPatch) -> None:
    sender = MagicMock()
    sender.send.return_value = True
    persisted: list[dict] = []

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
        startup_live_emitted=True,
        runtime_state=BotLensRuntimeState.LIVE.value,
        last_useful_progress_at="2026-01-01T00:00:10Z",
    )

    result = runtime_mod._handle_series_bootstrap_event(
        ctx,
        {
            "worker_id": "worker-1",
            "series_key": "instrument-btc|1m",
            "bridge_session_id": "session-2",
            "bridge_seq": 9,
            "reason": "bridge_queue_backpressure",
            "known_at": "2026-01-01T00:00:20Z",
            "event_time": "2026-01-01T00:00:20Z",
            "facts": [{"fact_type": "series_state_observed"}],
        },
        telemetry_sender=sender,
    )

    assert result == (0, 0.0, 0, True)
    assert sender.send.call_count == 0
    assert persisted[0]["phase"] == BotLifecyclePhase.DEGRADED.value
    assert ctx.degraded_loop_started_at == "2026-01-01T00:00:20Z"
    assert ctx.runtime_state == BotLensRuntimeState.DEGRADED.value


def test_handle_runtime_facts_event_recovers_from_degraded_without_reusing_bootstrap(monkeypatch: pytest.MonkeyPatch) -> None:
    sender = MagicMock()
    sender.send.return_value = True
    persisted: list[dict] = []

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
        startup_live_emitted=True,
        runtime_state=BotLensRuntimeState.DEGRADED.value,
        degraded_loop_started_at="2026-01-01T00:00:05Z",
        last_degraded_started_at="2026-01-01T00:00:05Z",
        degraded_reason_code="subscriber_gap",
        degraded_trigger_event="runtime_facts_drop_after_live",
        last_useful_progress_at="2026-01-01T00:00:04Z",
    )

    runtime_mod._handle_runtime_facts_event(
        ctx,
        {
            "worker_id": "worker-1",
            "run_seq": 12,
            "series_key": "instrument-btc|1m",
            "bridge_session_id": "session-2",
            "bridge_seq": 12,
            "known_at": "2026-01-01T00:00:20Z",
            "event_time": "2026-01-01T00:00:20Z",
            "facts": [{"fact_type": "candle_upserted"}],
        },
        telemetry_sender=sender,
    )

    assert sender.send.call_count == 1
    assert ctx.runtime_state == BotLensRuntimeState.LIVE.value
    assert ctx.degraded_loop_started_at is None
    assert persisted[-1]["phase"] == BotLifecyclePhase.LIVE.value


def test_handle_worker_error_stays_startup_failed_before_live_even_with_other_workers_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted: list[dict] = []
    monkeypatch.setattr(
        runtime_mod,
        "_persist_lifecycle_phase",
        lambda **kwargs: persisted.append(dict(kwargs)) or {"seq": len(persisted)},
    )

    ctx = runtime_mod.ContainerStartupContext(
        bot_id="bot-1",
        run_id="run-1",
        bot={},
        runtime_bot_config={},
        strategy_id="strategy-1",
        symbols=["BTC", "ETH"],
        symbol_shards=[["BTC"], ["ETH"]],
        wallet_config={},
        manager=MagicMock(),
        shared_wallet_proxy={},
        worker_symbols={"worker-1": ["BTC"], "worker-2": ["ETH"]},
        children={
            "worker-1": SimpleNamespace(exitcode=17),
            "worker-2": SimpleNamespace(exitcode=None),
        },
        first_snapshot_series={"BTC"},
        runtime_state=BotLensRuntimeState.INITIALIZING.value,
        startup_live_emitted=False,
    )

    runtime_mod._handle_worker_error(
        ctx,
        "worker-1",
        error="Worker worker-1 exited with code 17",
        observed_at="2026-01-01T00:00:20Z",
        exit_code=17,
    )

    assert ctx.runtime_state == BotLensRuntimeState.STARTUP_FAILED.value
    assert ctx.terminal_status_source == "worker_error"
    assert persisted[-1]["phase"] == BotLifecyclePhase.STARTUP_FAILED.value
    assert persisted[-1]["status"] == "startup_failed"


def test_handle_continuity_gap_event_transitions_live_to_degraded(monkeypatch: pytest.MonkeyPatch) -> None:
    persisted: list[dict] = []

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
        runtime_state=BotLensRuntimeState.LIVE.value,
        last_useful_progress_at="2026-01-01T00:00:00Z",
    )

    runtime_mod._handle_continuity_gap_event(
        ctx,
        {
            "reason": "subscriber_gap",
            "trigger_event": "runtime_facts_drop_after_live",
            "known_at": "2026-01-01T00:00:20Z",
            "event_time": "2026-01-01T00:00:20Z",
        },
    )

    assert ctx.runtime_state == BotLensRuntimeState.DEGRADED.value
    assert ctx.degraded_reason_code == "subscriber_gap"
    assert ctx.latest_pressure_snapshot["trigger"] == "continuity_gap"
    assert persisted[-1]["phase"] == BotLifecyclePhase.DEGRADED.value


def test_transition_runtime_state_persists_rejected_transition_for_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted: list[dict] = []

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
        runtime_state=BotLensRuntimeState.LIVE.value,
    )

    with pytest.raises(runtime_mod.InvalidRuntimeStateTransition):
        runtime_mod._transition_runtime_state(
            ctx,
            next_state=BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value,
            reason="illegal_regression",
            source_component="test_component",
            observed_at="2026-01-01T00:00:30Z",
        )

    assert ctx.runtime_state == BotLensRuntimeState.LIVE.value
    assert len(persisted) == 1
    assert persisted[0]["phase"] == BotLifecyclePhase.LIVE.value
    assert persisted[0]["failure"]["from_state"] == BotLensRuntimeState.LIVE.value
    assert persisted[0]["failure"]["attempted_to_state"] == BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value
    assert persisted[0]["failure"]["transition_reason"] == "illegal_regression"
    assert persisted[0]["failure"]["source_component"] == "test_component"


def test_handle_worker_phase_event_ignores_stale_runtime_subscribing_after_awaiting_first_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted: list[dict] = []

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
        worker_symbols={"worker-1": ["BTC"]},
        runtime_state=BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value,
    )

    runtime_mod._handle_worker_phase_event(
        ctx,
        {
            "kind": "worker_phase",
            "worker_id": "worker-1",
            "symbols": ["BTC"],
            "phase": BotLifecyclePhase.RUNTIME_SUBSCRIBING.value,
            "message": "Worker runtime subscribing to live facts stream.",
            "event_time": "2026-01-01T00:00:30Z",
        },
    )

    assert ctx.runtime_state == BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value
    assert persisted == []
    assert ctx.series_states["BTC"]["status"] == "awaiting_first_snapshot"
