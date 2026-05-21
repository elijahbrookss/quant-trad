from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

import pytest

from portal.backend.service.observability import BackendObserver, get_observability_sink, reset_observability_sink
from portal.backend.service.bots.botlens_mailbox import (
    FanoutEnvelope,
    FanoutSymbolDeltaBatch,
    RunMailbox,
    SymbolMailbox,
    _FANOUT_STOP,
)
from portal.backend.service.bots.botlens_projector_registry import _fanout_delivery_loop
from portal.backend.service.bots.botlens_run_stream import BotLensRunStream
from portal.backend.service.bots.botlens_state import CandleDelta, ProjectionBatch
from portal.backend.service.bots.botlens_transport import BotLensTransport


class _FakeWebSocket:
    def __init__(self) -> None:
        self.accepted = False
        self.messages: list[dict] = []
        self.closed = False

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, payload: str) -> None:
        self.messages.append(json.loads(payload))

    async def close(self, code: int = 1000) -> None:
        self.closed = True


class _FailingWebSocket(_FakeWebSocket):
    async def send_text(self, payload: str) -> None:
        raise RuntimeError("socket gone")


class _QueueMetrics:
    def __init__(self) -> None:
        self.emissions: list[dict] = []

    def emit(self, **payload) -> None:
        self.emissions.append(dict(payload))


def setup_function() -> None:
    reset_observability_sink()


def test_backend_observer_interval_gauge_throttles_emission() -> None:
    observer = BackendObserver(component="test_observer")

    with patch("portal.backend.service.observability.time.monotonic", side_effect=[0.0, 0.1, 1.2]):
        observer.maybe_gauge("viewer_active:run-1", "viewer_active_count", 1.0, run_id="run-1")
        observer.maybe_gauge("viewer_active:run-1", "viewer_active_count", 2.0, run_id="run-1")
        observer.maybe_gauge("viewer_active:run-1", "viewer_active_count", 3.0, run_id="run-1")

    metrics = [m for m in get_observability_sink().snapshot()["metrics"] if m["metric_name"] == "viewer_active_count"]
    assert [metric["value"] for metric in metrics] == [1.0, 3.0]


def test_symbol_mailbox_overflow_emits_drop_metric_and_event() -> None:
    mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m")
    batch = ProjectionBatch(
        batch_kind="botlens_runtime_facts",
        run_id="run-1",
        bot_id="bot-1",
        seq=1,
        event_time="2026-01-01T00:00:00Z",
        known_at="2026-01-01T00:00:00Z",
        symbol_key="instrument-btc|1m",
        bridge_session_id="sess-1",
        events=(),
    )
    for _ in range(mailbox.event_queue.maxsize):
        assert mailbox.enqueue_batch(batch) is True

    assert mailbox.enqueue_batch(batch) is False

    snapshot = get_observability_sink().snapshot()
    assert any(metric["metric_name"] == "symbol_fact_dropped_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "symbol_fact_queue_overflow" for event in snapshot["events"])


def test_bootstrap_supersede_emits_metric_and_event() -> None:
    mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m")
    batch = ProjectionBatch(
        batch_kind="botlens_runtime_bootstrap_facts",
        run_id="run-1",
        bot_id="bot-1",
        seq=1,
        event_time="2026-01-01T00:00:00Z",
        known_at="2026-01-01T00:00:00Z",
        symbol_key="instrument-btc|1m",
        bridge_session_id="sess-1",
        events=(),
    )
    mailbox.set_bootstrap(batch)
    mailbox.set_bootstrap(batch)

    snapshot = get_observability_sink().snapshot()
    assert any(metric["metric_name"] == "bootstrap_superseded_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "bootstrap_superseded" for event in snapshot["events"])


def test_run_lifecycle_queue_overflow_fails_loud() -> None:
    mailbox = RunMailbox(run_id="run-1", bot_id="bot-1")
    batch = ProjectionBatch(
        batch_kind="botlens_run_lifecycle",
        run_id="run-1",
        bot_id="bot-1",
        seq=1,
        event_time="2026-01-01T00:00:00Z",
        known_at="2026-01-01T00:00:00Z",
        events=(),
    )
    for _ in range(mailbox.lifecycle_queue.maxsize):
        mailbox.enqueue_lifecycle(batch)

    with pytest.raises(RuntimeError, match="run lifecycle queue overflow"):
        mailbox.enqueue_lifecycle(batch)

    snapshot = get_observability_sink().snapshot()
    assert any(metric["metric_name"] == "run_lifecycle_rejected_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "run_lifecycle_queue_overflow_failed_loud" for event in snapshot["events"])


def test_run_stream_broadcast_emits_live_delta_metrics_without_snapshot_buffering() -> None:
    async def scenario() -> None:
        stream = BotLensRunStream()
        stream.bind_run(run_id="run-1", bot_id="bot-1")
        transport = BotLensTransport()
        ws = _FakeWebSocket()
        await stream.add_run_viewer(
            run_id="run-1",
            ws=ws,
            selected_symbol_key="instrument-btc|1m",
        )
        assert stream.viewer_count_for_symbol("run-1", "instrument-btc|1m") == 1
        assert stream.viewer_count_for_symbol("run-1", "instrument-eth|1m") == 0

        prepared = transport.build_symbol_prepared_deltas(
            run_id="run-1",
            deltas=(
                CandleDelta(
                    symbol_key="instrument-btc|1m",
                    seq=7,
                    event_time="2026-01-01T00:00:00Z",
                    candle={"time": 7, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0},
                ),
            ),
        )[0]
        await stream.broadcast_live_delta(prepared)

        snapshot = get_observability_sink().snapshot()
        viewer_broadcasts = [metric for metric in snapshot["metrics"] if metric["metric_name"] == "viewer_broadcast_total"]
        assert viewer_broadcasts
        assert all(metric["tags"].get("bot_id") == "bot-1" for metric in viewer_broadcasts)
        assert any(metric["metric_name"] == "viewer_payload_bytes" for metric in snapshot["metrics"])
        assert any(metric["metric_name"] == "live_transport_payload_bytes" for metric in snapshot["metrics"])
        assert any(metric["metric_name"] == "live_transport_serialize_ms" for metric in snapshot["metrics"])
        assert all(metric["metric_name"] != "snapshot_buffer_drop_total" for metric in snapshot["metrics"])
        assert all(event["name"] != "viewer_snapshot_buffer_overflow" for event in snapshot["events"])

    asyncio.run(scenario())


def test_fanout_skips_symbol_delta_transport_when_no_viewer_demands_symbol() -> None:
    class _NoViewerRunStream:
        class _Transport:
            def build_symbol_prepared_deltas(self, **_kwargs):
                raise AssertionError("symbol live payload should not be built without viewer demand")

        transport = _Transport()

        def viewer_count_for_symbol(self, _run_id, _symbol_key):
            return 0

        def viewer_count_for_run(self, _run_id):
            return 0

    async def scenario() -> None:
        queue: asyncio.Queue = asyncio.Queue()
        await queue.put(
            FanoutEnvelope(
                run_id="run-1",
                item=FanoutSymbolDeltaBatch(
                    run_id="run-1",
                    deltas=(
                        CandleDelta(
                            symbol_key="instrument-btc|1m",
                            seq=7,
                            event_time="2026-01-01T00:00:00Z",
                            candle={"time": 7, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0},
                        ),
                    ),
                ),
                message_kind="symbol_projection_delta",
                payload_bytes=128,
            )
        )
        await queue.put(_FANOUT_STOP)
        metrics = _QueueMetrics()

        await _fanout_delivery_loop(
            bot_id="bot-1",
            run_id="run-1",
            fanout_channel=queue,
            fanout_queue_metrics=metrics,
            run_stream=_NoViewerRunStream(),
        )

        snapshot = get_observability_sink().snapshot()
        dropped = [
            metric
            for metric in snapshot["metrics"]
            if metric["metric_name"] == "live_transport_dropped_stale_count"
        ]
        assert dropped
        assert dropped[-1]["tags"]["source_reason"] == "no_symbol_viewer"
        assert dropped[-1]["tags"]["pipeline_stage"] == "candles"
        assert metrics.emissions

    asyncio.run(scenario())


def test_run_stream_send_failure_emits_terminal_send_metrics() -> None:
    async def scenario() -> None:
        stream = BotLensRunStream()
        stream.bind_run(run_id="run-1", bot_id="bot-1")
        ws = _FailingWebSocket()

        await stream.add_run_viewer(
            run_id="run-1",
            ws=ws,
            selected_symbol_key="instrument-btc|1m",
        )

        snapshot = get_observability_sink().snapshot()
        metric_names = [metric["metric_name"] for metric in snapshot["metrics"]]
        assert "viewer_send_total" in metric_names
        assert "viewer_send_fail_total" in metric_names
        assert "viewer_send_ms" in metric_names
        assert any(event["name"] == "viewer_send_failed" for event in snapshot["events"])
        assert all(event["name"] != "viewer_snapshot_failed" for event in snapshot["events"])

    asyncio.run(scenario())
