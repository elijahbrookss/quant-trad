from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

from portal.backend.service.observability import BackendObserver, get_observability_sink, reset_observability_sink
from portal.backend.service.bots.botlens_mailbox import SymbolMailbox
from portal.backend.service.bots.botlens_run_stream import BotLensRunStream
from portal.backend.service.bots.botlens_typed_deltas import SymbolTypedDeltaBuilder


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


def setup_function() -> None:
    reset_observability_sink()


def test_backend_observer_interval_gauge_throttles_emission() -> None:
    observer = BackendObserver(component="test_observer")

    with patch("portal.backend.service.observability.time.monotonic", side_effect=[0.0, 0.1, 1.2]):
        observer.maybe_gauge("viewer_active:run-1", "viewer_active_count", 1.0, run_id="run-1")
        observer.maybe_gauge("viewer_active:run-1", "viewer_active_count", 2.0, run_id="run-1")
        observer.maybe_gauge("viewer_active:run-1", "viewer_active_count", 3.0, run_id="run-1")

    metrics = [m for m in get_observability_sink().snapshot()["metrics"] if m["name"] == "viewer_active_count"]
    assert [metric["value"] for metric in metrics] == [1.0, 3.0]


def test_symbol_mailbox_overflow_emits_drop_metric_and_event() -> None:
    mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m")
    for index in range(mailbox.fact_queue.maxsize):
        assert mailbox.enqueue_facts({"seq": index}) is True

    assert mailbox.enqueue_facts({"seq": "overflow"}) is False

    snapshot = get_observability_sink().snapshot()
    assert any(metric["name"] == "symbol_fact_dropped_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "symbol_fact_queue_overflow" for event in snapshot["events"])


def test_bootstrap_supersede_emits_metric_and_event() -> None:
    mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m")
    mailbox.set_bootstrap({"seq": 1})
    mailbox.set_bootstrap({"seq": 2})

    snapshot = get_observability_sink().snapshot()
    assert any(metric["name"] == "bootstrap_superseded_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "bootstrap_superseded" for event in snapshot["events"])


def test_run_stream_snapshot_buffer_overflow_emits_metric_and_event() -> None:
    async def scenario() -> None:
        stream = BotLensRunStream()
        ws = _FakeWebSocket()
        await stream.add_run_viewer(
            run_id="run-1",
            ws=ws,
            selected_symbol_key="instrument-btc|1m",
        )

        for seq in range(1, 230):
            prepared = SymbolTypedDeltaBuilder.build(
                run_id="run-1",
                symbol_key="instrument-btc|1m",
                seq=seq,
                event_time="2026-01-01T00:00:00Z",
                delta={"runtime": {"status": "running", "seq": seq}},
            )[0]
            await stream.broadcast_typed_delta(prepared)

        snapshot = get_observability_sink().snapshot()
        assert any(metric["name"] == "snapshot_buffer_drop_total" for metric in snapshot["metrics"])
        assert any(event["name"] == "viewer_snapshot_buffer_overflow" for event in snapshot["events"])

    asyncio.run(scenario())


def test_run_stream_send_failure_emits_terminal_send_metrics() -> None:
    async def scenario() -> None:
        stream = BotLensRunStream()
        ws = _FailingWebSocket()

        await stream.add_run_viewer(
            run_id="run-1",
            ws=ws,
            selected_symbol_key="instrument-btc|1m",
        )

        snapshot = get_observability_sink().snapshot()
        metric_names = [metric["name"] for metric in snapshot["metrics"]]
        assert "viewer_send_total" in metric_names
        assert "viewer_send_fail_total" in metric_names
        assert "viewer_send_ms" in metric_names
        assert any(event["name"] == "viewer_send_failed" for event in snapshot["events"])
        assert all(event["name"] != "viewer_snapshot_failed" for event in snapshot["events"])

    asyncio.run(scenario())
