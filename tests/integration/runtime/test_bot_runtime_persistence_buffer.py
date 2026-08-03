from __future__ import annotations

import threading
import time

import pytest

from engines.bot_runtime.runtime.components.persistence_buffer import TradePersistenceBuffer
from engines.bot_runtime.runtime.mixins.runtime_persistence import RuntimePersistenceMixin


def _wait_until(predicate, *, timeout_s: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return bool(predicate())


def test_persistence_buffer_flushes_by_count():
    calls: list[tuple[str, str]] = []
    persisted = threading.Event()

    def record(kind: str, payload: dict) -> None:
        calls.append((kind, str(payload["id"])))
        if len(calls) == 2:
            persisted.set()

    buffer = TradePersistenceBuffer(
        max_batch_size=2,
        flush_interval_s=100,
        record_trade=lambda payload: record("trade", payload),
        record_trade_event=lambda payload: record("event", payload),
    )
    buffer.record_trade_entry({"id": "t1"})
    assert calls == []

    buffer.record_trade_event({"id": "e1"})
    assert persisted.wait(timeout=1.0)
    buffer.flush(reason="test", shutdown=True)
    assert calls == [("trade", "t1"), ("event", "e1")]


def test_persistence_buffer_flushes_on_close_event():
    persisted = threading.Event()
    calls: list[str] = []

    def record_event(payload: dict) -> None:
        calls.append(str(payload["id"]))
        persisted.set()

    buffer = TradePersistenceBuffer(
        max_batch_size=10,
        flush_interval_s=100,
        record_trade=lambda _payload: None,
        record_trade_event=record_event,
    )
    buffer.record_trade_event({"id": "close-event"}, event_type="close")
    assert persisted.wait(timeout=1.0)
    buffer.flush(reason="test", shutdown=True)
    assert calls == ["close-event"]


def test_persistence_buffer_copies_payloads_and_preserves_enqueue_order():
    calls: list[tuple[str, str, str]] = []
    buffer = TradePersistenceBuffer(
        max_batch_size=10,
        flush_interval_s=0,
        record_trade=lambda payload: calls.append(("trade", str(payload["id"]), str(payload["status"]))),
        record_trade_event=lambda payload: calls.append(("event", str(payload["id"]), "event")),
    )
    payload = {"id": "t1", "status": "open"}
    buffer.record_trade_entry(payload)
    payload["status"] = "mutated-after-enqueue"
    buffer.record_trade_event({"id": "e1"})
    buffer.record_trade_entry({"id": "t1", "status": "closed"})

    buffer.flush(reason="test", shutdown=True)

    assert calls == [
        ("trade", "t1", "open"),
        ("event", "e1", "event"),
        ("trade", "t1", "closed"),
    ]


def test_persistence_enqueue_does_not_wait_for_blocked_writer():
    writer_started = threading.Event()
    release_writer = threading.Event()
    second_returned = threading.Event()
    calls: list[str] = []

    def slow_record(payload: dict) -> None:
        calls.append(str(payload["id"]))
        writer_started.set()
        assert release_writer.wait(timeout=2.0)

    buffer = TradePersistenceBuffer(
        max_batch_size=1,
        queue_max=4,
        flush_interval_s=100,
        record_trade=slow_record,
        record_trade_event=lambda _payload: None,
    )
    buffer.record_trade_entry({"id": "t1"})
    assert writer_started.wait(timeout=1.0)

    def enqueue_second() -> None:
        buffer.record_trade_entry({"id": "t2"})
        second_returned.set()

    enqueue_thread = threading.Thread(target=enqueue_second)
    enqueue_thread.start()
    assert second_returned.wait(timeout=0.2)
    release_writer.set()
    enqueue_thread.join(timeout=1.0)
    buffer.flush(reason="test", shutdown=True)
    assert calls == ["t1", "t2"]


def test_persistence_writer_failure_propagates_at_terminal_drain():
    attempted = threading.Event()

    def fail_write(_payload: dict) -> None:
        attempted.set()
        raise ValueError("write exploded")

    buffer = TradePersistenceBuffer(
        max_batch_size=1,
        flush_interval_s=100,
        record_trade=fail_write,
        record_trade_event=lambda _payload: None,
    )
    buffer.record_trade_entry({"id": "t1"})
    assert attempted.wait(timeout=1.0)

    with pytest.raises(RuntimeError, match="bot_trade_persistence_failed.*write exploded"):
        buffer.flush(reason="runtime_loop_complete", shutdown=True)


def test_persistence_overflow_fails_without_dropping_accepted_writes():
    writer_started = threading.Event()
    release_writer = threading.Event()
    calls: list[str] = []

    def slow_record(payload: dict) -> None:
        calls.append(str(payload["id"]))
        if payload["id"] == "t1":
            writer_started.set()
            assert release_writer.wait(timeout=2.0)

    buffer = TradePersistenceBuffer(
        max_batch_size=1,
        queue_max=1,
        flush_interval_s=100,
        record_trade=slow_record,
        record_trade_event=lambda _payload: None,
    )
    buffer.record_trade_entry({"id": "t1"})
    assert writer_started.wait(timeout=1.0)
    buffer.record_trade_entry({"id": "t2"})

    with pytest.raises(RuntimeError, match="bot_trade_persistence_overflow"):
        buffer.record_trade_entry({"id": "t3"})

    release_writer.set()
    buffer.flush(reason="runtime_loop_failed", shutdown=True)
    assert calls == ["t1", "t2"]


def test_persistence_drain_timeout_is_terminal_and_shutdown_is_idempotent():
    writer_started = threading.Event()
    release_writer = threading.Event()
    writer_finished = threading.Event()

    def slow_record(_payload: dict) -> None:
        writer_started.set()
        release_writer.wait(timeout=2.0)
        writer_finished.set()

    timed_out = TradePersistenceBuffer(
        max_batch_size=1,
        flush_interval_s=100,
        record_trade=slow_record,
        record_trade_event=lambda _payload: None,
    )
    timed_out.record_trade_entry({"id": "t1"})
    assert writer_started.wait(timeout=1.0)
    with pytest.raises(RuntimeError, match="bot_trade_persistence_drain_timeout"):
        timed_out.flush(reason="runtime_loop_complete", shutdown=True, timeout_s=0.1)
    release_writer.set()
    assert writer_finished.wait(timeout=1.0)
    assert _wait_until(lambda: not bool(timed_out.metrics_snapshot()["worker_alive"]))

    clean = TradePersistenceBuffer(
        max_batch_size=1,
        record_trade=lambda _payload: None,
        record_trade_event=lambda _payload: None,
    )
    clean.record_trade_entry({"id": "t2"})
    clean.flush(reason="runtime_loop_complete", shutdown=True)
    clean.flush(reason="runtime_loop_complete", shutdown=True)
    with pytest.raises(RuntimeError, match="bot_trade_persistence_closed"):
        clean.record_trade_entry({"id": "late"})


def test_runtime_terminal_trade_drain_failure_is_not_swallowed():
    class FailingBuffer:
        def flush(self, **_kwargs) -> None:
            raise ValueError("terminal writer failed")

    class Harness(RuntimePersistenceMixin):
        def __init__(self) -> None:
            self._persistence_buffer = FailingBuffer()
            self.traces: list[dict] = []

        def _runtime_log_context(self, **context):
            return context

        def _record_step_trace(self, _step_name: str, **context):
            self.traces.append(context)

    harness = Harness()
    with pytest.raises(ValueError, match="terminal writer failed"):
        harness._flush_persistence_buffer("runtime_loop_complete")
    assert harness.traces[-1]["ok"] is False

    harness._flush_persistence_buffer("runtime_loop_failed", raise_on_error=False)
    assert harness.traces[-1]["ok"] is False
