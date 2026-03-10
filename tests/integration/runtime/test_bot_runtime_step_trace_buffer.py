from __future__ import annotations

import time
from types import SimpleNamespace

from engines.bot_runtime.runtime.components.step_trace_buffer import StepTracePersistenceBuffer
from tests.helpers.module_stubs import install_module_stubs


def test_step_trace_buffer_batches_and_flushes(monkeypatch):
    batches: list[int] = []

    def _batch_handler(payloads):
        batches.append(len(payloads))
        return len(payloads)

    install_module_stubs(
        monkeypatch,
        {"portal.backend.service.storage": SimpleNamespace(storage=SimpleNamespace(record_bot_run_steps_batch=_batch_handler))},
    )
    buffer = StepTracePersistenceBuffer(queue_max=64, batch_size=3, flush_interval_s=0.02, overflow_policy="drop_oldest")

    for index in range(7):
        buffer.record(
            {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "step_name": "step_series_state",
                "started_at": "2026-03-01T00:00:00Z",
                "ended_at": "2026-03-01T00:00:00.100000Z",
                "duration_ms": 100.0 + index,
                "ok": True,
                "context": {"i": index},
            }
        )

    buffer.flush(reason="test", shutdown=True, timeout_s=2.0)

    assert sum(batches) == 7
    assert max(batches) <= 3


def test_step_trace_buffer_drop_oldest_when_queue_full(monkeypatch):
    persisted = 0

    def _batch_handler(payloads):
        nonlocal persisted
        time.sleep(0.03)
        persisted += len(payloads)
        return len(payloads)

    install_module_stubs(
        monkeypatch,
        {"portal.backend.service.storage": SimpleNamespace(storage=SimpleNamespace(record_bot_run_steps_batch=_batch_handler))},
    )
    buffer = StepTracePersistenceBuffer(queue_max=4, batch_size=1, flush_interval_s=0.01, overflow_policy="drop_oldest")

    for index in range(80):
        buffer.record(
            {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "step_name": "step_series_state",
                "started_at": "2026-03-01T00:00:00Z",
                "ended_at": "2026-03-01T00:00:00.100000Z",
                "duration_ms": 100.0 + index,
                "ok": True,
                "context": {"i": index},
            }
        )

    buffer.flush(reason="test-overflow", shutdown=True, timeout_s=3.0)
    metrics = buffer.metrics_snapshot()

    assert persisted > 0
    assert metrics["dropped_count"] > 0
