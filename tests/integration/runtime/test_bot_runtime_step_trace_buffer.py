from __future__ import annotations

import time

from engines.bot_runtime.runtime.components.step_trace_buffer import StepTracePersistenceBuffer


def test_step_trace_buffer_batches_and_flushes():
    batches: list[list[dict]] = []

    def _batch_handler(payloads):
        batches.append([dict(payload) for payload in payloads])
        return len(payloads)

    buffer = StepTracePersistenceBuffer(
        queue_max=64,
        batch_size=3,
        flush_interval_s=0.02,
        overflow_policy="drop_oldest",
        record_batch=_batch_handler,
    )

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

    flattened = [row for batch in batches for row in batch]
    assert len(flattened) == 1
    assert flattened[0]["_step_trace_rollup"] is True
    assert flattened[0]["sample_count"] == 7
    assert flattened[0]["raw_sample_count"] == 7


def test_step_trace_buffer_drop_oldest_when_aggregate_keys_exceed_queue():
    persisted = 0

    def _batch_handler(payloads):
        nonlocal persisted
        time.sleep(0.03)
        persisted += len(payloads)
        return len(payloads)

    buffer = StepTracePersistenceBuffer(
        queue_max=4,
        batch_size=1,
        flush_interval_s=0.01,
        overflow_policy="drop_oldest",
        record_batch=_batch_handler,
    )

    for index in range(80):
        buffer.record(
            {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "step_name": f"step_series_state_{index}",
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
