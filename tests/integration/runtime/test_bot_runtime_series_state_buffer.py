from __future__ import annotations

from engines.bot_runtime.runtime.components.series_state_buffer import SeriesStatePersistenceBuffer


def test_series_state_buffer_batches_and_flushes():
    batches: list[int] = []

    def _batch_handler(payloads):
        batches.append(len(payloads))
        return len(payloads)

    buffer = SeriesStatePersistenceBuffer(
        queue_max=64,
        batch_size=3,
        flush_interval_s=0.02,
        enqueue_timeout_s=0.2,
        retry_interval_s=0.01,
        record_batch=_batch_handler,
    )

    for index in range(7):
        buffer.record(
            {
                "event_id": f"evt-{index}",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": index + 1,
                "event_type": "series_state.snapshot",
                "payload": {"bar_index": index},
            }
        )

    buffer.flush(reason="test", shutdown=True, timeout_s=2.0)

    assert sum(batches) == 7
    assert max(batches) <= 3


def test_series_state_buffer_retries_failed_batch():
    attempts = 0
    persisted = 0

    def _batch_handler(payloads):
        nonlocal attempts, persisted
        attempts += 1
        if attempts < 3:
            raise RuntimeError("db unavailable")
        persisted += len(payloads)
        return len(payloads)

    buffer = SeriesStatePersistenceBuffer(
        queue_max=32,
        batch_size=2,
        flush_interval_s=0.02,
        enqueue_timeout_s=0.2,
        retry_interval_s=0.01,
        record_batch=_batch_handler,
    )

    buffer.record(
        {
            "event_id": "evt-1",
            "bot_id": "bot-1",
            "run_id": "run-1",
            "seq": 1,
            "event_type": "series_state.snapshot",
            "payload": {"bar_index": 1},
        }
    )

    buffer.flush(reason="test-retry", shutdown=True, timeout_s=2.0)
    metrics = buffer.metrics_snapshot()

    assert persisted == 1
    assert attempts >= 3
    assert metrics["persist_error_count"] >= 2
