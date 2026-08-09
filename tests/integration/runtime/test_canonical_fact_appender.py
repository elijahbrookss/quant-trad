from __future__ import annotations

import threading

import pytest

from engines.bot_runtime.runtime.components.canonical_facts import (
    CanonicalFactAppender,
    CanonicalFactPersistenceBuffer,
    CanonicalFactProjectionDispatcher,
    _should_sample_debug_log,
    canonical_fact_payload,
)


class _FailingConsumer:
    def __init__(self, order: list[str]) -> None:
        self._order = order

    def consume(self, batch):  # noqa: ANN001
        self._order.append(f"consume:{batch.seq}")
        raise RuntimeError("transport unavailable")


class _RecordingConsumer:
    def __init__(self) -> None:
        self.seqs: list[int] = []

    def consume(self, batch):  # noqa: ANN001
        self.seqs.append(int(batch.seq))
        return (1, 0)


class _BlockingConsumer:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.seqs: list[int] = []

    def consume(self, batch):  # noqa: ANN001
        self.started.set()
        if not self.release.wait(timeout=2.0):
            raise RuntimeError("test projection consumer was not released")
        self.seqs.append(int(batch.seq))
        return (1, 0)


def _canonical_payload(*, known_at: str) -> dict:
    return {
        "series_key": "instrument-btc|1m",
        "known_at": known_at,
        "event_time": known_at,
        "facts": [
            {
                "fact_type": "trade_opened",
                "series_key": "instrument-btc|1m",
                "trade": {
                    "trade_id": f"trade-{known_at}",
                    "status": "open",
                    "direction": "long",
                    "qty": 1.0,
                    "entry_price": 1.5,
                    "opened_at": known_at,
                    "bar_time": known_at,
                },
            },
        ],
    }


def test_canonical_fact_debug_logging_is_sampled() -> None:
    assert _should_sample_debug_log(1)
    assert not _should_sample_debug_log(2)
    assert not _should_sample_debug_log(249)
    assert _should_sample_debug_log(250)
    assert _should_sample_debug_log(500)


def test_canonical_fact_appender_persists_before_non_authoritative_consumers() -> None:
    order: list[str] = []
    appended_payloads: list[dict] = []

    def _append(**kwargs):
        order.append(f"append:{kwargs['seq']}")
        appended_payloads.append(dict(kwargs["payload"]))
        return {"inserted_rows": 1}

    appender = CanonicalFactAppender(
        allocate_seq=lambda: 5,
        append_batch=_append,
        consumers=(_FailingConsumer(order),),
    )

    outcome = appender.append_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        batch_kind="botlens_runtime_facts",
        payload={
            "series_key": "instrument-btc|1m",
            "known_at": "2026-04-19T12:00:00Z",
            "event_time": "2026-04-19T12:00:00Z",
            "facts": [
                {
                    "fact_type": "runtime_state_observed",
                    "runtime": {"status": "running"},
                },
                {
                    "fact_type": "candle_upserted",
                    "series_key": "instrument-btc|1m",
                    "candle": {
                        "time": "2026-04-19T12:00:00Z",
                        "open": 1.0,
                        "high": 2.0,
                        "low": 0.5,
                        "close": 1.5,
                    },
                },
                {
                    "fact_type": "trade_opened",
                    "series_key": "instrument-btc|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "open",
                        "direction": "long",
                        "qty": 1.0,
                        "entry_price": 1.5,
                        "opened_at": "2026-04-19T12:00:00Z",
                        "bar_time": "2026-04-19T12:00:00Z",
                    },
                },
            ],
        },
    )

    assert order == ["append:5", "consume:5"]
    assert outcome is not None
    assert outcome.batch.seq == 5
    assert outcome.consumer_results[0].error == "transport unavailable"
    assert [fact["fact_type"] for fact in appended_payloads[0]["facts"]] == ["trade_opened"]
    assert [fact["fact_type"] for fact in outcome.batch.live_payload["facts"]] == [
        "runtime_state_observed",
        "candle_upserted",
        "trade_opened",
    ]


def test_canonical_fact_appender_batches_async_persistence_and_dispatches_seq() -> None:
    persisted_batches: list[list[dict]] = []
    seq = 0
    consumer = _RecordingConsumer()

    def _allocate() -> int:
        nonlocal seq
        seq += 1
        return seq

    def _append_batches(items):
        persisted_batches.append([dict(item) for item in items])
        return {"inserted_rows": len(items), "row_count": len(items)}

    buffer = CanonicalFactPersistenceBuffer(
        queue_max=8,
        batch_size=8,
        flush_interval_s=0.05,
        drain_timeout_s=2.0,
        append_batches=_append_batches,
    )
    appender = CanonicalFactAppender(
        allocate_seq=_allocate,
        persistence_buffer=buffer,
        consumers=(consumer,),
    )

    first = appender.append_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        batch_kind="botlens_runtime_facts",
        payload=_canonical_payload(known_at="2026-04-19T12:00:00Z"),
    )
    second = appender.append_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        batch_kind="botlens_runtime_facts",
        payload=_canonical_payload(known_at="2026-04-19T13:00:00Z"),
    )

    appender.flush(reason="test", shutdown=True, timeout_s=2.0)

    assert first is not None
    assert second is not None
    assert first.batch.append_result["queued"] is True
    assert second.batch.append_result["queued"] is True
    assert consumer.seqs == [1, 2]
    assert len(persisted_batches) == 1
    assert [item["seq"] for item in persisted_batches[0]] == [1, 2]
    assert persisted_batches[0][0]["payload"]["run_seq"] == 1
    assert persisted_batches[0][1]["payload"]["run_seq"] == 2


def test_canonical_fact_appender_queues_projection_dispatch_off_append_path() -> None:
    consumer = _BlockingConsumer()
    dispatcher = CanonicalFactProjectionDispatcher(
        consumers=(consumer,),
        queue_max=4,
        flush_interval_s=0.001,
        drain_timeout_s=2.0,
    )
    appender = CanonicalFactAppender(
        allocate_seq=lambda: 1,
        append_batch=lambda **_kwargs: {"inserted_rows": 1},
        projection_dispatcher=dispatcher,
    )

    outcome = appender.append_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        batch_kind="botlens_runtime_facts",
        payload=_canonical_payload(known_at="2026-04-19T12:00:00Z"),
    )

    assert outcome is not None
    assert outcome.consumer_results[0].consumer_name == "CanonicalFactProjectionDispatcher"
    assert outcome.consumer_results[0].result["queued"] is True
    assert consumer.started.wait(timeout=1.0)
    assert consumer.seqs == []

    consumer.release.set()
    appender.flush(reason="test", shutdown=True, timeout_s=2.0)

    assert consumer.seqs == [1]


def test_canonical_fact_projection_dispatcher_overflow_degrades_without_failing_run() -> None:
    seq = 0
    consumer = _BlockingConsumer()
    dispatcher = CanonicalFactProjectionDispatcher(
        consumers=(consumer,),
        queue_max=1,
        flush_interval_s=0.001,
        drain_timeout_s=2.0,
    )

    def _allocate() -> int:
        nonlocal seq
        seq += 1
        return seq

    appender = CanonicalFactAppender(
        allocate_seq=_allocate,
        append_batch=lambda **_kwargs: {"inserted_rows": 1},
        projection_dispatcher=dispatcher,
    )

    appender.append_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        batch_kind="botlens_runtime_facts",
        payload=_canonical_payload(known_at="2026-04-19T12:00:00Z"),
    )
    assert consumer.started.wait(timeout=1.0)
    appender.append_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        batch_kind="botlens_runtime_facts",
        payload=_canonical_payload(known_at="2026-04-19T12:01:00Z"),
    )

    degraded_outcome = appender.append_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        batch_kind="botlens_runtime_facts",
        payload=_canonical_payload(known_at="2026-04-19T12:02:00Z"),
    )
    assert degraded_outcome is not None
    assert degraded_outcome.consumer_results[0].error is None
    assert degraded_outcome.consumer_results[0].result["degraded"] is True
    assert degraded_outcome.consumer_results[0].result["overflow_policy"] == "drop_oldest"

    consumer.release.set()
    appender.flush(reason="test", shutdown=True, timeout_s=2.0)

    metrics = appender.metrics_snapshot()
    assert metrics["projection_overflow_count"] == 1.0
    assert metrics["projection_dropped_count"] == 1.0
    assert metrics["projection_degraded"] == 1.0


def test_canonical_fact_buffer_surfaces_writer_failure_on_drain() -> None:
    def _append_batches(_items):
        raise RuntimeError("db unavailable")

    buffer = CanonicalFactPersistenceBuffer(
        queue_max=4,
        batch_size=4,
        flush_interval_s=0.001,
        drain_timeout_s=1.0,
        append_batches=_append_batches,
    )
    appender = CanonicalFactAppender(
        allocate_seq=lambda: 1,
        persistence_buffer=buffer,
    )

    appender.append_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        batch_kind="botlens_runtime_facts",
        payload=_canonical_payload(known_at="2026-04-19T12:00:00Z"),
        dispatch=False,
    )

    with pytest.raises(RuntimeError, match="db unavailable"):
        appender.flush(reason="test", shutdown=True, timeout_s=1.0)


def test_canonical_fact_payload_filters_transport_and_observability_off_the_durable_path() -> None:
    filtered = canonical_fact_payload(
        {
            "facts": [
                {"fact_type": "runtime_state_observed", "runtime": {"status": "running"}},
                {"fact_type": "log_emitted", "log": {"message": "debug"}},
                {
                    "fact_type": "candle_upserted",
                    "series_key": "instrument-btc|1m",
                    "candle": {
                        "time": "2026-04-19T12:00:00Z",
                        "open": 1.0,
                        "high": 2.0,
                        "low": 0.5,
                        "close": 1.5,
                    },
                },
                {
                    "fact_type": "trade_opened",
                    "series_key": "instrument-btc|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "open",
                        "direction": "long",
                        "qty": 1.0,
                        "entry_price": 1.5,
                        "opened_at": "2026-04-19T12:00:00Z",
                        "bar_time": "2026-04-19T12:00:00Z",
                    },
                },
            ],
        }
    )

    assert [fact["fact_type"] for fact in filtered["facts"]] == ["trade_opened"]


def test_live_only_payload_is_sequenced_and_dispatched_without_durable_write() -> None:
    appended = False
    consumer = _RecordingConsumer()

    def _append(**_kwargs):
        nonlocal appended
        appended = True
        return {"inserted_rows": 1}

    appender = CanonicalFactAppender(
        allocate_seq=lambda: 9,
        append_batch=_append,
        consumers=(consumer,),
    )

    outcome = appender.append_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        batch_kind="botlens_runtime_facts",
        payload={
            "series_key": "instrument-btc|1m",
            "known_at": "2026-04-19T12:00:00Z",
            "event_time": "2026-04-19T12:00:00Z",
            "facts": [
                {"fact_type": "runtime_state_observed", "runtime": {"status": "running"}},
                {
                    "fact_type": "candle_upserted",
                    "series_key": "instrument-btc|1m",
                    "candle": {
                        "time": "2026-04-19T12:00:00Z",
                        "open": 1.0,
                        "high": 2.0,
                        "low": 0.5,
                        "close": 1.5,
                    },
                },
            ],
        },
    )

    assert outcome is not None
    assert outcome.batch.seq == 9
    assert outcome.batch.append_result["retention_action"] == "transport_only"
    assert consumer.seqs == [9]
    assert appended is False
