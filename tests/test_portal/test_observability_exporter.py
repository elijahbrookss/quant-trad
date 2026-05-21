from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.observability import BackendObserver, InMemoryObservabilitySink
from portal.backend.service.observability_exporter import ObservabilityExporter


@pytest.fixture
def capture_batches(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[dict]]:
    captured: dict[str, list[dict]] = {
        "metrics": [],
        "events": [],
    }

    def _capture_metrics(rows):
        captured["metrics"].extend(dict(row) for row in rows)
        return len(rows)

    def _capture_events(rows):
        captured["events"].extend(dict(row) for row in rows)
        return len(rows)

    monkeypatch.setattr(
        "portal.backend.service.observability_exporter.record_observability_metric_rollups_batch",
        _capture_metrics,
    )
    monkeypatch.setattr(
        "portal.backend.service.observability_exporter.record_observability_events_batch",
        _capture_events,
    )
    return captured


def test_exporter_flushes_metric_samples_and_events(capture_batches: dict[str, list[dict]]) -> None:
    sink = InMemoryObservabilitySink()
    observer = BackendObserver(component="test_component", sink=sink)
    exporter = ObservabilityExporter(
        sink=sink,
        metric_batch_size=10,
        event_batch_size=10,
        flush_interval_s=0.01,
        rollup_flush_lag_s=0.0,
        retry_interval_s=0.01,
    )

    observer.observe(
        "viewer_send_ms",
        12.5,
        bot_id="bot-1",
        run_id="run-1",
        series_key="instrument-btc|1m",
        queue_name="fanout_channel",
        source_reason="ingest",
        payload_size_bucket="large",
    )
    observer.event(
        "viewer_send_failed",
        run_id="run-1",
        series_key="instrument-btc|1m",
        failure_mode="connection_reset",
        bridge_session_id="bridge-1",
        bridge_seq=7,
        run_seq=9,
        message="send failed",
    )

    assert exporter.flush_once() is True
    assert capture_batches["metrics"]
    assert capture_batches["events"]

    metric_row = capture_batches["metrics"][0]
    assert metric_row["component"] == "test_component"
    assert metric_row["metric_name"] == "viewer_send_ms"
    assert metric_row["run_id"] == "run-1"
    assert metric_row["series_key"] == "instrument-btc|1m"
    assert metric_row["queue_name"] == "fanout_channel"
    assert metric_row["sample_count"] == 1
    assert metric_row["latest_value"] == 12.5
    assert metric_row["labels"] == {
        "source_reason": "ingest",
        "payload_size_bucket": "large",
    }

    event_row = capture_batches["events"][0]
    assert event_row["component"] == "test_component"
    assert event_row["event_name"] == "viewer_send_failed"
    assert event_row["run_id"] == "run-1"
    assert event_row["bridge_session_id"] == "bridge-1"
    assert event_row["bridge_seq"] == 7
    assert event_row["run_seq"] == 9
    assert event_row["message"] == "send failed"


def test_exporter_persists_overflow_event_when_pending_queue_drops(
    capture_batches: dict[str, list[dict]],
) -> None:
    sink = InMemoryObservabilitySink(pending_metrics_max=1, pending_events_max=1)
    observer = BackendObserver(component="test_component", sink=sink)
    exporter = ObservabilityExporter(
        sink=sink,
        metric_batch_size=10,
        event_batch_size=10,
        flush_interval_s=0.01,
        rollup_flush_lag_s=0.0,
        retry_interval_s=0.01,
    )

    observer.increment("metric_one", run_id="run-1")
    observer.increment("metric_two", run_id="run-1")
    observer.event("event_one", run_id="run-1")
    observer.event("event_two", run_id="run-1")

    assert exporter.flush_once() is True

    metric_names = [row["metric_name"] for row in capture_batches["metrics"]]
    event_names = [row["event_name"] for row in capture_batches["events"]]

    assert metric_names == ["metric_two"]
    assert "event_two" in event_names
    assert "observability_export_queue_overflow" in event_names


def test_exporter_coalesces_repeated_metric_samples_before_persistence(
    capture_batches: dict[str, list[dict]],
) -> None:
    sink = InMemoryObservabilitySink()
    observer = BackendObserver(component="test_component", sink=sink)
    exporter = ObservabilityExporter(
        sink=sink,
        metric_batch_size=10,
        event_batch_size=10,
        flush_interval_s=0.01,
        rollup_flush_lag_s=0.0,
        retry_interval_s=0.01,
    )

    observer.observe("run_notification_queue_wait_ms", 10.0, run_id="run-1")
    observer.observe("run_notification_queue_wait_ms", 20.0, run_id="run-1")
    observer.observe("run_notification_queue_wait_ms", 100.0, run_id="run-1")
    observer.increment("viewer_broadcast_total", run_id="run-1")
    observer.increment("viewer_broadcast_total", run_id="run-1")

    assert exporter.flush_once() is True

    rows = capture_batches["metrics"]
    queue_wait = next(row for row in rows if row["metric_name"] == "run_notification_queue_wait_ms")
    broadcasts = next(row for row in rows if row["metric_name"] == "viewer_broadcast_total")

    assert queue_wait["sample_count"] == 3
    assert queue_wait["value_min"] == 10.0
    assert queue_wait["value_max"] == 100.0
    assert queue_wait["p95_value"] == 100.0
    assert broadcasts["sample_count"] == 2
    assert broadcasts["value_sum"] == 2.0


def test_exporter_rollup_bounds_label_cardinality(capture_batches: dict[str, list[dict]]) -> None:
    sink = InMemoryObservabilitySink()
    observer = BackendObserver(component="test_component", sink=sink)
    exporter = ObservabilityExporter(
        sink=sink,
        metric_batch_size=10,
        event_batch_size=10,
        flush_interval_s=0.01,
        rollup_flush_lag_s=0.0,
        retry_interval_s=0.01,
    )

    observer.observe(
        "fanout_queue_wait_ms",
        10.0,
        run_id="run-1",
        source_reason="fanout",
        error="connection reset with unbounded details",
        trade_id="trade-1",
    )
    observer.observe(
        "fanout_queue_wait_ms",
        20.0,
        run_id="run-1",
        source_reason="fanout",
        error="different error should not split rollup",
        trade_id="trade-2",
    )

    assert exporter.flush_once() is True

    rows = [row for row in capture_batches["metrics"] if row["metric_name"] == "fanout_queue_wait_ms"]
    assert len(rows) == 1
    assert rows[0]["sample_count"] == 2
    assert rows[0]["labels"] == {"source_reason": "fanout"}


def test_exporter_high_volume_burst_writes_one_rollup_row(
    capture_batches: dict[str, list[dict]],
) -> None:
    sink = InMemoryObservabilitySink(pending_metrics_max=500)
    observer = BackendObserver(component="test_component", sink=sink)
    exporter = ObservabilityExporter(
        sink=sink,
        metric_batch_size=500,
        event_batch_size=10,
        flush_interval_s=0.01,
        rollup_flush_lag_s=0.0,
        retry_interval_s=0.01,
    )

    for value in range(100):
        observer.observe("symbol_fact_queue_wait_ms", float(value), run_id="run-1", queue_name="symbol_fact")

    assert exporter.flush_once() is True

    rows = [row for row in capture_batches["metrics"] if row["metric_name"] == "symbol_fact_queue_wait_ms"]
    assert len(rows) == 1
    assert rows[0]["sample_count"] == 100
    assert rows[0]["value_min"] == 0.0
    assert rows[0]["value_max"] == 99.0
    assert rows[0]["p99_value"] == 99.0


def test_exporter_skips_live_only_metric_families_with_policy_counters(
    capture_batches: dict[str, list[dict]],
) -> None:
    sink = InMemoryObservabilitySink(pending_metrics_max=500)
    observer = BackendObserver(component="botlens_run_stream", sink=sink)
    exporter = ObservabilityExporter(
        sink=sink,
        metric_batch_size=500,
        event_batch_size=10,
        flush_interval_s=0.01,
        rollup_flush_lag_s=0.0,
        retry_interval_s=0.01,
    )

    for value in range(101):
        observer.observe("viewer_broadcast_ms", float(value), run_id="run-1", message_kind="broadcast")

    assert exporter.flush_once() is True

    metric_names = [row["metric_name"] for row in capture_batches["metrics"]]
    assert "viewer_broadcast_ms" not in metric_names
    assert "observability_live_only_metric_records_skipped" in metric_names
    assert "observability_live_only_raw_samples_skipped" in metric_names

    skipped_records = next(
        row
        for row in capture_batches["metrics"]
        if row["metric_name"] == "observability_live_only_metric_records_skipped"
    )
    skipped_samples = next(
        row
        for row in capture_batches["metrics"]
        if row["metric_name"] == "observability_live_only_raw_samples_skipped"
    )
    assert skipped_records["value_sum"] == 2.0
    assert skipped_samples["value_sum"] == 101.0


def test_exporter_coalesces_repeated_overflow_events_before_persistence(
    capture_batches: dict[str, list[dict]],
) -> None:
    sink = InMemoryObservabilitySink()
    observer = BackendObserver(component="test_component", sink=sink)
    exporter = ObservabilityExporter(
        sink=sink,
        metric_batch_size=10,
        event_batch_size=10,
        flush_interval_s=0.01,
        rollup_flush_lag_s=0.0,
        retry_interval_s=0.01,
    )

    observer.event(
        "run_notification_queue_overflow",
        run_id="run-1",
        queue_name="run_notification_queue",
        depth=512,
    )
    observer.event(
        "run_notification_queue_overflow",
        run_id="run-1",
        queue_name="run_notification_queue",
        depth=512,
    )
    observer.event("viewer_send_failed", run_id="run-1", failure_mode="connection_reset")

    assert exporter.flush_once() is True

    event_names = [row["event_name"] for row in capture_batches["events"]]
    assert event_names.count("run_notification_queue_overflow") == 1
    assert event_names.count("viewer_send_failed") == 1

    overflow = next(
        row
        for row in capture_batches["events"]
        if row["event_name"] == "run_notification_queue_overflow"
    )
    assert overflow["details"]["export_aggregation"] == "batch_latest"
    assert overflow["details"]["export_sample_count"] == 2
    assert overflow["details"]["suppressed_duplicate_events"] == 1


def test_exporter_records_export_write_metrics_without_recursive_growth(
    capture_batches: dict[str, list[dict]],
) -> None:
    sink = InMemoryObservabilitySink()
    observer = BackendObserver(component="test_component", sink=sink)
    exporter = ObservabilityExporter(
        sink=sink,
        metric_batch_size=10,
        event_batch_size=10,
        flush_interval_s=0.01,
        rollup_flush_lag_s=0.0,
        retry_interval_s=0.01,
    )

    observer.observe("run_notification_queue_wait_ms", 42.0, run_id="run-1")
    observer.event("viewer_send_failed", run_id="run-1")

    assert exporter.flush_once() is True
    assert [row["metric_name"] for row in capture_batches["metrics"]] == [
        "run_notification_queue_wait_ms",
    ]

    assert exporter.flush_once() is True
    metric_names = [row["metric_name"] for row in capture_batches["metrics"]]
    assert "observability_raw_samples_seen" in metric_names
    assert "observability_metric_records_seen" in metric_names
    assert "observability_rollup_rows_written" in metric_names
    assert "observability_rollup_reduction_ratio" in metric_names
    assert "observability_source_budget_reduction_ratio" in metric_names
    assert "observability_export_db_ms" in metric_names

    exporter_rows = [
        row
        for row in capture_batches["metrics"]
        if str(row["metric_name"]).startswith("observability_")
    ]
    assert {row["component"] for row in exporter_rows} == {"observability_exporter"}
    assert {row["run_id"] for row in exporter_rows} == {"run-1"}
    assert {row["pipeline_stage"] for row in exporter_rows} == {"observability_export"}
    assert {
        row["storage_target"] for row in exporter_rows
    } == {"observability_metric_rollups", "observability_events"}

    assert exporter.flush_once() is False
    assert len(
        [
            row
            for row in capture_batches["metrics"]
            if str(row["metric_name"]).startswith("observability_")
        ]
    ) == len(exporter_rows)
