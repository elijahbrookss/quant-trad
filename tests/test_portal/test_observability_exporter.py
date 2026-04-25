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
        "portal.backend.service.observability_exporter.record_observability_metric_samples_batch",
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
