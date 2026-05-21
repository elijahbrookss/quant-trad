from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.storage.repos import observability


def test_raw_metric_sample_rows_are_not_part_of_storage_contract() -> None:
    assert not hasattr(observability, "_metric_sample_row")


def test_metric_rollup_row_normalizes_bucket_and_bounded_labels() -> None:
    row = observability._metric_rollup_row(
        {
            "observed_at": "2026-04-14T20:24:46Z",
            "component": "observability_exporter",
            "metric_name": "viewer_send_ms",
            "metric_kind": "histogram",
            "value_sum": 30.0,
            "value_min": 10.0,
            "value_max": 20.0,
            "latest_value": 20.0,
            "p95_value": 20.0,
            "p99_value": 20.0,
            "sample_count": 2,
            "raw_sample_count": 2,
            "source_metric_record_count": 1,
            "run_id": "run-1",
            "labels": {"source_reason": "fanout", "trade_id": "trade-1"},
        }
    )

    assert row.metric_name == "viewer_send_ms"
    assert row.bucket_seconds == 10
    assert row.bucket_start.isoformat() == "2026-04-14T20:24:40"
    assert row.sample_count == 2
    assert row.raw_sample_count == 2
    assert row.source_metric_record_count == 1
    assert row.value_sum == 30.0
    assert row.labels == {"source_reason": "fanout"}
    assert row.label_hash != "none"


def test_event_row_uses_event_record_model() -> None:
    row = observability._event_row(
        {
            "observed_at": "2026-04-14T20:24:46",
            "component": "observability_exporter",
            "event_name": "viewer_send_failed",
            "level": "WARN",
            "run_id": "run-1",
            "bridge_seq": 7,
            "details": {"error": "connection reset"},
        }
    )

    assert row.event_name == "viewer_send_failed"
    assert row.level == "WARN"
    assert row.run_id == "run-1"
    assert row.bridge_seq == 7
    assert row.details == {"error": "connection reset"}
