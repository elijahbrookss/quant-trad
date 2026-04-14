from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.storage.repos import observability


def test_metric_sample_row_uses_metric_record_model() -> None:
    row = observability._metric_sample_row(
        {
            "observed_at": "2026-04-14T20:24:46",
            "component": "observability_exporter",
            "metric_name": "viewer_send_ms",
            "metric_kind": "histogram",
            "value": 12.5,
            "run_id": "run-1",
            "labels": {"queue_name": "fanout"},
        }
    )

    assert row.metric_name == "viewer_send_ms"
    assert row.metric_kind == "histogram"
    assert row.value == 12.5
    assert row.run_id == "run-1"
    assert row.labels == {"queue_name": "fanout"}


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
