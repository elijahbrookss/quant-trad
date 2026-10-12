from __future__ import annotations

import pandas as pd
import pytest

from core.candle_continuity import summarize_candle_continuity
from portal.backend.service.market import candle_service as service


@pytest.fixture
def source(monkeypatch):
    frame = pd.DataFrame({
        "timestamp": pd.to_datetime([
            "2022-01-01T00:01:00Z", "2022-01-01T00:01:00Z",
            "2022-01-01T00:02:00Z", "2022-01-01T00:05:00Z",
        ]),
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
    })
    frame.attrs["gap_classification"] = [{
        "start": "2022-01-01T00:03:00Z", "end": "2022-01-01T00:05:00Z",
        "classification": "provider_missing_data", "reason_code": "provider_missing_data",
    }]
    frame.attrs["provider_call_performed"] = False
    monkeypatch.setattr(service.instrument_service, "get_instrument_record", lambda _: {
        "id": "fixture", "symbol": "SYNTH", "datasource": "fixture", "exchange": "fixture",
    })
    monkeypatch.setattr(service.canonical_candle_feed, "read_by_instrument", lambda *a, **kw: frame)
    return frame


def test_coverage_preserves_gap_evidence_and_boundaries_without_runtime_features(source, monkeypatch):
    def forbidden_features(_frame):
        raise AssertionError("coverage must not compute runtime features")

    monkeypatch.setattr(service, "_with_runtime_candle_features", forbidden_features)
    result = service.preflight_candle_coverage_by_instrument(
        "fixture", "2022-01-01T00:00:00Z", "2022-01-01T00:07:00Z", "1m",
    )
    reference = summarize_candle_continuity(
        [{"time": item.isoformat()} for item in source["timestamp"].sort_values()],
        expected_interval_seconds_value=60,
        gap_classification=source.attrs["gap_classification"],
    ).to_dict()
    assert result["continuity"] == reference
    assert result["row_count"] == 4
    assert result["status"] == "warning"
    assert result["missing_ranges"] == [
        {"start": "2022-01-01T00:00:00Z", "end": "2022-01-01T00:01:00Z"},
        {"start": "2022-01-01T00:06:00Z", "end": "2022-01-01T00:07:00Z"},
    ]
    assert "tr" not in source and "atr_wilder" not in source


def test_normal_candle_reads_still_include_runtime_features(source):
    result = service.fetch_ohlcv_by_instrument(
        "fixture", "2022-01-01T00:00:00Z", "2022-01-01T00:07:00Z", "1m",
    )
    assert "tr" in result and "atr_wilder" in result
    assert result.attrs["gap_classification"] == source.attrs["gap_classification"]
    assert result.attrs["derived_candle_features"]["schema_version"]
    assert "tr" not in source


def test_coverage_uses_preview_watermark_without_runtime_features(source, monkeypatch):
    observed = {}
    def read(*args, **kwargs):
        observed.update(kwargs)
        return source
    monkeypatch.setattr(service.canonical_candle_feed, "read_by_instrument", read)
    with service.market_data_preview_read_scope(as_of_commit_seq=73, source_revision="test"):
        result = service.preflight_candle_coverage_by_instrument(
            "fixture", "2022-01-01T00:00:00Z", "2022-01-01T00:07:00Z", "1m",
        )
    assert result["row_count"] == 4
    assert observed["as_of_commit_seq"] == 73


def test_coverage_retains_empty_and_failed_read_states(source, monkeypatch):
    monkeypatch.setattr(service.canonical_candle_feed, "read_by_instrument", lambda *a, **kw: source.iloc[:0])
    result = service.preflight_candle_coverage_by_instrument(
        "fixture", "2022-01-01T00:00:00Z", "2022-01-01T00:07:00Z", "1m",
    )
    assert result["row_count"] == 0
    assert result["continuity"]["final_status"] == "missing"
    def failed(*args, **kwargs):
        raise RuntimeError("fixture read failed")
    monkeypatch.setattr(service.canonical_candle_feed, "read_by_instrument", failed)
    result = service.preflight_candle_coverage_by_instrument(
        "fixture", "2022-01-01T00:00:00Z", "2022-01-01T00:07:00Z", "1m",
    )
    assert result["status"] == "error"
    assert "fixture read failed" in result["message"]
