from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("pandas")

import pandas as pd

from engines.indicator_engine.contracts import RuntimeOutput
from portal.backend.service.indicators.indicator_service import runtime_validation


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [10.0, 11.0],
        },
        index=pd.to_datetime(["2026-02-01T00:00:00Z", "2026-02-01T01:00:00Z"], utc=True),
    )


def test_runtime_validation_summarizes_output_presence_and_readiness(monkeypatch) -> None:
    monkeypatch.setattr(runtime_validation, "load_indicator_record", lambda inst_id, ctx=None: {"id": inst_id})
    monkeypatch.setattr(
        runtime_validation,
        "build_meta_from_record",
        lambda record, ctx=None: {
            "id": record["id"],
            "type": "candle_stats",
            "runtime_supported": True,
        },
    )
    monkeypatch.setattr(
        runtime_validation.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ES",
            "datasource": "ALPACA",
            "exchange": "cme",
        },
    )
    monkeypatch.setattr(
        runtime_validation,
        "build_runtime_indicator_graph",
        lambda *args, **kwargs: ({}, ["indicator"]),
    )
    monkeypatch.setattr(
        runtime_validation.candle_service,
        "fetch_ohlcv_by_instrument",
        lambda *args, **kwargs: _frame(),
    )

    class _FakeEngine:
        output_types = {"indicator-1.candle_stats": "metric"}

        def __init__(self, indicators):
            _ = indicators
            self.calls = 0

        def step(self, *, bar, bar_time, include_overlays, include_details):
            _ = bar, include_overlays, include_details
            self.calls += 1
            ready = self.calls == 2
            return SimpleNamespace(
                outputs={
                    "indicator-1.candle_stats": RuntimeOutput(
                        bar_time=bar_time,
                        ready=ready,
                        value={"atr": 1.5} if ready else {},
                    )
                },
                guard_metrics=(),
                guard_warnings=(),
            )

    monkeypatch.setattr(runtime_validation, "IndicatorExecutionEngine", _FakeEngine)

    payload = runtime_validation.validate_runtime_for_instance(
        "indicator-1",
        "2026-02-01T00:00:00Z",
        "2026-02-01T02:00:00Z",
        "1h",
        instrument_id="instrument-1",
    )

    output = payload["outputs"]["indicator-1.candle_stats"]
    assert payload["schema_version"] == "indicator_runtime_validation.v1"
    assert payload["status"] == "passed"
    assert payload["bars_evaluated"] == 2
    assert output["present_bars"] == 2
    assert output["ready_bars"] == 1
    assert output["not_ready_bars"] == 1
    assert output["first_ready_at"] == "2026-02-01T01:00:00Z"
    assert output["observed_fields"] == ["atr"]


def test_runtime_validation_reports_readiness_assertion_failures(monkeypatch) -> None:
    monkeypatch.setattr(runtime_validation, "load_indicator_record", lambda inst_id, ctx=None: {"id": inst_id})
    monkeypatch.setattr(
        runtime_validation,
        "build_meta_from_record",
        lambda record, ctx=None: {
            "id": record["id"],
            "type": "candle_stats",
            "runtime_supported": True,
        },
    )
    monkeypatch.setattr(runtime_validation, "build_runtime_indicator_graph", lambda *args, **kwargs: ({}, ["indicator"]))
    monkeypatch.setattr(runtime_validation.candle_service, "fetch_ohlcv", lambda *args, **kwargs: _frame())

    class _NeverReadyEngine:
        output_types = {"indicator-1.candle_stats": "metric"}

        def __init__(self, indicators):
            _ = indicators

        def step(self, *, bar, bar_time, include_overlays, include_details):
            _ = bar, include_overlays, include_details
            return SimpleNamespace(
                outputs={
                    "indicator-1.candle_stats": RuntimeOutput(
                        bar_time=bar_time,
                        ready=False,
                        value={},
                    )
                },
                guard_metrics=(),
                guard_warnings=(),
            )

    monkeypatch.setattr(runtime_validation, "IndicatorExecutionEngine", _NeverReadyEngine)

    payload = runtime_validation.validate_runtime_for_instance(
        "indicator-1",
        "2026-02-01T00:00:00Z",
        "2026-02-01T02:00:00Z",
        "1h",
        symbol="ES",
        datasource="ALPACA",
        require_ready_by_end=True,
    )

    assert payload["status"] == "failed"
    assert payload["validation_errors"] == [
        {
            "code": "OUTPUT_NOT_READY_BY_END",
            "output_ref": "indicator-1.candle_stats",
            "last_ready_at": None,
        }
    ]
