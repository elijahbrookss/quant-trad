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


def _indicator_stub() -> SimpleNamespace:
    return SimpleNamespace(
        configure_overlay_history=lambda *, history_bars: None
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
        lambda *args, **kwargs: ({}, [_indicator_stub()]),
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


def test_runtime_output_evidence_collects_per_bar_declared_values(monkeypatch) -> None:
    monkeypatch.setattr(runtime_validation, "load_indicator_record", lambda inst_id, ctx=None: {"id": inst_id})
    monkeypatch.setattr(
        runtime_validation,
        "build_meta_from_record",
        lambda record, ctx=None: {
            "id": record["id"],
            "type": "candle_stats",
            "name": "Candle Stats",
            "params": {"warmup_bars": 1},
            "dependencies": [],
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
        lambda *args, **kwargs: ({}, [_indicator_stub()]),
    )
    monkeypatch.setattr(runtime_validation.candle_service, "fetch_ohlcv_by_instrument", lambda *args, **kwargs: _frame())

    class _FakeEngine:
        output_types = {"indicator-1.candle_stats": "metric"}

        def __init__(self, indicators):
            _ = indicators
            self.calls = 0

        def step(self, *, bar, bar_time, include_overlays, include_details):
            _ = bar, include_overlays, include_details
            self.calls += 1
            return SimpleNamespace(
                outputs={
                    "indicator-1.candle_stats": RuntimeOutput(
                        bar_time=bar_time,
                        ready=True,
                        value={"range_pct": 0.01 * self.calls},
                    )
                },
                guard_metrics=(),
                guard_warnings=(),
            )

    monkeypatch.setattr(runtime_validation, "IndicatorExecutionEngine", _FakeEngine)

    payload = runtime_validation.collect_runtime_output_evidence_for_instance(
        "indicator-1",
        "2026-02-01T00:00:00Z",
        "2026-02-01T02:00:00Z",
        "1h",
        instrument_id="instrument-1",
    )

    assert payload["schema_version"] == "indicator_output_evidence.v1"
    assert payload["bars_evaluated"] == 2
    assert payload["ready_counts"] == {"candle_stats": 2}
    assert payload["candles"][0]["time"] == "2026-02-01T00:00:00Z"
    assert payload["outputs"][1]["value"] == {"range_pct": 0.02}


def test_runtime_output_evidence_applies_explicit_param_overrides(monkeypatch) -> None:
    captured = {}

    monkeypatch.setattr(runtime_validation, "load_indicator_record", lambda inst_id, ctx=None: {"id": inst_id})
    monkeypatch.setattr(
        runtime_validation,
        "build_meta_from_record",
        lambda record, ctx=None: {
            "id": record["id"],
            "type": "candle_stats",
            "name": "Candle Stats",
            "params": {"warmup_bars": 1},
            "dependencies": [],
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

    def fake_build_runtime_indicator_graph(*args, **kwargs):
        captured["preloaded_metas"] = kwargs["preloaded_metas"]
        return {}, [_indicator_stub()]

    monkeypatch.setattr(runtime_validation, "build_runtime_indicator_graph", fake_build_runtime_indicator_graph)
    monkeypatch.setattr(runtime_validation.candle_service, "fetch_ohlcv_by_instrument", lambda *args, **kwargs: _frame())

    class _FakeEngine:
        output_types = {"indicator-1.candle_stats": "metric"}

        def __init__(self, indicators):
            _ = indicators

        def step(self, *, bar, bar_time, include_overlays, include_details):
            _ = bar, include_overlays, include_details
            return SimpleNamespace(
                outputs={
                    "indicator-1.candle_stats": RuntimeOutput(
                        bar_time=bar_time,
                        ready=True,
                        value={"range_pct": 0.02},
                    )
                },
                guard_metrics=(),
                guard_warnings=(),
            )

    monkeypatch.setattr(runtime_validation, "IndicatorExecutionEngine", _FakeEngine)

    payload = runtime_validation.collect_runtime_output_evidence_for_instance(
        "indicator-1",
        "2026-02-01T00:00:00Z",
        "2026-02-01T02:00:00Z",
        "1h",
        instrument_id="instrument-1",
        indicator_param_overrides={"warmup_bars": 2},
    )

    assert captured["preloaded_metas"]["indicator-1"]["params"] == {"warmup_bars": 2}
    assert payload["indicator"]["base_params"] == {"warmup_bars": 1}
    assert payload["indicator"]["params"] == {"warmup_bars": 2}
    assert payload["indicator"]["param_overrides"] == {"warmup_bars": 2}


def test_runtime_output_evidence_injects_declared_market_inputs_per_bar(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        runtime_validation,
        "load_indicator_record",
        lambda inst_id, ctx=None: {"id": inst_id},
    )
    monkeypatch.setattr(
        runtime_validation,
        "build_meta_from_record",
        lambda record, ctx=None: {
            "id": record["id"],
            "type": "candle_stats",
            "name": "Candle Stats",
            "params": {},
            "dependencies": [],
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
        lambda *args, **kwargs: ({}, [_indicator_stub()]),
    )
    monkeypatch.setattr(
        runtime_validation.candle_service,
        "fetch_ohlcv_by_instrument",
        lambda *args, **kwargs: _frame(),
    )

    class _Resolver:
        calls = []

        def resolve(self, **kwargs):
            self.calls.append(kwargs)
            return {"indicator-1": {"reference_price": {"value": 100}}}

    observed = []

    class _FakeEngine:
        output_types = {"indicator-1.candle_stats": "metric"}

        def __init__(self, indicators):
            _ = indicators

        def step(self, *, bar, bar_time, include_overlays, include_details, market_data_inputs):
            _ = bar, include_overlays, include_details
            observed.append(market_data_inputs)
            return SimpleNamespace(
                outputs={
                    "indicator-1.candle_stats": RuntimeOutput(
                        bar_time=bar_time,
                        ready=True,
                        value={"range_pct": 0.02},
                    )
                },
                guard_metrics=(),
                guard_warnings=(),
            )

    monkeypatch.setattr(runtime_validation, "IndicatorExecutionEngine", _FakeEngine)
    resolver = _Resolver()
    declarations = {
        "indicator-1": (
            {
                "key": "reference_price",
                "fact_type": "market.reference_price",
            },
        )
    }

    payload = runtime_validation.collect_runtime_output_evidence_for_instance(
        "indicator-1",
        "2026-02-01T00:00:00Z",
        "2026-02-01T02:00:00Z",
        "1h",
        instrument_id="instrument-1",
        market_data_resolver=resolver,
        market_data_requirements_by_consumer=declarations,
    )

    assert len(resolver.calls) == 2
    assert observed == [
        {"indicator-1": {"reference_price": {"value": 100}}},
        {"indicator-1": {"reference_price": {"value": 100}}},
    ]
    assert payload["candles"][0]["close_time"] == "2026-02-01T01:00:00Z"
    assert payload["candles"][0]["known_at"] == "2026-02-01T01:00:00Z"


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
    monkeypatch.setattr(
        runtime_validation,
        "build_runtime_indicator_graph",
        lambda *args, **kwargs: ({}, [_indicator_stub()]),
    )
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
