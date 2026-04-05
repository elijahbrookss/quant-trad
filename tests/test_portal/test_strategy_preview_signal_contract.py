from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("pandas")

import pandas as pd

from engines.indicator_engine.contracts import RuntimeOutput
from portal.backend.service.strategies.strategy_service import facade, typed_preview


def _single_bar_frame() -> pd.DataFrame:
    timestamp = pd.to_datetime(["2026-02-01T00:00:00Z"], utc=True)
    return pd.DataFrame(
        {
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10.0],
        },
        index=timestamp,
    )


def test_strategy_preview_response_separates_machine_and_ui(monkeypatch) -> None:
    selected_artifact = {
        "decision_id": "decision-1",
        "strategy_id": "strategy-1",
        "strategy_hash": "hash-1",
        "instrument_id": "instrument-1",
        "symbol": "ES",
        "timeframe": "1h",
        "bar_epoch": 1769904000,
        "bar_time": "2026-02-01T00:00:00Z",
        "decision_time": "2026-02-01T00:00:00Z",
        "rule_id": "rule-1",
        "rule_name": "Breakout Long",
        "evaluation_result": "matched_selected",
        "emitted_intent": "enter_long",
        "trigger": {
            "event_key": "breakout_long",
            "output_ref": "indicator-1.balance_breakout",
        },
        "guard_results": [
            {
                "type": "context_match",
                "output_ref": "indicator-1.market_state",
                "field": "state",
                "expected": ["trend"],
                "actual": "trend",
                "ready": True,
                "matched": True,
            },
            {
                "type": "metric_match",
                "output_ref": "indicator-1.profile_stats",
                "field": "width",
                "operator": ">=",
                "expected": 10,
                "actual": 12.5,
                "ready": True,
                "matched": True,
            },
        ],
    }

    class _FakeEngine:
        output_types = {
            "indicator-1.balance_breakout": "signal",
            "indicator-1.market_state": "context",
            "indicator-1.profile_stats": "metric",
        }

        def __init__(self, indicators):
            _ = indicators

        def step(self, *, bar, bar_time, include_overlays=False):
            _ = bar, bar_time, include_overlays
            return SimpleNamespace(
                outputs={
                    "indicator-1.balance_breakout": RuntimeOutput(
                        bar_time=bar_time,
                        ready=True,
                        value={
                            "events": [
                                {
                                    "key": "breakout_long",
                                    "direction": "long",
                                    "known_at": "2026-02-01T00:00:00Z",
                                }
                            ]
                        },
                    ),
                    "indicator-1.market_state": RuntimeOutput(
                        bar_time=bar_time,
                        ready=True,
                        value={"state": "trend", "bias": "long"},
                    ),
                    "indicator-1.profile_stats": RuntimeOutput(
                        bar_time=bar_time,
                        ready=True,
                        value={"width": 12.5, "poc": 100.25},
                    ),
                },
                overlays={},
            )

    monkeypatch.setattr(
        typed_preview.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ES",
            "datasource": "ALPACA",
            "exchange": "cme",
        },
    )
    monkeypatch.setattr(
        typed_preview,
        "get_instance_meta",
        lambda indicator_id: {
            "id": indicator_id,
            "runtime_supported": True,
        },
    )
    monkeypatch.setattr(
        typed_preview,
        "build_runtime_indicator_graph",
        lambda *args, **kwargs: ({}, ["indicator"]),
    )
    monkeypatch.setattr(typed_preview, "IndicatorExecutionEngine", _FakeEngine)
    monkeypatch.setattr(
        typed_preview.candle_service,
        "fetch_ohlcv_by_instrument",
        lambda *args, **kwargs: _single_bar_frame(),
    )
    monkeypatch.setattr(
        typed_preview,
        "evaluate_strategy_bar",
        lambda **kwargs: SimpleNamespace(
            artifacts=[dict(selected_artifact)],
            selected_artifact=dict(selected_artifact),
        ),
    )

    payload = typed_preview.evaluate_strategy_preview(
        record=SimpleNamespace(name="Strategy One", indicator_ids=["indicator-1"], rules={}),
        strategy_id="strategy-1",
        preview_id="preview-1",
        start="2026-02-01T00:00:00Z",
        end="2026-02-01T01:00:00Z",
        interval="1h",
        instrument_ids=["instrument-1"],
        compiled_strategy=SimpleNamespace(strategy_hash="hash-1"),
        selected_variant={
            "id": "variant-default",
            "name": "default",
            "param_overrides": {"conviction_min": 0.5},
            "is_default": True,
        },
        resolved_params={"conviction_min": 0.5},
    )

    assert payload["preview_id"] == "preview-1"
    assert payload["source_type"] == "strategy_preview"
    assert payload["strategy_hash"] == "hash-1"
    assert payload["variant"]["id"] == "variant-default"
    assert payload["variant"]["resolved_params"] == {"conviction_min": 0.5}
    instrument_payload = payload["instruments"]["instrument-1"]
    signal = instrument_payload["machine"]["signals"][0]
    assert signal == {
        "signal_id": "decision-1",
        "source_type": "strategy_preview",
        "source_id": "preview-1",
        "decision_id": "decision-1",
        "strategy_id": "strategy-1",
        "strategy_hash": "hash-1",
        "instrument_id": "instrument-1",
        "symbol": "ES",
        "timeframe": "1h",
        "bar_epoch": 1769904000,
        "bar_time": "2026-02-01T00:00:00Z",
        "decision_time": "2026-02-01T00:00:00Z",
        "rule_id": "rule-1",
        "rule_name": "Breakout Long",
        "intent": "enter_long",
        "direction": "long",
        "event_key": "breakout_long",
    }
    assert instrument_payload["signals"] == instrument_payload["machine"]["signals"]
    assert instrument_payload["decision_artifacts"] == instrument_payload["machine"]["decision_artifacts"]
    assert instrument_payload["overlays"] == instrument_payload["ui"]["overlays"]
    selected_artifact_payload = instrument_payload["machine"]["decision_artifacts"][0]
    assert sorted(selected_artifact_payload["observed_outputs"].keys()) == [
        "indicator-1.balance_breakout",
        "indicator-1.market_state",
        "indicator-1.profile_stats",
    ]
    assert sorted(selected_artifact_payload["referenced_outputs"].keys()) == [
        "indicator-1.balance_breakout",
        "indicator-1.market_state",
        "indicator-1.profile_stats",
    ]
    assert selected_artifact_payload["observed_outputs"]["indicator-1.balance_breakout"]["event_keys"] == ["breakout_long"]
    assert selected_artifact_payload["observed_outputs"]["indicator-1.market_state"]["fields"] == {
        "state": "trend",
        "bias": "long",
    }
    assert selected_artifact_payload["observed_outputs"]["indicator-1.profile_stats"]["fields"] == {
        "width": 12.5,
        "poc": 100.25,
    }
    markers = instrument_payload["ui"]["overlays"][-1]["payload"]["markers"]
    assert markers[0]["signal_id"] == "decision-1"
    assert markers[0]["source_id"] == "preview-1"


def test_strategy_preview_store_returns_signal_detail() -> None:
    store = facade.StrategyPreviewStore()
    payload = {
        "preview_id": "preview-1",
        "strategy_id": "strategy-1",
        "instruments": {
            "instrument-1": {
                "instrument_id": "instrument-1",
                "symbol": "ES",
                "window": {"start": "2026-02-01T00:00:00Z", "end": "2026-02-01T01:00:00Z"},
                "status": "ok",
                "missing_indicators": [],
                "machine": {
                    "signals": [
                        {
                            "signal_id": "decision-1",
                            "source_type": "strategy_preview",
                            "source_id": "preview-1",
                            "decision_id": "decision-1",
                        }
                    ],
                    "decision_artifacts": [
                        {
                            "decision_id": "decision-1",
                            "rule_id": "rule-1",
                            "observed_outputs": {
                                "indicator-1.market_state": {
                                    "type": "context",
                                    "ready": True,
                                    "fields": {"state": "trend"},
                                }
                            },
                            "referenced_outputs": {
                                "indicator-1.market_state": {
                                    "type": "context",
                                    "ready": True,
                                    "fields": {"state": "trend"},
                                }
                            },
                        }
                    ],
                },
                "overlays": [
                    {
                        "payload": {
                            "markers": [
                                {
                                    "signal_id": "decision-1",
                                    "time": 1769904000,
                                }
                            ]
                        }
                    }
                ],
            }
        },
    }

    store.put(payload)
    detail = store.get_signal_detail("strategy-1", "preview-1", "decision-1")

    assert detail["signal"]["signal_id"] == "decision-1"
    assert detail["signal"]["source_id"] == "preview-1"
    assert detail["audit"]["decision_artifact"]["rule_id"] == "rule-1"
    assert detail["audit"]["decision_artifact"]["observed_outputs"] == {
        "indicator-1.market_state": {
            "type": "context",
            "ready": True,
            "fields": {"state": "trend"},
        }
    }
    assert detail["audit"]["decision_artifact"]["referenced_outputs"] == {
        "indicator-1.market_state": {
            "type": "context",
            "ready": True,
            "fields": {"state": "trend"},
        }
    }
    assert detail["ui"]["markers"] == [{"signal_id": "decision-1", "time": 1769904000}]
