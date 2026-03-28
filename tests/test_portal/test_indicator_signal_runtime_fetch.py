from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pandas", reason="pandas required for signal runtime fetch tests")

import pandas as pd

from portal.backend.service.indicators.indicator_service import signals
from portal.backend.service.indicators.indicator_service.runtime_contract import (
    SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT,
)


def _single_candle_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10.0],
        },
        index=pd.to_datetime(["2026-02-01T00:00:00Z"], utc=True),
    )


def test_signal_executor_uses_canonical_candle_service_with_instrument_context(monkeypatch) -> None:
    captured = {}

    def _fake_fetch(ctx, *, datasource=None, exchange=None):
        captured["ctx"] = ctx
        captured["datasource"] = datasource
        captured["exchange"] = exchange
        return _single_candle_frame()

    class _FakeEngine:
        output_types = {}

        def __init__(self, indicators):
            captured["indicators"] = indicators

        def step(self, *, bar, bar_time, include_overlays=False):
            _ = bar, bar_time, include_overlays
            return SimpleNamespace(outputs={})

    monkeypatch.setattr(
        signals.IndicatorSignalExecutor,
        "_load_meta",
        lambda self, inst_id: {
            "id": inst_id,
            "type": "fake_runtime_indicator",
            "runtime_supported": True,
            "typed_outputs": [],
            "datasource": "ALPACA",
            "exchange": "cme",
        },
    )
    monkeypatch.setattr(
        signals,
        "build_runtime_indicator_graph",
        lambda *args, **kwargs: (
            {str(args[0][0]): {"id": str(args[0][0]), "type": "fake_runtime_indicator"}},
            ["indicator"],
        ),
    )
    monkeypatch.setattr(signals.candle_service, "fetch_ohlcv_for_context", _fake_fetch)
    monkeypatch.setattr(signals, "IndicatorExecutionEngine", _FakeEngine)

    payload = signals.IndicatorSignalExecutor().execute(
        "indicator-1",
        "2026-02-01T00:00:00Z",
        "2026-02-01T01:00:00Z",
        "1h",
        symbol="ES",
        datasource="ALPACA",
        exchange="cme",
        instrument_id="instrument-1",
        config={},
    )

    assert payload["runtime_path"] == SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
    assert captured["ctx"].instrument_id == "instrument-1"
    assert captured["datasource"] == "ALPACA"
    assert captured["exchange"] == "cme"


def test_signal_executor_resolves_market_context_from_instrument(monkeypatch) -> None:
    captured = {}

    def _fake_fetch(ctx, *, datasource=None, exchange=None):
        captured["ctx"] = ctx
        captured["datasource"] = datasource
        captured["exchange"] = exchange
        return _single_candle_frame()

    class _FakeEngine:
        output_types = {}

        def __init__(self, indicators):
            _ = indicators

        def step(self, *, bar, bar_time, include_overlays=False):
            _ = bar, bar_time, include_overlays
            return SimpleNamespace(outputs={})

    monkeypatch.setattr(
        signals.IndicatorSignalExecutor,
        "_load_meta",
        lambda self, inst_id: {
            "id": inst_id,
            "type": "fake_runtime_indicator",
            "runtime_supported": True,
            "typed_outputs": [],
        },
    )
    monkeypatch.setattr(
        signals.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "BTCUSD",
            "datasource": "COINBASE",
            "exchange": "coinbase_direct",
        },
    )
    monkeypatch.setattr(
        signals,
        "build_runtime_indicator_graph",
        lambda *args, **kwargs: (
            {str(args[0][0]): {"id": str(args[0][0]), "type": "fake_runtime_indicator"}},
            ["indicator"],
        ),
    )
    monkeypatch.setattr(signals.candle_service, "fetch_ohlcv_for_context", _fake_fetch)
    monkeypatch.setattr(signals, "IndicatorExecutionEngine", _FakeEngine)

    payload = signals.IndicatorSignalExecutor().execute(
        "indicator-1",
        "2026-02-01T00:00:00Z",
        "2026-02-01T01:00:00Z",
        "1h",
        instrument_id="instrument-1",
        config={},
    )

    assert payload["runtime_path"] == SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
    assert captured["ctx"].symbol == "BTCUSD"
    assert captured["ctx"].instrument_id == "instrument-1"
    assert captured["datasource"] == "COINBASE"
    assert captured["exchange"] == "coinbase_direct"


def test_signal_executor_enriches_contract_fields_from_replay_context(monkeypatch) -> None:
    class _FakeEngine:
        output_types = {"indicator-1.balance_breakout": "signal"}

        def __init__(self, indicators):
            _ = indicators

        def step(self, *, bar, bar_time, include_overlays=False):
            _ = bar, bar_time, include_overlays
            return SimpleNamespace(
                outputs={
                    "indicator-1.balance_breakout": SimpleNamespace(
                        ready=True,
                        value={
                            "events": [
                                {
                                    "key": "balance_breakout_long",
                                    "direction": "long",
                                    "confidence": 0.8,
                                }
                            ]
                        },
                    )
                }
            )

    monkeypatch.setattr(
        signals.IndicatorSignalExecutor,
        "_load_meta",
        lambda self, inst_id: {
            "id": inst_id,
            "type": "fake_runtime_indicator",
            "runtime_supported": True,
            "typed_outputs": [{"name": "balance_breakout", "type": "signal", "enabled": True}],
            "datasource": "ALPACA",
            "exchange": "cme",
        },
    )
    monkeypatch.setattr(
        signals,
        "build_runtime_indicator_graph",
        lambda *args, **kwargs: (
            {str(args[0][0]): {"id": str(args[0][0]), "type": "fake_runtime_indicator"}},
            ["indicator"],
        ),
    )
    monkeypatch.setattr(
        signals.candle_service,
        "fetch_ohlcv_for_context",
        lambda *args, **kwargs: _single_candle_frame(),
    )
    monkeypatch.setattr(signals, "IndicatorExecutionEngine", _FakeEngine)

    payload = signals.IndicatorSignalExecutor().execute(
        "indicator-1",
        "2026-02-01T00:00:00Z",
        "2026-02-01T01:00:00Z",
        "1h",
        symbol="ES",
        datasource="ALPACA",
        exchange="cme",
        instrument_id="instrument-1",
        config={},
    )

    assert payload["runtime_path"] == SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
    event = payload["signals"][0]
    assert event["signal_id"].startswith("sig_")
    assert event["timeframe_seconds"] == 3600
    assert event["series_key"] == "instrument-1|1h"
    assert event["event_time"] == "2026-02-01T00:00:00Z"
    assert event["known_at"] == "2026-02-01T00:00:00Z"
    assert "pattern_id" not in event
    assert event["metadata"] == {"datasource": "ALPACA", "exchange": "cme"}
    assert len(payload["overlays"]) == 1
    overlay = payload["overlays"][0]
    assert overlay["type"] == "indicator_signal"
    assert overlay["source"] == "signal"
    assert overlay["overlay_name"] == "balance_breakout"
    assert overlay["payload"]["bubbles"][0]["time"] == 1769904000
    assert overlay["payload"]["bubbles"][0]["price"] == 100.5
    assert overlay["payload"]["bubbles"][0]["meta"] == "Balance Breakout"
    assert overlay["payload"]["bubbles"][0]["signal_id"] == event["signal_id"]


def test_signal_executor_preserves_event_contract_metadata_when_present(monkeypatch) -> None:
    class _FakeEngine:
        output_types = {"indicator-1.balance_breakout": "signal"}

        def __init__(self, indicators):
            _ = indicators

        def step(self, *, bar, bar_time, include_overlays=False):
            _ = bar, bar_time, include_overlays
            return SimpleNamespace(
                outputs={
                    "indicator-1.balance_breakout": SimpleNamespace(
                        ready=True,
                        value={
                            "events": [
                                {
                                    "key": "balance_breakout_long",
                                    "pattern_id": "balance_breakout_v2",
                                    "known_at": "2026-02-01T00:00:00Z",
                                    "metadata": {
                                        "trace_id": "trace-1",
                                        "trigger_price": 100.5,
                                        "reference": {
                                            "kind": "price_level",
                                            "family": "value_area",
                                            "name": "VAH",
                                            "label": "VAH",
                                            "price": 101.25,
                                            "precision": 2,
                                            "source": "market_profile",
                                            "key": "profile-1",
                                            "context": {
                                                "profile_key": "profile-1",
                                            },
                                        },
                                    },
                                }
                            ]
                        },
                    )
                }
            )

    monkeypatch.setattr(
        signals.IndicatorSignalExecutor,
        "_load_meta",
        lambda self, inst_id: {
            "id": inst_id,
            "type": "fake_runtime_indicator",
            "runtime_supported": True,
            "typed_outputs": [{"name": "balance_breakout", "type": "signal", "enabled": True}],
            "datasource": "ALPACA",
            "exchange": "cme",
        },
    )
    monkeypatch.setattr(
        signals,
        "build_runtime_indicator_graph",
        lambda *args, **kwargs: (
            {str(args[0][0]): {"id": str(args[0][0]), "type": "fake_runtime_indicator"}},
            ["indicator"],
        ),
    )
    monkeypatch.setattr(
        signals.candle_service,
        "fetch_ohlcv_for_context",
        lambda *args, **kwargs: _single_candle_frame(),
    )
    monkeypatch.setattr(signals, "IndicatorExecutionEngine", _FakeEngine)

    payload = signals.IndicatorSignalExecutor().execute(
        "indicator-1",
        "2026-02-01T00:00:00Z",
        "2026-02-01T01:00:00Z",
        "1h",
        symbol="ES",
        datasource="ALPACA",
        exchange="cme",
        instrument_id="instrument-1",
        config={},
    )

    event = payload["signals"][0]
    assert event["signal_id"].startswith("sig_")
    assert event["pattern_id"] == "balance_breakout_v2"
    assert event["series_key"] == "instrument-1|1h"
    assert event["known_at"] == "2026-02-01T00:00:00Z"
    assert event["timeframe_seconds"] == 3600
    assert "rule_id" not in event
    assert event["metadata"] == {
        "trace_id": "trace-1",
        "trigger_price": 100.5,
        "reference": {
            "kind": "price_level",
            "family": "value_area",
            "name": "VAH",
            "label": "VAH",
            "price": 101.25,
            "precision": 2,
            "source": "market_profile",
            "key": "profile-1",
            "context": {
                "profile_key": "profile-1",
            },
        },
        "datasource": "ALPACA",
        "exchange": "cme",
    }
    bubble = payload["overlays"][0]["payload"]["bubbles"][0]
    assert bubble["signal_id"] == event["signal_id"]
    assert bubble["label"] == "Balance Breakout Long"
    assert bubble["meta"] == "VAH 101.25"
    assert bubble["detail"] == "Trigger 100.50"
    assert bubble["trigger_price"] == 100.5
    assert bubble["reference"] == {
        "kind": "price_level",
        "family": "value_area",
        "name": "VAH",
        "label": "VAH",
        "price": 101.25,
        "precision": 2,
        "source": "market_profile",
        "key": "profile-1",
        "context": {
            "profile_key": "profile-1",
        },
    }
