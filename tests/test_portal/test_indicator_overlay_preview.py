from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("pandas")
import pandas as pd

from engines.bot_runtime.core.domain import Candle
from indicators.market_profile.compute.engine import MarketProfileIndicator
from portal.backend.service.indicators.indicator_service import api as indicator_api
from portal.backend.service.indicators.indicator_service.api import _collect_runtime_overlays
from indicators.market_profile.runtime.typed_indicator import TypedMarketProfileIndicator


def _runtime_overlay(value: dict, *, ready: bool = True):
    return SimpleNamespace(ready=ready, value=value)


def test_collect_runtime_overlays_transforms_market_profile_payload_at_current_epoch():
    overlays = _collect_runtime_overlays(
        {
            "ind.value_area": _runtime_overlay(
                {
                    "type": "market_profile",
                    "payload": {
                        "boxes": [],
                        "markers": [],
                        "bubbles": [],
                        "profiles": [
                            {
                                "start": 100,
                                "end": 110,
                                "VAH": 12.0,
                                "VAL": 10.0,
                                "POC": 11.0,
                                "session_count": 1,
                                "precision": 2,
                                "known_at": 110,
                                "formed_at": 110,
                            }
                        ],
                        "profile_params": {
                            "use_merged_value_areas": False,
                            "extend_value_area_to_chart_end": True,
                            "start": "1970-01-01T00:01:40Z",
                            "end": "1970-01-01T00:03:20Z",
                        },
                    },
                }
            )
        },
        current_epoch=200,
    )

    assert len(overlays) == 1
    payload = overlays[0]["payload"]
    assert payload["overlay_id"] == "ind.value_area"
    assert len(payload["boxes"]) == 1
    assert payload["boxes"][0]["x1"] == 100
    assert payload["boxes"][0]["x2"] == 200


def test_market_profile_runtime_indicator_emits_signal_output_without_overlay_markers():
    indicator = TypedMarketProfileIndicator(
        indicator_id="mp-1",
        version="v1",
        params={"bin_size": 1.0, "price_precision": 2},
        source_facts={
            "symbol": "ES",
            "profiles": [
                {
                    "start": 1735689600,
                    "end": 1735696800,
                    "VAH": 101.0,
                    "VAL": 99.0,
                    "POC": 100.0,
                    "session_count": 1,
                    "precision": 2,
                    "formed_at": 1735696800,
                    "known_at": 1735696800,
                }
            ],
            "profile_params": {
                "use_merged_value_areas": False,
                "extend_value_area_to_chart_end": True,
            },
        },
    )

    candles = [
        Candle(time=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0),
        Candle(time=datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc), open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0),
        Candle(time=datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc), open=103.0, high=104.0, low=103.0, close=104.0, volume=1.0),
    ]

    for candle in candles:
        indicator.apply_bar(candle, {})

    outputs = indicator.snapshot()
    overlays = indicator.overlay_snapshot()

    assert outputs["balance_breakout"].ready is True
    assert outputs["balance_breakout"].value["events"] == [
        {
            "key": "balance_breakout_long",
            "direction": "long",
            "metadata": {
                "trigger_price": 104.0,
                "reference": {
                    "kind": "price_level",
                    "family": "value_area",
                    "name": "VAH",
                    "label": "VAH",
                    "price": 101.0,
                    "precision": 2,
                    "source": "market_profile",
                    "key": "2025-01-01T00:00:00+00:00:2025-01-01T02:00:00+00:00:1",
                    "context": {
                        "profile_key": "2025-01-01T00:00:00+00:00:2025-01-01T02:00:00+00:00:1",
                        "active_value_area": {
                            "vah": 101.0,
                            "val": 99.0,
                            "poc": 100.0,
                        },
                    },
                },
            },
        }
    ]
    assert "value_area" in overlays
    assert set(overlays.keys()) == {"value_area"}
    assert overlays["value_area"].value["payload"]["markers"] == []


def test_market_profile_runtime_indicator_waits_until_projected_strategy_bar_close() -> None:
    indicator = TypedMarketProfileIndicator(
        indicator_id="mp-1",
        version="v1",
        params={"bin_size": 1.0, "price_precision": 2},
        source_facts={
            "symbol": "ES",
            "profiles": [
                {
                    "start": 1735725600,  # 2026-01-01 10:00 UTC
                    "end": 1735729200,    # 2026-01-01 11:00 UTC projected strategy boundary
                    "source_start": 1735725600,
                    "source_end": 1735727400,  # 2026-01-01 10:30 UTC source-session end
                    "VAH": 101.0,
                    "VAL": 99.0,
                    "POC": 100.0,
                    "session_count": 1,
                    "precision": 2,
                    "formed_at": 1735727400,
                    "known_at": 1735729200,
                }
            ],
            "profile_params": {
                "use_merged_value_areas": False,
                "extend_value_area_to_chart_end": True,
                "strategy_timeframe": "1h",
            },
        },
    )

    first_candle = Candle(
        time=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=1.0,
    )
    second_candle = Candle(
        time=datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=1.0,
    )

    indicator.apply_bar(first_candle, {})
    assert indicator.snapshot()["value_area_metrics"].ready is False

    indicator.apply_bar(second_candle, {})
    assert indicator.snapshot()["value_area_metrics"].ready is True


def test_market_profile_to_lightweight_profiles_include_known_at_for_preview_transform():
    index = pd.date_range("2026-01-01T00:00:00Z", periods=96, freq="30min")
    frame = pd.DataFrame(
        {
            "open": [100.0 + (i * 0.1) for i in range(len(index))],
            "high": [101.0 + (i * 0.1) for i in range(len(index))],
            "low": [99.0 + (i * 0.1) for i in range(len(index))],
            "close": [100.5 + (i * 0.1) for i in range(len(index))],
            "volume": [10.0 for _ in range(len(index))],
        },
        index=index,
    )

    indicator = MarketProfileIndicator(frame, use_merged_value_areas=False, extend_value_area_to_chart_end=True)

    payload = indicator.to_lightweight(frame)

    assert payload["profiles"]
    assert all(profile.get("formed_at") is not None for profile in payload["profiles"])
    assert all(profile.get("known_at") is not None for profile in payload["profiles"])


def test_overlay_preview_can_collect_state_at_requested_cursor_epoch(monkeypatch):
    index = pd.date_range("2026-03-01T00:00:00Z", periods=3, freq="1h")
    frame = pd.DataFrame(
        {
            "open": [10.0, 11.0, 12.0],
            "high": [11.0, 12.0, 13.0],
            "low": [9.0, 10.0, 11.0],
            "close": [10.5, 11.5, 12.5],
            "volume": [1.0, 1.0, 1.0],
        },
        index=index,
    )

    monkeypatch.setattr(
        indicator_api,
        "get_instance_meta",
        lambda inst_id, ctx=None: {
            "id": inst_id,
            "type": "market_profile",
            "runtime_supported": True,
            "datasource": "BINANCE",
            "exchange": "binance",
        },
    )
    monkeypatch.setattr(
        indicator_api,
        "build_runtime_indicator_graph",
        lambda *args, **kwargs: (None, [SimpleNamespace()]),
    )
    monkeypatch.setattr(
        indicator_api.candle_service,
        "fetch_ohlcv_by_instrument",
        lambda *args, **kwargs: frame,
    )

    fake_engine = None

    class FakeEngine:
        def __init__(self, indicators):
            self.calls = []

        def step(self, *, bar, bar_time, include_overlays):
            epoch = int(bar_time.timestamp())
            self.calls.append((epoch, include_overlays))
            overlays = {}
            if include_overlays:
                overlays = {
                    "indicator-1.cursor": SimpleNamespace(
                        ready=True,
                        value={
                            "type": "indicator_signal",
                            "payload": {
                                "bubbles": [
                                    {
                                        "time": epoch,
                                        "price": float(bar.close),
                                        "label": f"cursor-{epoch}",
                                    }
                                ]
                            },
                        },
                    )
                }
            return SimpleNamespace(overlays=overlays)

    def _engine_factory(indicators):
        nonlocal fake_engine
        fake_engine = FakeEngine(indicators)
        return fake_engine

    monkeypatch.setattr(indicator_api, "IndicatorExecutionEngine", _engine_factory)

    target_epoch = int(index[1].timestamp())
    payload = indicator_api.overlays_for_instance(
        "indicator-1",
        start=index[0].isoformat(),
        end=index[-1].isoformat(),
        interval="1h",
        symbol="BTC/USD",
        datasource="BINANCE",
        exchange="binance",
        instrument_id="instrument-1",
        overlay_options={"cursor_epoch": target_epoch},
    )

    assert payload["overlay_state"] == {
        "mode": "cursor",
        "cursor_epoch": target_epoch,
        "cursor_time": "2026-03-01T01:00:00Z",
        "requested_cursor_epoch": target_epoch,
    }
    assert fake_engine is not None
    assert fake_engine.calls == [
        (int(index[0].timestamp()), False),
        (target_epoch, True),
        (int(index[2].timestamp()), False),
    ]
    assert payload["overlays"][0]["payload"]["bubbles"][0]["time"] == target_epoch


def test_overlay_preview_rejects_cursor_epoch_not_aligned_to_window_candle(monkeypatch):
    index = pd.date_range("2026-03-01T00:00:00Z", periods=2, freq="1h")
    frame = pd.DataFrame(
        {
            "open": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.0, 10.0],
            "close": [10.5, 11.5],
            "volume": [1.0, 1.0],
        },
        index=index,
    )

    monkeypatch.setattr(
        indicator_api,
        "get_instance_meta",
        lambda inst_id, ctx=None: {
            "id": inst_id,
            "type": "market_profile",
            "runtime_supported": True,
            "datasource": "BINANCE",
            "exchange": "binance",
        },
    )
    monkeypatch.setattr(
        indicator_api,
        "build_runtime_indicator_graph",
        lambda *args, **kwargs: (None, [SimpleNamespace()]),
    )
    monkeypatch.setattr(
        indicator_api.candle_service,
        "fetch_ohlcv_by_instrument",
        lambda *args, **kwargs: frame,
    )
    monkeypatch.setattr(
        indicator_api,
        "IndicatorExecutionEngine",
        lambda indicators: SimpleNamespace(step=lambda **kwargs: SimpleNamespace(overlays={})),
    )

    with pytest.raises(ValueError, match="cursor_epoch aligned to a candle"):
        indicator_api.overlays_for_instance(
            "indicator-1",
            start=index[0].isoformat(),
            end=index[-1].isoformat(),
            interval="1h",
            symbol="BTC/USD",
            datasource="BINANCE",
            exchange="binance",
            instrument_id="instrument-1",
            overlay_options={"cursor_epoch": int(index[0].timestamp()) + 1},
        )
