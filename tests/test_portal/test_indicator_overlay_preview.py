from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("pandas")
import pandas as pd

from engines.bot_runtime.core.domain import Candle
from indicators.market_profile.compute.engine import MarketProfileIndicator
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
        overlay_payload={
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
    assert outputs["balance_breakout"].value["events"] == [{"key": "balance_breakout_long"}]
    assert "value_area" in overlays
    assert set(overlays.keys()) == {"value_area"}
    assert overlays["value_area"].value["payload"]["markers"] == []


def test_market_profile_runtime_indicator_waits_until_projected_strategy_bar_close() -> None:
    indicator = TypedMarketProfileIndicator(
        indicator_id="mp-1",
        version="v1",
        params={"bin_size": 1.0, "price_precision": 2},
        overlay_payload={
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
