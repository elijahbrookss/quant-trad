from __future__ import annotations

from datetime import datetime, timedelta, timezone

from engines.bot_runtime.core.domain import Candle
from indicators.candle_stats.definition import CandleStatsIndicator
from indicators.candle_stats.runtime import TypedCandleStatsIndicator


def _candle(index: int, *, close: float, half_range: float) -> Candle:
    time = datetime(2026, 6, 1, tzinfo=timezone.utc) + timedelta(hours=index)
    return Candle(
        time=time,
        end=time + timedelta(hours=1),
        known_at=time + timedelta(hours=1),
        open=close,
        high=close + half_range,
        low=close - half_range,
        close=close,
        volume=1000.0,
    )


def _indicator() -> TypedCandleStatsIndicator:
    params = CandleStatsIndicator.resolve_config(
        {
            "atr_short_window": 1,
            "atr_long_window": 3,
            "atr_z_window": 3,
            "directional_efficiency_window": 1,
            "slope_window": 1,
            "range_window": 1,
            "expansion_window": 1,
            "volume_window": 1,
            "overlap_window": 1,
            "slope_stability_lookback": 1,
            "warmup_bars": 3,
            "atr_expansion_signal_threshold": 0.5,
        },
        strict_unknown=True,
    )
    return TypedCandleStatsIndicator(
        indicator_id="candle-stats-1",
        version="v1",
        params=params,
    )


def test_candle_stats_emits_atr_expansion_signal_on_threshold_cross_only() -> None:
    indicator = _indicator()
    bars = [
        _candle(0, close=100.0, half_range=0.5),
        _candle(1, close=100.5, half_range=0.5),
        _candle(2, close=101.0, half_range=5.0),
        _candle(3, close=101.5, half_range=5.0),
    ]

    first_two = bars[:2]
    for bar in first_two:
        indicator.apply_bar(bar, {})
        assert indicator.snapshot()["atr_expansion"].ready is False

    indicator.apply_bar(bars[2], {})
    signal_output = indicator.snapshot()["atr_expansion"]

    assert signal_output.ready is True
    events = signal_output.value["events"]
    assert [event["key"] for event in events] == ["atr_expansion_long"]
    event = events[0]
    assert event["direction"] == "long"
    assert event["known_at"] == int(bars[2].end_time.timestamp())
    assert event["metadata"]["signal_style"] == "threshold_cross"
    assert event["metadata"]["threshold"] == 0.5
    assert event["metadata"]["previous_atr_zscore"] <= 0.5
    assert event["metadata"]["atr_zscore"] > 0.5
    assert event["metadata"]["trigger_price"] == bars[2].close

    indicator.apply_bar(bars[3], {})

    assert indicator.snapshot()["atr_expansion"].ready is True
    assert indicator.snapshot()["atr_expansion"].value["events"] == []
