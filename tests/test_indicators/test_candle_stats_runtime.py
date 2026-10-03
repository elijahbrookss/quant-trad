from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from engines.indicator_engine import IndicatorExecutionEngine
from engines.indicator_engine.contracts import IndicatorGapRejectedError

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


def _step(engine: IndicatorExecutionEngine, bar: Candle) -> None:
    engine.step(bar=bar, bar_time=bar.time)


@pytest.mark.parametrize("rewarm_bars", [0, 5])
def test_gap_recovery_matches_fresh_post_gap_timeline(rewarm_bars: int) -> None:
    recovered = _indicator()
    recovered.configure_overlay_history(history_bars=2)
    engine = IndicatorExecutionEngine([recovered])
    for index in range(8):
        _step(engine, _candle(index, close=1000.0 + index, half_range=100.0))
    start = _candle(20, close=100.0, half_range=0.5).time
    actions = engine.handle_gap(
        policy="reset_rewarm", gap={"classification": "recorded_data_gap"},
        next_bar_time=start, rewarm_bars=rewarm_bars,
    )
    assert actions[0]["rewarm_bars"] == max(3, rewarm_bars)
    assert actions[0]["action"] == "reset_and_rewarm"
    assert all(not output.ready for output in recovered.snapshot().values())
    assert all(not overlay.ready for overlay in recovered.overlay_snapshot().values())
    fresh = _indicator()
    fresh.configure_overlay_history(history_bars=2)
    fresh_engine = IndicatorExecutionEngine([fresh])
    hold = max(3, rewarm_bars)
    for index in range(hold + 3):
        bar = _candle(20 + index, close=100.0, half_range=5.0 if index == hold else 0.5)
        _step(engine, bar)
        _step(fresh_engine, bar)
        for key, output in recovered.snapshot().items():
            assert output.bar_time == bar.time
            if index < hold:
                assert output.ready is False
                assert output.value == {}
            else:
                expected = fresh.snapshot()[key]
                assert output.ready == expected.ready
                assert output.value == expected.value
        assert recovered.overlay_snapshot() == fresh.overlay_snapshot()
    # No pre-gap price, previous threshold, or EMA seed survives recovery.
    assert recovered.snapshot()["candle_stats"].value == fresh.snapshot()["candle_stats"].value


def test_second_gap_restarts_rewarm_and_preserves_known_at() -> None:
    from dataclasses import replace

    indicator = _indicator()
    engine = IndicatorExecutionEngine([indicator])
    for index in range(3):
        _step(engine, _candle(index, close=100.0, half_range=0.5))
    for start in [10, 30]:
        engine.handle_gap(
            policy="reset_rewarm", gap={"classification": "recorded_data_gap"},
            next_bar_time=_candle(start, close=100.0, half_range=0.5).time,
            rewarm_bars=0,
        )
        for offset in range(3):
            _step(engine, _candle(start + offset, close=100.0, half_range=0.5))
            assert indicator.snapshot()["atr_expansion"].ready is False
    bar = _candle(33, close=100.0, half_range=5.0)
    bar = replace(bar, known_at=bar.end_time + timedelta(minutes=7))
    _step(engine, bar)
    event = indicator.snapshot()["atr_expansion"].value["events"][0]
    assert event["known_at"] == int(bar.known_at.timestamp())
    assert indicator.snapshot()["atr_expansion"].bar_time == bar.time


def test_rewarm_does_not_override_indicator_natural_readiness() -> None:
    params = CandleStatsIndicator.resolve_config({"warmup_bars": 1})
    indicator = TypedCandleStatsIndicator(indicator_id="slow", version="v1", params=params)
    indicator.handle_gap(policy="reset_rewarm", gap={}, next_bar_time=_candle(10, close=100, half_range=1).time, rewarm_bars=1)
    for index in range(10, 20):
        indicator.apply_bar(_candle(index, close=100, half_range=1), {})
        assert all(not output.ready for output in indicator.snapshot().values())


def test_existing_gap_policies_keep_their_explicit_behavior() -> None:
    indicator = _indicator()
    control = _indicator()
    for index in range(3):
        bar = _candle(index, close=100.0, half_range=0.5)
        indicator.apply_bar(bar, {})
        control.apply_bar(bar, {})
    next_bar = _candle(10, close=100.0, half_range=5.0)
    with pytest.raises(IndicatorGapRejectedError):
        indicator.handle_gap(policy="reject", gap={}, next_bar_time=next_bar.time, rewarm_bars=0)
    action = indicator.handle_gap(policy="continue_degraded", gap={}, next_bar_time=next_bar.time, rewarm_bars=0)
    assert action["action"] == "continued_degraded"
    indicator.apply_bar(next_bar, {})
    control.apply_bar(next_bar, {})
    assert indicator.snapshot() == control.snapshot()
    with pytest.raises(RuntimeError, match="indicator_gap_policy_invalid"):
        indicator.handle_gap(policy="unknown", gap={}, next_bar_time=next_bar.time, rewarm_bars=0)


def test_invalid_rewarm_fails_before_state_is_cleared() -> None:
    indicator = _indicator()
    for index in range(3):
        indicator.apply_bar(_candle(index, close=100.0, half_range=0.5), {})
    before = indicator.snapshot()
    with pytest.raises(RuntimeError, match="candle_stats_gap_rewarm_invalid"):
        indicator.handle_gap(policy="reset_rewarm", gap={}, next_bar_time=_candle(10, close=100, half_range=1).time, rewarm_bars=-1)
    assert indicator.snapshot() == before



def test_crossing_during_rewarm_is_not_emitted_later() -> None:
    indicator = _indicator()
    indicator.handle_gap(
        policy="reset_rewarm", gap={},
        next_bar_time=_candle(10, close=100, half_range=1).time, rewarm_bars=4,
    )
    for index, half_range in enumerate([0.5, 0.5, 5.0, 5.0]):
        indicator.apply_bar(_candle(10 + index, close=100, half_range=half_range), {})
        assert indicator.snapshot()["atr_expansion"].ready is False
    indicator.apply_bar(_candle(14, close=100, half_range=5.0), {})
    assert indicator.snapshot()["atr_expansion"].ready is True
    assert indicator.snapshot()["atr_expansion"].value["events"] == []
