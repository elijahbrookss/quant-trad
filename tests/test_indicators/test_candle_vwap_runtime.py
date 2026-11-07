"""Numerical, session, availability and prefix contracts using synthetic native bars."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine import IndicatorExecutionEngine
from engines.indicator_engine.contracts import IndicatorGapRejectedError
from indicators.candle_vwap.definition import CandleVWAPIndicator
from indicators.candle_vwap.manifest import MANIFEST, NATIVE_TIMEFRAMES
from indicators.config import IndicatorExecutionContext
from indicators.manifest import validate_indicator_manifest
from indicators.registry import get_indicator_definition

ORIGIN = datetime(2026, 1, 1, tzinfo=timezone.utc)
STEP = timedelta(minutes=15)


def candle(index, price=100., volume=1., *, origin=ORIGIN, step=STEP):
    start = origin+index*step
    return Candle(time=start, end=start+step, known_at=start+step,
                  open=price, high=price, low=price, close=price, volume=volume)


def build(timeframe="15m"):
    params = CandleVWAPIndicator.resolve_config({"source_timeframe": timeframe}, strict_unknown=True)
    context = IndicatorExecutionContext(symbol="SYNTHETIC", instrument_id="synthetic-1",
        start=ORIGIN.isoformat(), end=(ORIGIN+timedelta(days=2)).isoformat(), interval=timeframe)
    return CandleVWAPIndicator.build_runtime_indicator(indicator_id="vwap-1", meta={"version": "v1"},
        resolved_params=params, strategy_indicator_metas={}, execution_context=context)


def test_registered_typed_contract_calculates_hlc3_volume_weighted_mean():
    validate_indicator_manifest(MANIFEST)
    assert get_indicator_definition("candle_vwap") is CandleVWAPIndicator
    runtime = build()
    assert all(not value.ready for value in runtime.snapshot().values())
    engine = IndicatorExecutionEngine([runtime])
    first = replace(candle(0, 100, 2), high=108, low=96, close=102)  # HLC3 = 102
    second = replace(candle(1, 110, 6), high=114, low=102, close=108)  # HLC3 = 108
    engine.step(bar=first, bar_time=first.time)
    frame = engine.step(bar=second, bar_time=second.time)
    metrics = frame.outputs["vwap-1.vwap_metrics"]
    assert metrics.ready and metrics.indicator_commit_seq == 2
    assert metrics.value["vwap"] == pytest.approx((102*2+108*6)/8)
    assert metrics.value["cumulative_volume"] == 8
    assert metrics.value["close_distance"] == pytest.approx(1.5)
    assert metrics.value["close_distance_bps"] == pytest.approx(1.5/106.5*10000)
    assert set(frame.outputs) == {f"vwap-1.{output.name}" for output in MANIFEST.outputs}


@pytest.mark.parametrize("timeframe,seconds", NATIVE_TIMEFRAMES.items())
def test_each_supported_native_timeframe_resets_on_utc_midnight(timeframe, seconds):
    runtime = build(timeframe)
    step = timedelta(seconds=seconds)
    for index in range(86400//seconds+1):
        runtime.apply_bar(candle(index, 100 if index < 86400//seconds else 200, step=step), {})
    output = runtime.snapshot()
    assert output["vwap_metrics"].value["vwap"] == pytest.approx(200)
    assert output["vwap_metrics"].value["cumulative_volume"] == 1
    assert output["vwap_context"].value["fields"]["observed_bars"] == 1
    assert output["vwap_context"].value["fields"]["session_start"] == (ORIGIN+timedelta(days=1)).isoformat()
    assert output["vwap_cross"].value["events"] == []


def test_partial_first_session_remains_unready_until_next_anchor():
    runtime = build()
    for index in range(94, 97):
        runtime.apply_bar(candle(index), {})
        assert all(value.ready == (index == 96) for value in runtime.snapshot().values())
        if index < 96:
            assert runtime.snapshot()["vwap_metrics"].value["vwap"] is None
            assert runtime.snapshot()["vwap_context"].value["state_key"] == "warming_session"


def test_zero_volume_is_not_a_gap_or_a_cross_and_does_not_add_weight():
    runtime = build()
    runtime.apply_bar(candle(0, 100, 0), {})
    assert not runtime.snapshot()["vwap_metrics"].ready
    assert runtime.snapshot()["vwap_context"].value["state_key"] == "awaiting_volume"
    runtime.apply_bar(candle(1, 100, 2), {})
    runtime.apply_bar(candle(2, 200, 0), {})
    assert runtime.snapshot()["vwap_metrics"].ready
    assert runtime.snapshot()["vwap_metrics"].value["vwap"] == pytest.approx(100)
    assert runtime.snapshot()["vwap_cross"].value["events"] == []
    runtime.apply_bar(candle(3, 90, 2), {})
    assert runtime.snapshot()["vwap_cross"].value["events"] == []


def test_crosses_are_current_bar_events_with_source_availability():
    runtime = build()
    bars = [candle(i, price) for i, price in enumerate([100, 110, 90, 90])]
    for bar in bars[:2]:
        runtime.apply_bar(bar, {})
    event = runtime.snapshot()["vwap_cross"].value["events"][0]
    assert event["key"] == "close_cross_above_vwap"
    assert event["known_at"] == bars[1].known_at.isoformat()
    runtime.apply_bar(bars[2], {})
    assert runtime.snapshot()["vwap_cross"].value["events"][0]["key"] == "close_cross_below_vwap"
    runtime.apply_bar(bars[3], {})
    assert runtime.snapshot()["vwap_cross"].value["events"] == []


def test_prefix_invariance_including_session_reset_and_snapshot_isolation():
    bars = [candle(i, 100+i%7, i%4) for i in range(100)]
    whole = build()
    outputs = []
    for bar in bars:
        whole.apply_bar(bar, {})
        outputs.append(whole.snapshot())
    for length in (1, 2, 20, 95, 96, 97, 100):
        prefix = build()
        for index, bar in enumerate(bars[:length]):
            prefix.apply_bar(bar, {})
            assert prefix.snapshot() == outputs[index]
    snapshot = whole.snapshot()
    snapshot["vwap_context"].value["fields"]["observed_bars"] = -1
    assert whole.snapshot()["vwap_context"].value["fields"]["observed_bars"] == 4


def test_gap_duplicate_and_reordered_bars_fail_without_resetting_anchor():
    for next_index in (0, 2, -1):
        runtime = build()
        runtime.apply_bar(candle(0), {})
        with pytest.raises(IndicatorGapRejectedError):
            runtime.apply_bar(candle(next_index), {})


@pytest.mark.parametrize("policy", ["reject", "reset_rewarm", "continue_degraded"])
def test_recorded_gaps_are_always_rejected_by_version_one(policy):
    with pytest.raises(IndicatorGapRejectedError):
        build().handle_gap(policy=policy, gap={"classification": "missing"},
                           next_bar_time=ORIGIN+2*STEP, rewarm_bars=0)


@pytest.mark.parametrize("changes", [
    {"volume": None}, {"volume": -1}, {"volume": float("nan")}, {"volume": float("inf")},
    {"volume": True}, {"close": float("nan")}, {"low": 0}, {"high": 99},
    {"end": ORIGIN+timedelta(hours=1)}, {"known_at": ORIGIN}, {"time": ORIGIN.replace(tzinfo=None)},
])
def test_invalid_source_fails_loudly(changes):
    with pytest.raises(ValueError, match="candle_vwap_source_invalid"):
        build().apply_bar(replace(candle(0), **changes), {})


def test_delayed_availability_is_preserved_and_backwards_availability_rejected():
    runtime = build()
    runtime.apply_bar(replace(candle(0), known_at=ORIGIN+3*STEP), {})
    assert runtime.snapshot()["vwap_context"].value["fields"]["known_at"] == (ORIGIN+3*STEP).isoformat()
    with pytest.raises(ValueError, match="availability mismatch"):
        runtime.apply_bar(candle(1), {})


def test_offset_clocks_use_utc_session_and_missing_availability_uses_interval_end():
    runtime = build()
    offset = timezone(timedelta(hours=-5))
    bar = candle(0)
    runtime.apply_bar(replace(bar, time=bar.time.astimezone(offset), end=bar.end.astimezone(offset), known_at=None), {})
    fields = runtime.snapshot()["vwap_context"].value["fields"]
    assert fields["session_start"] == ORIGIN.isoformat()
    assert fields["known_at"] == bar.end.isoformat()


def test_configuration_rejects_unknown_timeframe_and_primary_timeframe_mismatch():
    with pytest.raises(ValueError):
        CandleVWAPIndicator.resolve_config({"source_timeframe": "1d"}, strict_unknown=True)
    with pytest.raises(ValueError):
        CandleVWAPIndicator.resolve_config({"reset_by": "cumulative"}, strict_unknown=True)
    with pytest.raises(ValueError, match="primary native timeframe"):
        CandleVWAPIndicator.build_runtime_indicator(indicator_id="vwap-1", meta={},
            resolved_params={"source_timeframe": "15m"}, strategy_indicator_metas={},
            execution_context=IndicatorExecutionContext(symbol="SYNTHETIC", start=ORIGIN.isoformat(),
                end=(ORIGIN+timedelta(days=1)).isoformat(), interval="1h"))
