"""Regress source-open leakage through the canonical MP build/engine path."""
from datetime import timedelta

import pandas as pd
import pytest

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.config import IndicatorExecutionContext
from indicators.market_profile.definition import MarketProfileIndicator
from indicators.registry import get_indicator_manifest


def source_frame(days=3):
    index = pd.date_range("2025-01-01", periods=48 * days, freq="30min", tz="UTC")
    frame = pd.DataFrame({"open": 100., "high": 102., "low": 98., "close": 100., "volume": 10.}, index=index)
    frame["close_time"] = index + pd.Timedelta(minutes=30)
    frame["known_at"] = frame["close_time"]
    return frame


def build(frame, timeframe="15m", version="v3", merged=False):
    context = IndicatorExecutionContext(symbol="TEST", start="2025-01-01", end="2025-01-05", interval=timeframe)
    params = MarketProfileIndicator.resolve_config({"bin_size": 1., "use_merged_value_areas": merged, "min_merge_sessions": 2})
    facts = MarketProfileIndicator.build_runtime_source_facts(resolved_params=params, execution_context=context,
        source_frame=frame, version=version)
    indicator = MarketProfileIndicator.build_runtime_indicator(indicator_id="mp", meta={"version": version},
        resolved_params=params, strategy_indicator_metas={}, execution_context=context, source_facts=facts)
    return indicator, facts


def candle(open_time, minutes=15, *, known_at=None):
    start = pd.Timestamp(open_time).to_pydatetime()
    return Candle(time=start, end=start + timedelta(minutes=minutes), known_at=known_at,
        open=100., high=103., low=97., close=100., volume=10.)


@pytest.mark.parametrize("minutes,timeframe", [(5,"5m"), (15,"15m"), (30,"30m"), (60,"1h"), (240,"4h"), (1440,"1d")])
def test_profile_is_admitted_at_source_close_on_completed_chart_bar(minutes, timeframe):
    indicator, facts = build(source_frame(1), timeframe)
    midnight = pd.Timestamp("2025-01-02T00:00:00Z")
    profile = facts["profiles"][0]
    assert profile["source_end"] == int((midnight-pd.Timedelta(minutes=30)).timestamp())
    assert profile["formed_at"] == profile["source_known_at"] == profile["known_at"] == int(midnight.timestamp())
    assert profile["source_availability_basis"] == "canonical_known_at"
    engine = IndicatorExecutionEngine([indicator])
    before = candle(midnight-pd.Timedelta(minutes=minutes*2), minutes)
    frame = engine.step(bar=before, bar_time=before.time, include_overlays=True)
    assert not frame.outputs["mp.value_area_metrics"].ready
    assert not frame.overlays["mp.value_area"].ready
    exact = candle(midnight-pd.Timedelta(minutes=minutes), minutes)
    frame = engine.step(bar=exact, bar_time=exact.time, include_overlays=True)
    assert frame.outputs["mp.value_area_metrics"].ready
    assert frame.overlays["mp.value_area"].ready
    assert frame.overlays["mp.value_area"].value["known_at"] == int(midnight.timestamp())


def test_legacy_v2_reproduces_2345_leak_while_v3_withholds_it():
    old, old_facts = build(source_frame(1), version="v2")
    new, new_facts = build(source_frame(1))
    bar = candle("2025-01-01T23:30:00Z")
    old.apply_bar(bar, {})
    new.apply_bar(bar, {})
    assert old.snapshot()["value_area_metrics"].ready
    assert not new.snapshot()["value_area_metrics"].ready
    assert old_facts["profiles"][0]["known_at"] == int(bar.time.timestamp())
    assert new_facts["profiles"][0]["known_at"] > int(bar.end_time.timestamp())
    assert {k: old_facts["profiles"][0][k] for k in ("VAH", "VAL", "POC", "start", "end")} == {
        k: new_facts["profiles"][0][k] for k in ("VAH", "VAL", "POC", "start", "end")}
    assert get_indicator_manifest("market_profile", "v3").outputs == get_indicator_manifest("market_profile", "v2").outputs


def test_maximum_included_source_availability_and_chart_projection_are_preserved():
    source = source_frame(1)
    # An earlier source row arrives later than the final row: final-row time is insufficient.
    source.loc[source.index[3], "known_at"] = pd.Timestamp("2025-01-02T00:07:00Z")
    indicator, facts = build(source, "1h")
    assert facts["profiles"][0]["source_known_at"] == int(pd.Timestamp("2025-01-02T00:07:00Z").timestamp())
    assert facts["profiles"][0]["known_at"] == int(pd.Timestamp("2025-01-02T01:00:00Z").timestamp())
    indicator.apply_bar(candle("2025-01-01T23:00:00Z", 60), {})
    assert not indicator.snapshot()["value_area_metrics"].ready
    indicator.apply_bar(candle("2025-01-02T00:00:00Z", 60), {})
    assert indicator.snapshot()["value_area_metrics"].ready


def test_chart_canonical_availability_is_used_instead_of_chart_open_or_close():
    source = source_frame(1)
    source["known_at"] = source["known_at"].where(source.index != source.index[-1], pd.Timestamp("2025-01-02T00:07:00Z"))
    indicator, _ = build(source)
    bar = candle("2025-01-01T23:45:00Z", known_at=pd.Timestamp("2025-01-02T00:15:00Z").to_pydatetime())
    indicator.apply_bar(bar, {})
    assert indicator.snapshot()["value_area_metrics"].ready
    assert indicator.overlay_snapshot()["value_area"].value["known_at"] == int(bar.known_at.timestamp())


def test_subsecond_source_availability_cannot_be_rounded_back_to_midnight():
    source = source_frame(1)
    source.loc[source.index[-1], "known_at"] = pd.Timestamp("2025-01-02T00:00:00.001Z")
    indicator, facts = build(source)
    assert facts["profiles"][0]["known_at"] == int(pd.Timestamp("2025-01-02T00:15:00Z").timestamp())
    indicator.apply_bar(candle("2025-01-01T23:45:00Z"), {})
    assert not indicator.snapshot()["value_area_metrics"].ready


@pytest.mark.parametrize("merged", [False, True])
@pytest.mark.parametrize("cutoff", ["2025-01-02T23:15:00Z", "2025-01-02T23:45:00Z", "2025-01-03T00:00:00Z", "2025-01-03T09:15:00Z"])
def test_every_typed_output_and_overlay_is_invariant_to_future_source_suffix(merged, cutoff):
    source = source_frame()
    boundary = pd.Timestamp(cutoff)
    prefix = source.loc[source.known_at <= boundary].copy()
    suffix_changed = source.copy()
    suffix_changed.loc[suffix_changed.known_at > boundary, "high"] = 300.
    engines = [IndicatorExecutionEngine([build(s, merged=merged)[0]]) for s in (source, prefix, suffix_changed)]
    for timestamp in pd.date_range("2025-01-01T00:00:00Z", boundary-pd.Timedelta(minutes=15), freq="15min"):
        bar = candle(timestamp)
        frames = [engine.step(bar=bar, bar_time=bar.time, include_overlays=True) for engine in engines]
        assert frames[0].outputs == frames[1].outputs == frames[2].outputs
        assert frames[0].overlays == frames[1].overlays == frames[2].overlays


def test_future_known_source_row_missing_from_prefix_cannot_form_partial_profile():
    source = source_frame(1)
    source.loc[source.index[3], "known_at"] = pd.Timestamp("2025-01-02T02:00:00Z")
    prefix = source.loc[source.known_at <= pd.Timestamp("2025-01-02T00:00:00Z")]
    for frame in (source, prefix):
        indicator, facts = build(frame)
        indicator.apply_bar(candle("2025-01-01T23:45:00Z"), {})
        assert not indicator.snapshot()["value_area_metrics"].ready
    assert facts["profile_availability"]["withheld_sessions"][0]["reason"] == "incomplete_native_day"


def test_inferred_native_close_is_explicit_and_incomplete_day_is_withheld():
    indicator, facts = build(source_frame(1).drop(columns=["known_at", "close_time"]))
    assert facts["profiles"][0]["source_availability_basis"] == "inferred_native_close"
    _, partial = build(source_frame(1).iloc[:-1])
    assert partial["profiles"] == []
    assert partial["profile_availability"]["withheld_sessions"][0]["source_bars"] == 47


@pytest.mark.parametrize("field,value", [("known_at", None), ("known_at", "invalid"),
    ("known_at", "2024-01-01T00:00:00Z"), ("close_time", "2025-01-01T00:15:00Z")])
def test_invalid_explicit_source_clocks_fail_loud(field, value):
    source = source_frame(1)
    source[field] = source[field].astype(object)
    source.loc[source.index[0], field] = value
    with pytest.raises(ValueError, match="market_profile_source_clock_invalid"):
        build(source)


def test_v3_rejects_legacy_source_facts_and_unclosed_chart_bar():
    from indicators.market_profile.runtime.typed_indicator import TypedMarketProfileIndicator
    with pytest.raises(RuntimeError, match="source_contract_invalid"):
        TypedMarketProfileIndicator(indicator_id="mp", version="v3", params={}, source_facts={})
    indicator, _ = build(source_frame(1))
    bar = candle("2025-01-01T23:45:00Z")
    bar.end = None
    with pytest.raises(RuntimeError, match="bar_clock_invalid"):
        indicator.apply_bar(bar, {})


def test_v3_rejects_whole_window_auto_bin_inference():
    from indicators.market_profile.compute.engine import MarketProfileIndicator as ComputeProfile
    indicator = ComputeProfile(source_frame(1), bin_size=None)
    with pytest.raises(ValueError, match="fixed positive bin_size"):
        indicator.build_runtime_source_facts(causal_availability=True)
    context = IndicatorExecutionContext(symbol="TEST", start="2025-01-01", end="2025-01-03", interval="15m")
    params = MarketProfileIndicator.resolve_config({"bin_size": 0})
    with pytest.raises(ValueError, match="fixed positive bin_size"):
        MarketProfileIndicator.build_runtime_source_facts(resolved_params=params,
            execution_context=context, source_frame=source_frame(1), version="v3")
