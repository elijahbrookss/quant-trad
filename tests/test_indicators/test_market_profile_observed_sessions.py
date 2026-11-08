"""V4 retains honestly incomplete observed context without revising its past."""
from copy import deepcopy
from dataclasses import replace

import pandas as pd
import pytest

from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.registry import get_indicator_manifest
from tests.test_indicators.test_market_profile_availability import source_frame as base_source_frame, build, candle


def source_frame(days=3):
    frame = base_source_frame(days)
    frame["source_identity_key"] = "pinned-native-source"
    frame["known_at_method"] = "interval_close_inferred"
    frame["revision"] = 1
    frame["market_commit_seq"] = list(range(1, len(frame)+1))
    return frame


def quality(indicator):
    return indicator.snapshot()["profile_quality"].value


@pytest.mark.parametrize("absent", [[10], [42, 43]])
def test_missing_native_bars_preserve_closed_observed_day_with_explicit_quality(absent):
    source = source_frame(1).drop(source_frame(1).index[absent])
    # An unverified label, even with future detection, is not closure proof.
    source.attrs["gap_classification"] = [{"classification": "disclosed_closure",
        "start": "2025-01-01T21:00:00Z", "end": "2025-01-01T22:00:00Z", "detected_at": "2026-01-01"}]
    indicator, facts = build(source, version="v4")
    legacy, old = build(source, version="v2")
    indicator.apply_bar(candle("2025-01-01T23:30:00Z"), {})
    assert not indicator.snapshot()["value_area_metrics"].ready
    indicator.apply_bar(candle("2025-01-01T23:45:00Z"), {})
    legacy.apply_bar(candle("2025-01-01T23:45:00Z"), {})
    assert indicator.snapshot()["value_area_metrics"] == legacy.snapshot()["value_area_metrics"]
    value = quality(indicator)
    assert value["state_key"] == "observed_incomplete"
    fields = value["fields"]
    assert fields["observed_bars"] == 48-len(absent)
    assert fields["missing_bars"] == len(absent)
    assert fields["missing_bar_price_impact"] == "unknown"
    assert fields["predictive_qualification"] is False
    assert fields["sessions"][0]["closure_verified"] is False
    assert len(fields["sessions"][0]["missing_open_times"]) == len(absent)
    assert len(facts["profiles"]) == 1


@pytest.mark.parametrize("timeframe,minutes", [("5m",5), ("15m",15), ("30m",30), ("1h",60), ("4h",240), ("1d",1440)])
def test_observed_session_is_published_at_utc_close_across_chart_timeframes(timeframe, minutes):
    source = source_frame(1).iloc[:-1]
    indicator, facts = build(source, timeframe, version="v4")
    boundary = pd.Timestamp("2025-01-02T00:00:00Z")
    indicator.apply_bar(candle(boundary-pd.Timedelta(minutes=minutes*2), minutes), {})
    assert not indicator.snapshot()["profile_quality"].ready
    indicator.apply_bar(candle(boundary-pd.Timedelta(minutes=minutes), minutes), {})
    assert indicator.snapshot()["profile_quality"].ready
    assert facts["profiles"][0]["known_at"] == int(boundary.timestamp())
    assert quality(indicator)["fields"]["observed_bars"] == 47


def test_late_row_replaces_day_once_with_new_identity_and_preserves_saved_snapshot():
    source = source_frame(1)
    source.loc[source.index[10], "known_at"] = pd.Timestamp("2025-01-02T02:00:00Z")
    indicator, facts = build(source, version="v4")
    assert [event["quality"]["observed_bars"] for event in facts["profiles"]] == [47,48]
    indicator.apply_bar(candle("2025-01-01T23:45:00Z"), {})
    first = indicator.snapshot(); saved = deepcopy(first)
    first_key = quality(indicator)["fields"]["active_profile_key"]
    indicator.apply_bar(candle("2025-01-02T01:45:00Z"), {})
    assert quality(indicator)["state_key"] == "complete_observed_slots"
    assert quality(indicator)["fields"]["active_profile_key"] != first_key
    assert len(quality(indicator)["fields"]["sessions"]) == 1
    assert first == saved


def test_late_older_day_remerges_in_session_order_without_counting_an_extra_session():
    source = source_frame(2)
    source.loc[source.index[10], "known_at"] = pd.Timestamp("2025-01-03T02:00:00Z")
    indicator, _ = build(source, version="v4", merged=True)
    indicator.apply_bar(candle("2025-01-02T23:45:00Z"), {})
    assert quality(indicator)["fields"]["observed_bars"] == 95
    saved = deepcopy(indicator.snapshot())
    indicator.apply_bar(candle("2025-01-03T01:45:00Z"), {})
    fields = quality(indicator)["fields"]
    assert fields["observed_bars"] == 96
    assert len(fields["sessions"]) == 2
    assert [row["session"] for row in fields["sessions"]] == ["2025-01-01T00:00:00+00:00", "2025-01-02T00:00:00+00:00"]
    assert saved["profile_quality"].value["fields"]["observed_bars"] == 95


@pytest.mark.parametrize("merged", [False, True])
@pytest.mark.parametrize("cutoff", ["2025-01-02T23:45:00Z", "2025-01-03T00:00:00Z", "2025-01-03T02:00:00Z", "2025-01-03T09:15:00Z"])
def test_typed_quality_signals_and_overlays_preserve_prefix_under_late_and_changed_future_rows(merged, cutoff):
    source = source_frame(3)
    source.loc[source.index[10], "known_at"] = pd.Timestamp("2025-01-03T02:00:00Z")
    source = source.drop(source.index[42])
    boundary = pd.Timestamp(cutoff)
    prefix = source.loc[source.known_at <= boundary]
    changed = source.copy()
    changed.loc[changed.known_at > boundary, "high"] = 300.
    engines = [IndicatorExecutionEngine([build(frame, version="v4", merged=merged)[0]]) for frame in (source,prefix,changed)]
    for i, timestamp in enumerate(pd.date_range("2025-01-01T00:00:00Z", boundary-pd.Timedelta(minutes=15), freq="15min")):
        price = (100.,104.,99.,103.,100.)[i%5]
        bar = replace(candle(timestamp), close=price, high=max(price+1,103.))
        frames = [engine.step(bar=bar, bar_time=bar.time, include_overlays=True) for engine in engines]
        assert frames[0].outputs == frames[1].outputs == frames[2].outputs
        assert frames[0].overlays == frames[1].overlays == frames[2].overlays


@pytest.mark.parametrize("failure", ["absent_known_at", "missing_known_at", "absent_close_time", "invalid_known_at", "naive_known_at"])
def test_unknown_availability_fails_closed_without_import_time_substitution(failure):
    source = source_frame(1)
    if failure.startswith("absent"):
        source = source.drop(columns=[failure[len("absent_"):]])
    elif failure == "missing_known_at":
        source.loc[source.index[4], "known_at"] = pd.NaT
    elif failure == "naive_known_at":
        source["known_at"] = source.known_at.dt.tz_localize(None)
    else:
        source["known_at"] = source.known_at.astype(object)
        source.loc[source.index[4], "known_at"] = "invalid"
    with pytest.raises(ValueError, match="market_profile_availability_unknown"):
        build(source, version="v4")


def test_inferred_clock_method_and_partial_window_boundaries_remain_visible():
    source = source_frame(1).iloc[42:].copy()
    source["known_at_method"] = "interval_close_inferred"
    indicator, _ = build(source, version="v4")
    indicator.apply_bar(candle("2025-01-01T23:45:00Z"), {})
    fields = quality(indicator)["fields"]
    assert fields["availability_inferred"] is True
    assert fields["availability_methods"] == ["interval_close_inferred"]
    assert fields["observed_bars"] == 6 and fields["missing_bars"] == 42
    assert fields["predictive_qualification"] is False


def test_merged_sensitivity_is_observed_member_deletion_not_an_estimate_of_missing_prices():
    source = source_frame(3)
    source.loc[source.index[48:96], ["open","high","low","close"]] += 1.
    source = source.drop(source.index[42])
    indicator, _ = build(source, version="v4", merged=True)
    indicator.apply_bar(candle("2025-01-03T23:45:00Z"), {})
    fields = quality(indicator)["fields"]
    assert len(fields["sessions"]) == 3
    assert fields["leave_one_session_out_poc_min"] == pytest.approx(98.)
    assert fields["leave_one_session_out_poc_max"] == pytest.approx(98.5)
    assert fields["max_leave_one_session_out_poc_shift"] == pytest.approx(1/3)
    assert fields["missing_bar_price_impact"] == "unknown"


def test_v4_manifest_does_not_change_retained_version_outputs():
    old = get_indicator_manifest("market_profile", "v3")
    new = get_indicator_manifest("market_profile", "v4")
    assert new.outputs[:-1] == old.outputs
    assert new.outputs[-1].name == "profile_quality"
    assert get_indicator_manifest("market_profile").version == "v1"


def test_mixed_source_identity_cannot_be_hidden_by_a_daily_aggregate():
    source = source_frame(1)
    source.loc[source.index[4], "source_identity_key"] = "another-source"
    with pytest.raises(ValueError, match="one explicit source identity"):
        build(source, version="v4")
