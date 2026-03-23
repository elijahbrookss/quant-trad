from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pd = pytest.importorskip("pandas")

from indicators.config import IndicatorExecutionContext
from indicators.market_profile.definition import MarketProfileIndicator as MarketProfileDefinition
from indicators.market_profile import MarketProfileIndicator, Profile, ValueArea
from indicators.market_profile.compute.internal.bin_size import infer_precision_from_step
from indicators.market_profile.compute.internal.computation import (
    build_tpo_histogram,
    extract_value_area,
)
from indicators.market_profile.compute.internal.merging import calculate_overlap
from indicators.market_profile.compute.internal.runtime_profiles import IncrementalRuntimeProfileResolver
from indicators.market_profile.overlays import market_profile_overlay_transformer


def _sample_df() -> pd.DataFrame:
    idx = pd.to_datetime(
        [
            datetime(2025, 1, 1, 10, tzinfo=timezone.utc),
            datetime(2025, 1, 1, 11, tzinfo=timezone.utc),
            datetime(2025, 1, 2, 10, tzinfo=timezone.utc),
            datetime(2025, 1, 2, 11, tzinfo=timezone.utc),
        ],
        utc=True,
    )
    return pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0, 103.0],
            "high": [101.0, 102.0, 103.0, 104.0],
            "low": [99.0, 100.0, 101.0, 102.0],
            "close": [100.0, 101.0, 103.0, 103.0],
            "volume": [1000.0, 1100.0, 1200.0, 1300.0],
        },
        index=idx,
    )


def _profile(
    *,
    start: str,
    end: str,
    val: float,
    vah: float,
    poc: float,
    session_count: int = 1,
) -> Profile:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")
    return Profile(
        start=start_ts,
        end=end_ts,
        value_area=ValueArea(val=val, vah=vah, poc=poc),
        session_count=session_count,
        precision=2,
    )


def _runtime_profile_payload(
    *,
    start: str,
    end: str,
    known_at: str,
    val: float,
    vah: float,
    poc: float,
    session_count: int = 1,
) -> dict:
    return {
        "start": int(pd.Timestamp(start, tz="UTC").timestamp()),
        "end": int(pd.Timestamp(end, tz="UTC").timestamp()),
        "known_at": int(pd.Timestamp(known_at, tz="UTC").timestamp()),
        "formed_at": int(pd.Timestamp(end, tz="UTC").timestamp()),
        "VAH": vah,
        "VAL": val,
        "POC": poc,
        "session_count": session_count,
        "precision": 2,
    }


def test_build_runtime_data_request_normalizes_boundaries_to_utc() -> None:
    ctx = IndicatorExecutionContext(
        symbol="ES",
        start="2025-01-01",
        end="2025-01-02",
        interval="1h",
    )
    resolved = MarketProfileDefinition.resolve_config(
        {
            "bin_size": 1.0,
            "days_back": 2,
        },
        strict_unknown=True,
    )

    request = MarketProfileDefinition.build_runtime_data_request(
        resolved_params=resolved,
        execution_context=ctx,
    )

    assert request.interval == "30m"
    assert pd.Timestamp(request.start).tzinfo is not None
    assert pd.Timestamp(request.end).tzinfo is not None


def test_build_tpo_histogram_and_extract_value_area() -> None:
    df_day1 = _sample_df().loc["2025-01-01 10:00":"2025-01-01 11:00"]

    hist = build_tpo_histogram(
        df_day1,
        bin_size=1.0,
        bin_precision=infer_precision_from_step(1.0),
    )
    assert hist == {99.0: 1, 100.0: 2, 101.0: 2, 102.0: 1}

    value_area = extract_value_area(hist, price_precision=2)
    assert value_area is not None
    assert value_area.poc == pytest.approx(100.0)
    assert value_area.val == pytest.approx(99.0)
    assert value_area.vah == pytest.approx(101.0)


def test_build_tpo_histogram_uses_integer_bins_for_non_aligned_ranges() -> None:
    df = pd.DataFrame(
        {
            "low": [99.4, 100.6, 103.2],
            "high": [100.6, 99.4, 103.2],
        },
        index=pd.to_datetime(
            [
                "2025-01-01T10:00:00Z",
                "2025-01-01T11:00:00Z",
                "2025-01-01T12:00:00Z",
            ],
            utc=True,
        ),
    )

    hist = build_tpo_histogram(
        df,
        bin_size=1.0,
        bin_precision=infer_precision_from_step(1.0),
    )

    assert hist == {99.0: 2, 100.0: 2, 103.0: 1}


def test_select_bin_size_accepts_string() -> None:
    indicator = MarketProfileIndicator(_sample_df(), bin_size="0.5")
    assert indicator.bin_size == pytest.approx(0.5)

    fallback = MarketProfileIndicator(_sample_df(), bin_size="")
    assert fallback.bin_size > 0


def test_calculate_overlap() -> None:
    overlap = calculate_overlap(100.0, 105.0, 103.0, 108.0)
    assert overlap == pytest.approx(0.4)


def test_get_merged_profiles_returns_profile_dataclasses() -> None:
    indicator = MarketProfileIndicator(
        _sample_df(),
        bin_size=1.0,
        use_merged_value_areas=True,
        merge_threshold=0.5,
        min_merge_sessions=2,
    )
    indicator._profiles = [
        _profile(
            start="2025-01-01T10:00:00+00:00",
            end="2025-01-01T11:00:00+00:00",
            val=99.0,
            vah=101.0,
            poc=100.0,
        ),
        _profile(
            start="2025-01-02T10:00:00+00:00",
            end="2025-01-02T11:00:00+00:00",
            val=100.0,
            vah=102.0,
            poc=101.0,
        ),
    ]

    merged = indicator.get_merged_profiles(threshold=0.5, min_sessions=2)

    assert len(merged) == 1
    merged_profile = merged[0]
    assert isinstance(merged_profile, Profile)
    assert merged_profile.start == pd.Timestamp("2025-01-01T10:00:00+00:00")
    assert merged_profile.end == pd.Timestamp("2025-01-02T11:00:00+00:00")
    assert merged_profile.val == pytest.approx(99.0)
    assert merged_profile.vah == pytest.approx(102.0)
    assert merged_profile.poc == pytest.approx(100.5)
    assert merged_profile.session_count == 2


def test_market_profile_computation_sorts_and_groups_sessions_without_index_copy_drift() -> None:
    base = _sample_df()
    unsorted = base.iloc[[3, 0, 2, 1]].copy()
    unsorted.index = pd.DatetimeIndex(unsorted.index.tz_convert(None))

    indicator = MarketProfileIndicator(
        unsorted,
        bin_size=1.0,
        use_merged_value_areas=False,
    )

    profiles = indicator.get_profiles()
    assert len(profiles) == 2
    assert profiles[0].start == pd.Timestamp("2025-01-01T10:00:00Z")
    assert profiles[0].end == pd.Timestamp("2025-01-01T11:00:00Z")
    assert profiles[1].start == pd.Timestamp("2025-01-02T10:00:00Z")
    assert profiles[1].end == pd.Timestamp("2025-01-02T11:00:00Z")


def test_incremental_runtime_profile_resolver_extends_contiguous_cluster() -> None:
    resolver = IncrementalRuntimeProfileResolver(
        profiles_payload=[
            _runtime_profile_payload(
                start="2025-01-01T10:00:00Z",
                end="2025-01-01T11:00:00Z",
                known_at="2025-01-01T11:00:00Z",
                val=99.0,
                vah=101.0,
                poc=100.0,
            ),
            _runtime_profile_payload(
                start="2025-01-02T10:00:00Z",
                end="2025-01-02T11:00:00Z",
                known_at="2025-01-02T11:00:00Z",
                val=100.0,
                vah=102.0,
                poc=101.0,
            ),
            _runtime_profile_payload(
                start="2025-01-03T10:00:00Z",
                end="2025-01-03T11:00:00Z",
                known_at="2025-01-03T11:00:00Z",
                val=100.5,
                vah=102.5,
                poc=101.5,
            ),
        ],
        profile_params={
            "use_merged_value_areas": True,
            "merge_threshold": 0.5,
            "min_merge_sessions": 2,
        },
        symbol="ES",
    )

    before_second, summary_before_second = resolver.resolve(
        current_epoch=int(pd.Timestamp("2025-01-01T11:00:00Z").timestamp())
    )
    assert before_second == []
    assert summary_before_second == {"known_profiles": 1, "merged_profiles": 0}

    after_second, summary_after_second = resolver.resolve(
        current_epoch=int(pd.Timestamp("2025-01-02T11:00:00Z").timestamp())
    )
    assert len(after_second) == 1
    assert summary_after_second == {"known_profiles": 2, "merged_profiles": 1}
    assert after_second[0].val == pytest.approx(99.0)
    assert after_second[0].vah == pytest.approx(102.0)
    assert after_second[0].session_count == 2

    after_third, summary_after_third = resolver.resolve(
        current_epoch=int(pd.Timestamp("2025-01-03T11:00:00Z").timestamp())
    )
    assert len(after_third) == 1
    assert summary_after_third == {"known_profiles": 3, "merged_profiles": 1}
    assert after_third[0].val == pytest.approx(99.0)
    assert after_third[0].vah == pytest.approx(102.5)
    assert after_third[0].session_count == 3


def test_incremental_runtime_profile_resolver_does_not_reopen_broken_chain() -> None:
    resolver = IncrementalRuntimeProfileResolver(
        profiles_payload=[
            _runtime_profile_payload(
                start="2025-01-01T10:00:00Z",
                end="2025-01-01T11:00:00Z",
                known_at="2025-01-01T11:00:00Z",
                val=99.0,
                vah=101.0,
                poc=100.0,
            ),
            _runtime_profile_payload(
                start="2025-01-02T10:00:00Z",
                end="2025-01-02T11:00:00Z",
                known_at="2025-01-02T11:00:00Z",
                val=100.0,
                vah=102.0,
                poc=101.0,
            ),
            _runtime_profile_payload(
                start="2025-01-03T10:00:00Z",
                end="2025-01-03T11:00:00Z",
                known_at="2025-01-03T11:00:00Z",
                val=120.0,
                vah=122.0,
                poc=121.0,
            ),
            _runtime_profile_payload(
                start="2025-01-04T10:00:00Z",
                end="2025-01-04T11:00:00Z",
                known_at="2025-01-04T11:00:00Z",
                val=100.5,
                vah=102.5,
                poc=101.5,
            ),
        ],
        profile_params={
            "use_merged_value_areas": True,
            "merge_threshold": 0.5,
            "min_merge_sessions": 2,
        },
        symbol="ES",
    )

    resolved, summary = resolver.resolve(
        current_epoch=int(pd.Timestamp("2025-01-04T11:00:00Z").timestamp())
    )

    assert summary == {"known_profiles": 4, "merged_profiles": 1}
    assert len(resolved) == 1
    assert resolved[0].start == pd.Timestamp("2025-01-01T10:00:00Z")
    assert resolved[0].end == pd.Timestamp("2025-01-02T11:00:00Z")
    assert resolved[0].session_count == 2


def test_to_lightweight_extends_boxes_to_chart_end_by_default() -> None:
    indicator = MarketProfileIndicator(
        _sample_df(),
        bin_size=1.0,
        use_merged_value_areas=False,
    )
    start = pd.Timestamp("2025-01-01T10:00:00+00:00")
    end = start + timedelta(hours=2)
    chart_end = end + timedelta(hours=2)
    indicator._profiles = [
        _profile(
            start=start.isoformat(),
            end=end.isoformat(),
            val=99.0,
            vah=101.0,
            poc=100.0,
        )
    ]
    plot_idx = pd.date_range(start=start, end=chart_end, freq="30min", tz="UTC")
    plot_df = pd.DataFrame({"open": [100.0] * len(plot_idx)}, index=plot_idx)

    payload = indicator.to_lightweight(plot_df)

    assert payload["boxes"]
    assert payload["boxes"][0]["x2"] == int(chart_end.timestamp())


def test_to_lightweight_respects_indicator_extend_flag() -> None:
    indicator = MarketProfileIndicator(
        _sample_df(),
        bin_size=1.0,
        use_merged_value_areas=False,
        extend_value_area_to_chart_end=False,
    )
    start = pd.Timestamp("2025-01-02T09:30:00+00:00")
    end = start + timedelta(hours=2)
    chart_end = end + timedelta(hours=3)
    indicator._profiles = [
        _profile(
            start=start.isoformat(),
            end=end.isoformat(),
            val=95.0,
            vah=105.0,
            poc=100.0,
        )
    ]
    plot_idx = pd.date_range(start=start, end=chart_end, freq="30min", tz="UTC")
    plot_df = pd.DataFrame({"open": [100.0] * len(plot_idx)}, index=plot_idx)

    payload = indicator.to_lightweight(plot_df)

    assert payload["boxes"]
    assert payload["boxes"][0]["x2"] == int(end.timestamp())


def test_overlay_transformer_builds_boxes_from_runtime_payload() -> None:
    indicator = MarketProfileIndicator(
        _sample_df(),
        bin_size=1.0,
        use_merged_value_areas=False,
        extend_value_area_to_chart_end=True,
    )
    start = pd.Timestamp("2025-01-01T10:00:00+00:00")
    end = start + timedelta(hours=1)
    chart_end = end + timedelta(hours=3)
    indicator._profiles = [
        _profile(
            start=start.isoformat(),
            end=end.isoformat(),
            val=99.0,
            vah=101.0,
            poc=100.0,
        )
    ]
    payload = indicator.build_runtime_signal_payload(
        indicator_id="ind-1",
        params={
            "use_merged_value_areas": False,
            "extend_value_area_to_chart_end": True,
            "start": int(start.timestamp()),
            "end": int(chart_end.timestamp()),
        },
        symbol="ES",
        chart_timeframe="30m",
    )
    overlay = {"type": "market-profile", "payload": payload, "symbol": "ES"}

    transformed = market_profile_overlay_transformer(
        overlay,
        current_epoch=int(chart_end.timestamp()),
    )

    assert transformed is not None
    boxes = transformed["payload"]["boxes"]
    assert len(boxes) == 1
    assert boxes[0]["x1"] == int(start.timestamp())
    assert boxes[0]["x2"] == int(chart_end.timestamp())


def test_build_runtime_source_facts_keeps_profiles_unmerged_for_walk_forward_runtime() -> None:
    indicator = MarketProfileIndicator(
        _sample_df(),
        bin_size=1.0,
        use_merged_value_areas=True,
        merge_threshold=0.5,
        min_merge_sessions=2,
    )
    indicator._profiles = [
        _profile(
            start="2025-01-01T10:00:00+00:00",
            end="2025-01-01T11:00:00+00:00",
            val=99.0,
            vah=101.0,
            poc=100.0,
        ),
        _profile(
            start="2025-01-02T10:00:00+00:00",
            end="2025-01-02T11:00:00+00:00",
            val=100.0,
            vah=102.0,
            poc=101.0,
        ),
    ]

    payload = indicator.build_runtime_source_facts(
        params={
            "use_merged_value_areas": True,
            "merge_threshold": 0.5,
            "min_merge_sessions": 2,
        },
        symbol="ES",
        chart_timeframe="1h",
    )

    assert payload["profile_params"]["profiles_premerged"] is False
    assert len(payload["profiles"]) == 2


def test_runtime_signal_payload_projects_profile_to_strategy_timeframe() -> None:
    indicator = MarketProfileIndicator(
        _sample_df(),
        bin_size=1.0,
        use_merged_value_areas=False,
        extend_value_area_to_chart_end=False,
    )
    source_start = pd.Timestamp("2025-01-01T10:00:00+00:00")
    source_end = pd.Timestamp("2025-01-01T10:30:00+00:00")
    indicator._profiles = [
        _profile(
            start=source_start.isoformat(),
            end=source_end.isoformat(),
            val=99.0,
            vah=101.0,
            poc=100.0,
        )
    ]

    payload = indicator.build_runtime_signal_payload(
        indicator_id="ind-1",
        params={
            "use_merged_value_areas": False,
            "extend_value_area_to_chart_end": False,
        },
        symbol="ES",
        chart_timeframe="1h",
    )

    profile = payload["profiles"][0]

    assert profile["source_start"] == int(source_start.timestamp())
    assert profile["source_end"] == int(source_end.timestamp())
    assert profile["start"] == int(pd.Timestamp("2025-01-01T10:00:00+00:00").timestamp())
    assert profile["end"] == int(pd.Timestamp("2025-01-01T11:00:00+00:00").timestamp())
    assert profile["formed_at"] == int(source_end.timestamp())
    assert profile["known_at"] == int(pd.Timestamp("2025-01-01T11:00:00+00:00").timestamp())
    assert payload["profile_boundary_semantics"] == "strategy_timeframe_projection"
