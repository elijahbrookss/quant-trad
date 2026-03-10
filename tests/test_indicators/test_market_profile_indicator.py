from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pd = pytest.importorskip("pandas")

from indicators.config import DataContext
from indicators.market_profile import MarketProfileIndicator, Profile, ValueArea
from indicators.market_profile.compute.internal.bin_size import infer_precision_from_step
from indicators.market_profile.compute.internal.computation import (
    build_tpo_histogram,
    extract_value_area,
)
from indicators.market_profile.compute.internal.merging import calculate_overlap
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


class _ProviderStub:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df
        self.last_ctx: DataContext | None = None

    def get_ohlcv(self, ctx: DataContext) -> pd.DataFrame:
        self.last_ctx = ctx
        return self.df.copy()


def test_from_context_normalizes_boundaries_to_utc() -> None:
    provider = _ProviderStub(_sample_df())
    ctx = DataContext(
        symbol="ES",
        start="2025-01-01",
        end="2025-01-02",
        interval="1h",
    )

    indicator = MarketProfileIndicator.from_context(
        provider=provider,
        ctx=ctx,
        bin_size=1.0,
        days_back=2,
    )

    assert provider.last_ctx is not None
    assert pd.Timestamp(provider.last_ctx.start).tzinfo is not None
    assert pd.Timestamp(provider.last_ctx.end).tzinfo is not None
    assert all(profile.start.tzinfo is not None for profile in indicator.get_profiles())


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
    assert boxes[0]["y1"] == pytest.approx(99.0)
    assert boxes[0]["y2"] == pytest.approx(101.0)
