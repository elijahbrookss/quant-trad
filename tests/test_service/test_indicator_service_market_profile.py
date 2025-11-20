import pytest

pd = pytest.importorskip("pandas")

from indicators.market_profile import MarketProfileIndicator
from portal.backend.service.indicator_service import (
    _build_market_profile_overlay_indicator,
    _extract_ctor_params,
)


def _make_df(end_timestamp: str) -> pd.DataFrame:
    index = pd.date_range(
        start="2025-08-01 13:30:00+00:00",
        end=end_timestamp,
        freq="15min",
        tz="UTC",
    )
    data = {
        "open": [70.0 + idx for idx in range(len(index))],
        "high": [70.5 + idx for idx in range(len(index))],
        "low": [69.5 + idx for idx in range(len(index))],
        "close": [70.2 + idx for idx in range(len(index))],
    }
    return pd.DataFrame(data, index=index)


def test_market_profile_overlay_indicator_uses_requested_dataframe():
    original_df = _make_df("2025-08-01 19:30:00+00:00")
    updated_df = _make_df("2025-08-01 19:45:00+00:00")

    base_indicator = MarketProfileIndicator(original_df, interval="15m")

    overlay_indicator = _build_market_profile_overlay_indicator(
        base_indicator,
        updated_df,
        interval="15m",
        symbol="CL",
    )

    assert overlay_indicator is not base_indicator

    original_profile = base_indicator.daily_profiles[0]
    overlay_profile = overlay_indicator.daily_profiles[0]

    assert original_profile["end_date"].isoformat() == "2025-08-01T19:30:00+00:00"
    assert overlay_profile["end_date"].isoformat() == "2025-08-01T19:45:00+00:00"

    assert overlay_indicator.use_merged_value_areas == base_indicator.use_merged_value_areas
    assert overlay_indicator.merge_threshold == base_indicator.merge_threshold
    assert overlay_indicator.min_merge_sessions == base_indicator.min_merge_sessions


def test_market_profile_overlay_reinfers_bin_size_when_symbol_changes():
    narrow_index = pd.date_range(
        start="2025-08-01 09:30:00+00:00",
        periods=20,
        freq="15min",
        tz="UTC",
    )
    wide_index = pd.date_range(
        start="2025-08-01 09:30:00+00:00",
        periods=20,
        freq="15min",
        tz="UTC",
    )

    narrow_df = pd.DataFrame(
        {
            "open": [100 + idx * 0.1 for idx in range(len(narrow_index))],
            "high": [100.2 + idx * 0.1 for idx in range(len(narrow_index))],
            "low": [99.8 + idx * 0.1 for idx in range(len(narrow_index))],
            "close": [100.1 + idx * 0.1 for idx in range(len(narrow_index))],
        },
        index=narrow_index,
    )
    wide_df = pd.DataFrame(
        {
            "open": [10_000 + idx * 50 for idx in range(len(wide_index))],
            "high": [10_050 + idx * 50 for idx in range(len(wide_index))],
            "low": [9_950 + idx * 50 for idx in range(len(wide_index))],
            "close": [10_020 + idx * 50 for idx in range(len(wide_index))],
        },
        index=wide_index,
    )

    indicator = MarketProfileIndicator(narrow_df, interval="15m")
    indicator.symbol = "ES"

    overlay = _build_market_profile_overlay_indicator(
        indicator,
        wide_df,
        interval="15m",
        symbol="CL",
    )

    assert overlay is not indicator
    assert overlay.bin_size != pytest.approx(indicator.bin_size)
    assert getattr(overlay, "symbol", None) == "CL"


def test_market_profile_overlay_respects_explicit_bin_size_on_symbol_change():
    df = _make_df("2025-08-01 19:30:00+00:00")
    indicator = MarketProfileIndicator(df, interval="15m", bin_size=5.0)
    indicator.symbol = "ES"

    overlay = _build_market_profile_overlay_indicator(
        indicator,
        df,
        interval="15m",
        symbol="CL",
    )

    assert overlay.bin_size == pytest.approx(5.0)


def test_extract_ctor_params_omits_auto_bin_size():
    df = _make_df("2025-08-02 19:30:00+00:00")

    auto_indicator = MarketProfileIndicator(df, interval="15m")
    auto_capture = _extract_ctor_params(auto_indicator)
    assert "bin_size" not in auto_capture

    manual_indicator = MarketProfileIndicator(df, interval="15m", bin_size=2.5)
    manual_capture = _extract_ctor_params(manual_indicator)
    assert manual_capture["bin_size"] == pytest.approx(2.5)
