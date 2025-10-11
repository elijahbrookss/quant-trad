import pytest

pd = pytest.importorskip("pandas")

from indicators.market_profile import MarketProfileIndicator
from portal.backend.service.indicator_service import (
    _build_market_profile_overlay_indicator,
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
