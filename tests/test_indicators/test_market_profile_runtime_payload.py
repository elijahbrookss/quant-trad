from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from indicators.market_profile import MarketProfileIndicator


def _sample_df() -> pd.DataFrame:
    idx = pd.date_range("2025-01-01 00:00:00+00:00", periods=6, freq="30min", tz="UTC")
    return pd.DataFrame(
        {
            "open": [100.0, 100.5, 101.0, 101.5, 102.0, 102.5],
            "high": [101.0, 101.5, 102.0, 102.5, 103.0, 103.5],
            "low": [99.5, 100.0, 100.5, 101.0, 101.5, 102.0],
            "close": [100.5, 101.0, 101.5, 102.0, 102.5, 103.0],
            "volume": [10, 11, 12, 13, 14, 15],
        },
        index=idx,
    )


def test_build_runtime_signal_payload_injects_merge_defaults_for_partial_params() -> None:
    indicator = MarketProfileIndicator(_sample_df(), bin_size=0.5)
    payload = indicator.build_runtime_signal_payload(
        indicator_id="ind-1",
        params={"days_back": 30},
        symbol="BIP-TEST",
        chart_timeframe="1h",
    )

    params = payload["profile_params"]
    assert "use_merged_value_areas" in params
    assert "merge_threshold" in params
    assert "min_merge_sessions" in params
    assert params["days_back"] == 30
    assert payload["profile_chart_timeframe"] == "1h"
