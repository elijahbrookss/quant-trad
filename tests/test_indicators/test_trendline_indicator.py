from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from indicators.config import IndicatorExecutionContext
from indicators.trendline import TrendlineIndicator, TrendlineIndicatorDefinition


@pytest.fixture
def dummy_df():
    idx = pd.date_range("2025-01-01 09:30", periods=30, freq="30min")
    close = [100.0 + (0.5 * i) + ((-1) ** i * 0.05) for i in range(30)]
    return pd.DataFrame(
        {
            "open": close,
            "high": close + 0.2,
            "low": close - 0.2,
            "close": close,
            "volume": [1000] * 30,
        },
        index=idx,
    )


def test_definition_builds_compute_request_from_execution_context() -> None:
    resolved = TrendlineIndicatorDefinition.resolve_config(
        {
            "lookbacks": [3, 5],
            "tolerance": 0.01,
            "timeframe": "1d",
            "algo": "pivot_ransac",
        },
        strict_unknown=True,
    )
    execution_context = IndicatorExecutionContext(
        symbol="CL",
        start="2025-05-01T00:00:00+00:00",
        end="2025-06-13T00:00:00+00:00",
        interval="15m",
    )

    request = TrendlineIndicatorDefinition.build_compute_data_request(
        resolved_params=resolved,
        execution_context=execution_context,
    )

    assert request.symbol == "CL"
    assert request.interval == "15m"
    assert resolved["lookbacks"] == [3, 5]


def test_find_pivots(dummy_df) -> None:
    indicator = TrendlineIndicator(
        dummy_df,
        lookbacks=[2],
        tolerance=0.01,
        min_span_bars=4,
        algo="pivot_ransac",
    )
    highs, lows = indicator._find_pivots(2)
    assert isinstance(highs, list) and isinstance(lows, list)
    assert all(isinstance(point, tuple) and len(point) == 2 for point in highs + lows)
    assert all(
        isinstance(point[0], pd.Timestamp) and isinstance(point[1], (int, float))
        for point in highs + lows
    )


def test_to_lightweight_structure_and_bounds(dummy_df) -> None:
    indicator = TrendlineIndicator(
        dummy_df,
        lookbacks=[3],
        tolerance=0.01,
        min_span_bars=4,
        algo="pivot_ransac",
        projection_bars=40,
    )
    payload = indicator.to_lightweight(plot_df=dummy_df, include_touches=True)

    assert isinstance(payload, dict)
    assert "segments" in payload and "markers" in payload
    segments, markers = payload["segments"], payload["markers"]
    assert isinstance(segments, list) and isinstance(markers, list)

    for segment in segments:
        for key in ("x1", "x2", "y1", "y2", "lineStyle", "lineWidth", "color"):
            assert key in segment
        assert segment["x2"] >= segment["x1"]
        assert segment["lineStyle"] in (0, 2)

    for marker in markers:
        assert marker.get("shape") == "circle"
        assert marker.get("subtype") == "touch"
        assert "price" in marker and isinstance(marker["price"], (int, float))


def test_projection_toggle(dummy_df) -> None:
    indicator = TrendlineIndicator(
        dummy_df,
        lookbacks=[3],
        tolerance=0.01,
        min_span_bars=4,
        algo="pivot_ransac",
        projection_bars=0,
    )
    payload = indicator.to_lightweight(plot_df=dummy_df, include_touches=False)

    assert all(
        segment.get("lineStyle") == 0 for segment in payload["segments"]
    )
