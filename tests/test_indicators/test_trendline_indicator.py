from __future__ import annotations

import numpy as np
import pytest

pd = pytest.importorskip("pandas")

from indicators.config import IndicatorExecutionContext
from indicators.trendline import TrendlineIndicator, TrendlineIndicatorDefinition
from indicators.trendline.compute.engine import _ransac_line


@pytest.fixture
def dummy_df():
    idx = pd.date_range("2025-01-01 09:30", periods=30, freq="30min")
    close = [100.0 + (0.5 * i) + ((-1) ** i * 0.05) for i in range(30)]
    return pd.DataFrame(
        {
            "open": close,
            "high": [value + 0.2 for value in close],
            "low": [value - 0.2 for value in close],
            "close": close,
            "volume": [1000] * 30,
        },
        index=idx,
    )


@pytest.fixture
def pivot_rich_df():
    idx = pd.date_range("2025-01-01 09:30", periods=32, freq="30min")
    close = [100.0 + (0.1 * i) for i in range(len(idx))]
    return pd.DataFrame(
        {
            "open": close,
            "high": [
                value + (2.0 if i % 2 else 0.2)
                for i, value in enumerate(close)
            ],
            "low": [
                value - (2.0 if i % 2 == 0 else 0.2)
                for i, value in enumerate(close)
            ],
            "close": close,
            "volume": [1000] * len(idx),
        },
        index=idx,
    )


def _build_pivot_rich_indicator(pivot_rich_df, *, projection_bars: int):
    return TrendlineIndicator(
        pivot_rich_df,
        lookbacks=[1],
        tolerance=0.01,
        min_span_bars=4,
        pivot_dedupe_frac=0.0,
        algo="pivot_ransac",
        projection_bars=projection_bars,
        ransac_trials=40,
        ransac_tol_frac=1e-9,
        ransac_min_inliers=3,
        max_lines_per_side=1,
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


def test_find_pivots(pivot_rich_df) -> None:
    indicator = _build_pivot_rich_indicator(
        pivot_rich_df,
        projection_bars=4,
    )
    highs, lows = indicator._find_pivots(1)

    assert [point[0] for point in highs] == list(pivot_rich_df.index[1:31:2])
    assert [point[0] for point in lows] == list(pivot_rich_df.index[2:31:2])
    assert all(isinstance(point, tuple) and len(point) == 2 for point in highs + lows)
    assert all(
        isinstance(point[0], pd.Timestamp) and isinstance(point[1], (int, float))
        for point in highs + lows
    )


def test_to_lightweight_structure_and_bounds(pivot_rich_df) -> None:
    indicator = _build_pivot_rich_indicator(
        pivot_rich_df,
        projection_bars=40,
    )
    payload = indicator.to_lightweight(plot_df=pivot_rich_df, include_touches=True)

    assert indicator.lines
    assert {line["side"] for line in indicator.lines} == {"support", "resistance"}
    segments, markers = payload["segments"], payload["markers"]
    assert segments
    assert markers
    assert {segment["lineStyle"] for segment in segments} == {0, 2}
    assert {marker["position"] for marker in markers} == {"aboveBar", "belowBar"}

    for segment in segments:
        for key in ("x1", "x2", "y1", "y2", "lineStyle", "lineWidth", "color"):
            assert key in segment
        assert segment["x2"] >= segment["x1"]
        assert segment["lineStyle"] in (0, 2)

    for marker in markers:
        assert marker.get("shape") == "circle"
        assert marker.get("subtype") == "touch"
        assert "price" in marker and isinstance(marker["price"], (int, float))


def test_projection_toggle(pivot_rich_df) -> None:
    indicator = _build_pivot_rich_indicator(
        pivot_rich_df,
        projection_bars=0,
    )
    payload = indicator.to_lightweight(plot_df=pivot_rich_df, include_touches=False)

    assert indicator.lines
    assert payload["segments"]
    assert all(segment.get("lineStyle") == 0 for segment in payload["segments"])


def test_ransac_sampling_is_repeatable_for_identical_inputs() -> None:
    x = np.arange(8, dtype=float)
    y = np.array([1.0, 3.0, 5.0, 7.0, 20.0, 25.0, 30.0, 35.0])

    signatures = set()
    for _ in range(8):
        result = _ransac_line(
            x,
            y,
            trials=40,
            tol_frac=1e-12,
            min_inliers=4,
        )
        assert result is not None
        slope, intercept, inliers = result
        signatures.add(
            (
                round(slope, 12),
                round(intercept, 12),
                tuple(bool(value) for value in inliers),
            )
        )

    assert len(signatures) == 1
