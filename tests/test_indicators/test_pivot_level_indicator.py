from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from indicators.config import IndicatorExecutionContext
from indicators.pivot_level import Level, PivotLevelIndicator, PivotLevelIndicatorDefinition


@pytest.fixture
def dummy_df():
    index = pd.date_range("2025-01-01", periods=10, freq="h")
    data = {
        "open": [100] * 10,
        "high": [101, 103, 99, 102, 105, 98, 101, 104, 103, 99],
        "low": [95, 97, 96, 98, 97, 94, 96, 95, 98, 96],
        "close": [100] * 10,
        "volume": [1000] * 10,
    }
    return pd.DataFrame(data, index=index)


def test_definition_builds_compute_request_from_indicator_timeframe() -> None:
    resolved = PivotLevelIndicatorDefinition.resolve_config(
        {
            "timeframe": "4h",
            "lookbacks": [2, 3, 5],
            "threshold": 0.005,
            "days_back": 180,
        },
        strict_unknown=True,
    )
    execution_context = IndicatorExecutionContext(
        symbol="CL",
        start="2025-05-15T00:00:00+00:00",
        end="2025-06-13T00:00:00+00:00",
        interval="15m",
    )

    request = PivotLevelIndicatorDefinition.build_compute_data_request(
        resolved_params=resolved,
        execution_context=execution_context,
    )

    assert request.symbol == "CL"
    assert request.interval == "4h"
    assert resolved["lookbacks"] == (2, 3, 5)


def test_find_pivots(dummy_df) -> None:
    indicator = PivotLevelIndicator(dummy_df, timeframe="1h", lookbacks=(2,))
    highs, lows = indicator._find_pivots(2)
    assert isinstance(highs, list) and isinstance(lows, list)
    assert all(isinstance(point, tuple) for point in highs + lows)


def test_compute_generates_levels(dummy_df) -> None:
    indicator = PivotLevelIndicator(dummy_df, timeframe="1h", lookbacks=(2,))
    assert isinstance(indicator.levels, list)
    assert all(isinstance(level, Level) for level in indicator.levels)


def test_nearest_support_and_resistance(dummy_df) -> None:
    indicator = PivotLevelIndicator(dummy_df, timeframe="1h", lookbacks=(2,))
    support = indicator.nearest_support(100)
    resistance = indicator.nearest_resistance(100)
    if support:
        assert isinstance(support, Level)
    if resistance:
        assert isinstance(resistance, Level)


def test_distance_to_level(dummy_df) -> None:
    indicator = PivotLevelIndicator(dummy_df, timeframe="1h", lookbacks=(2,))
    if indicator.levels:
        distance = indicator.distance_to_level(indicator.levels[0], 100)
        assert isinstance(distance, float)
        assert distance >= 0
