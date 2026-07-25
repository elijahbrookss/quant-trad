from __future__ import annotations

import pandas as pd
import pytest

from engines.bot_runtime.core.domain.candle_factory import (
    CandleFrameValidationError,
    build_candles_from_dataframe,
)
from engines.bot_runtime.strategy.series_builder_parts.series_construction import (
    SeriesBuilderConstructionMixin,
)
from portal.backend.service.indicators.indicator_service.signals import (
    _build_candles as build_indicator_preview_candles,
)
from portal.backend.service.strategies.strategy_service.typed_preview import (
    _build_candles as build_strategy_preview_candles,
)


def _frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        index=pd.to_datetime(
            [f"2025-01-01T00:{minute:02d}:00Z" for minute in range(len(rows))],
            utc=True,
        ),
    )


def test_canonical_builder_preserves_order_end_time_and_prior_lookback() -> None:
    rows = [
        {
            "open": float(index + 1),
            "high": float(index + 2),
            "low": float(index),
            "close": float(index + 1.5),
            "volume": float(index + 10),
        }
        for index in range(16)
    ]
    candles = build_candles_from_dataframe(_frame(rows).iloc[::-1], timeframe="1m")

    assert len(candles) == 16
    assert candles[0].time < candles[-1].time
    assert candles[0].end == candles[1].time
    assert candles[14].lookback_15["avg_range_15"] is None
    assert candles[15].lookback_15 == {
        "avg_range_15": 2.0,
        "avg_atr_15": None,
        "avg_volume_15": 17.0,
    }


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("open", "not-a-price", "field=open reason=not_numeric"),
        ("close", float("nan"), "field=close reason=not_finite"),
        ("high", 0.5, "field=high reason=high=0.5 below open_or_close=1.5"),
        ("low", 1.25, "field=low reason=low=1.25 above open_or_close=1.0"),
        ("volume", -1.0, "field=volume reason=negative"),
    ],
)
def test_canonical_builder_rejects_malformed_rows_with_location(
    field: str,
    value: object,
    reason: str,
) -> None:
    row = {
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 10.0,
    }
    row[field] = value

    with pytest.raises(
        CandleFrameValidationError,
        match=f"row=0 timestamp=2025-01-01T00:00:00\\+00:00 {reason}",
    ):
        build_candles_from_dataframe(_frame([row]))


def test_canonical_builder_rejects_duplicate_timestamps() -> None:
    frame = _frame(
        [
            {"open": 1, "high": 2, "low": 0.5, "close": 1.5},
            {"open": 2, "high": 3, "low": 1.5, "close": 2.5},
        ]
    )
    frame.index = [frame.index[0], frame.index[0]]

    with pytest.raises(CandleFrameValidationError, match="duplicate timestamp="):
        build_candles_from_dataframe(frame)


def test_runtime_and_preview_paths_share_strict_builder() -> None:
    frame = _frame(
        [{"open": 1.0, "high": 2.0, "low": 0.5, "close": "malformed"}]
    )

    for builder in (
        SeriesBuilderConstructionMixin._build_candles,
        build_indicator_preview_candles,
        build_strategy_preview_candles,
    ):
        with pytest.raises(
            CandleFrameValidationError,
            match="field=close reason=not_numeric",
        ):
            builder(frame)
