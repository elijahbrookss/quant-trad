from __future__ import annotations

from datetime import datetime, timezone

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.strategy.series_builder_parts.series_construction import SeriesBuilderConstructionMixin


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_runtime_series_gap_detection_keeps_closure_backed_gap_visible() -> None:
    candles = [
        Candle(time=_dt("2026-01-11T09:00:00Z"), open=2.09, high=2.09, low=2.09, close=2.09),
        Candle(time=_dt("2026-01-11T11:00:00Z"), open=2.10, high=2.10, low=2.10, close=2.10),
    ]

    summary = SeriesBuilderConstructionMixin._runtime_series_candle_continuity(
        candles,
        timeframe="1h",
        gap_classification=[
            {
                "start": "2026-01-11T10:00:00Z",
                "end": "2026-01-11T11:00:00Z",
                "classification": "provider_missing_data",
                "reason_code": "source_sparse",
                "evidence": "portal_candle_closure",
            }
        ],
    )

    assert summary.detected_gap_count == 1
    assert summary.missing_candle_estimate == 1
    assert summary.gap_count_by_type["provider_missing_data"] == 1
    assert summary.gap_count_by_type["unknown_gap"] == 0
