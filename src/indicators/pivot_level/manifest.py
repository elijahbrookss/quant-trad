"""Pivot level indicator manifest."""

from __future__ import annotations

from indicators.manifest import (
    IndicatorManifest,
    IndicatorParam,
    IndicatorRuntimeInput,
    TIMEFRAME_OPTIONS,
)


MANIFEST = IndicatorManifest(
    type="pivot_level",
    version="v1",
    label="Pivot Levels",
    description="Support and resistance levels clustered from pivot highs and lows.",
    params=(
        IndicatorParam(
            key="timeframe",
            type="string",
            label="Source Timeframe",
            description="Source timeframe used to compute pivot levels.",
            required=True,
            options=TIMEFRAME_OPTIONS,
        ),
        IndicatorParam(
            key="lookbacks",
            type="int_list",
            label="Lookbacks",
            description="Pivot lookback windows used when finding highs and lows.",
            default=[10, 20, 50],
        ),
        IndicatorParam(
            key="threshold",
            type="float",
            label="Dedup Threshold",
            description="Fractional threshold used to merge nearby levels.",
            default=0.005,
        ),
        IndicatorParam(
            key="days_back",
            type="int",
            label="Days Back",
            description="Historical lookback window used for the source timeframe.",
            default=180,
            advanced=True,
        ),
    ),
    runtime_inputs=(
        IndicatorRuntimeInput(
            source_timeframe_param="timeframe",
            lookback_days_param="days_back",
        ),
    ),
)

__all__ = ["MANIFEST"]
