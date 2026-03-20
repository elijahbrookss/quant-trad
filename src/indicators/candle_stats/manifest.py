"""Candle stats indicator manifest."""

from __future__ import annotations

from indicators.manifest import IndicatorManifest, IndicatorOutput, IndicatorOverlay, IndicatorParam


MANIFEST = IndicatorManifest(
    type="candle_stats",
    version="v1",
    label="Candle Stats",
    description="Walk-forward candle statistics and volatility metrics for downstream strategy logic.",
    params=(
        IndicatorParam(
            key="atr_short_window",
            type="int",
            label="ATR Short Window",
            description="Short EMA window used for ATR.",
            default=14,
        ),
        IndicatorParam(
            key="atr_long_window",
            type="int",
            label="ATR Long Window",
            description="Long EMA window used for ATR.",
            default=50,
        ),
        IndicatorParam(
            key="atr_z_window",
            type="int",
            label="ATR Z-Score Window",
            description="History window used when standardizing ATR.",
            default=100,
            advanced=True,
            group="stability",
        ),
        IndicatorParam(
            key="directional_efficiency_window",
            type="int",
            label="Directional Efficiency Window",
            description="Window used to measure directional efficiency.",
            default=20,
            advanced=True,
            group="stability",
        ),
        IndicatorParam(
            key="slope_window",
            type="int",
            label="Slope Window",
            description="Bars used to compute close slope.",
            default=20,
        ),
        IndicatorParam(
            key="range_window",
            type="int",
            label="Range Window",
            description="Bars used to compute rolling range width.",
            default=20,
            advanced=True,
        ),
        IndicatorParam(
            key="expansion_window",
            type="int",
            label="Expansion Window",
            description="Bars used to compute expansion percentage.",
            default=20,
            advanced=True,
        ),
        IndicatorParam(
            key="volume_window",
            type="int",
            label="Volume Window",
            description="Bars used for volume ratio calculations.",
            default=50,
            advanced=True,
        ),
        IndicatorParam(
            key="overlap_window",
            type="int",
            label="Overlap Window",
            description="Bars used for body overlap scoring.",
            default=8,
            advanced=True,
        ),
        IndicatorParam(
            key="slope_stability_lookback",
            type="int",
            label="Slope Stability Lookback",
            description="History depth used to normalize slope stability.",
            default=150,
            advanced=True,
            group="stability",
        ),
        IndicatorParam(
            key="warmup_bars",
            type="int",
            label="Warmup Bars",
            description="Bars required before runtime outputs become ready.",
            default=200,
            advanced=True,
            group="stability",
        ),
    ),
    outputs=(
        IndicatorOutput(
            name="candle_stats",
            type="metric",
            label="Candle Stats",
            fields=(
                "body_pct",
                "upper_wick_pct",
                "lower_wick_pct",
                "range_pct",
                "atr_short",
                "atr_long",
                "atr_ratio",
                "atr_zscore",
                "directional_efficiency",
                "close_slope",
                "slope_stability",
                "range_width",
                "expansion_pct",
                "volume_ratio",
                "body_overlap_pct",
            ),
        ),
    ),
    overlays=(
        IndicatorOverlay(
            name="atr_short",
            overlay_type="candle_stats_atr_short",
            label="ATR Short",
            description="Short ATR line.",
        ),
        IndicatorOverlay(
            name="atr_long",
            overlay_type="candle_stats_atr_long",
            label="ATR Long",
            description="Long ATR line.",
        ),
        IndicatorOverlay(
            name="atr_zscore",
            overlay_type="candle_stats_atr_zscore",
            label="ATR Z-Score",
            description="ATR z-score line.",
        ),
    ),
)

__all__ = ["MANIFEST"]
