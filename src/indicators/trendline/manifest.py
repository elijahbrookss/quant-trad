"""Trendline indicator manifest."""

from __future__ import annotations

from indicators.manifest import (
    IndicatorManifest,
    IndicatorOption,
    IndicatorOverlay,
    IndicatorParam,
    TIMEFRAME_OPTIONS,
)


MANIFEST = IndicatorManifest(
    type="trendline",
    version="v1",
    label="Trendline",
    description="Pivot-anchored trendline detection with support and resistance segments.",
    params=(
        IndicatorParam(
            key="lookbacks",
            type="int_list",
            label="Lookbacks",
            description="Pivot lookback windows used to collect candidate pivots.",
            default=[5],
        ),
        IndicatorParam(
            key="tolerance",
            type="float",
            label="Touch Tolerance",
            description="Tolerance used when counting touches against a line.",
            default=0.0015,
        ),
        IndicatorParam(
            key="timeframe",
            type="string",
            label="Timeframe Label",
            description="Display label attached to the detected trendlines.",
            default="1d",
            options=TIMEFRAME_OPTIONS,
            advanced=True,
        ),
        IndicatorParam(
            key="min_span_bars",
            type="int",
            label="Minimum Span Bars",
            description="Minimum distance between earliest and latest pivot in a line window.",
            default=12,
            advanced=True,
        ),
        IndicatorParam(
            key="window_size",
            type="int",
            label="Window Size",
            description="Pivot window size used when fitting candidate lines.",
            default=3,
            advanced=True,
        ),
        IndicatorParam(
            key="max_windows_per_side",
            type="int",
            label="Max Windows Per Side",
            description="Maximum candidate windows evaluated per line side.",
            default=2,
            advanced=True,
        ),
        IndicatorParam(
            key="pivot_dedupe_frac",
            type="float",
            label="Pivot Dedup Fraction",
            description="Fractional threshold used to merge nearby pivots.",
            default=0.005,
            advanced=True,
        ),
        IndicatorParam(
            key="enforce_direction",
            type="bool",
            label="Enforce Direction",
            description="Require line slope to match the local price move.",
            default=True,
            advanced=True,
        ),
        IndicatorParam(
            key="algo",
            type="string",
            label="Algorithm",
            description="Trendline extraction algorithm.",
            default="pivot_ransac",
            options=(
                IndicatorOption(
                    "pivot_ransac",
                    "Pivot RANSAC",
                    "Robustly fit support and resistance lines from pivot clusters.",
                ),
            ),
            advanced=True,
        ),
        IndicatorParam(
            key="projection_bars",
            type="int",
            label="Projection Bars",
            description="Bars to project beyond the solid segment.",
            default=40,
            advanced=True,
        ),
        IndicatorParam(
            key="ransac_trials",
            type="int",
            label="RANSAC Trials",
            description="Random samples used when fitting pivot lines.",
            default=250,
            advanced=True,
            group="ransac",
        ),
        IndicatorParam(
            key="ransac_tol_frac",
            type="float",
            label="RANSAC Tolerance",
            description="Relative error tolerance used to classify inliers.",
            default=0.003,
            advanced=True,
            group="ransac",
        ),
        IndicatorParam(
            key="ransac_min_inliers",
            type="int",
            label="RANSAC Minimum Inliers",
            description="Minimum pivot inliers required to accept a line.",
            default=3,
            advanced=True,
            group="ransac",
        ),
        IndicatorParam(
            key="max_lines_per_side",
            type="int",
            label="Max Lines Per Side",
            description="Maximum support and resistance lines extracted.",
            default=2,
            advanced=True,
        ),
    ),
    overlays=(
        IndicatorOverlay(
            name="trendline",
            overlay_type="trendline",
            label="Trendline",
            description="Trendline segments and touch markers.",
        ),
    ),
)

__all__ = ["MANIFEST"]
