"""VWAP indicator manifest."""

from __future__ import annotations

from indicators.manifest import (
    IndicatorManifest,
    IndicatorOption,
    IndicatorOverlay,
    IndicatorParam,
)


MANIFEST = IndicatorManifest(
    type="vwap",
    version="v1",
    label="VWAP Bands",
    description="Anchored VWAP with rolling standard-deviation bands.",
    color_mode="palette",
    params=(
        IndicatorParam(
            key="stddev_window",
            type="int",
            label="Standard Deviation Window",
            description="Rolling window used when computing deviation bands.",
            default=20,
        ),
        IndicatorParam(
            key="stddev_multipliers",
            type="float_list",
            label="Band Multipliers",
            description="Standard-deviation multipliers to plot above and below VWAP.",
            default=[1.0, 2.0],
        ),
        IndicatorParam(
            key="reset_by",
            type="string",
            label="Reset By",
            description="Reset VWAP daily or keep it cumulative.",
            default="D",
            options=(
                IndicatorOption("D", "Daily Session", "Reset VWAP at each daily session boundary."),
                IndicatorOption("cumulative", "Cumulative", "Keep VWAP anchored across the full loaded range."),
            ),
            advanced=True,
        ),
    ),
    overlays=(
        IndicatorOverlay(
            name="vwap_bands",
            overlay_type="vwap_bands",
            label="VWAP Bands",
            description="VWAP line and deviation bands.",
        ),
    ),
)

__all__ = ["MANIFEST"]
