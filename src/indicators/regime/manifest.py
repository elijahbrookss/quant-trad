"""Regime indicator manifest."""

from __future__ import annotations

from indicators.manifest import (
    IndicatorDependency,
    IndicatorManifest,
    IndicatorOutput,
    IndicatorOverlay,
    IndicatorParam,
)


MANIFEST = IndicatorManifest(
    type="regime",
    version="v1",
    label="Regime",
    description="Walk-forward market regime classification built from candle stats.",
    color_mode="palette",
    params=(
        IndicatorParam(
            key="min_confidence",
            type="float",
            label="Minimum Confidence",
            description="Minimum confidence required for regime acceptance.",
            default=0.60,
        ),
        IndicatorParam(
            key="structure_min_confidence",
            type="float",
            label="Structure Minimum Confidence",
            description="Minimum confidence required for structure classification.",
            default=0.45,
        ),
        IndicatorParam(
            key="structure_confirm_bars",
            type="int",
            label="Structure Confirm Bars",
            description="Bars required to confirm structure changes.",
            default=3,
            advanced=True,
            group="confirmation",
        ),
        IndicatorParam(
            key="volatility_confirm_bars",
            type="int",
            label="Volatility Confirm Bars",
            description="Bars required to confirm volatility changes.",
            default=4,
            advanced=True,
            group="confirmation",
        ),
        IndicatorParam(
            key="liquidity_confirm_bars",
            type="int",
            label="Liquidity Confirm Bars",
            description="Bars required to confirm liquidity changes.",
            default=3,
            advanced=True,
            group="confirmation",
        ),
        IndicatorParam(
            key="expansion_confirm_bars",
            type="int",
            label="Expansion Confirm Bars",
            description="Bars required to confirm expansion changes.",
            default=3,
            advanced=True,
            group="confirmation",
        ),
        IndicatorParam(
            key="smoothing_alpha",
            type="float",
            label="Smoothing Alpha",
            description="Smoothing factor applied before stabilizing regime transitions.",
            default=0.25,
            advanced=True,
        ),
    ),
    outputs=(
        IndicatorOutput(
            name="market_regime",
            type="context",
            label="Market Regime",
            state_keys=("trend", "range", "transition", "chop", "unknown"),
            fields=(
                "regime_key",
                "volatility_state",
                "liquidity_state",
                "expansion_state",
            ),
        ),
    ),
    overlays=(
        IndicatorOverlay(
            name="regime",
            overlay_type="regime_overlay",
            label="Regime Overlay",
            description="Regime boxes rendered across the active bar range.",
        ),
        IndicatorOverlay(
            name="regime_markers",
            overlay_type="regime_markers",
            label="Regime Markers",
            description="Regime change markers rendered at transition bars.",
        ),
    ),
    dependencies=(
        IndicatorDependency(
            indicator_type="candle_stats",
            output_name="candle_stats",
            label="Candle Stats",
            description="Regime classification requires candle stats runtime metrics.",
        ),
    ),
)

__all__ = ["MANIFEST"]
