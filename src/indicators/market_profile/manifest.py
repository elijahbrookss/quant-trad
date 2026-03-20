"""Market profile indicator manifest."""

from __future__ import annotations

from indicators.manifest import (
    IndicatorManifest,
    IndicatorOutput,
    IndicatorOverlay,
    IndicatorParam,
    IndicatorRuntimeInput,
)

DEFAULT_BIN_SIZE = 0.25
DEFAULT_PRICE_PRECISION = 4
DEFAULT_USE_MERGED_VALUE_AREAS = True
DEFAULT_MERGE_THRESHOLD = 0.6
DEFAULT_MIN_MERGE_SESSIONS = 3
DEFAULT_EXTEND_VALUE_AREA_TO_CHART_END = True
DEFAULT_DAYS_BACK = 180


MANIFEST = IndicatorManifest(
    type="market_profile",
    version="v1",
    label="Market Profile",
    description="Session-based value area, balance, and breakout state with chart overlays.",
    params=(
        IndicatorParam(
            key="bin_size",
            type="float",
            label="Bin Size",
            description="Price increment used when building the profile histogram.",
            default=DEFAULT_BIN_SIZE,
        ),
        IndicatorParam(
            key="price_precision",
            type="int",
            label="Price Precision",
            description="Decimal precision used when rounding profile levels.",
            default=DEFAULT_PRICE_PRECISION,
            advanced=True,
        ),
        IndicatorParam(
            key="use_merged_value_areas",
            type="bool",
            label="Merge Value Areas",
            description="Merge adjacent sessions when the overlap threshold is satisfied.",
            default=DEFAULT_USE_MERGED_VALUE_AREAS,
        ),
        IndicatorParam(
            key="merge_threshold",
            type="float",
            label="Merge Threshold",
            description="Minimum overlap ratio required to merge adjacent sessions.",
            default=DEFAULT_MERGE_THRESHOLD,
            advanced=True,
            group="merge",
        ),
        IndicatorParam(
            key="min_merge_sessions",
            type="int",
            label="Minimum Merge Sessions",
            description="Minimum number of sessions required when merging.",
            default=DEFAULT_MIN_MERGE_SESSIONS,
            advanced=True,
            group="merge",
        ),
        IndicatorParam(
            key="extend_value_area_to_chart_end",
            type="bool",
            label="Extend Value Area To End",
            description="Extend active value area boxes forward to the chart end.",
            default=DEFAULT_EXTEND_VALUE_AREA_TO_CHART_END,
        ),
        IndicatorParam(
            key="days_back",
            type="int",
            label="Days Back",
            description="Historical lookback window used for compute previews.",
            default=DEFAULT_DAYS_BACK,
            advanced=True,
        ),
    ),
    outputs=(
        IndicatorOutput(
            name="value_area_metrics",
            type="metric",
            label="Value Area Metrics",
            fields=("poc", "vah", "val", "value_area_width"),
        ),
        IndicatorOutput(
            name="value_location",
            type="context",
            label="Value Location",
            state_keys=("inside_value", "above_value", "below_value"),
        ),
        IndicatorOutput(
            name="balance_state",
            type="context",
            label="Balance State",
            state_keys=("balanced", "imbalanced"),
        ),
        IndicatorOutput(
            name="balance_breakout",
            type="signal",
            label="Balance Breakout",
            event_keys=("balance_breakout_long", "balance_breakout_short"),
        ),
    ),
    overlays=(
        IndicatorOverlay(
            name="value_area",
            overlay_type="market_profile",
            label="Value Area",
            description="Value area boxes and related profile markers.",
        ),
    ),
    runtime_inputs=(
        IndicatorRuntimeInput(
            source_timeframe="30m",
            lookback_days=DEFAULT_DAYS_BACK,
            lookback_days_param="days_back",
        ),
    ),
)

__all__ = [
    "DEFAULT_BIN_SIZE",
    "DEFAULT_DAYS_BACK",
    "DEFAULT_EXTEND_VALUE_AREA_TO_CHART_END",
    "DEFAULT_MERGE_THRESHOLD",
    "DEFAULT_MIN_MERGE_SESSIONS",
    "DEFAULT_PRICE_PRECISION",
    "DEFAULT_USE_MERGED_VALUE_AREAS",
    "MANIFEST",
]
