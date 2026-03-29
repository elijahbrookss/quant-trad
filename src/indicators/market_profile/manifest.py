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
DEFAULT_BREAKOUT_CONFIRM_BARS = 2
DEFAULT_RECLAIM_MAX_BARS = 6
DEFAULT_RETEST_ATR_PERIOD = 14
DEFAULT_RETEST_MIN_ACCEPTANCE_BARS = 2
DEFAULT_RETEST_MIN_EXCURSION_ATR = 1.0
DEFAULT_RETEST_MAX_BARS = 6
DEFAULT_RETEST_TOUCH_TOLERANCE_ATR = 0.2
DEFAULT_RETEST_MAX_PENETRATION_ATR = 0.35
DEFAULT_RETEST_HOLD_CONFIRM_BARS = 1


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
        IndicatorParam(
            key="breakout_confirm_bars",
            type="int",
            label="Breakout Confirm Bars",
            description="Consecutive closes outside value required to confirm a breakout, including the initial breakout bar.",
            default=DEFAULT_BREAKOUT_CONFIRM_BARS,
            advanced=True,
            group="signals",
        ),
        IndicatorParam(
            key="reclaim_max_bars",
            type="int",
            label="Reclaim Max Bars",
            description="Maximum bars allowed after a confirmed breakout for the outside-to-inside-to-outside reclaim sequence to complete.",
            default=DEFAULT_RECLAIM_MAX_BARS,
            advanced=True,
            group="signals",
        ),
        IndicatorParam(
            key="retest_atr_period",
            type="int",
            label="Retest ATR Period",
            description="ATR lookback used to normalize structural retest acceptance and touch thresholds.",
            default=DEFAULT_RETEST_ATR_PERIOD,
            advanced=True,
            group="signals",
        ),
        IndicatorParam(
            key="retest_min_acceptance_bars",
            type="int",
            label="Retest Min Acceptance Bars",
            description="Minimum bars that must remain outside value after confirmation before a structural retest is eligible.",
            default=DEFAULT_RETEST_MIN_ACCEPTANCE_BARS,
            advanced=True,
            group="signals",
        ),
        IndicatorParam(
            key="retest_min_excursion_atr",
            type="float",
            label="Retest Min Excursion ATR",
            description="Minimum excursion away from VAH or VAL, in ATR units, required before a pullback can qualify as a structural retest.",
            default=DEFAULT_RETEST_MIN_EXCURSION_ATR,
            advanced=True,
            group="signals",
        ),
        IndicatorParam(
            key="retest_max_bars",
            type="int",
            label="Retest Max Bars",
            description="Maximum bars allowed after acceptance is established for a structural retest to touch, hold, and confirm.",
            default=DEFAULT_RETEST_MAX_BARS,
            advanced=True,
            group="signals",
        ),
        IndicatorParam(
            key="retest_touch_tolerance_atr",
            type="float",
            label="Retest Touch Tolerance ATR",
            description="How close price must come to VAH or VAL, in ATR units, to count as a structural retest touch.",
            default=DEFAULT_RETEST_TOUCH_TOLERANCE_ATR,
            advanced=True,
            group="signals",
        ),
        IndicatorParam(
            key="retest_max_penetration_atr",
            type="float",
            label="Retest Max Penetration ATR",
            description="Maximum allowed penetration back through VAH or VAL, in ATR units, before a structural retest is invalidated.",
            default=DEFAULT_RETEST_MAX_PENETRATION_ATR,
            advanced=True,
            group="signals",
        ),
        IndicatorParam(
            key="retest_hold_confirm_bars",
            type="int",
            label="Retest Hold Confirm Bars",
            description="Consecutive closes back in the breakout direction required after the structural retest touch before the retest fires.",
            default=DEFAULT_RETEST_HOLD_CONFIRM_BARS,
            advanced=True,
            group="signals",
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
        IndicatorOutput(
            name="confirmed_balance_breakout",
            type="signal",
            label="Confirmed Balance Breakout",
            event_keys=("confirmed_balance_breakout_long", "confirmed_balance_breakout_short"),
        ),
        IndicatorOutput(
            name="balance_reclaim",
            type="signal",
            label="Balance Reclaim",
            event_keys=("balance_reclaim_long", "balance_reclaim_short"),
        ),
        IndicatorOutput(
            name="balance_retest",
            type="signal",
            label="Balance Retest",
            event_keys=("balance_retest_long", "balance_retest_short"),
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
    "DEFAULT_BREAKOUT_CONFIRM_BARS",
    "DEFAULT_EXTEND_VALUE_AREA_TO_CHART_END",
    "DEFAULT_MERGE_THRESHOLD",
    "DEFAULT_MIN_MERGE_SESSIONS",
    "DEFAULT_PRICE_PRECISION",
    "DEFAULT_RECLAIM_MAX_BARS",
    "DEFAULT_RETEST_ATR_PERIOD",
    "DEFAULT_RETEST_HOLD_CONFIRM_BARS",
    "DEFAULT_RETEST_MAX_BARS",
    "DEFAULT_RETEST_MAX_PENETRATION_ATR",
    "DEFAULT_RETEST_MIN_ACCEPTANCE_BARS",
    "DEFAULT_RETEST_MIN_EXCURSION_ATR",
    "DEFAULT_RETEST_TOUCH_TOLERANCE_ATR",
    "DEFAULT_USE_MERGED_VALUE_AREAS",
    "MANIFEST",
]
