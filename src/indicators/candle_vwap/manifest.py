"""Versioned public contract for candle VWAP, separate from retired VWAP Bands."""

from indicators.manifest import (
    IndicatorManifest, IndicatorMarketInput, IndicatorOption, IndicatorOutput,
    IndicatorParam, IndicatorRuntimeInput,
)

NATIVE_TIMEFRAMES = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}

MANIFEST = IndicatorManifest(
    type="candle_vwap", version="v1", label="Candle VWAP",
    description="Daily UTC HLC3 VWAP approximation weighted by native candle volume; not exact transaction VWAP.",
    params=(
        IndicatorParam(key="source_timeframe", type="string", label="Native Source Timeframe",
                       description="Must equal the runtime's native primary candle timeframe.", default="15m",
                       options=tuple(IndicatorOption(key, key) for key in NATIVE_TIMEFRAMES)),
    ),
    outputs=(
        IndicatorOutput(name="vwap_context", type="context", label="Session VWAP Context",
                        state_keys=("warming_session", "awaiting_volume", "above", "at", "below"),
                        fields=("session_start", "session_end", "session_timezone", "price_basis",
                                "source_timeframe", "volume_semantics", "session_complete_from_anchor",
                                "observed_bars", "positive_volume_bars", "zero_volume_bar",
                                "close_time", "known_at", "gap_contract", "price_comparison_rel_tol")),
        IndicatorOutput(name="vwap_metrics", type="metric", label="Session VWAP Metrics",
                        fields=("vwap", "close_distance", "close_distance_bps", "cumulative_volume")),
        IndicatorOutput(name="vwap_cross", type="signal", label="Close Crosses Session VWAP",
                        event_keys=("close_cross_above_vwap", "close_cross_below_vwap")),
    ),
    market_inputs=(IndicatorMarketInput(required_fields=(
        "open_time", "close_time", "open", "high", "low", "close", "volume", "known_at")),),
    runtime_inputs=(IndicatorRuntimeInput(source_timeframe_param="source_timeframe"),),
)
