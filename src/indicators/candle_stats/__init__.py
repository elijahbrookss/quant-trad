"""Candle stats indicator type declaration."""

from __future__ import annotations

from typing import Any, Mapping

from .runtime import TypedCandleStatsIndicator


class CandleStatsIndicator:
    NAME = "candle_stats"
    REQUIRED_PARAMS: tuple[str, ...] = ()
    OUTPUTS = (
        {
            "name": "candle_stats",
            "type": "metric",
            "label": "Candle Stats",
            "fields": (
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
        },
    )
    DEFAULT_PARAMS = {
        "atr_short_window": 14,
        "atr_long_window": 50,
        "atr_z_window": 100,
        "directional_efficiency_window": 20,
        "slope_window": 20,
        "range_window": 20,
        "expansion_window": 20,
        "volume_window": 50,
        "overlap_window": 8,
        "slope_stability_lookback": 150,
        "warmup_bars": 200,
    }

    def __init__(
        self,
        atr_short_window: int,
        atr_long_window: int,
        atr_z_window: int,
        directional_efficiency_window: int,
        slope_window: int,
        range_window: int,
        expansion_window: int,
        volume_window: int,
        overlap_window: int,
        slope_stability_lookback: int,
        warmup_bars: int,
    ) -> None:
        self.atr_short_window = int(atr_short_window)
        self.atr_long_window = int(atr_long_window)
        self.atr_z_window = int(atr_z_window)
        self.directional_efficiency_window = int(directional_efficiency_window)
        self.slope_window = int(slope_window)
        self.range_window = int(range_window)
        self.expansion_window = int(expansion_window)
        self.volume_window = int(volume_window)
        self.overlap_window = int(overlap_window)
        self.slope_stability_lookback = int(slope_stability_lookback)
        self.warmup_bars = int(warmup_bars)

    @classmethod
    def from_context(cls, provider: Any, ctx: Any, **params: Any) -> "CandleStatsIndicator":
        resolved = dict(cls.DEFAULT_PARAMS)
        resolved.update(params)
        return cls(**resolved)

    @classmethod
    def build_runtime_indicator(
        cls,
        *,
        indicator_id: str,
        meta: Mapping[str, Any],
        resolved_params: Mapping[str, Any],
        strategy_indicator_metas: Mapping[str, Mapping[str, Any]],
    ) -> TypedCandleStatsIndicator:
        return TypedCandleStatsIndicator(
            indicator_id=indicator_id,
            version=str(meta.get("version") or "v1"),
            params=dict(resolved_params),
        )


__all__ = ["CandleStatsIndicator"]
