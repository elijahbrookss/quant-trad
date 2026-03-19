"""Lightweight market profile type definition used by runtime/service wiring."""

from __future__ import annotations

from typing import Any, Mapping

from .params import DEFAULT_PARAMS
from .runtime.typed_indicator import TypedMarketProfileIndicator


class MarketProfileIndicator:
    NAME = "market_profile"
    REQUIRED_PARAMS: tuple[str, ...] = ()
    OUTPUTS = (
        {
            "name": "value_area_metrics",
            "type": "metric",
            "label": "Value Area Metrics",
            "fields": ("poc", "vah", "val", "value_area_width"),
        },
        {
            "name": "value_location",
            "type": "context",
            "label": "Value Location",
            "state_keys": ("inside_value", "above_value", "below_value"),
        },
        {
            "name": "balance_state",
            "type": "context",
            "label": "Balance State",
            "state_keys": ("balanced", "imbalanced"),
        },
        {
            "name": "balance_breakout",
            "type": "signal",
            "label": "Balance Breakout",
            "event_keys": ("balance_breakout_long", "balance_breakout_short"),
        },
    )
    OVERLAYS = (
        {"name": "value_area", "overlay_type": "market_profile"},
        {"name": "breakout_markers", "overlay_type": "market_profile"},
    )
    DEFAULT_PARAMS = DEFAULT_PARAMS

    def __init__(
        self,
        bin_size: float,
        price_precision: int,
        use_merged_value_areas: bool,
        merge_threshold: float,
        min_merge_sessions: int,
        extend_value_area_to_chart_end: bool,
        days_back: int,
    ) -> None:
        self.bin_size = float(bin_size)
        self.price_precision = int(price_precision)
        self.use_merged_value_areas = bool(use_merged_value_areas)
        self.merge_threshold = float(merge_threshold)
        self.min_merge_sessions = int(min_merge_sessions)
        self.extend_value_area_to_chart_end = bool(extend_value_area_to_chart_end)
        self.days_back = int(days_back)

    @classmethod
    def from_context(cls, provider: Any, ctx: Any, **params: Any):
        from .compute.engine import MarketProfileIndicator as ComputeMarketProfileIndicator

        resolved = dict(cls.DEFAULT_PARAMS)
        resolved.update(params)
        return ComputeMarketProfileIndicator.from_context(provider, ctx, **resolved)

    @classmethod
    def build_runtime_indicator(
        cls,
        *,
        indicator_id: str,
        meta: Mapping[str, Any],
        resolved_params: Mapping[str, Any],
        strategy_indicator_metas: Mapping[str, Mapping[str, Any]],
    ) -> TypedMarketProfileIndicator:
        return TypedMarketProfileIndicator(
            indicator_id=indicator_id,
            version=str(meta.get("version") or "v1"),
            params=dict(resolved_params),
        )


__all__ = ["MarketProfileIndicator"]
