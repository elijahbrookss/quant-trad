"""Regime indicator exports."""

from __future__ import annotations

from typing import Any, Mapping

from .config import (
    RegimeBlockConfig,
    RegimeRuntimeConfig,
    RegimeStabilizerConfig,
    default_regime_runtime_config,
)
from .engine import RegimeEngineV1, RegimeOutput
from .overlays import build_regime_overlay, build_regime_overlays
from .stabilizer import RegimeStabilizer


class RegimeIndicator:
    NAME = "regime"
    REQUIRED_PARAMS: tuple[str, ...] = ()
    OUTPUTS = (
        {
            "name": "market_regime",
            "type": "context",
            "label": "Market Regime",
            "state_keys": (
                "trend",
                "range",
                "transition",
                "chop",
                "unknown",
            ),
            "fields": (
                "regime_key",
                "volatility_state",
                "liquidity_state",
                "expansion_state",
            ),
        },
    )
    OVERLAYS = (
        {"name": "regime", "overlay_type": "regime_overlay"},
        {"name": "regime_markers", "overlay_type": "regime_markers"},
    )
    DEFAULT_PARAMS = {
        "min_confidence": 0.60,
        "structure_min_confidence": 0.45,
        "structure_confirm_bars": 3,
        "volatility_confirm_bars": 4,
        "liquidity_confirm_bars": 3,
        "expansion_confirm_bars": 3,
        "smoothing_alpha": 0.25,
    }

    def __init__(
        self,
        min_confidence: float,
        structure_min_confidence: float,
        structure_confirm_bars: int,
        volatility_confirm_bars: int,
        liquidity_confirm_bars: int,
        expansion_confirm_bars: int,
        smoothing_alpha: float,
    ) -> None:
        self.min_confidence = float(min_confidence)
        self.structure_min_confidence = float(structure_min_confidence)
        self.structure_confirm_bars = int(structure_confirm_bars)
        self.volatility_confirm_bars = int(volatility_confirm_bars)
        self.liquidity_confirm_bars = int(liquidity_confirm_bars)
        self.expansion_confirm_bars = int(expansion_confirm_bars)
        self.smoothing_alpha = float(smoothing_alpha)

    @classmethod
    def from_context(cls, provider: Any, ctx: Any, **params: Any) -> "RegimeIndicator":
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
    ) -> "TypedRegimeIndicator":
        from .runtime import TypedRegimeIndicator, resolve_regime_dependency

        dependency_indicator_id = resolve_regime_dependency(
            indicator_id=indicator_id,
            meta=meta,
            strategy_indicator_metas=strategy_indicator_metas,
        )
        return TypedRegimeIndicator(
            indicator_id=indicator_id,
            version=str(meta.get("version") or "v1"),
            params=dict(resolved_params),
            candle_stats_indicator_id=dependency_indicator_id,
        )


__all__ = [
    "RegimeBlockConfig",
    "RegimeEngineV1",
    "RegimeIndicator",
    "RegimeOutput",
    "RegimeRuntimeConfig",
    "RegimeStabilizer",
    "RegimeStabilizerConfig",
    "build_regime_overlay",
    "build_regime_overlays",
    "default_regime_runtime_config",
]
