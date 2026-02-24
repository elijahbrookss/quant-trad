"""Trendline indicator plugin manifest."""

from __future__ import annotations

from engines.indicator_engine.basic_engines import build_trendline_engine
from engines.indicator_engine.plugin_adapters import (
    generic_overlay_entries,
    generic_rule_payload,
)
from engines.indicator_engine.plugin_registry import indicator_plugin_manifest
from indicators.trendline.overlays import trendline_overlay_adapter


@indicator_plugin_manifest(
    indicator_type="trendline",
    engine_factory=lambda _meta: build_trendline_engine(),
    evaluation_mode="rolling",
    signal_emitter=lambda payload, candle, previous: generic_rule_payload(
        snapshot_payload=payload,
        candle=candle,
        previous_candle=previous,
    ),
    overlay_projector=generic_overlay_entries,
    signal_overlay_adapter=trendline_overlay_adapter,
)
class _TrendlinePlugin:
    pass
