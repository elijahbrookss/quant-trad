"""Pivot Level indicator plugin manifest."""

from __future__ import annotations

from engines.indicator_engine.basic_engines import build_pivot_engine
from engines.indicator_engine.plugin_adapters import (
    generic_overlay_entries,
    generic_rule_payload,
)
from engines.indicator_engine.plugin_registry import indicator_plugin_manifest
from indicators.pivot_level.overlays import pivot_signals_to_overlays


@indicator_plugin_manifest(
    indicator_type="pivot_level",
    engine_factory=lambda _meta: build_pivot_engine(),
    evaluation_mode="rolling",
    signal_emitter=lambda payload, candle, previous: generic_rule_payload(
        snapshot_payload=payload,
        candle=candle,
        previous_candle=previous,
    ),
    overlay_projector=generic_overlay_entries,
    signal_overlay_adapter=pivot_signals_to_overlays,
)
class _PivotLevelPlugin:
    pass
