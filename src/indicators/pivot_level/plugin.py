"""Pivot Level indicator plugin manifest."""

from __future__ import annotations

from engines.indicator_engine.basic_engines import build_pivot_engine
from engines.indicator_engine.plugin_adapters import (
    generic_overlay_entries,
    generic_rule_payload,
)
from engines.indicator_engine.plugin_registry import (
    IndicatorPluginManifest,
    SignalCatalogEntry,
    SignalDirectionSpec,
    register_plugin,
)
from indicators.pivot_level.overlays import pivot_signals_to_overlays


_DEFAULT_DIRECTIONS = (
    SignalDirectionSpec(
        id="long",
        label="Long",
        description="Setup that supports a long bias.",
    ),
    SignalDirectionSpec(
        id="short",
        label="Short",
        description="Setup that supports a short bias.",
    ),
)


register_plugin(
    IndicatorPluginManifest(
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
        signal_rules=(
            SignalCatalogEntry(
                id="pivot_breakout",
                label="Breakout",
                description="Pivot level breakout confirmation.",
                signal_type="breakout",
                directions=_DEFAULT_DIRECTIONS,
            ),
            SignalCatalogEntry(
                id="pivot_retest",
                label="Retest",
                description="Pivot level retest confirmation.",
                signal_type="retest",
                aliases=("pivot_level_retest",),
                directions=_DEFAULT_DIRECTIONS,
            ),
        ),
    )
)
