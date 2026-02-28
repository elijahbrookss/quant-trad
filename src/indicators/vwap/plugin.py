"""VWAP indicator plugin manifest."""

from __future__ import annotations

from engines.indicator_engine.basic_engines import VWAPStateEngine
from engines.indicator_engine.plugin_adapters import (
    generic_overlay_entries,
    generic_rule_payload,
)
from engines.indicator_engine.plugin_registry import IndicatorPluginManifest, register_plugin
from indicators.vwap.overlays import vwap_overlay_adapter


register_plugin(
    IndicatorPluginManifest(
        indicator_type="vwap",
        engine_factory=lambda _meta: VWAPStateEngine(),
        evaluation_mode="session",
        signal_emitter=lambda payload, candle, previous: generic_rule_payload(
            snapshot_payload=payload,
            candle=candle,
            previous_candle=previous,
        ),
        overlay_projector=generic_overlay_entries,
        signal_overlay_adapter=vwap_overlay_adapter,
    )
)
