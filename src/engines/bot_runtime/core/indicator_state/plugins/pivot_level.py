"""Pivot Level indicator plugin manifest."""

from __future__ import annotations

from ..basic_engines import build_pivot_engine
from .common import generic_overlay_entries, generic_rule_payload
from .registry import indicator_plugin_manifest


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
)
class _PivotLevelPlugin:
    pass
