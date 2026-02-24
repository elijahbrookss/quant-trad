"""Market Profile plugin manifest wiring.

Indicator-specific signal/overlay logic lives in src/indicators/market_profile/signals.
"""

from __future__ import annotations

from typing import Any, Mapping

from indicators.market_profile.signals import (
    market_profile_overlay_entries,
    market_profile_rule_payload,
)
from indicators.market_profile.overlays import market_profile_overlay_adapter

from engines.indicator_engine.plugin_registry import indicator_plugin_manifest
from indicators.market_profile.runtime.state_engine import (
    MarketProfileEngineConfig,
    MarketProfileStateEngine,
)


def _market_profile_engine_factory(meta: Mapping[str, Any]) -> MarketProfileStateEngine:
    params = dict(meta.get("params") or {}) if isinstance(meta, Mapping) else {}
    config = MarketProfileEngineConfig(
        params=params,
        overlay_color=str(meta.get("color") or "").strip() or None,
    )
    return MarketProfileStateEngine(config)


@indicator_plugin_manifest(
    indicator_type="market_profile",
    engine_factory=_market_profile_engine_factory,
    evaluation_mode="session",
    signal_emitter=lambda payload, candle, previous: market_profile_rule_payload(
        snapshot_payload=payload,
        candle=candle,
        previous_candle=previous,
    ),
    overlay_projector=market_profile_overlay_entries,
    signal_overlay_adapter=market_profile_overlay_adapter,
)
class _MarketProfilePlugin:
    pass
