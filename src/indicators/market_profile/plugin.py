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

from engines.indicator_engine.plugin_registry import (
    IndicatorPluginManifest,
    SignalCatalogEntry,
    SignalDirectionSpec,
    register_plugin,
)
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


register_plugin(
    IndicatorPluginManifest(
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
        signal_rules=(
            SignalCatalogEntry(
                id="market_profile_breakout",
                label="Breakout",
                description="Value-area breakout confirmation.",
                signal_type="breakout",
                aliases=(
                    "market_profile_breakout_v3_confirmed",
                    "market_profile_breakout_v3",
                    "market_profile_breakout_v2",
                ),
                directions=(
                    SignalDirectionSpec(
                        id="long",
                        label="Long breakout",
                        description="Breakout above the active value area high (VAH) that confirms continuation.",
                    ),
                    SignalDirectionSpec(
                        id="short",
                        label="Short breakdown",
                        description="Breakdown below the active value area low (VAL) signalling downside momentum.",
                    ),
                ),
            ),
            SignalCatalogEntry(
                id="market_profile_retest",
                label="Retest",
                description="Value-area breakout retest confirmation.",
                signal_type="retest",
                aliases=(
                    "market_profile_retest_v3",
                    "market_profile_retest_v2",
                ),
                directions=(
                    SignalDirectionSpec(
                        id="long",
                        label="Long retest",
                        description=(
                            "Breakout above VAH with a successful retest hold or a reclaim of VAL after a breakout,"
                            " favouring continuation to the upside."
                        ),
                    ),
                    SignalDirectionSpec(
                        id="short",
                        label="Short retest",
                        description=(
                            "Breakdown below VAH with a rejection retest or a breakdown of VAL that holds, "
                            "signalling continuation lower."
                        ),
                    ),
                ),
            ),
        ),
    )
)
