"""Market Profile indicator plugin manifest."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping

from engines.bot_runtime.core.domain import Candle

from ..contracts import OverlayProjectionInput
from ..market_profile_engine import MarketProfileEngineConfig, MarketProfileStateEngine
from .registry import indicator_plugin_manifest


def market_profile_overlay_entries(projection_input: OverlayProjectionInput) -> Mapping[str, Mapping[str, Any]]:
    profiles = list((projection_input.snapshot.payload or {}).get("profiles") or [])
    profile_params = dict((projection_input.snapshot.payload or {}).get("profile_params") or {})
    overlay_color = (projection_input.snapshot.payload or {}).get("overlay_color")
    normalized_profiles: List[Dict[str, Any]] = []
    for profile in profiles:
        if not isinstance(profile, Mapping):
            continue
        copied = dict(profile)
        for key in ("start", "end", "formed_at", "known_at"):
            value = copied.get(key)
            if hasattr(value, "isoformat"):
                copied[key] = value.isoformat()
        normalized_profiles.append(copied)
    if not normalized_profiles:
        return {}

    entry: Dict[str, Any] = {
        "type": "market_profile",
        "payload": {
            "profiles": normalized_profiles,
            "profile_params": profile_params,
            "boxes": [],
            "markers": [],
            "bubbles": [],
            "price_lines": [],
            "polylines": [],
            "touch_points": [],
        },
    }
    if isinstance(overlay_color, str) and overlay_color.strip():
        entry["color"] = overlay_color

    return {"market_profile:overlay": entry}


def _market_profile_engine_factory(meta: Mapping[str, Any]) -> MarketProfileStateEngine:
    params = dict(meta.get("params") or {}) if isinstance(meta, Mapping) else {}
    config = MarketProfileEngineConfig(
        params=params,
        overlay_color=str(meta.get("color") or "").strip() or None,
    )
    return MarketProfileStateEngine(config)


def market_profile_rule_payload(
    *,
    snapshot_payload: Mapping[str, Any],
    candle: Candle,
    previous_candle: Candle | None,
) -> Dict[str, Any]:
    profiles = list(snapshot_payload.get("profiles") or [])
    emitted: List[Dict[str, Any]] = []
    if not profiles:
        return {"signals": emitted}

    prior_close = float(previous_candle.close) if isinstance(previous_candle, Candle) else float(candle.open)
    now_close = float(candle.close)

    for profile in profiles:
        if not isinstance(profile, Mapping):
            continue
        vah = profile.get("VAH")
        val = profile.get("VAL")
        if vah is None or val is None:
            continue
        try:
            vah_f = float(vah)
            val_f = float(val)
        except (TypeError, ValueError):
            continue
        if prior_close <= vah_f < now_close:
            emitted.append({"time": candle.time, "type": "breakout", "direction": "long", "rule_id": "market_profile_breakout", "pattern_id": "market_profile_breakout", "level_type": "VAH", "level_price": vah_f, "value_area_id": profile.get("session")})
        if prior_close >= val_f > now_close:
            emitted.append({"time": candle.time, "type": "breakout", "direction": "short", "rule_id": "market_profile_breakout", "pattern_id": "market_profile_breakout", "level_type": "VAL", "level_price": val_f, "value_area_id": profile.get("session")})

    return {"signals": emitted}


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
)
class _MarketProfilePlugin:
    pass
