"""Market Profile indicator plugin manifest."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping

from engines.bot_runtime.core.domain import Candle

from ..contracts import OverlayProjectionInput
from ..market_profile_engine import MarketProfileStateEngine
from .registry import indicator_plugin_manifest


def market_profile_overlay_entries(projection_input: OverlayProjectionInput) -> Mapping[str, Mapping[str, Any]]:
    profiles = list((projection_input.snapshot.payload or {}).get("profiles") or [])
    entries: Dict[str, Mapping[str, Any]] = {}
    for idx, profile in enumerate(profiles):
        if not isinstance(profile, Mapping):
            continue
        session = str(profile.get("session") or "").strip()
        vah = profile.get("VAH")
        val = profile.get("VAL")
        if not session or vah is None or val is None:
            continue
        try:
            vah_f = float(vah)
            val_f = float(val)
            day_start = f"{session}T00:00:00+00:00"
            day_end = f"{session}T23:59:59+00:00"
        except (TypeError, ValueError):
            continue
        key = f"market_profile:{session}:{vah_f}:{val_f}:{idx}"
        entries[key] = {
            "type": "market_profile",
            "payload": {
                "boxes": [{"x1": day_start, "x2": day_end, "y1": val_f, "y2": vah_f}],
                "markers": [],
                "bubbles": [],
                "price_lines": [],
                "polylines": [],
            },
        }
    return entries


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
    engine_factory=lambda _meta: MarketProfileStateEngine(),
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
