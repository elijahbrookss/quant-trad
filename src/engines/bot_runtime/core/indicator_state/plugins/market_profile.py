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
    profile_params = snapshot_payload.get("profile_params")
    try:
        from indicators.market_profile._internal.runtime_profiles import resolve_effective_profiles
    except Exception:
        return {"signals": emitted}
    effective_profiles, _summary = resolve_effective_profiles(
        profiles_payload=profiles,
        profile_params=profile_params if isinstance(profile_params, Mapping) else {},
        current_epoch=int(candle.time.timestamp()),
    )
    if not effective_profiles:
        return {"signals": emitted}

    emitted_signatures: set[tuple[Any, ...]] = set()
    bar_epoch = int(candle.time.timestamp())
    for profile in effective_profiles:
        vah_f = float(profile.vah)
        val_f = float(profile.val)
        profile_key = _profile_identity(profile)

        if prior_close <= vah_f < now_close:
            signature = (bar_epoch, profile_key, "VAH", "above", "market_profile_breakout")
            if signature not in emitted_signatures:
                emitted_signatures.add(signature)
                emitted.append(
                    _build_breakout_signal(
                        candle=candle,
                        profile=profile,
                        level_type="VAH",
                        level_price=vah_f,
                        breakout_direction="above",
                    )
                )
        if prior_close >= val_f > now_close:
            signature = (bar_epoch, profile_key, "VAL", "below", "market_profile_breakout")
            if signature not in emitted_signatures:
                emitted_signatures.add(signature)
                emitted.append(
                    _build_breakout_signal(
                        candle=candle,
                        profile=profile,
                        level_type="VAL",
                        level_price=val_f,
                        breakout_direction="below",
                    )
                )

    return {"signals": emitted}

def _profile_identity(profile: Any) -> str:
    start = profile.start.isoformat() if hasattr(profile.start, "isoformat") else str(profile.start)
    end = profile.end.isoformat() if hasattr(profile.end, "isoformat") else str(profile.end)
    return f"{start}:{end}:{int(getattr(profile, 'session_count', 1) or 1)}"


def _build_breakout_signal(
    *,
    candle: Candle,
    profile: Any,
    level_type: str,
    level_price: float,
    breakout_direction: str,
) -> Dict[str, Any]:
    direction = "long" if breakout_direction == "above" else "short"
    pointer_direction = "up" if breakout_direction == "above" else "down"
    return {
        "time": candle.time,
        "type": "breakout",
        "source": "MarketProfile",
        "rule_id": "market_profile_breakout",
        "pattern_id": "market_profile_breakout",
        "level_type": level_type,
        "level_price": level_price,
        "value_area_id": _profile_identity(profile),
        "breakout_direction": breakout_direction,
        "pointer_direction": pointer_direction,
        "direction": direction,
        "trigger_time": candle.time,
        "trigger_close": float(candle.close),
        "VAH": float(profile.vah),
        "VAL": float(profile.val),
        "POC": float(profile.poc),
        "value_area_start": profile.start,
        "value_area_end": profile.end,
        "session_count": int(getattr(profile, "session_count", 1) or 1),
        "known_at": profile.end,
        "formed_at": profile.end,
    }


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
