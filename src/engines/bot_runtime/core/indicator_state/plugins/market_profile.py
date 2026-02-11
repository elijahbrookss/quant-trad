"""Market Profile indicator plugin manifest."""

from __future__ import annotations

import logging
import threading
from bisect import bisect_right
from datetime import datetime
from typing import Any, Dict, List, Mapping, MutableMapping

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.domain import timeframe_to_seconds

from ..contracts import OverlayProjectionInput
from ..market_profile_engine import MarketProfileEngineConfig, MarketProfileStateEngine
from .registry import indicator_plugin_manifest

log = logging.getLogger(__name__)
_PROFILE_RESOLUTION_CACHE: Dict[tuple[Any, ...], List[Any]] = {}
_PROFILE_CACHE_LOCK = threading.Lock()
_PROFILE_KNOWN_EPOCHS_CACHE: Dict[tuple[Any, ...], List[int]] = {}
_PROFILE_RUNTIME_VIEW_CACHE: Dict[tuple[Any, ...], List[Dict[str, Any]]] = {}
_MP_RUNTIME_STATE: Dict[tuple[str, str, str], Dict[str, Any]] = {}


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

    current_epoch = int(candle.time.timestamp())
    params_map = profile_params if isinstance(profile_params, Mapping) else {}
    indicator_id = str(snapshot_payload.get("_indicator_id") or "")
    symbol = str(snapshot_payload.get("symbol") or "")
    runtime_scope = str(snapshot_payload.get("_runtime_scope") or "")
    source_timeframe = str(snapshot_payload.get("source_timeframe") or "").strip().lower()
    chart_timeframe = str(snapshot_payload.get("chart_timeframe") or "").strip().lower()
    source_timeframe_seconds = _safe_int(snapshot_payload.get("source_timeframe_seconds"))
    if source_timeframe_seconds is None and source_timeframe:
        source_timeframe_seconds = timeframe_to_seconds(source_timeframe)
    if source_timeframe_seconds is None or source_timeframe_seconds <= 0:
        raise RuntimeError(
            "market_profile_runtime_invalid_source_timeframe_seconds: source timeframe seconds required for lockout semantics"
        )
    merge_threshold = _safe_float(params_map.get("merge_threshold"))
    min_merge_sessions = _safe_int(params_map.get("min_merge_sessions"))
    use_merged = bool(params_map.get("use_merged_value_areas"))

    timeline_key = (
        indicator_id,
        symbol,
        len(profiles),
        _profile_known_marker(profiles[0] if profiles else None),
        _profile_known_marker(profiles[-1] if profiles else None),
    )
    with _PROFILE_CACHE_LOCK:
        known_epochs = _PROFILE_KNOWN_EPOCHS_CACHE.get(timeline_key)
    if known_epochs is None:
        parsed_known_epochs: List[int] = []
        for profile in profiles:
            if not isinstance(profile, Mapping):
                continue
            known_epoch = _to_epoch(profile.get("known_at"))
            if known_epoch is None:
                raise RuntimeError(
                    "market_profile_profile_missing_known_at: every profile must include known_at for runtime signal evaluation"
                )
            parsed_known_epochs.append(known_epoch)
        parsed_known_epochs.sort()
        with _PROFILE_CACHE_LOCK:
            _PROFILE_KNOWN_EPOCHS_CACHE[timeline_key] = parsed_known_epochs
        known_epochs = parsed_known_epochs
    known_count = bisect_right(known_epochs, current_epoch) if known_epochs else 0

    cache_key = (
        indicator_id,
        symbol,
        use_merged,
        merge_threshold,
        min_merge_sessions,
        known_count,
    )
    with _PROFILE_CACHE_LOCK:
        cached_profiles = _PROFILE_RESOLUTION_CACHE.get(cache_key)
    if cached_profiles is None:
        effective_profiles, _summary = resolve_effective_profiles(
            profiles_payload=profiles,
            profile_params=params_map,
            current_epoch=current_epoch,
            symbol=symbol,
        )
        runtime_views = [_build_runtime_view(profile) for profile in effective_profiles]
        with _PROFILE_CACHE_LOCK:
            _PROFILE_RESOLUTION_CACHE[cache_key] = effective_profiles
            _PROFILE_RUNTIME_VIEW_CACHE[cache_key] = runtime_views
        cache_hit = False
    else:
        effective_profiles = cached_profiles
        with _PROFILE_CACHE_LOCK:
            runtime_views = _PROFILE_RUNTIME_VIEW_CACHE.get(cache_key)
        if runtime_views is None:
            runtime_views = [_build_runtime_view(profile) for profile in effective_profiles]
            with _PROFILE_CACHE_LOCK:
                _PROFILE_RUNTIME_VIEW_CACHE[cache_key] = runtime_views
        cache_hit = True
    if not effective_profiles:
        return {
            "signals": emitted,
            "diagnostics": {
                "profile_cache_hit": 1 if cache_hit else 0,
                "profile_cache_miss": 0 if cache_hit else 1,
                "known_profiles": int(known_count),
                "merged_profiles": 0,
                "breakouts_emitted": 0,
                "retests_emitted": 0,
                "active_breakouts": 0,
                "profiles_considered": 0,
                "candidate_count": 0,
                "candidate_chosen": 0,
                "source_timeframe_seconds": int(source_timeframe_seconds),
                "timeframe_mismatch_warning": 0,
            },
        }

    runtime_key = (indicator_id, symbol, runtime_scope)
    runtime_state = _get_runtime_state(runtime_key=runtime_key, current_epoch=current_epoch)
    mismatch_timeframes = bool(source_timeframe and chart_timeframe and source_timeframe != chart_timeframe)
    if mismatch_timeframes and not bool(runtime_state.get("chart_timeframe_mismatch_warned")):
        runtime_state["chart_timeframe_mismatch_warned"] = True
        log.warning(
            "event=market_profile_signal_timeframe_mismatch indicator_id=%s symbol=%s chart_timeframe=%s source_timeframe=%s source_timeframe_seconds=%s",
            indicator_id,
            symbol,
            chart_timeframe,
            source_timeframe,
            source_timeframe_seconds,
        )

    bar_index = int(runtime_state.get("bar_index") or 0)
    emitted_signatures: set[tuple[Any, ...]] = set()
    confirm_bars = _resolve_int(
        params_map,
        keys=(
            "market_profile_breakout_v2_confirm_bars",
            "market_profile_breakout_confirmation_bars",
            "confirmed_bars",
            "confirm_bars",
        ),
        default=3,
        min_value=1,
    )
    lockout_bars = _resolve_int(
        params_map,
        keys=(
            "market_profile_breakout_v2_lockout_bars",
            "market_profile_breakout_lockout_bars",
            "lockout_bars",
        ),
        default=3,
        min_value=0,
    )
    retest_min_bars = _resolve_int(
        params_map,
        keys=("market_profile_retest_v2_min_bars", "market_profile_retest_min_bars", "retest_min_bars"),
        default=3,
        min_value=0,
    )
    retest_max_lookback = _resolve_int(
        params_map,
        keys=("market_profile_retest_v2_max_lookback", "market_profile_retest_max_bars", "retest_max_lookback"),
        default=50,
        min_value=1,
    )
    retest_tolerance_pct = _resolve_float(
        params_map,
        keys=("market_profile_retest_v2_tolerance_pct", "market_profile_retest_tolerance_pct", "retest_tolerance_pct"),
        default=0.5,
        min_value=0.0,
    )
    extend_to_end = bool(params_map.get("extend_value_area_to_chart_end", True))
    nearby_threshold_pct = _resolve_float(
        params_map,
        keys=("market_profile_signal_nearby_pct", "signal_nearby_pct", "nearby_profile_pct"),
        default=0.35,
        min_value=0.0,
    )
    lockout_seconds = int(lockout_bars) * int(source_timeframe_seconds)

    active_breakouts: List[Dict[str, Any]] = runtime_state.setdefault("active_breakouts", [])
    profile_states: MutableMapping[str, Dict[str, Any]] = runtime_state.setdefault("profile_states", {})
    bar_epoch = int(candle.time.timestamp())
    breakout_emitted = 0
    retest_emitted = 0

    candidate_profiles = _select_nearby_profiles(
        runtime_views=runtime_views,
        bar_epoch=bar_epoch,
        extend_to_end=extend_to_end,
        close_price=now_close,
        previous_close=prior_close,
        candle_low=float(candle.low),
        candle_high=float(candle.high),
        nearby_threshold_pct=nearby_threshold_pct,
    )
    chosen_candidate = _select_single_candidate_profile(candidate_profiles)
    chosen_profile_key = str(chosen_candidate["profile_key"]) if isinstance(chosen_candidate, Mapping) else ""
    chosen_selection_reason = (
        str(chosen_candidate.get("selection_reason") or "") if isinstance(chosen_candidate, Mapping) else ""
    )
    chosen_selection_score = (
        _safe_float(chosen_candidate.get("selection_score")) if isinstance(chosen_candidate, Mapping) else None
    )

    active_profile_keys = {str(item["profile_key"]) for item in runtime_views}
    if active_breakouts:
        active_breakouts = [
            item
            for item in active_breakouts
            if str(item.get("profile_key") or "") in active_profile_keys
        ]

    for runtime_view in runtime_views:
        profile = runtime_view["profile"]
        vah_f = float(runtime_view["vah"])
        val_f = float(runtime_view["val"])
        profile_key = str(runtime_view["profile_key"])
        profile_state = profile_states.setdefault(
            profile_key,
            {
                "streak_above_vah": 0,
                "streak_below_val": 0,
                "crossed_up_active": False,
                "crossed_down_active": False,
                "last_breakout_above_idx": -10_000_000,
                "last_breakout_below_idx": -10_000_000,
                "last_breakout_above_time": None,
                "last_breakout_below_time": None,
            },
        )

        prev_streak_above = int(profile_state.get("streak_above_vah") or 0)
        prev_streak_below = int(profile_state.get("streak_below_val") or 0)
        if now_close > vah_f:
            if prev_streak_above == 0:
                profile_state["crossed_up_active"] = _crossed_up(
                    prior_close=prior_close,
                    current_close=now_close,
                    level=vah_f,
                )
            profile_state["streak_above_vah"] = prev_streak_above + 1
        else:
            profile_state["streak_above_vah"] = 0
            profile_state["crossed_up_active"] = False

        if now_close < val_f:
            if prev_streak_below == 0:
                profile_state["crossed_down_active"] = _crossed_dn(
                    prior_close=prior_close,
                    current_close=now_close,
                    level=val_f,
                )
            profile_state["streak_below_val"] = prev_streak_below + 1
        else:
            profile_state["streak_below_val"] = 0
            profile_state["crossed_down_active"] = False

        if profile_key != chosen_profile_key:
            continue

        above_streak = int(profile_state["streak_above_vah"])
        below_streak = int(profile_state["streak_below_val"])
        above_run_len = above_streak
        below_run_len = below_streak

        breakout_above_ready = (
            bool(profile_state.get("crossed_up_active"))
            and above_streak == confirm_bars
            and _lockout_elapsed(
                last_time_epoch=_safe_int(profile_state.get("last_breakout_above_time")),
                current_epoch=bar_epoch,
                lockout_seconds=lockout_seconds,
            )
        )
        breakout_below_ready = (
            bool(profile_state.get("crossed_down_active"))
            and below_streak == confirm_bars
            and _lockout_elapsed(
                last_time_epoch=_safe_int(profile_state.get("last_breakout_below_time")),
                current_epoch=bar_epoch,
                lockout_seconds=lockout_seconds,
            )
        )

        if breakout_above_ready:
            signature = (bar_epoch, profile_key, "VAH", "above", "market_profile_breakout_v2")
            if signature not in emitted_signatures:
                emitted_signatures.add(signature)
                profile_state["last_breakout_above_idx"] = bar_index
                profile_state["last_breakout_above_time"] = bar_epoch
                breakout_signal = _build_breakout_signal(
                    candle=candle,
                    bar_index=bar_index,
                    profile=profile,
                    level_type="VAH",
                    level_price=vah_f,
                    breakout_direction="above",
                    confirm_bars=confirm_bars,
                    lockout_bars=lockout_bars,
                    source_timeframe_seconds=int(source_timeframe_seconds),
                    streak_count=above_streak,
                    run_length=above_run_len,
                    chosen_profile_key=chosen_profile_key,
                    selection_reason=chosen_selection_reason,
                    candidate_count=len(candidate_profiles),
                    selection_score=chosen_selection_score,
                )
                emitted.append(breakout_signal)
                breakout_emitted += 1
                log.debug(
                    "event=mp_breakout_emitted indicator_id=%s symbol=%s profile_key=%s side=above level_type=VAH level_price=%.6f prior_close=%.6f close=%.6f streak=%s run_len=%s confirm_bars=%s lockout_bars=%s bar_time=%s",
                    indicator_id,
                    symbol,
                    profile_key,
                    vah_f,
                    prior_close,
                    now_close,
                    above_streak,
                    above_run_len,
                    confirm_bars,
                    lockout_bars,
                    candle.time,
                )
                active_breakouts.append(
                    {
                        "breakout_id": str(breakout_signal.get("breakout_id") or ""),
                        "value_area_id": str(breakout_signal.get("value_area_id") or ""),
                        "breakout_direction": "above",
                        "level_type": "VAH",
                        "level_price": vah_f,
                        "breakout_bar_index": bar_index,
                        "breakout_time_epoch": bar_epoch,
                        "breakout_time": candle.time,
                        "VAH": vah_f,
                        "VAL": val_f,
                        "source_timeframe_seconds": int(source_timeframe_seconds),
                        "profile_key": profile_key,
                        "formed_at": profile.end,
                    }
                )
        if breakout_below_ready:
            signature = (bar_epoch, profile_key, "VAL", "below", "market_profile_breakout_v2")
            if signature not in emitted_signatures:
                emitted_signatures.add(signature)
                profile_state["last_breakout_below_idx"] = bar_index
                profile_state["last_breakout_below_time"] = bar_epoch
                breakout_signal = _build_breakout_signal(
                    candle=candle,
                    bar_index=bar_index,
                    profile=profile,
                    level_type="VAL",
                    level_price=val_f,
                    breakout_direction="below",
                    confirm_bars=confirm_bars,
                    lockout_bars=lockout_bars,
                    source_timeframe_seconds=int(source_timeframe_seconds),
                    streak_count=below_streak,
                    run_length=below_run_len,
                    chosen_profile_key=chosen_profile_key,
                    selection_reason=chosen_selection_reason,
                    candidate_count=len(candidate_profiles),
                    selection_score=chosen_selection_score,
                )
                emitted.append(breakout_signal)
                breakout_emitted += 1
                log.debug(
                    "event=mp_breakout_emitted indicator_id=%s symbol=%s profile_key=%s side=below level_type=VAL level_price=%.6f prior_close=%.6f close=%.6f streak=%s run_len=%s confirm_bars=%s lockout_bars=%s bar_time=%s",
                    indicator_id,
                    symbol,
                    profile_key,
                    val_f,
                    prior_close,
                    now_close,
                    below_streak,
                    below_run_len,
                    confirm_bars,
                    lockout_bars,
                    candle.time,
                )
                active_breakouts.append(
                    {
                        "breakout_id": str(breakout_signal.get("breakout_id") or ""),
                        "value_area_id": str(breakout_signal.get("value_area_id") or ""),
                        "breakout_direction": "below",
                        "level_type": "VAL",
                        "level_price": val_f,
                        "breakout_bar_index": bar_index,
                        "breakout_time_epoch": bar_epoch,
                        "breakout_time": candle.time,
                        "VAH": vah_f,
                        "VAL": val_f,
                        "source_timeframe_seconds": int(source_timeframe_seconds),
                        "profile_key": profile_key,
                        "formed_at": profile.end,
                    }
                )

    candle_body_high = max(float(candle.open), float(candle.close))
    candle_body_low = min(float(candle.open), float(candle.close))
    remaining_breakouts: List[Dict[str, Any]] = []
    for active in active_breakouts:
        breakout_idx = int(active.get("breakout_bar_index") or -1)
        if breakout_idx < 0:
            continue
        bars_since = bar_index - breakout_idx
        if bars_since < retest_min_bars:
            remaining_breakouts.append(active)
            continue
        if bars_since > retest_max_lookback:
            continue

        level_price = _safe_float(active.get("level_price"))
        if level_price is None:
            continue
        direction = str(active.get("breakout_direction") or "").strip().lower()
        tolerance = abs(level_price) * (retest_tolerance_pct / 100.0)

        if direction == "above":
            body_touches = candle_body_low <= (level_price + tolerance)
            close_not_far = now_close >= (level_price - tolerance)
            valid_retest = body_touches and close_not_far
            retest_role = "resistance"
        elif direction == "below":
            body_touches = candle_body_high >= (level_price - tolerance)
            close_not_far = now_close <= (level_price + tolerance)
            valid_retest = body_touches and close_not_far
            retest_role = "support"
        else:
            valid_retest = False
            retest_role = "retest"

        if not valid_retest:
            remaining_breakouts.append(active)
            continue

        retest_signal = _build_retest_signal(
            candle=candle,
            bar_index=bar_index,
            breakout=active,
            bars_since_breakout=bars_since,
            retest_tolerance_pct=retest_tolerance_pct,
            retest_role=retest_role,
        )
        emitted.append(retest_signal)
        retest_emitted += 1
        log.debug(
            "event=mp_retest_emitted indicator_id=%s symbol=%s profile_key=%s breakout_id=%s level_type=%s level_price=%.6f close=%.6f bars_since=%s tolerance_pct=%.4f bar_time=%s",
            indicator_id,
            symbol,
            str(active.get("profile_key") or ""),
            str(active.get("breakout_id") or ""),
            str(active.get("level_type") or ""),
            level_price,
            now_close,
            int(bars_since),
            float(retest_tolerance_pct),
            candle.time,
        )

    runtime_state["active_breakouts"] = remaining_breakouts
    runtime_state["bar_index"] = bar_index + 1
    runtime_state["last_epoch"] = current_epoch

    return {
        "signals": emitted,
        "diagnostics": {
            "profile_cache_hit": 1 if cache_hit else 0,
            "profile_cache_miss": 0 if cache_hit else 1,
            "known_profiles": int(known_count),
            "merged_profiles": int(len(effective_profiles)),
            "breakouts_emitted": int(breakout_emitted),
            "retests_emitted": int(retest_emitted),
            "active_breakouts": int(len(remaining_breakouts)),
            "profiles_considered": int(len(candidate_profiles)),
            "candidate_count": int(len(candidate_profiles)),
            "candidate_chosen": 1 if chosen_profile_key else 0,
            "source_timeframe_seconds": int(source_timeframe_seconds),
            "timeframe_mismatch_warning": 1 if mismatch_timeframes else 0,
        },
    }

def _profile_identity(profile: Any) -> str:
    start = profile.start.isoformat() if hasattr(profile.start, "isoformat") else str(profile.start)
    end = profile.end.isoformat() if hasattr(profile.end, "isoformat") else str(profile.end)
    return f"{start}:{end}:{int(getattr(profile, 'session_count', 1) or 1)}"


def _profile_known_marker(profile: Any) -> str:
    if not isinstance(profile, Mapping):
        return ""
    value = profile.get("known_at")
    if hasattr(value, "isoformat"):
        try:
            return str(value.isoformat())
        except Exception:
            return str(value)
    return str(value) if value is not None else ""


def _to_epoch(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp())
    if hasattr(value, "timestamp"):
        try:
            return int(value.timestamp())
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            from pandas import Timestamp

            parsed = Timestamp(text)
            if parsed.tzinfo is None:
                parsed = parsed.tz_localize("UTC")
            else:
                parsed = parsed.tz_convert("UTC")
            return int(parsed.timestamp())
        except Exception:
            return None
    return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _resolve_int(
    params: Mapping[str, Any],
    *,
    keys: tuple[str, ...],
    default: int,
    min_value: int = 0,
) -> int:
    for key in keys:
        value = params.get(key)
        parsed = _safe_int(value)
        if parsed is not None:
            return max(min_value, parsed)
    return max(min_value, int(default))


def _resolve_float(
    params: Mapping[str, Any],
    *,
    keys: tuple[str, ...],
    default: float,
    min_value: float = 0.0,
) -> float:
    for key in keys:
        value = params.get(key)
        parsed = _safe_float(value)
        if parsed is not None:
            return max(min_value, parsed)
    return max(min_value, float(default))


def _get_runtime_state(*, runtime_key: tuple[str, str, str], current_epoch: int) -> Dict[str, Any]:
    with _PROFILE_CACHE_LOCK:
        state = _MP_RUNTIME_STATE.get(runtime_key)
        if state is None:
            state = {"bar_index": 0, "last_epoch": None, "profile_states": {}, "active_breakouts": []}
            _MP_RUNTIME_STATE[runtime_key] = state
            return state
        last_epoch = state.get("last_epoch")
        if isinstance(last_epoch, int) and current_epoch <= last_epoch:
            state = {"bar_index": 0, "last_epoch": None, "profile_states": {}, "active_breakouts": []}
            _MP_RUNTIME_STATE[runtime_key] = state
        return state


def _build_runtime_view(profile: Any) -> Dict[str, Any]:
    end_epoch = _to_epoch(getattr(profile, "end", None))
    if end_epoch is None:
        raise RuntimeError(
            "market_profile_runtime_profile_invalid: profile missing end epoch for runtime evaluation"
        )
    return {
        "profile": profile,
        "vah": float(profile.vah),
        "val": float(profile.val),
        "poc": float(profile.poc),
        "profile_key": _profile_identity(profile),
        "end_epoch": int(end_epoch),
        # Known-at semantics: a profile only becomes tradable/usable once known.
        # For runtime Profile objects this corresponds to session end.
        "known_epoch": int(end_epoch),
    }


def _select_nearby_profiles(
    *,
    runtime_views: List[Dict[str, Any]],
    bar_epoch: int,
    extend_to_end: bool,
    close_price: float,
    previous_close: float,
    candle_low: float,
    candle_high: float,
    nearby_threshold_pct: float,
) -> List[Dict[str, Any]]:
    if not runtime_views:
        return []
    selected: List[Dict[str, Any]] = []
    for runtime_view in runtime_views:
        known_epoch = int(runtime_view["known_epoch"])
        end_epoch = int(runtime_view["end_epoch"])
        if bar_epoch < known_epoch:
            continue
        if (not extend_to_end) and bar_epoch > end_epoch:
            continue
        vah = float(runtime_view["vah"])
        val = float(runtime_view["val"])
        in_or_cross = (
            (val <= close_price <= vah)
            or (val <= previous_close <= vah)
            or (previous_close <= vah < close_price)
            or (previous_close >= val > close_price)
            or (candle_low <= vah <= candle_high)
            or (candle_low <= val <= candle_high)
        )
        midpoint = (vah + val) / 2.0
        if in_or_cross:
            selected.append(
                {
                    **runtime_view,
                    "selection_reason": "in_or_cross",
                    "selection_score": abs(close_price - float(runtime_view["poc"])),
                    "selection_tie_score": abs(close_price - midpoint),
                }
            )
            continue

        distance = min(
            abs(close_price - vah),
            abs(close_price - val),
            abs(previous_close - vah),
            abs(previous_close - val),
        )
        threshold_abs = abs(close_price) * (nearby_threshold_pct / 100.0)
        if distance <= threshold_abs:
            selected.append(
                {
                    **runtime_view,
                    "selection_reason": "nearby_threshold",
                    "selection_score": abs(close_price - float(runtime_view["poc"])),
                    "selection_tie_score": abs(close_price - midpoint),
                }
            )
    return selected


def _select_single_candidate_profile(candidates: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    if not candidates:
        return None
    ordered = sorted(
        candidates,
        key=lambda item: (
            float(item.get("selection_score") or 0.0),
            float(item.get("selection_tie_score") or 0.0),
            int(item.get("known_epoch") or 0),
            str(item.get("profile_key") or ""),
        ),
    )
    return ordered[0]


def _crossed_up(*, prior_close: float, current_close: float, level: float) -> bool:
    return prior_close <= level and current_close > level


def _crossed_dn(*, prior_close: float, current_close: float, level: float) -> bool:
    return prior_close >= level and current_close < level


def _lockout_elapsed(*, last_time_epoch: int | None, current_epoch: int, lockout_seconds: int) -> bool:
    if last_time_epoch is None:
        return True
    if lockout_seconds <= 0:
        return True
    return (int(last_time_epoch) + int(lockout_seconds)) < int(current_epoch)


def _build_breakout_signal(
    *,
    candle: Candle,
    bar_index: int,
    profile: Any,
    level_type: str,
    level_price: float,
    breakout_direction: str,
    confirm_bars: int,
    lockout_bars: int,
    source_timeframe_seconds: int,
    streak_count: int,
    run_length: int,
    chosen_profile_key: str,
    selection_reason: str,
    candidate_count: int,
    selection_score: float | None,
) -> Dict[str, Any]:
    direction = "long" if breakout_direction == "above" else "short"
    pointer_direction = "up" if breakout_direction == "above" else "down"
    profile_id = _profile_identity(profile)
    breakout_id = f"{profile_id}:{level_type}:{bar_index}"
    return {
        "time": candle.time,
        "type": "breakout",
        "source": "MarketProfile",
        "rule_id": "market_profile_breakout_v2",
        "pattern_id": "breakout_v2",
        "rule_aliases": [
            "market_profile_breakout_v2",
            "market_profile_breakout_v2_rule",
            "market_profile_breakout",
            "market_profile_breakout_rule",
        ],
        "level_type": level_type,
        "level_price": level_price,
        "value_area_id": profile_id,
        "breakout_direction": breakout_direction,
        "pointer_direction": pointer_direction,
        "direction": direction,
        "trigger_time": candle.time,
        "bar_index": bar_index,
        "trigger_index": bar_index,
        "trigger_close": float(candle.close),
        "trigger_open": float(candle.open),
        "trigger_high": float(candle.high),
        "trigger_low": float(candle.low),
        "confirm_bars": confirm_bars,
        "lockout_bars": lockout_bars,
        "source_timeframe_seconds": int(source_timeframe_seconds),
        "streak_count": int(streak_count),
        "run_length": int(run_length),
        "chosen_profile_key": str(chosen_profile_key or profile_id),
        "selection_reason": str(selection_reason or "in_or_cross"),
        "candidate_count": int(candidate_count),
        "selection_score": selection_score,
        "confirm_indices": list(range(max(0, bar_index - confirm_bars + 1), bar_index + 1)),
        "breakout_id": breakout_id,
        "VAH": float(profile.vah),
        "VAL": float(profile.val),
        "POC": float(profile.poc),
        "value_area_start": profile.start,
        "value_area_end": profile.end,
        "session_count": int(getattr(profile, "session_count", 1) or 1),
        "known_at": profile.end,
        "formed_at": profile.end,
    }


def _build_retest_signal(
    *,
    candle: Candle,
    bar_index: int,
    breakout: Mapping[str, Any],
    bars_since_breakout: int,
    retest_tolerance_pct: float,
    retest_role: str,
) -> Dict[str, Any]:
    breakout_direction = str(breakout.get("breakout_direction") or "").strip().lower()
    trade_direction = "long" if breakout_direction == "above" else "short"
    pointer_direction = "up" if breakout_direction == "above" else "down"
    level_type = str(breakout.get("level_type") or "")
    level_price = float(breakout.get("level_price") or 0.0)
    return {
        "time": candle.time,
        "type": "retest",
        "source": "MarketProfile",
        "rule_id": "market_profile_retest_v2",
        "pattern_id": "retest_v2",
        "rule_aliases": [
            "market_profile_retest_v2",
            "market_profile_retest_v2_rule",
            "market_profile_retest",
            "market_profile_retest_rule",
        ],
        "level_type": level_type,
        "level_price": level_price,
        "value_area_id": str(breakout.get("value_area_id") or ""),
        "breakout_id": str(breakout.get("breakout_id") or ""),
        "breakout_direction": breakout_direction,
        "direction": trade_direction,
        "pointer_direction": pointer_direction,
        "retest_role": retest_role,
        "trigger_time": candle.time,
        "bar_index": bar_index,
        "trigger_index": bar_index,
        "breakout_bar_index": int(breakout.get("breakout_bar_index") or 0),
        "bars_since_breakout": int(bars_since_breakout),
        "source_timeframe_seconds": int(breakout.get("source_timeframe_seconds") or 0),
        "retest_tolerance_pct": float(retest_tolerance_pct),
        "retest_close": float(candle.close),
        "trigger_close": float(candle.close),
        "trigger_open": float(candle.open),
        "trigger_high": float(candle.high),
        "trigger_low": float(candle.low),
        "VAH": _safe_float(breakout.get("VAH")),
        "VAL": _safe_float(breakout.get("VAL")),
        "formed_at": breakout.get("formed_at"),
        "known_at": breakout.get("formed_at"),
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
