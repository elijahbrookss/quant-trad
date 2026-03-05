"""Market Profile runtime signal + overlay projection logic."""

from __future__ import annotations

import logging
import threading
import hashlib
from bisect import bisect_right
from datetime import datetime
from typing import Any, Dict, List, Mapping, MutableMapping

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.domain import timeframe_to_seconds
from signals.contract import (
    assert_no_execution_fields,
    assert_signal_contract,
    assert_signal_time_is_closed_bar,
)

from engines.indicator_engine.contracts import OverlayProjectionInput

log = logging.getLogger(__name__)
_PROFILE_RESOLUTION_CACHE: Dict[tuple[Any, ...], List[Any]] = {}
_PROFILE_CACHE_LOCK = threading.Lock()
_PROFILE_KNOWN_EPOCHS_CACHE: Dict[tuple[Any, ...], List[int]] = {}
_PROFILE_RUNTIME_VIEW_CACHE: Dict[tuple[Any, ...], List[Dict[str, Any]]] = {}


def _bias_from_direction(direction: str) -> str:
    normalized = str(direction or "").strip().lower()
    if normalized in {"long", "up", "above", "bull", "bullish", "buy"}:
        return "bullish"
    if normalized in {"short", "down", "below", "bear", "bearish", "sell"}:
        return "bearish"
    return "neutral"


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

    prior_open = float(previous_candle.open) if isinstance(previous_candle, Candle) else float(candle.open)
    prior_close = float(previous_candle.close) if isinstance(previous_candle, Candle) else float(candle.open)
    now_close = float(candle.close)
    now_open = float(candle.open)
    profile_params = snapshot_payload.get("profile_params")
    try:
        from indicators.market_profile.compute.internal.runtime_profiles import resolve_effective_profiles
    except Exception:
        return {"signals": emitted}

    current_epoch = int(candle.time.timestamp())
    params_map = profile_params if isinstance(profile_params, Mapping) else {}
    indicator_id = str(snapshot_payload.get("_indicator_id") or "")
    symbol = str(snapshot_payload.get("symbol") or "")
    runtime_scope = str(snapshot_payload.get("_runtime_scope") or "")
    chart_timeframe = str(snapshot_payload.get("chart_timeframe") or "").strip().lower()
    chart_timeframe_seconds = _safe_int(snapshot_payload.get("chart_timeframe_seconds"))
    if chart_timeframe_seconds is None and chart_timeframe:
        chart_timeframe_seconds = timeframe_to_seconds(chart_timeframe)
    if chart_timeframe_seconds is None or chart_timeframe_seconds <= 0:
        raise RuntimeError(
            "market_profile_runtime_invalid_chart_timeframe_seconds: chart timeframe seconds required for lockout semantics"
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
    runtime_key = (indicator_id, symbol, runtime_scope)
    runtime_state = _get_runtime_state(snapshot_payload=snapshot_payload, runtime_key=runtime_key, current_epoch=current_epoch)
    bar_epoch = int(candle.time.timestamp())
    v3_outcome = _market_profile_breakout_v3_payload(
        snapshot_payload=snapshot_payload,
        candle=candle,
        previous_candle=previous_candle,
        runtime_state=runtime_state,
        runtime_views=runtime_views,
        cache_hit=cache_hit,
        known_count=known_count,
        indicator_id=indicator_id,
        symbol=symbol,
        runtime_scope=runtime_scope,
        bar_epoch=bar_epoch,
        params_map=params_map,
        chart_timeframe=chart_timeframe,
        chart_timeframe_seconds=chart_timeframe_seconds,
        bar_index=int(runtime_state.get("bar_index") or 0),
    )
    emitted.extend(list(v3_outcome.get("signals") or []))
    runtime_state["bar_index"] = int(runtime_state.get("bar_index") or 0) + 1
    runtime_state["last_epoch"] = current_epoch
    return {
        "signals": emitted,
        "diagnostics": dict(v3_outcome.get("diagnostics") or {}),
    }

    if not effective_profiles:
        v3_diag = dict(v3_outcome.get("diagnostics") or {})
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
                "source_timeframe_seconds": int(chart_timeframe_seconds),
                "timeframe_mismatch_warning": 0,
                "v3_candidate_count": int(v3_diag.get("candidate_count") or 0),
                "v3_pending_count": int(v3_diag.get("pending_count") or 0),
                "v3_emitted": int(len(v3_outcome.get("signals") or [])),
            },
        }

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
        default=1,
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
        default=0.15,
        min_value=0.0,
    )
    extend_to_end = bool(params_map.get("extend_value_area_to_chart_end", True))
    nearby_threshold_pct = _resolve_float(
        params_map,
        keys=("market_profile_signal_nearby_pct", "signal_nearby_pct", "nearby_profile_pct"),
        default=0.35,
        min_value=0.0,
    )
    lockout_seconds = int(lockout_bars) * int(chart_timeframe_seconds)

    active_breakouts: List[Dict[str, Any]] = runtime_state.setdefault("active_breakouts", [])
    profile_states: MutableMapping[str, Dict[str, Any]] = runtime_state.setdefault("profile_states", {})
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
        prior_body_low = min(prior_open, prior_close)
        prior_body_high = max(prior_open, prior_close)
        now_body_low = min(now_open, now_close)
        now_body_high = max(now_open, now_close)
        now_fully_above_vah = _body_fully_above(now_body_low=now_body_low, level=vah_f)
        now_fully_below_val = _body_fully_below(now_body_high=now_body_high, level=val_f)
        now_fully_inside_va = _body_fully_inside(
            now_body_low=now_body_low,
            now_body_high=now_body_high,
            val=val_f,
            vah=vah_f,
        )
        profile_state = profile_states.setdefault(
            profile_key,
            {
                "streak_above_vah": 0,
                "streak_below_val": 0,
                "streak_inside_va": 0,
                "last_breakout_above_idx": -10_000_000,
                "last_breakout_below_idx": -10_000_000,
                "last_breakout_above_time": None,
                "last_breakout_below_time": None,
            },
        )

        prev_streak_above = int(profile_state.get("streak_above_vah") or 0)
        prev_streak_below = int(profile_state.get("streak_below_val") or 0)
        if now_fully_above_vah:
            profile_state["streak_above_vah"] = prev_streak_above + 1
        else:
            profile_state["streak_above_vah"] = 0

        if now_fully_below_val:
            profile_state["streak_below_val"] = prev_streak_below + 1
        else:
            profile_state["streak_below_val"] = 0

        prev_inside_streak = int(profile_state.get("streak_inside_va") or 0)
        if now_fully_inside_va:
            profile_state["streak_inside_va"] = prev_inside_streak + 1
        else:
            profile_state["streak_inside_va"] = 0

        if profile_key != chosen_profile_key:
            continue

        above_streak = int(profile_state["streak_above_vah"])
        below_streak = int(profile_state["streak_below_val"])
        above_run_len = above_streak
        below_run_len = below_streak

        above_crossed_vah_ready = above_streak == confirm_bars
        breakout_above_ready = (
            above_crossed_vah_ready
            and _lockout_elapsed(
                last_time_epoch=_safe_int(profile_state.get("last_breakout_above_time")),
                current_epoch=bar_epoch,
                lockout_seconds=lockout_seconds,
            )
        )
        below_crossed_val_ready = below_streak == confirm_bars
        breakout_below_ready = (
            below_crossed_val_ready
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
                above_level_type = "VAH"
                above_level_price = vah_f
                above_variant = "crossed_vah_to_outside"
                breakout_signal = _build_breakout_signal(
                    candle=candle,
                    bar_index=bar_index,
                    profile=profile,
                    level_type=above_level_type,
                    level_price=above_level_price,
                    breakout_direction="above",
                    breakout_variant=above_variant,
                    confirm_bars=confirm_bars,
                    lockout_bars=lockout_bars,
                    source_timeframe_seconds=int(chart_timeframe_seconds),
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
                    "event=mp_breakout_emitted indicator_id=%s symbol=%s profile_key=%s side=above breakout_variant=%s level_type=%s level_price=%.6f prior_close=%.6f close=%.6f prior_body_low=%.6f prior_body_high=%.6f body_low=%.6f body_high=%.6f streak=%s run_len=%s confirm_bars=%s lockout_bars=%s bar_time=%s",
                    indicator_id,
                    symbol,
                    profile_key,
                    above_variant,
                    above_level_type,
                    above_level_price,
                    prior_close,
                    now_close,
                    prior_body_low,
                    prior_body_high,
                    now_body_low,
                    now_body_high,
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
                        "level_type": above_level_type,
                        "level_price": above_level_price,
                        "breakout_bar_index": bar_index,
                        "breakout_time_epoch": bar_epoch,
                        "breakout_time": candle.time,
                        "VAH": vah_f,
                        "VAL": val_f,
                        "source_timeframe_seconds": int(chart_timeframe_seconds),
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
                below_level_type = "VAL"
                below_level_price = val_f
                below_variant = "crossed_val_to_outside"
                breakout_signal = _build_breakout_signal(
                    candle=candle,
                    bar_index=bar_index,
                    profile=profile,
                    level_type=below_level_type,
                    level_price=below_level_price,
                    breakout_direction="below",
                    breakout_variant=below_variant,
                    confirm_bars=confirm_bars,
                    lockout_bars=lockout_bars,
                    source_timeframe_seconds=int(chart_timeframe_seconds),
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
                    "event=mp_breakout_emitted indicator_id=%s symbol=%s profile_key=%s side=below breakout_variant=%s level_type=%s level_price=%.6f prior_close=%.6f close=%.6f prior_body_low=%.6f prior_body_high=%.6f body_low=%.6f body_high=%.6f streak=%s run_len=%s confirm_bars=%s lockout_bars=%s bar_time=%s",
                    indicator_id,
                    symbol,
                    profile_key,
                    below_variant,
                    below_level_type,
                    below_level_price,
                    prior_close,
                    now_close,
                    prior_body_low,
                    prior_body_high,
                    now_body_low,
                    now_body_high,
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
                        "level_type": below_level_type,
                        "level_price": below_level_price,
                        "breakout_bar_index": bar_index,
                        "breakout_time_epoch": bar_epoch,
                        "breakout_time": candle.time,
                        "VAH": vah_f,
                        "VAL": val_f,
                        "source_timeframe_seconds": int(chart_timeframe_seconds),
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
            close_on_side = now_close >= level_price
            valid_retest = body_touches and close_on_side
            retest_role = "resistance"
        elif direction == "below":
            body_touches = candle_body_high >= (level_price - tolerance)
            close_on_side = now_close <= level_price
            valid_retest = body_touches and close_on_side
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

    diagnostics = {
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
        "source_timeframe_seconds": int(chart_timeframe_seconds),
        "lockout_timeframe_seconds": int(chart_timeframe_seconds),
        "timeframe_mismatch_warning": 0,
    }
    v3_diag = dict(v3_outcome.get("diagnostics") or {})
    diagnostics["v3_candidate_count"] = int(v3_diag.get("candidate_count") or 0)
    diagnostics["v3_pending_count"] = int(v3_diag.get("pending_count") or 0)
    diagnostics["v3_emitted"] = int(len(v3_outcome.get("signals") or []))
    return {
        "signals": emitted,
        "diagnostics": diagnostics,
    }

def _profile_identity(profile: Any) -> str:
    try:
        from indicators.market_profile.compute.internal.runtime_profiles import profile_identity

        return profile_identity(profile)
    except Exception:
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


def _normalize_runtime_mapping(runtime_state: MutableMapping[str, Any], key: str) -> MutableMapping[str, Any]:
    value = runtime_state.get(key)
    if isinstance(value, MutableMapping):
        return value
    normalized: Dict[str, Any] = {}
    runtime_state[key] = normalized
    return normalized


def _normalize_runtime_list(runtime_state: MutableMapping[str, Any], key: str) -> List[Dict[str, Any]]:
    value = runtime_state.get(key)
    if isinstance(value, list):
        return value
    normalized: List[Dict[str, Any]] = []
    runtime_state[key] = normalized
    return normalized


def _prune_epoch_mapping(mapping: MutableMapping[str, Any], *, min_epoch: int) -> int:
    removed = 0
    for key in list(mapping.keys()):
        epoch = _safe_int(mapping.get(key))
        if epoch is None or epoch < int(min_epoch):
            mapping.pop(key, None)
            removed += 1
    return removed


def _cap_mapping_by_epoch(mapping: MutableMapping[str, Any], *, max_entries: int) -> int:
    if max_entries <= 0:
        removed = len(mapping)
        mapping.clear()
        return removed
    extra = len(mapping) - int(max_entries)
    if extra <= 0:
        return 0
    ordered_keys = sorted(
        list(mapping.keys()),
        key=lambda key: (
            int(_safe_int(mapping.get(key)) or -1),
            str(key),
        ),
    )
    for key in ordered_keys[:extra]:
        mapping.pop(key, None)
    return int(extra)


def _prune_pending_entries(pending: MutableMapping[str, Any], *, min_epoch: int) -> int:
    removed = 0
    for key in list(pending.keys()):
        entry = pending.get(key)
        if not isinstance(entry, Mapping):
            pending.pop(key, None)
            removed += 1
            continue
        started_epoch = _safe_int(entry.get("started_epoch"))
        if started_epoch is not None and started_epoch < int(min_epoch):
            pending.pop(key, None)
            removed += 1
    return removed


def _cap_pending_entries(pending: MutableMapping[str, Any], *, max_entries: int) -> int:
    if max_entries <= 0:
        removed = len(pending)
        pending.clear()
        return removed
    extra = len(pending) - int(max_entries)
    if extra <= 0:
        return 0
    ordered_keys = sorted(
        list(pending.keys()),
        key=lambda key: (
            int(_safe_int((pending.get(key) or {}).get("started_epoch")) or -1)
            if isinstance(pending.get(key), Mapping)
            else -1,
            str(key),
        ),
    )
    for key in ordered_keys[:extra]:
        pending.pop(key, None)
    return int(extra)


def _prune_active_breakouts(
    active_breakouts: List[Dict[str, Any]], *, min_epoch: int
) -> int:
    kept: List[Dict[str, Any]] = []
    removed = 0
    for entry in active_breakouts:
        if not isinstance(entry, Mapping):
            removed += 1
            continue
        breakout_epoch = _safe_int(entry.get("breakout_epoch"))
        if breakout_epoch is not None and breakout_epoch < int(min_epoch):
            removed += 1
            continue
        kept.append(dict(entry))
    if removed > 0 or len(kept) != len(active_breakouts):
        active_breakouts[:] = kept
    return removed


def _cap_active_breakouts(active_breakouts: List[Dict[str, Any]], *, max_entries: int) -> int:
    if max_entries <= 0:
        removed = len(active_breakouts)
        active_breakouts.clear()
        return removed
    extra = len(active_breakouts) - int(max_entries)
    if extra <= 0:
        return 0
    del active_breakouts[:extra]
    return int(extra)


def _apply_v3_runtime_state_bounds(
    *,
    pending: MutableMapping[str, Any],
    last_emit_epoch: MutableMapping[str, Any],
    emitted_signatures: MutableMapping[str, Any],
    active_breakouts: List[Dict[str, Any]],
    retest_signatures: MutableMapping[str, Any],
    min_epoch: int,
    pending_max_entries: int,
    active_breakouts_max_entries: int,
    history_max_entries: int,
) -> Dict[str, int]:
    return {
        "state_pruned_pending": int(_prune_pending_entries(pending, min_epoch=min_epoch)),
        "state_pruned_last_emit_epoch": int(_prune_epoch_mapping(last_emit_epoch, min_epoch=min_epoch)),
        "state_pruned_emitted_signatures": int(_prune_epoch_mapping(emitted_signatures, min_epoch=min_epoch)),
        "state_pruned_active_breakouts": int(_prune_active_breakouts(active_breakouts, min_epoch=min_epoch)),
        "state_pruned_retest_signatures": int(_prune_epoch_mapping(retest_signatures, min_epoch=min_epoch)),
        "state_capped_pending": int(_cap_pending_entries(pending, max_entries=pending_max_entries)),
        "state_capped_last_emit_epoch": int(_cap_mapping_by_epoch(last_emit_epoch, max_entries=history_max_entries)),
        "state_capped_emitted_signatures": int(_cap_mapping_by_epoch(emitted_signatures, max_entries=history_max_entries)),
        "state_capped_active_breakouts": int(_cap_active_breakouts(active_breakouts, max_entries=active_breakouts_max_entries)),
        "state_capped_retest_signatures": int(_cap_mapping_by_epoch(retest_signatures, max_entries=history_max_entries)),
    }


def _get_runtime_state(
    *, snapshot_payload: Mapping[str, Any], runtime_key: tuple[str, str, str], current_epoch: int
) -> Dict[str, Any]:
    storage = snapshot_payload.get("_runtime_state_storage")
    if not isinstance(storage, MutableMapping):
        storage = {}
        if isinstance(snapshot_payload, MutableMapping):
            snapshot_payload["_runtime_state_storage"] = storage
    # Runtime signal state must persist across sequential bars within a run.
    # Key by runtime identity, not per-bar epoch.
    temporal_key = f"{runtime_key[0]}|{runtime_key[1]}|{runtime_key[2]}"
    state = storage.get(temporal_key)
    if not isinstance(state, dict):
        state = {"bar_index": 0, "last_epoch": None, "profile_states": {}, "active_breakouts": []}
        storage[temporal_key] = state
    last_epoch = state.get("last_epoch")
    if isinstance(last_epoch, int) and current_epoch <= last_epoch:
        state = {"bar_index": 0, "last_epoch": None, "profile_states": {}, "active_breakouts": []}
        storage[temporal_key] = state
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
            boundary_distance = min(
                abs(close_price - vah),
                abs(close_price - val),
                abs(previous_close - vah),
                abs(previous_close - val),
            )
            selected.append(
                {
                    **runtime_view,
                    "selection_reason": "in_or_cross",
                    "selection_score": boundary_distance,
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
                    "selection_score": distance,
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


def _body_fully_above(*, now_body_low: float, level: float) -> bool:
    return now_body_low > level


def _body_fully_below(*, now_body_high: float, level: float) -> bool:
    return now_body_high < level


def _body_fully_inside(*, now_body_low: float, now_body_high: float, val: float, vah: float) -> bool:
    return now_body_low >= val and now_body_high <= vah


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
    breakout_variant: str,
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
    bias = _bias_from_direction(direction)
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
        "breakout_variant": str(breakout_variant or ""),
        "pointer_direction": pointer_direction,
        "direction": direction,
        "bias": bias,
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
    bias = _bias_from_direction(trade_direction)
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
        "bias": bias,
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


def _body_bounds(candle: Candle) -> tuple[float, float]:
    open_f = float(candle.open)
    close_f = float(candle.close)
    return (min(open_f, close_f), max(open_f, close_f))


def _is_merged_profile(profile: Any) -> bool:
    if bool(getattr(profile, "is_merged", False)):
        return True
    session_count = _safe_int(getattr(profile, "session_count", None))
    return bool(session_count is not None and session_count > 1)


def _resolve_breakout_v3_candidate(
    *,
    runtime_views: List[Dict[str, Any]],
    previous_candle: Candle | None,
    candle: Candle,
    epsilon: float = 1e-9,
) -> tuple[Dict[str, Any] | None, int]:
    if not runtime_views:
        return None, 0
    if not isinstance(previous_candle, Candle):
        # First bar in a walk-forward window has no prior body to compare against.
        # Candidate evaluation is undefined without previous bar context, so skip.
        return None, 0

    prev_body_low, prev_body_high = _body_bounds(previous_candle)
    curr_body_low, curr_body_high = _body_bounds(candle)
    curr_close = float(candle.close)
    curr_open = float(candle.open)
    prev_open = float(previous_candle.open)
    prev_close = float(previous_candle.close)

    candidates: List[Dict[str, Any]] = []
    for runtime_view in runtime_views:
        vah = float(runtime_view["vah"])
        val = float(runtime_view["val"])
        va_width = max(vah - val, float(epsilon))
        profile_key = str(runtime_view["profile_key"])
        profile = runtime_view["profile"]
        variants: List[Dict[str, Any]] = []

        if prev_body_high <= vah and curr_body_high > vah and curr_close > vah:
            penetration = max(0.0, curr_body_high - vah)
            variants.append(
                {
                    "variant": "breakout_up",
                    "boundary_type": "VAH",
                    "boundary_price": vah,
                    "penetration": penetration,
                    "direction": "long",
                }
            )
        if prev_body_low >= val and curr_body_low < val and curr_close < val:
            penetration = max(0.0, val - curr_body_low)
            variants.append(
                {
                    "variant": "breakout_down",
                    "boundary_type": "VAL",
                    "boundary_price": val,
                    "penetration": penetration,
                    "direction": "short",
                }
            )
        if prev_body_low > vah and curr_body_low <= vah and (val <= curr_close <= vah):
            penetration = max(0.0, vah - curr_body_low)
            variants.append(
                {
                    "variant": "breakin_from_above",
                    "boundary_type": "VAH",
                    "boundary_price": vah,
                    "penetration": penetration,
                    "direction": "long",
                }
            )
        if prev_body_high < val and curr_body_high >= val and (val <= curr_close <= vah):
            penetration = max(0.0, curr_body_high - val)
            variants.append(
                {
                    "variant": "breakin_from_below",
                    "boundary_type": "VAL",
                    "boundary_price": val,
                    "penetration": penetration,
                    "direction": "short",
                }
            )

        if not variants:
            continue

        # Deterministic variant priority for edge cases where a malformed profile could satisfy >1.
        priority = ("breakout_up", "breakout_down", "breakin_from_above", "breakin_from_below")
        variant = sorted(variants, key=lambda item: priority.index(str(item["variant"])))[0]
        score = float(variant["penetration"]) / va_width
        candidates.append(
            {
                "profile": profile,
                "profile_key": profile_key,
                "value_area_id": _profile_identity(profile),
                "known_at": getattr(profile, "end", None),
                "vah": vah,
                "val": val,
                "va_width": float(vah - val),
                "is_merged": _is_merged_profile(profile),
                "variant": str(variant["variant"]),
                "boundary_type": str(variant["boundary_type"]),
                "boundary_price": float(variant["boundary_price"]),
                "penetration": float(variant["penetration"]),
                "score": float(score),
                "direction": str(variant["direction"]),
                "candidate_diagnostics": {
                    "prev_open": prev_open,
                    "prev_close": prev_close,
                    "prev_body_low": prev_body_low,
                    "prev_body_high": prev_body_high,
                    "candidate_open": curr_open,
                    "candidate_close": curr_close,
                    "candidate_body_low": curr_body_low,
                    "candidate_body_high": curr_body_high,
                    "penetration": float(variant["penetration"]),
                },
            }
        )

    if not candidates:
        return None, 0

    ordered = sorted(
        candidates,
        key=lambda item: (
            -float(item["score"]),
            0 if bool(item["is_merged"]) else 1,
            float(item["va_width"]),
            str(item["profile_key"]),
        ),
    )
    chosen = dict(ordered[0])
    chosen["chosen_score"] = float(chosen["score"])
    chosen["tie_break_notes"] = (
        f"score_desc,merged_pref,width_asc,profile_key_asc;"
        f"merged={1 if chosen['is_merged'] else 0};width={float(chosen['va_width']):.8f}"
    )
    return chosen, len(candidates)


def _breakout_v3_confirm_valid(*, variant: str, body_low: float, body_high: float, val: float, vah: float) -> bool:
    if variant == "breakout_up":
        return body_low > vah
    if variant == "breakout_down":
        return body_high < val
    if variant in {"breakin_from_above", "breakin_from_below"}:
        return body_low >= val and body_high <= vah
    return False


def _lockout_elapsed_v3(*, last_time_epoch: int | None, current_epoch: int, lockout_seconds: int) -> bool:
    if last_time_epoch is None or lockout_seconds <= 0:
        return True
    return (int(current_epoch) - int(last_time_epoch)) > int(lockout_seconds)


def _short_trace_id(signature: str) -> str:
    digest = hashlib.sha1(str(signature).encode("utf-8")).hexdigest()
    return f"mpv3-{digest[:12]}"


def _build_breakout_v3_confirmed_signal(
    *,
    candle: Candle,
    bar_index: int,
    timeframe_seconds: int,
    indicator_id: str,
    runtime_scope: str,
    symbol: str,
    confirm_bars: int,
    lockout_seconds: int,
    pending_entry: Mapping[str, Any],
    confirm_streak: int,
    signature: str,
) -> Dict[str, Any]:
    variant = str(pending_entry.get("variant") or "")
    direction = "long" if variant in {"breakout_up", "breakin_from_above"} else "short"
    pointer_direction = "up" if direction == "long" else "down"
    bias = _bias_from_direction(direction)
    profile_key = str(pending_entry.get("profile_key") or "")
    started_bar_index = int(pending_entry.get("started_bar_index") or 0)
    diagnostics = dict(pending_entry.get("candidate_diagnostics") or {})
    diagnostics["chosen_score"] = _safe_float(pending_entry.get("chosen_score"))
    diagnostics["tie_break_notes"] = str(pending_entry.get("tie_break_notes") or "")
    diagnostics["event_signature"] = signature
    trace_id = _short_trace_id(signature)
    diagnostics["trace_id"] = trace_id
    signal_epoch = int(candle.time.timestamp())
    return {
        "time": candle.time,
        "signal_time": signal_epoch,
        "signal_type": "breakout",
        "type": "breakout",
        "source": "MarketProfile",
        "symbol": symbol,
        "indicator_id": indicator_id,
        "runtime_scope": runtime_scope,
        "rule_id": "market_profile_breakout",
        "pattern_id": "value_area_breakout",
        "rule_aliases": [
            "market_profile_breakout",
            "market_profile_breakout_rule",
            "market_profile_breakout_v3_confirmed",
            "market_profile_breakout_v3_confirmed_rule",
            "market_profile_breakout_v3",
            "market_profile_breakout_v3_rule",
        ],
        "profile_key": profile_key,
        "value_area_id": str(pending_entry.get("value_area_id") or profile_key),
        "known_at": pending_entry.get("known_at"),
        "variant": variant,
        "boundary_type": str(pending_entry.get("boundary_type") or ""),
        "boundary_price": float(pending_entry.get("boundary_price") or 0.0),
        "VAH": _safe_float(pending_entry.get("VAH")),
        "VAL": _safe_float(pending_entry.get("VAL")),
        "bar_time": candle.time,
        "bar_index": int(bar_index),
        "timeframe_seconds": int(timeframe_seconds),
        "confirm_bars": int(confirm_bars),
        "lockout_seconds": int(lockout_seconds),
        "started_bar_index": started_bar_index,
        "confirm_streak_at_emit": int(confirm_streak),
        "direction": direction,
        "bias": bias,
        "pointer_direction": pointer_direction,
        "trace_id": trace_id,
        "event_signature": signature,
        "candidate_diagnostics": diagnostics,
        "metadata": {
            "signal_time": signal_epoch,
            "rule_id": "market_profile_breakout",
            "pattern_id": "value_area_breakout",
            "indicator_id": indicator_id,
            "runtime_scope": runtime_scope,
            "timeframe_seconds": int(timeframe_seconds),
            "symbol": symbol,
            "known_at": pending_entry.get("known_at"),
            "trace_id": trace_id,
            "direction": direction,
            "bias": bias,
            "VAH": _safe_float(pending_entry.get("VAH")),
            "VAL": _safe_float(pending_entry.get("VAL")),
        },
    }


def _build_breakout_v3_retest_signal(
    *,
    candle: Candle,
    bar_index: int,
    timeframe_seconds: int,
    indicator_id: str,
    runtime_scope: str,
    symbol: str,
    breakout_entry: Mapping[str, Any],
    bars_since_breakout: int,
    retest_tolerance_pct: float,
) -> Dict[str, Any]:
    direction = str(breakout_entry.get("direction") or "long").strip().lower()
    pointer_direction = "up" if direction == "long" else "down"
    bias = _bias_from_direction(direction)
    profile_key = str(breakout_entry.get("profile_key") or "")
    boundary_type = str(breakout_entry.get("boundary_type") or "")
    boundary_price = float(breakout_entry.get("boundary_price") or 0.0)
    breakout_started_bar_index = int(breakout_entry.get("started_bar_index") or 0)
    breakout_signature = str(breakout_entry.get("event_signature") or "")
    signature = f"retest_v3|{profile_key}|{boundary_type}|{breakout_started_bar_index}|{bar_index}"
    trace_id = _short_trace_id(signature)
    signal_epoch = int(candle.time.timestamp())
    return {
        "time": candle.time,
        "signal_time": signal_epoch,
        "signal_type": "retest",
        "type": "retest",
        "source": "MarketProfile",
        "symbol": symbol,
        "indicator_id": indicator_id,
        "runtime_scope": runtime_scope,
        "rule_id": "market_profile_retest",
        "pattern_id": "value_area_retest",
        "rule_aliases": [
            "market_profile_retest",
            "market_profile_retest_rule",
            "market_profile_retest_v3",
            "market_profile_retest_v3_rule",
        ],
        "profile_key": profile_key,
        "value_area_id": str(breakout_entry.get("value_area_id") or profile_key),
        "known_at": breakout_entry.get("known_at"),
        "variant": str(breakout_entry.get("variant") or ""),
        "boundary_type": boundary_type,
        "boundary_price": boundary_price,
        "VAH": _safe_float(breakout_entry.get("VAH")),
        "VAL": _safe_float(breakout_entry.get("VAL")),
        "bar_time": candle.time,
        "bar_index": int(bar_index),
        "timeframe_seconds": int(timeframe_seconds),
        "direction": direction,
        "bias": bias,
        "pointer_direction": pointer_direction,
        "breakout_started_bar_index": breakout_started_bar_index,
        "bars_since_breakout": int(bars_since_breakout),
        "retest_tolerance_pct": float(retest_tolerance_pct),
        "breakout_event_signature": breakout_signature,
        "event_signature": signature,
        "trace_id": trace_id,
        "metadata": {
            "signal_time": signal_epoch,
            "rule_id": "market_profile_retest",
            "pattern_id": "value_area_retest",
            "indicator_id": indicator_id,
            "runtime_scope": runtime_scope,
            "timeframe_seconds": int(timeframe_seconds),
            "symbol": symbol,
            "known_at": breakout_entry.get("known_at"),
            "trace_id": trace_id,
            "direction": direction,
            "bias": bias,
            "VAH": _safe_float(breakout_entry.get("VAH")),
            "VAL": _safe_float(breakout_entry.get("VAL")),
        },
    }


def _market_profile_breakout_v3_payload(
    *,
    snapshot_payload: Mapping[str, Any],
    candle: Candle,
    previous_candle: Candle | None,
    runtime_state: MutableMapping[str, Any],
    runtime_views: List[Dict[str, Any]],
    cache_hit: bool,
    known_count: int,
    indicator_id: str,
    symbol: str,
    runtime_scope: str,
    bar_epoch: int,
    params_map: Mapping[str, Any],
    chart_timeframe: str,
    chart_timeframe_seconds: int,
    bar_index: int,
) -> Dict[str, Any]:
    timeframe_seconds = _safe_int(chart_timeframe_seconds)
    if timeframe_seconds is None or timeframe_seconds <= 0:
        raise RuntimeError(
            "market_profile_breakout_v3_missing_timeframe_seconds: chart timeframe seconds required for lockout math"
        )

    confirm_bars = _resolve_int(
        params_map,
        keys=("market_profile_breakout_v3_confirm_bars", "confirm_bars"),
        default=3,
        min_value=1,
    )
    lockout_bars = _resolve_int(
        params_map,
        keys=("market_profile_breakout_v3_lockout_bars", "lockout_bars"),
        default=confirm_bars,
        min_value=0,
    )
    retest_min_bars = _resolve_int(
        params_map,
        keys=("market_profile_retest_min_bars", "market_profile_retest_v3_min_bars", "retest_min_bars"),
        default=3,
        min_value=0,
    )
    retest_max_lookback = _resolve_int(
        params_map,
        keys=("market_profile_retest_max_bars", "market_profile_retest_v3_max_lookback", "retest_max_lookback"),
        default=50,
        min_value=1,
    )
    retest_tolerance_pct = _resolve_float(
        params_map,
        keys=("market_profile_retest_tolerance_pct", "market_profile_retest_v3_tolerance_pct", "retest_tolerance_pct"),
        default=0.15,
        min_value=0.0,
    )
    lockout_seconds = int(lockout_bars) * int(timeframe_seconds)

    emitted: List[Dict[str, Any]] = []
    diagnostic_counts: Dict[str, int] = {
        "candidate_lockout_blocked": 0,
        "candidate_enqueued_pending": 0,
        "candidate_already_pending": 0,
        "pending_profile_missing": 0,
        "pending_waiting_next_bar": 0,
        "pending_confirm_invalid": 0,
        "pending_confirm_progress": 0,
        "pending_duplicate_signature": 0,
        "breakout_emitted": 0,
        "retest_waiting_min_bars": 0,
        "retest_expired_max_lookback": 0,
        "retest_missing_boundary_price": 0,
        "retest_condition_rejected": 0,
        "retest_duplicate_signature": 0,
        "retest_emitted": 0,
    }
    last_processed_bar_index = _safe_int(runtime_state.get("v3_last_processed_bar_index"))
    pending = _normalize_runtime_mapping(runtime_state, "v3_pending")
    last_emit_epoch = _normalize_runtime_mapping(runtime_state, "v3_last_emit_epoch")
    emitted_signatures = _normalize_runtime_mapping(runtime_state, "v3_emitted_signatures")
    active_breakouts = _normalize_runtime_list(runtime_state, "v3_active_breakouts")
    retest_signatures = _normalize_runtime_mapping(runtime_state, "v3_retest_signatures")

    state_ttl_bars = _resolve_int(
        params_map,
        keys=("market_profile_breakout_v3_state_ttl_bars",),
        default=max(128, retest_max_lookback + confirm_bars + lockout_bars + 8),
        min_value=1,
    )
    pending_max_entries = _resolve_int(
        params_map,
        keys=("market_profile_breakout_v3_pending_max_entries",),
        default=max(64, int(len(runtime_views) * 4)),
        min_value=1,
    )
    active_breakouts_max_entries = _resolve_int(
        params_map,
        keys=("market_profile_breakout_v3_active_breakouts_max_entries",),
        default=max(64, int(retest_max_lookback * 2)),
        min_value=1,
    )
    history_max_entries = _resolve_int(
        params_map,
        keys=("market_profile_breakout_v3_history_max_entries",),
        default=max(256, int(active_breakouts_max_entries * 4)),
        min_value=1,
    )
    min_state_epoch = int(bar_epoch) - int(state_ttl_bars * int(timeframe_seconds))

    if last_processed_bar_index is not None and bar_index <= last_processed_bar_index:
        pending.clear()
        last_emit_epoch.clear()
        emitted_signatures.clear()
        active_breakouts.clear()
        retest_signatures.clear()

    state_bounds = _apply_v3_runtime_state_bounds(
        pending=pending,
        last_emit_epoch=last_emit_epoch,
        emitted_signatures=emitted_signatures,
        active_breakouts=active_breakouts,
        retest_signatures=retest_signatures,
        min_epoch=min_state_epoch,
        pending_max_entries=pending_max_entries,
        active_breakouts_max_entries=active_breakouts_max_entries,
        history_max_entries=history_max_entries,
    )
    for key, value in state_bounds.items():
        diagnostic_counts[key] = int(diagnostic_counts.get(key, 0)) + int(value)

    active_views: Dict[str, Dict[str, Any]] = {str(item["profile_key"]): item for item in runtime_views}

    chosen_candidate: Dict[str, Any] | None = None
    num_candidates = 0
    if runtime_views:
        chosen_candidate, num_candidates = _resolve_breakout_v3_candidate(
            runtime_views=runtime_views,
            previous_candle=previous_candle,
            candle=candle,
        )

    if isinstance(chosen_candidate, Mapping):
        profile_key = str(chosen_candidate.get("profile_key") or "")
        variant = str(chosen_candidate.get("variant") or "")
        pending_key = f"{profile_key}|{variant}"
        previous_emit = _safe_int(last_emit_epoch.get(pending_key))
        if _lockout_elapsed_v3(last_time_epoch=previous_emit, current_epoch=bar_epoch, lockout_seconds=lockout_seconds):
            if pending_key not in pending:
                pending[pending_key] = {
                    "profile_key": profile_key,
                    "value_area_id": str(chosen_candidate.get("value_area_id") or profile_key),
                    "known_at": chosen_candidate.get("known_at"),
                    "variant": variant,
                    "boundary_type": str(chosen_candidate.get("boundary_type") or ""),
                    "boundary_price": float(chosen_candidate.get("boundary_price") or 0.0),
                    "started_bar_index": int(bar_index),
                    "started_epoch": int(bar_epoch),
                    "streak": 0,
                    "candidate_diagnostics": dict(chosen_candidate.get("candidate_diagnostics") or {}),
                    "penetration": float(chosen_candidate.get("penetration") or 0.0),
                    "chosen_score": float(chosen_candidate.get("chosen_score") or 0.0),
                    "tie_break_notes": str(chosen_candidate.get("tie_break_notes") or ""),
                }
                diagnostic_counts["candidate_enqueued_pending"] += 1
            else:
                diagnostic_counts["candidate_already_pending"] += 1
        else:
            diagnostic_counts["candidate_lockout_blocked"] += 1

    body_low, body_high = _body_bounds(candle)
    for pending_key in list(pending.keys()):
        entry = pending.get(pending_key)
        if not isinstance(entry, Mapping):
            pending.pop(pending_key, None)
            continue
        profile_key = str(entry.get("profile_key") or "")
        runtime_view = active_views.get(profile_key)
        if runtime_view is None:
            pending.pop(pending_key, None)
            diagnostic_counts["pending_profile_missing"] += 1
            continue
        if int(entry.get("started_bar_index") or 0) >= bar_index:
            diagnostic_counts["pending_waiting_next_bar"] += 1
            continue

        val = float(runtime_view["val"])
        vah = float(runtime_view["vah"])
        variant = str(entry.get("variant") or "")
        boundary_type = str(entry.get("boundary_type") or "")
        boundary_price = _safe_float(entry.get("boundary_price")) or 0.0
        expected_boundary = vah if boundary_type == "VAH" else val if boundary_type == "VAL" else None
        if expected_boundary is not None and abs(float(expected_boundary) - float(boundary_price)) > 1e-9:
            log.warning(
                "event=market_profile_breakout_v3_boundary_mismatch indicator_id=%s symbol=%s profile_key=%s boundary_type=%s boundary_price=%.8f expected=%.8f vah=%.8f val=%.8f bar_index=%s",
                indicator_id,
                symbol,
                profile_key,
                boundary_type,
                float(boundary_price),
                float(expected_boundary),
                float(vah),
                float(val),
                bar_index,
            )
        confirm_valid = _breakout_v3_confirm_valid(
            variant=variant,
            body_low=body_low,
            body_high=body_high,
            val=val,
            vah=vah,
        )
        if not confirm_valid:
            pending.pop(pending_key, None)
            diagnostic_counts["pending_confirm_invalid"] += 1
            continue

        next_streak = int(entry.get("streak") or 0) + 1
        entry = dict(entry)
        entry["streak"] = next_streak
        entry["VAH"] = float(vah)
        entry["VAL"] = float(val)
        pending[pending_key] = entry
        if next_streak != confirm_bars:
            diagnostic_counts["pending_confirm_progress"] += 1
            continue

        started_bar_index = int(entry.get("started_bar_index") or 0)
        signature = f"breakout_v3|{profile_key}|{variant}|{started_bar_index}"
        if signature in emitted_signatures:
            pending.pop(pending_key, None)
            diagnostic_counts["pending_duplicate_signature"] += 1
            continue

        signal = _build_breakout_v3_confirmed_signal(
            candle=candle,
            bar_index=bar_index,
            timeframe_seconds=timeframe_seconds,
            indicator_id=indicator_id,
            runtime_scope=runtime_scope,
            symbol=symbol,
            confirm_bars=confirm_bars,
            lockout_seconds=lockout_seconds,
            pending_entry=entry,
            confirm_streak=next_streak,
            signature=signature,
        )
        assert_signal_contract(signal)
        assert_signal_time_is_closed_bar(signal, candle)
        assert_no_execution_fields(signal)
        emitted.append(signal)
        diagnostic_counts["breakout_emitted"] += 1
        emitted_signatures[signature] = int(bar_epoch)
        last_emit_epoch[pending_key] = int(bar_epoch)
        active_breakouts.append(
            {
                "event_signature": signature,
                "profile_key": profile_key,
                "value_area_id": str(entry.get("value_area_id") or profile_key),
                "known_at": entry.get("known_at"),
                "variant": variant,
                "boundary_type": str(entry.get("boundary_type") or ""),
                "boundary_price": float(entry.get("boundary_price") or 0.0),
                "VAH": float(vah),
                "VAL": float(val),
                "direction": str(signal.get("direction") or ""),
                "started_bar_index": int(entry.get("started_bar_index") or 0),
                "breakout_bar_index": int(bar_index),
                "breakout_epoch": int(bar_epoch),
            }
        )
        pending.pop(pending_key, None)
        log.debug(
            "event=market_profile_breakout_v3_confirmed indicator_id=%s symbol=%s profile_key=%s variant=%s bar_index=%s payload=%s",
            indicator_id,
            symbol,
            profile_key,
            variant,
            bar_index,
            signal,
        )

    breakout_count = len(emitted)
    remaining_breakouts: List[Dict[str, Any]] = []
    for breakout in active_breakouts:
        if not isinstance(breakout, Mapping):
            continue
        breakout_bar_index = int(breakout.get("breakout_bar_index") or -1)
        if breakout_bar_index < 0:
            continue
        bars_since = int(bar_index - breakout_bar_index)
        if bars_since < retest_min_bars:
            remaining_breakouts.append(dict(breakout))
            diagnostic_counts["retest_waiting_min_bars"] += 1
            continue
        if bars_since > retest_max_lookback:
            diagnostic_counts["retest_expired_max_lookback"] += 1
            continue

        boundary_price = _safe_float(breakout.get("boundary_price"))
        if boundary_price is None:
            remaining_breakouts.append(dict(breakout))
            diagnostic_counts["retest_missing_boundary_price"] += 1
            continue
        tolerance = abs(float(boundary_price)) * (float(retest_tolerance_pct) / 100.0)
        body_touches = (body_low - tolerance) <= float(boundary_price) <= (body_high + tolerance)
        direction = str(breakout.get("direction") or "").strip().lower()
        close_price = float(candle.close)
        if direction == "long":
            close_on_side = close_price >= float(boundary_price)
        elif direction == "short":
            close_on_side = close_price <= float(boundary_price)
        else:
            close_on_side = False
        if not (body_touches and close_on_side):
            remaining_breakouts.append(dict(breakout))
            diagnostic_counts["retest_condition_rejected"] += 1
            continue

        retest_signature = (
            f"retest_v3|{str(breakout.get('event_signature') or '')}|{int(bar_index)}"
        )
        if retest_signature in retest_signatures:
            diagnostic_counts["retest_duplicate_signature"] += 1
            continue
        retest_signal = _build_breakout_v3_retest_signal(
            candle=candle,
            bar_index=bar_index,
            timeframe_seconds=timeframe_seconds,
            indicator_id=indicator_id,
            runtime_scope=runtime_scope,
            symbol=symbol,
            breakout_entry=breakout,
            bars_since_breakout=bars_since,
            retest_tolerance_pct=float(retest_tolerance_pct),
        )
        assert_signal_contract(retest_signal)
        assert_signal_time_is_closed_bar(retest_signal, candle)
        assert_no_execution_fields(retest_signal)
        emitted.append(retest_signal)
        diagnostic_counts["retest_emitted"] += 1
        retest_signatures[retest_signature] = int(bar_epoch)
        log.info(
            "event=market_profile_retest_v3_emitted indicator_id=%s symbol=%s profile_key=%s direction=%s boundary_type=%s boundary_price=%.8f tolerance=%.8f body_low=%.8f body_high=%.8f close=%.8f bars_since_breakout=%s trace_id=%s",
            indicator_id,
            symbol,
            str(breakout.get("profile_key") or ""),
            direction,
            str(breakout.get("boundary_type") or ""),
            float(boundary_price),
            float(tolerance),
            float(body_low),
            float(body_high),
            float(close_price),
            bars_since,
            str(retest_signal.get("trace_id") or ""),
        )

    active_breakouts = remaining_breakouts
    state_bounds_after = _apply_v3_runtime_state_bounds(
        pending=pending,
        last_emit_epoch=last_emit_epoch,
        emitted_signatures=emitted_signatures,
        active_breakouts=active_breakouts,
        retest_signatures=retest_signatures,
        min_epoch=min_state_epoch,
        pending_max_entries=pending_max_entries,
        active_breakouts_max_entries=active_breakouts_max_entries,
        history_max_entries=history_max_entries,
    )
    for key, value in state_bounds_after.items():
        diagnostic_counts[key] = int(diagnostic_counts.get(key, 0)) + int(value)

    runtime_state["v3_last_processed_bar_index"] = int(bar_index)
    runtime_state["v3_pending"] = pending
    runtime_state["v3_last_emit_epoch"] = last_emit_epoch
    runtime_state["v3_emitted_signatures"] = emitted_signatures
    runtime_state["v3_active_breakouts"] = active_breakouts
    runtime_state["v3_retest_signatures"] = retest_signatures
    return {
        "signals": emitted,
        "diagnostics": {
            "profile_cache_hit": 1 if cache_hit else 0,
            "profile_cache_miss": 0 if cache_hit else 1,
            "known_profiles": int(known_count),
            "merged_profiles": int(len(runtime_views)),
            "breakouts_emitted": int(breakout_count),
            "retests_emitted": int(len(emitted) - breakout_count),
            "active_breakouts": int(len(remaining_breakouts)),
            "profiles_considered": int(len(runtime_views)),
            "candidate_count": int(num_candidates),
            "candidate_chosen": 1 if isinstance(chosen_candidate, Mapping) else 0,
            "chosen_profile_key": str(chosen_candidate.get("profile_key") or "") if isinstance(chosen_candidate, Mapping) else "",
            "pending_count": int(len(pending)),
            "num_effective_profiles": int(len(runtime_views)),
            "num_candidates": int(num_candidates),
            "source_timeframe_seconds": int(timeframe_seconds),
            "lockout_timeframe_seconds": int(timeframe_seconds),
            "timeframe_mismatch_warning": 0,
            "state_ttl_bars": int(state_ttl_bars),
            "state_pending_max_entries": int(pending_max_entries),
            "state_active_breakouts_max_entries": int(active_breakouts_max_entries),
            "state_history_max_entries": int(history_max_entries),
            "candidate_lockout_blocked": int(diagnostic_counts["candidate_lockout_blocked"]),
            "candidate_enqueued_pending": int(diagnostic_counts["candidate_enqueued_pending"]),
            "candidate_already_pending": int(diagnostic_counts["candidate_already_pending"]),
            "pending_profile_missing": int(diagnostic_counts["pending_profile_missing"]),
            "pending_waiting_next_bar": int(diagnostic_counts["pending_waiting_next_bar"]),
            "pending_confirm_invalid": int(diagnostic_counts["pending_confirm_invalid"]),
            "pending_confirm_progress": int(diagnostic_counts["pending_confirm_progress"]),
            "pending_duplicate_signature": int(diagnostic_counts["pending_duplicate_signature"]),
            "retest_waiting_min_bars": int(diagnostic_counts["retest_waiting_min_bars"]),
            "retest_expired_max_lookback": int(diagnostic_counts["retest_expired_max_lookback"]),
            "retest_missing_boundary_price": int(diagnostic_counts["retest_missing_boundary_price"]),
            "retest_condition_rejected": int(diagnostic_counts["retest_condition_rejected"]),
            "retest_duplicate_signature": int(diagnostic_counts["retest_duplicate_signature"]),
            "state_pruned_pending": int(diagnostic_counts.get("state_pruned_pending") or 0),
            "state_pruned_last_emit_epoch": int(diagnostic_counts.get("state_pruned_last_emit_epoch") or 0),
            "state_pruned_emitted_signatures": int(diagnostic_counts.get("state_pruned_emitted_signatures") or 0),
            "state_pruned_active_breakouts": int(diagnostic_counts.get("state_pruned_active_breakouts") or 0),
            "state_pruned_retest_signatures": int(diagnostic_counts.get("state_pruned_retest_signatures") or 0),
            "state_capped_pending": int(diagnostic_counts.get("state_capped_pending") or 0),
            "state_capped_last_emit_epoch": int(diagnostic_counts.get("state_capped_last_emit_epoch") or 0),
            "state_capped_emitted_signatures": int(diagnostic_counts.get("state_capped_emitted_signatures") or 0),
            "state_capped_active_breakouts": int(diagnostic_counts.get("state_capped_active_breakouts") or 0),
            "state_capped_retest_signatures": int(diagnostic_counts.get("state_capped_retest_signatures") or 0),
        },
    }
