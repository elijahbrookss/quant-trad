"""Runtime profile resolution shared by overlays and runtime signal emitters."""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from indicators.market_profile.compute.models import Profile, ValueArea
from indicators.market_profile.compute.internal.merging import merge_profiles

logger = logging.getLogger(__name__)
_LAST_RESOLUTION_LOG_SIGNATURE: Dict[Tuple[str, str, str, str], Tuple[int, int]] = {}


def _should_log_resolution(
    *,
    mode: str,
    symbol: Optional[str],
    merge_threshold: Any,
    min_merge_sessions: Any,
    known_profiles: int,
    merged_profiles: int,
) -> bool:
    key = (
        mode,
        str(symbol or ""),
        str(merge_threshold) if merge_threshold is not None else "",
        str(min_merge_sessions) if min_merge_sessions is not None else "",
    )
    signature = (int(known_profiles), int(merged_profiles))
    previous = _LAST_RESOLUTION_LOG_SIGNATURE.get(key)
    if previous == signature:
        return False
    _LAST_RESOLUTION_LOG_SIGNATURE[key] = signature
    return True


def resolve_effective_profiles(
    *,
    profiles_payload: Sequence[Any],
    profile_params: Mapping[str, Any],
    current_epoch: int,
    bot_id: Optional[str] = None,
    symbol: Optional[str] = None,
    strategy_id: Optional[str] = None,
) -> Tuple[List[Profile], Dict[str, int]]:
    """Resolve known-at profiles and apply merge policy from profile params."""
    started = perf_counter()

    known_profiles: List[Profile] = []
    for entry in profiles_payload:
        if not isinstance(entry, Mapping):
            continue
        profile = _profile_from_payload(entry)
        if profile is None:
            continue
        known_epoch = _to_epoch_seconds(entry.get("known_at"))
        if known_epoch is None:
            raise RuntimeError(
                "market_profile_profile_missing_known_at: every profile must include known_at for known-at gating"
            )
        if int(known_epoch) > int(current_epoch):
            continue
        known_profiles.append(profile)

    if not known_profiles:
        return [], {"known_profiles": 0, "merged_profiles": 0}

    if bool(profile_params.get("profiles_premerged")):
        summary = {
            "known_profiles": len(known_profiles),
            "merged_profiles": len(known_profiles),
        }
        if len(known_profiles) > 0 and _should_log_resolution(
            mode="premerged",
            symbol=symbol,
            merge_threshold=profile_params.get("merge_threshold"),
            min_merge_sessions=profile_params.get("min_merge_sessions"),
            known_profiles=summary["known_profiles"],
            merged_profiles=summary["merged_profiles"],
        ):
            logger.debug(
                "event=runtime_profile_resolution mode=premerged known_profiles=%s merged_profiles=%s duration_ms=%.3f",
                summary["known_profiles"],
                summary["merged_profiles"],
                (perf_counter() - started) * 1000.0,
            )
        return known_profiles, summary

    use_merged = bool(profile_params.get("use_merged_value_areas"))
    if not use_merged:
        summary = {
            "known_profiles": len(known_profiles),
            "merged_profiles": len(known_profiles),
        }
        if len(known_profiles) > 0 and _should_log_resolution(
            mode="known_only",
            symbol=symbol,
            merge_threshold=None,
            min_merge_sessions=None,
            known_profiles=summary["known_profiles"],
            merged_profiles=summary["merged_profiles"],
        ):
            logger.debug(
                "event=runtime_profile_resolution mode=known_only known_profiles=%s merged_profiles=%s duration_ms=%.3f",
                summary["known_profiles"],
                summary["merged_profiles"],
                (perf_counter() - started) * 1000.0,
            )
        return known_profiles, summary

    merge_threshold = profile_params.get("merge_threshold")
    min_merge_sessions = profile_params.get("min_merge_sessions")
    if merge_threshold is None or min_merge_sessions is None:
        # Fail closed to preserve known-at correctness when merge policy is incomplete.
        return [], {"known_profiles": len(known_profiles), "merged_profiles": 0}

    merged_profiles = merge_profiles(
        known_profiles,
        float(merge_threshold),
        int(min_merge_sessions),
        bot_id=bot_id,
        symbol=symbol,
        strategy_id=strategy_id,
    )
    summary = {
        "known_profiles": len(known_profiles),
        "merged_profiles": len(merged_profiles),
    }
    if _should_log_resolution(
        mode="merged",
        symbol=symbol,
        merge_threshold=merge_threshold,
        min_merge_sessions=min_merge_sessions,
        known_profiles=summary["known_profiles"],
        merged_profiles=summary["merged_profiles"],
    ):
        logger.debug(
            "event=runtime_profile_resolution mode=merged known_profiles=%s merged_profiles=%s merge_threshold=%s min_merge_sessions=%s duration_ms=%.3f",
            summary["known_profiles"],
            summary["merged_profiles"],
            merge_threshold,
            min_merge_sessions,
            (perf_counter() - started) * 1000.0,
        )
    return merged_profiles, summary


def profile_identity(profile: Profile) -> str:
    start = profile.start.isoformat() if hasattr(profile.start, "isoformat") else str(profile.start)
    end = profile.end.isoformat() if hasattr(profile.end, "isoformat") else str(profile.end)
    return f"{start}:{end}:{int(getattr(profile, 'session_count', 1) or 1)}"


def _profile_from_payload(entry: Mapping[str, Any]) -> Optional[Profile]:
    start_epoch = _to_epoch_seconds(entry.get("start"))
    end_epoch = _to_epoch_seconds(entry.get("end"))
    if start_epoch is None or end_epoch is None:
        return None

    try:
        vah = float(entry.get("VAH"))
        val = float(entry.get("VAL"))
        poc = float(entry.get("POC"))
    except (TypeError, ValueError):
        return None

    session_count = int(entry.get("session_count") or 1)
    precision = int(entry.get("precision") or 4)
    return Profile(
        start=pd.Timestamp(start_epoch, unit="s", tz="UTC"),
        end=pd.Timestamp(end_epoch, unit="s", tz="UTC"),
        value_area=ValueArea(vah=vah, val=val, poc=poc),
        session_count=session_count,
        precision=precision,
    )


def _to_epoch_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
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
            parsed = pd.Timestamp(text)
        except Exception:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.tz_localize("UTC")
        else:
            parsed = parsed.tz_convert("UTC")
        return int(parsed.timestamp())
    return None
