"""Runtime profile resolution shared by overlays and runtime signal emitters."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from indicators.market_profile.domain import Profile, ValueArea
from indicators.market_profile._internal.merging import merge_profiles


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

    known_profiles: List[Profile] = []
    for entry in profiles_payload:
        if not isinstance(entry, Mapping):
            continue
        profile = _profile_from_payload(entry)
        if profile is None:
            continue
        if int(profile.end.timestamp()) > int(current_epoch):
            continue
        known_profiles.append(profile)

    if not known_profiles:
        return [], {"known_profiles": 0, "merged_profiles": 0}

    use_merged = bool(profile_params.get("use_merged_value_areas"))
    if not use_merged:
        return known_profiles, {
            "known_profiles": len(known_profiles),
            "merged_profiles": len(known_profiles),
        }

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
    return merged_profiles, {
        "known_profiles": len(known_profiles),
        "merged_profiles": len(merged_profiles),
    }


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
