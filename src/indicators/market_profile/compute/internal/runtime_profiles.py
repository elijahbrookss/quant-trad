"""Runtime profile resolution shared by overlays and runtime signal emitters."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from indicators.market_profile.compute.models import Profile, ValueArea
from indicators.market_profile.compute.internal.merging import calculate_overlap, merge_profiles

logger = logging.getLogger(__name__)
_LAST_RESOLUTION_LOG_SIGNATURE: Dict[Tuple[str, str, str, str], Tuple[int, int]] = {}


@dataclass(frozen=True)
class RuntimeProfileEntry:
    profile: Profile
    known_at_epoch: int


@dataclass
class _MergeCluster:
    start: pd.Timestamp
    end: pd.Timestamp
    val: float
    vah: float
    poc_sum: float
    profile_count: int
    precision: int

    @classmethod
    def from_profile(cls, profile: Profile) -> "_MergeCluster":
        return cls(
            start=profile.start,
            end=profile.end,
            val=float(profile.val),
            vah=float(profile.vah),
            poc_sum=float(profile.poc),
            profile_count=1,
            precision=int(profile.precision),
        )

    def overlap_with(self, profile: Profile) -> float:
        return calculate_overlap(
            float(self.val),
            float(self.vah),
            float(profile.val),
            float(profile.vah),
        )

    def extend(self, profile: Profile) -> None:
        self.val = min(float(self.val), float(profile.val))
        self.vah = max(float(self.vah), float(profile.vah))
        self.end = profile.end
        self.poc_sum += float(profile.poc)
        self.profile_count += 1

    def to_profile(self) -> Profile:
        avg_poc = self.poc_sum / max(1, int(self.profile_count))
        return Profile(
            start=self.start,
            end=self.end,
            value_area=ValueArea(vah=float(self.vah), val=float(self.val), poc=float(avg_poc)),
            session_count=int(self.profile_count),
            precision=int(self.precision),
        )


class IncrementalRuntimeProfileResolver:
    """Incrementally advance known-at profile state for walk-forward runtime use."""

    def __init__(
        self,
        *,
        profiles_payload: Sequence[Any],
        profile_params: Mapping[str, Any],
        bot_id: Optional[str] = None,
        symbol: Optional[str] = None,
        strategy_id: Optional[str] = None,
    ) -> None:
        self._entries = _parse_runtime_profile_entries(profiles_payload)
        self._profile_params = dict(profile_params or {})
        self._bot_id = bot_id
        self._symbol = symbol
        self._strategy_id = strategy_id
        self._next_entry_index = 0
        self._known_profiles: List[Profile] = []
        self._closed_merged_profiles: List[Profile] = []
        self._active_cluster: Optional[_MergeCluster] = None
        self._last_epoch: Optional[int] = None
        self._effective_profiles: List[Profile] = []
        self._summary: Dict[str, int] = {"known_profiles": 0, "merged_profiles": 0}

    def resolve(self, *, current_epoch: int) -> Tuple[List[Profile], Dict[str, int]]:
        started = perf_counter()
        epoch = int(current_epoch)
        if self._last_epoch is not None and epoch < int(self._last_epoch):
            raise RuntimeError(
                "market_profile_runtime_epoch_invalid: current_epoch moved backwards during walk-forward execution"
            )
        self._last_epoch = epoch

        changed = False
        while self._next_entry_index < len(self._entries):
            entry = self._entries[self._next_entry_index]
            if int(entry.known_at_epoch) > epoch:
                break
            self._known_profiles.append(entry.profile)
            self._next_entry_index += 1
            changed = True
            if self._should_incrementally_merge():
                self._advance_merge_chain(entry.profile)

        if not self._known_profiles:
            self._effective_profiles = []
            self._summary = {"known_profiles": 0, "merged_profiles": 0}
            return [], dict(self._summary)

        if self._is_premerged():
            if changed:
                self._effective_profiles = list(self._known_profiles)
                self._summary = {
                    "known_profiles": len(self._known_profiles),
                    "merged_profiles": len(self._known_profiles),
                }
                self._log_resolution(mode="premerged", started=started)
            return list(self._effective_profiles), dict(self._summary)

        if not self._uses_merged_value_areas():
            if changed:
                self._effective_profiles = list(self._known_profiles)
                self._summary = {
                    "known_profiles": len(self._known_profiles),
                    "merged_profiles": len(self._known_profiles),
                }
                self._log_resolution(mode="known_only", started=started)
            return list(self._effective_profiles), dict(self._summary)

        if not self._merge_policy_complete():
            self._effective_profiles = []
            self._summary = {
                "known_profiles": len(self._known_profiles),
                "merged_profiles": 0,
            }
            return [], dict(self._summary)

        if changed:
            self._effective_profiles = list(self._closed_merged_profiles)
            active_profile = self._active_effective_profile()
            if active_profile is not None:
                self._effective_profiles.append(active_profile)
            self._summary = {
                "known_profiles": len(self._known_profiles),
                "merged_profiles": len(self._effective_profiles),
            }
            self._log_resolution(mode="merged", started=started)
        return list(self._effective_profiles), dict(self._summary)

    def _is_premerged(self) -> bool:
        return bool(self._profile_params.get("profiles_premerged"))

    def _uses_merged_value_areas(self) -> bool:
        return bool(self._profile_params.get("use_merged_value_areas"))

    def _merge_policy_complete(self) -> bool:
        return (
            self._profile_params.get("merge_threshold") is not None
            and self._profile_params.get("min_merge_sessions") is not None
        )

    def _should_incrementally_merge(self) -> bool:
        return (
            not self._is_premerged()
            and self._uses_merged_value_areas()
            and self._merge_policy_complete()
        )

    def _merge_threshold(self) -> float:
        return float(self._profile_params.get("merge_threshold"))

    def _min_merge_sessions(self) -> int:
        return int(self._profile_params.get("min_merge_sessions"))

    def _advance_merge_chain(self, profile: Profile) -> None:
        if self._active_cluster is None:
            self._active_cluster = _MergeCluster.from_profile(profile)
            return
        if self._active_cluster.overlap_with(profile) < self._merge_threshold():
            self._finalize_active_cluster()
            self._active_cluster = _MergeCluster.from_profile(profile)
            return
        self._active_cluster.extend(profile)

    def _finalize_active_cluster(self) -> None:
        if self._active_cluster is None:
            return
        if int(self._active_cluster.profile_count) >= self._min_merge_sessions():
            self._closed_merged_profiles.append(self._active_cluster.to_profile())
        self._active_cluster = None

    def _active_effective_profile(self) -> Optional[Profile]:
        if self._active_cluster is None:
            return None
        if int(self._active_cluster.profile_count) < self._min_merge_sessions():
            return None
        return self._active_cluster.to_profile()

    def _log_resolution(self, *, mode: str, started: float) -> None:
        merge_threshold = self._profile_params.get("merge_threshold") if mode == "merged" else None
        min_merge_sessions = self._profile_params.get("min_merge_sessions") if mode == "merged" else None
        if not _should_log_resolution(
            mode=mode,
            symbol=self._symbol,
            merge_threshold=merge_threshold,
            min_merge_sessions=min_merge_sessions,
            known_profiles=self._summary["known_profiles"],
            merged_profiles=self._summary["merged_profiles"],
        ):
            return
        if mode == "merged":
            logger.debug(
                "event=runtime_profile_resolution mode=merged known_profiles=%s merged_profiles=%s merge_threshold=%s min_merge_sessions=%s duration_ms=%.3f",
                self._summary["known_profiles"],
                self._summary["merged_profiles"],
                merge_threshold,
                min_merge_sessions,
                (perf_counter() - started) * 1000.0,
            )
            return
        logger.debug(
            "event=runtime_profile_resolution mode=%s known_profiles=%s merged_profiles=%s duration_ms=%.3f",
            mode,
            self._summary["known_profiles"],
            self._summary["merged_profiles"],
            (perf_counter() - started) * 1000.0,
        )


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

    entries = _parse_runtime_profile_entries(profiles_payload)
    known_profiles = [
        entry.profile
        for entry in entries
        if int(entry.known_at_epoch) <= int(current_epoch)
    ]

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


def _parse_runtime_profile_entries(profiles_payload: Sequence[Any]) -> List[RuntimeProfileEntry]:
    parsed: List[RuntimeProfileEntry] = []
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
        parsed.append(
            RuntimeProfileEntry(
                profile=profile,
                known_at_epoch=int(known_epoch),
            )
        )
    parsed.sort(
        key=lambda item: (
            int(item.known_at_epoch),
            int(item.profile.start.timestamp()),
            int(item.profile.end.timestamp()),
        )
    )
    return parsed


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
