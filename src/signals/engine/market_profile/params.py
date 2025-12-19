from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from indicators.market_profile import MarketProfileIndicator


@dataclass(frozen=True)
class MarketProfileParams:
    """Resolved Market Profile merge parameters."""

    use_merged_value_areas: bool
    merge_threshold: float
    min_merge_sessions: int

    def signature(self, *, va_source: str = "stored_indicator") -> dict[str, Any]:
        return {
            "va_source": va_source,
            "use_merged_value_areas": self.use_merged_value_areas,
            "merge_threshold": self.merge_threshold,
            "min_merge_sessions": self.min_merge_sessions,
        }


def resolve_market_profile_params(
    indicator: Any,
    *,
    use_merged_value_areas: Any = None,
    merge_threshold: Any = None,
    min_merge_sessions: Any = None,
) -> MarketProfileParams:
    """Resolve merge parameters from indicator + optional overrides."""

    resolved_use_merged = (
        getattr(indicator, "use_merged_value_areas", True)
        if use_merged_value_areas is None
        else bool(use_merged_value_areas)
    )

    resolved_threshold = getattr(indicator, "merge_threshold", 0.6)
    if merge_threshold is not None:
        try:
            resolved_threshold = float(merge_threshold)
        except (TypeError, ValueError):
            resolved_threshold = getattr(indicator, "merge_threshold", 0.6)

    default_min_sessions = getattr(
        indicator,
        "min_merge_sessions",
        getattr(MarketProfileIndicator, "DEFAULT_MIN_MERGE_SESSIONS", 3),
    )
    resolved_min_sessions = default_min_sessions
    if min_merge_sessions is not None:
        try:
            resolved_min_sessions = int(min_merge_sessions)
        except (TypeError, ValueError):
            resolved_min_sessions = default_min_sessions

    return MarketProfileParams(
        use_merged_value_areas=resolved_use_merged,
        merge_threshold=resolved_threshold,
        min_merge_sessions=resolved_min_sessions,
    )


__all__ = ["MarketProfileParams", "resolve_market_profile_params"]
