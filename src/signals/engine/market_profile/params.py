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
    """Resolve merge parameters from indicator + optional overrides.

    IMPORTANT: No silent defaults. All params must be present on indicator instance.
    If params are missing, this will raise ValueError (fail-fast).
    """

    # Resolve use_merged_value_areas
    if use_merged_value_areas is not None:
        resolved_use_merged = bool(use_merged_value_areas)
    else:
        # Read from indicator - FAIL if missing
        if not hasattr(indicator, "use_merged_value_areas"):
            raise ValueError(
                "Market Profile indicator missing 'use_merged_value_areas' attribute. "
                "This should have been populated during creation. "
                "Indicator may need to be recreated."
            )
        resolved_use_merged = indicator.use_merged_value_areas

    # Resolve merge_threshold
    if merge_threshold is not None:
        try:
            resolved_threshold = float(merge_threshold)
        except (TypeError, ValueError):
            if not hasattr(indicator, "merge_threshold"):
                raise ValueError(
                    "Market Profile indicator missing 'merge_threshold' attribute. "
                    "Indicator may need to be recreated."
                )
            resolved_threshold = indicator.merge_threshold
    else:
        if not hasattr(indicator, "merge_threshold"):
            raise ValueError(
                "Market Profile indicator missing 'merge_threshold' attribute. "
                "Indicator may need to be recreated."
            )
        resolved_threshold = indicator.merge_threshold

    # Resolve min_merge_sessions
    if min_merge_sessions is not None:
        try:
            resolved_min_sessions = int(min_merge_sessions)
        except (TypeError, ValueError):
            if not hasattr(indicator, "min_merge_sessions"):
                raise ValueError(
                    "Market Profile indicator missing 'min_merge_sessions' attribute. "
                    "Indicator may need to be recreated."
                )
            resolved_min_sessions = indicator.min_merge_sessions
    else:
        if not hasattr(indicator, "min_merge_sessions"):
            raise ValueError(
                "Market Profile indicator missing 'min_merge_sessions' attribute. "
                "Indicator may need to be recreated."
            )
        resolved_min_sessions = indicator.min_merge_sessions

    return MarketProfileParams(
        use_merged_value_areas=resolved_use_merged,
        merge_threshold=resolved_threshold,
        min_merge_sessions=resolved_min_sessions,
    )


__all__ = ["MarketProfileParams", "resolve_market_profile_params"]
