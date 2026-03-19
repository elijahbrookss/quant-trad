"""Shared market profile parameter defaults."""

DEFAULT_MIN_MERGE_SESSIONS = 3
DEFAULT_DAYS_BACK = 180
DEFAULT_PARAMS = {
    "bin_size": 0.25,
    "price_precision": 4,
    "use_merged_value_areas": True,
    "merge_threshold": 0.6,
    "min_merge_sessions": DEFAULT_MIN_MERGE_SESSIONS,
    "extend_value_area_to_chart_end": True,
    "days_back": DEFAULT_DAYS_BACK,
}

__all__ = [
    "DEFAULT_DAYS_BACK",
    "DEFAULT_MIN_MERGE_SESSIONS",
    "DEFAULT_PARAMS",
]
