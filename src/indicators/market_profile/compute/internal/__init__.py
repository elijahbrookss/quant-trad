"""
Internal implementation details for Market Profile indicator.

This package contains helper functions and utilities that are not part of the public API.
Users creating new indicators don't need to understand these internals.
"""

from .computation import build_tpo_histogram, extract_value_area
from .bin_size import infer_bin_size, select_bin_size, infer_precision_from_step
from .merging import merge_profiles, calculate_overlap
from .runtime_profiles import resolve_effective_profiles

__all__ = [
    # Computation
    "build_tpo_histogram",
    "extract_value_area",
    # Bin size
    "infer_bin_size",
    "select_bin_size",
    "infer_precision_from_step",
    # Merging
    "merge_profiles",
    "calculate_overlap",
    "resolve_effective_profiles",
]
