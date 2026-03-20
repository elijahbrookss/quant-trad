"""Lazy internal exports for Market Profile helpers."""

from __future__ import annotations

from typing import Any

__all__ = [
    "build_tpo_histogram",
    "extract_value_area",
    "infer_bin_size",
    "select_bin_size",
    "infer_precision_from_step",
    "merge_profiles",
    "calculate_overlap",
    "resolve_effective_profiles",
]


def __getattr__(name: str) -> Any:
    if name in {"build_tpo_histogram", "extract_value_area"}:
        from .computation import build_tpo_histogram, extract_value_area

        exports = {
            "build_tpo_histogram": build_tpo_histogram,
            "extract_value_area": extract_value_area,
        }
        return exports[name]
    if name in {"infer_bin_size", "select_bin_size", "infer_precision_from_step"}:
        from .bin_size import infer_bin_size, infer_precision_from_step, select_bin_size

        exports = {
            "infer_bin_size": infer_bin_size,
            "select_bin_size": select_bin_size,
            "infer_precision_from_step": infer_precision_from_step,
        }
        return exports[name]
    if name in {"merge_profiles", "calculate_overlap"}:
        from .merging import calculate_overlap, merge_profiles

        exports = {
            "merge_profiles": merge_profiles,
            "calculate_overlap": calculate_overlap,
        }
        return exports[name]
    if name == "resolve_effective_profiles":
        from .runtime_profiles import resolve_effective_profiles

        return resolve_effective_profiles
    raise AttributeError(name)
