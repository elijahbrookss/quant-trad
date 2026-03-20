from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Tuple


INDICATOR_META_KEY = "__qt_indicator_meta__"


def split_indicator_payload(raw_params: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    params = dict(raw_params or {}) if isinstance(raw_params, dict) else {}
    raw_meta = params.pop(INDICATOR_META_KEY, None)
    dependencies: List[Dict[str, Any]] = []
    if isinstance(raw_meta, dict):
        raw_dependencies = raw_meta.get("dependencies")
        if isinstance(raw_dependencies, list):
            dependencies = [dict(item) for item in raw_dependencies if isinstance(item, dict)]
    return params, dependencies


def merge_indicator_payload(
    params: Mapping[str, Any] | None,
    dependencies: Sequence[Mapping[str, Any]] | None,
) -> Dict[str, Any]:
    stored = dict(params or {})
    normalized_dependencies = [dict(item) for item in (dependencies or []) if isinstance(item, Mapping)]
    if normalized_dependencies:
        stored[INDICATOR_META_KEY] = {
            "dependencies": normalized_dependencies,
        }
    else:
        stored.pop(INDICATOR_META_KEY, None)
    return stored


__all__ = [
    "INDICATOR_META_KEY",
    "merge_indicator_payload",
    "split_indicator_payload",
]
