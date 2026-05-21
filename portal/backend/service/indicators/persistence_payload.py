from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Tuple


INDICATOR_META_KEY = "__qt_indicator_meta__"
_UNSET = object()


def split_indicator_payload(
    raw_params: Any,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Dict[str, Any]] | None]:
    params = dict(raw_params or {}) if isinstance(raw_params, dict) else {}
    raw_meta = params.pop(INDICATOR_META_KEY, None)
    dependencies: List[Dict[str, Any]] = []
    output_prefs: Dict[str, Dict[str, Any]] | None = None
    if isinstance(raw_meta, dict):
        raw_dependencies = raw_meta.get("dependencies")
        if isinstance(raw_dependencies, list):
            dependencies = [dict(item) for item in raw_dependencies if isinstance(item, dict)]
        if "output_prefs" in raw_meta:
            raw_output_prefs = raw_meta.get("output_prefs")
            output_prefs = {}
            if isinstance(raw_output_prefs, Mapping):
                for output_name, prefs in raw_output_prefs.items():
                    normalized_output_name = str(output_name).strip()
                    if not normalized_output_name or not isinstance(prefs, Mapping):
                        continue
                    output_prefs[normalized_output_name] = dict(prefs)
    return params, dependencies, output_prefs


def merge_indicator_payload(
    params: Mapping[str, Any] | None,
    dependencies: Sequence[Mapping[str, Any]] | None,
    *,
    output_prefs: Mapping[str, Mapping[str, Any]] | object = _UNSET,
) -> Dict[str, Any]:
    stored = dict(params or {})
    normalized_dependencies = [dict(item) for item in (dependencies or []) if isinstance(item, Mapping)]
    meta: Dict[str, Any] = {}
    if normalized_dependencies:
        meta["dependencies"] = normalized_dependencies
    if output_prefs is not _UNSET:
        normalized_output_prefs: Dict[str, Dict[str, Any]] = {}
        if isinstance(output_prefs, Mapping):
            for output_name, prefs in output_prefs.items():
                normalized_output_name = str(output_name).strip()
                if not normalized_output_name or not isinstance(prefs, Mapping):
                    continue
                normalized_output_prefs[normalized_output_name] = dict(prefs)
        meta["output_prefs"] = normalized_output_prefs
    if meta:
        stored[INDICATOR_META_KEY] = meta
    else:
        stored.pop(INDICATOR_META_KEY, None)
    return stored


__all__ = [
    "INDICATOR_META_KEY",
    "merge_indicator_payload",
    "split_indicator_payload",
]
