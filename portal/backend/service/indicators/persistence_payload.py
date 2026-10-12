from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Tuple


INDICATOR_META_KEY = "__qt_indicator_meta__"
_UNSET = object()


def split_indicator_payload(
    raw_params: Any,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
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
    *, version: str = "v1",
) -> Dict[str, Any]:
    stored = dict(params or {})
    normalized_dependencies = [dict(item) for item in (dependencies or []) if isinstance(item, Mapping)]
    meta: Dict[str, Any] = {}
    if version != "v1":
        meta["runtime_version"] = version
    if normalized_dependencies:
        meta["dependencies"] = normalized_dependencies
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


def indicator_payload_version(raw_params: Any) -> str:
    meta = (raw_params or {}).get(INDICATOR_META_KEY, {})
    version = meta.get("runtime_version", "v1") if isinstance(meta, Mapping) else "v1"
    if not isinstance(version, str) or not version:
        raise ValueError("indicator_version_invalid: persisted runtime version must be a nonempty string")
    return version
