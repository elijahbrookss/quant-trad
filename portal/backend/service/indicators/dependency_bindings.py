from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional

from indicators.manifest import IndicatorManifest


def normalize_dependency_bindings(bindings: Any) -> List[Dict[str, str]]:
    if bindings in (None, ""):
        return []
    if not isinstance(bindings, Sequence) or isinstance(bindings, (str, bytes)):
        raise ValueError("indicator_dependencies_invalid: bindings must be a list")
    normalized: List[Dict[str, str]] = []
    for index, item in enumerate(bindings):
        if not isinstance(item, Mapping):
            raise ValueError(
                f"indicator_dependencies_invalid: binding[{index}] must be an object"
            )
        indicator_id = str(item.get("indicator_id") or "").strip()
        output_name = str(item.get("output_name") or "").strip()
        indicator_type = str(item.get("indicator_type") or "").strip()
        if not indicator_id:
            raise ValueError(
                f"indicator_dependencies_invalid: binding[{index}].indicator_id required"
            )
        if not output_name:
            raise ValueError(
                f"indicator_dependencies_invalid: binding[{index}].output_name required"
            )
        normalized.append(
            {
                "indicator_id": indicator_id,
                "output_name": output_name,
                "indicator_type": indicator_type,
            }
        )
    return normalized


def validate_dependency_bindings(
    *,
    manifest: IndicatorManifest,
    bindings: Any,
    ctx: Any,
    indicator_id: Optional[str] = None,
) -> List[Dict[str, str]]:
    expected = list(manifest.dependencies)
    normalized = normalize_dependency_bindings(bindings)
    if not expected:
        if normalized:
            raise ValueError(
                f"{manifest.type} indicator does not accept dependency bindings"
            )
        return []

    bindings_by_output: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for binding in normalized:
        bindings_by_output[str(binding["output_name"])].append(binding)

    resolved: List[Dict[str, str]] = []
    for dependency in expected:
        candidates = bindings_by_output.get(str(dependency.output_name), [])
        if not candidates:
            raise ValueError(
                f"{manifest.type} indicator missing explicit dependency binding for "
                f"{dependency.indicator_type}.{dependency.output_name}"
            )
        if len(candidates) > 1:
            raise ValueError(
                f"{manifest.type} indicator has multiple bindings for "
                f"{dependency.indicator_type}.{dependency.output_name}"
            )
        binding = candidates[0]
        target_indicator_id = str(binding.get("indicator_id") or "").strip()
        if indicator_id and target_indicator_id == str(indicator_id).strip():
            raise ValueError(
                f"{manifest.type} indicator cannot depend on itself: {target_indicator_id}"
            )
        record = ctx.repository.get(target_indicator_id)
        if not record:
            raise ValueError(
                f"{manifest.type} indicator dependency not found: {target_indicator_id}"
            )
        target_type = str(record.get("type") or "").strip()
        if target_type != str(dependency.indicator_type or "").strip():
            raise ValueError(
                f"{manifest.type} indicator dependency type mismatch for "
                f"{dependency.output_name}: expected {dependency.indicator_type}, got {target_type or 'unknown'}"
            )
        bound_type = str(binding.get("indicator_type") or "").strip()
        if bound_type and bound_type != target_type:
            raise ValueError(
                f"{manifest.type} indicator dependency binding mismatch for "
                f"{dependency.output_name}: binding says {bound_type}, record is {target_type}"
            )
        resolved.append(
            {
                "indicator_id": target_indicator_id,
                "indicator_type": target_type,
                "output_name": str(dependency.output_name),
            }
        )

    matched_keys = {
        (item["indicator_id"], item["output_name"])
        for item in resolved
    }
    extras = [
        item for item in normalized
        if (item["indicator_id"], item["output_name"]) not in matched_keys
    ]
    if extras:
        raise ValueError(
            f"{manifest.type} indicator received unexpected dependency bindings: {extras}"
        )

    return resolved


def find_indicator_dependents(
    *,
    indicator_id: str,
    ctx: Any,
    excluding_ids: Sequence[str] | None = None,
) -> List[Dict[str, str]]:
    target_id = str(indicator_id or "").strip()
    if not target_id:
        return []
    excluded = {str(item).strip() for item in (excluding_ids or []) if str(item).strip()}
    dependents: List[Dict[str, str]] = []
    for record in list(ctx.repository.load() or []):
        dependent_id = str(record.get("id") or "").strip()
        if not dependent_id or dependent_id == target_id or dependent_id in excluded:
            continue
        for binding in normalize_dependency_bindings(record.get("dependencies")):
            if str(binding.get("indicator_id") or "").strip() != target_id:
                continue
            dependents.append(
                {
                    "indicator_id": dependent_id,
                    "name": str(record.get("name") or dependent_id),
                    "type": str(record.get("type") or ""),
                    "output_name": str(binding.get("output_name") or ""),
                }
            )
    dependents.sort(key=lambda item: (item["type"], item["name"], item["indicator_id"]))
    return dependents


def assert_indicator_delete_allowed(
    *,
    indicator_id: str,
    ctx: Any,
    deleting_ids: Sequence[str] | None = None,
) -> None:
    dependents = find_indicator_dependents(
        indicator_id=indicator_id,
        ctx=ctx,
        excluding_ids=deleting_ids,
    )
    if not dependents:
        return
    summary = ", ".join(
        f"{item['name']} ({item['indicator_id']})"
        for item in dependents
    )
    raise RuntimeError(
        "indicator_delete_blocked: dependent indicators still reference "
        f"{indicator_id}: {summary}"
    )


__all__ = [
    "assert_indicator_delete_allowed",
    "find_indicator_dependents",
    "normalize_dependency_bindings",
    "validate_dependency_bindings",
]
