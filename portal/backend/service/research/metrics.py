"""Generic metric extraction and ranking for research evidence."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from numbers import Real
from typing import Any


_SKIPPED_RESULT_KEYS = {"detector", "events"}


def extract_numeric_metrics(result: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Flatten comparable numeric values from a research check result.

    This is intentionally generic. It does not know check families, indicators,
    or signal names; it only exposes numeric fields emitted by the result
    contract.
    """

    metrics: list[dict[str, Any]] = []
    for key, value in result.items():
        if str(key) in _SKIPPED_RESULT_KEYS:
            continue
        _collect_numeric_metrics(value, path=str(key), metrics=metrics)
    return metrics


def metric_value(result: Mapping[str, Any], path: str) -> float | None:
    found, value = _lookup_path(result, path)
    if not found:
        return None
    return _numeric_value(value)


def build_leaderboard(
    evaluations: Sequence[Mapping[str, Any]],
    *,
    rank_by: str,
    rank_direction: str,
    display_metrics: Sequence[str] | None = None,
) -> dict[str, Any]:
    rank_path = str(rank_by or "").strip()
    if not rank_path:
        raise ValueError("ranking.rank_by is required")
    direction = str(rank_direction or "").strip().lower()
    if direction not in {"asc", "desc"}:
        raise ValueError("ranking.direction must be 'asc' or 'desc'")
    display_paths = [str(item).strip() for item in (display_metrics or []) if str(item).strip()]

    ranked: list[dict[str, Any]] = []
    unranked: list[dict[str, Any]] = []
    for evaluation in evaluations:
        result = _mapping(evaluation.get("result"), "evaluation.result")
        variant = _mapping(evaluation.get("variant"), "evaluation.variant")
        scope = _mapping(evaluation.get("scope"), "evaluation.scope")
        status = str(result.get("status") or "")
        if status != "completed":
            unranked.append(_unranked_row(evaluation, reason=f"status={status or '<empty>'}"))
            continue
        value = metric_value(result, rank_path)
        if value is None:
            raise ValueError(
                "rank metric missing from completed check result: "
                f"variant_id={variant.get('id')} scope_id={scope.get('id')} rank_by={rank_path}"
            )
        displayed = []
        for metric_path in display_paths:
            metric_value_found = metric_value(result, metric_path)
            if metric_value_found is None:
                raise ValueError(
                    "display metric missing from completed check result: "
                    f"variant_id={variant.get('id')} scope_id={scope.get('id')} metric={metric_path}"
                )
            displayed.append({"path": metric_path, "value": metric_value_found})
        ranked.append(
            {
                "variant_id": variant.get("id"),
                "variant_label": variant.get("label"),
                "scope_id": scope.get("id"),
                "status": status,
                "recommendation": result.get("recommendation"),
                "sample_count": result.get("sample_count"),
                "rank_metric": {"path": rank_path, "value": value},
                "display_metrics": displayed,
                "caveat_count": len(result.get("caveats") or []),
            }
        )

    ranked.sort(key=lambda row: float(row["rank_metric"]["value"]), reverse=direction == "desc")
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    return {
        "schema_version": "research_metric_leaderboard.v1",
        "rank_by": rank_path,
        "direction": direction,
        "display_metrics": display_paths,
        "rows": ranked,
        "unranked": unranked,
    }


def _collect_numeric_metrics(value: Any, *, path: str, metrics: list[dict[str, Any]]) -> None:
    number = _numeric_value(value)
    if number is not None:
        metrics.append({"path": path, "name": path.rsplit(".", 1)[-1], "value": number})
        return
    if isinstance(value, Mapping):
        for key, nested in value.items():
            _collect_numeric_metrics(nested, path=f"{path}.{key}", metrics=metrics)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for index, nested in enumerate(value):
            _collect_numeric_metrics(nested, path=f"{path}.{index}", metrics=metrics)


def _lookup_path(payload: Mapping[str, Any], path: str) -> tuple[bool, Any]:
    current: Any = payload
    for part in [item for item in str(path or "").split(".") if item]:
        if isinstance(current, Mapping):
            if part not in current:
                return False, None
            current = current[part]
            continue
        if isinstance(current, Sequence) and not isinstance(current, (str, bytes)):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return False, None
            continue
        return False, None
    return True, current


def _numeric_value(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, Real):
        return None
    number = float(value)
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _unranked_row(evaluation: Mapping[str, Any], *, reason: str) -> dict[str, Any]:
    variant = _mapping(evaluation.get("variant"), "evaluation.variant")
    scope = _mapping(evaluation.get("scope"), "evaluation.scope")
    result = _mapping(evaluation.get("result"), "evaluation.result")
    return {
        "variant_id": variant.get("id"),
        "variant_label": variant.get("label"),
        "scope_id": scope.get("id"),
        "status": result.get("status"),
        "reason": reason,
    }
