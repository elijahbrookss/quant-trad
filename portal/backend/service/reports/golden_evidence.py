"""Read-only golden comparison evidence adapter for report comparison."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from utils.log_context import build_log_context, with_log_context

from .schemas import FirstDivergenceDTO, GoldenEvidenceDTO


logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_GOLDEN_ROOT = REPO_ROOT / "logs" / "reports"
GOLDEN_COMPARISON_PATTERNS = ("comparison_summary*.json",)


def read_golden_comparison_evidence(
    left_run_id: str,
    right_run_id: str,
    *,
    search_roots: Optional[Sequence[Path | str]] = None,
) -> GoldenEvidenceDTO:
    """Return normalized evidence from the latest existing golden artifact for a run pair.

    This reader never builds reports and never invokes golden comparison logic.
    It only normalizes already-written `comparison_summary*.json` artifacts.
    """

    match = _find_latest_artifact(left_run_id, right_run_id, search_roots=search_roots)
    if match is None:
        return _missing_evidence()
    path, payload = match
    return _normalize_artifact(left_run_id, right_run_id, path, payload)


def _missing_evidence(status: str = "not_available") -> GoldenEvidenceDTO:
    return GoldenEvidenceDTO(
        available=False,
        status=status,
        first_divergence=FirstDivergenceDTO(
            present=False,
            divergence_type=status,
            explanation="Golden evidence not available.",
            source="golden",
        ),
    )


def _find_latest_artifact(
    left_run_id: str,
    right_run_id: str,
    *,
    search_roots: Optional[Sequence[Path | str]] = None,
) -> Optional[tuple[Path, Mapping[str, Any]]]:
    candidates: list[tuple[int, str, Path, Mapping[str, Any]]] = []
    expected = {left_run_id, right_run_id}
    for path in _comparison_artifact_paths(search_roots):
        try:
            payload = json.loads(path.read_text())
        except Exception as exc:  # noqa: BLE001 - artifact scan should skip corrupt unrelated files.
            logger.warning(
                with_log_context(
                    "golden_comparison_artifact_read_failed",
                    build_log_context(path=str(path), error=str(exc)),
                )
            )
            continue
        if _artifact_run_ids(payload) != expected:
            continue
        try:
            mtime_ns = path.stat().st_mtime_ns
        except OSError:
            mtime_ns = 0
        candidates.append((mtime_ns, str(path), path, payload))
    if not candidates:
        return None
    _, _, path, payload = max(candidates, key=lambda row: (row[0], row[1]))
    return path, payload


def _comparison_artifact_paths(search_roots: Optional[Sequence[Path | str]]) -> Iterable[Path]:
    roots = [Path(root) for root in search_roots] if search_roots else [DEFAULT_GOLDEN_ROOT]
    seen: set[Path] = set()
    for root in roots:
        if root.is_file():
            paths = [root]
        elif root.exists():
            paths = []
            for pattern in GOLDEN_COMPARISON_PATTERNS:
                paths.extend(root.rglob(pattern))
        else:
            paths = []
        for path in paths:
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            yield path


def _artifact_run_ids(payload: Mapping[str, Any]) -> set[str]:
    run_ids = payload.get("run_ids")
    if isinstance(run_ids, Sequence) and not isinstance(run_ids, (str, bytes, bytearray)):
        return {str(run_id) for run_id in run_ids if str(run_id).strip()}
    artifacts = payload.get("artifacts")
    if isinstance(artifacts, Mapping):
        return {str(key) for key in artifacts if key != "comparison" and str(key).strip()}
    return set()


def _normalize_artifact(
    left_run_id: str,
    right_run_id: str,
    path: Path,
    payload: Mapping[str, Any],
) -> GoldenEvidenceDTO:
    artifact_run_ids = [str(run_id) for run_id in payload.get("run_ids") or []]
    reversed_order = len(artifact_run_ids) >= 2 and artifact_run_ids[0] == right_run_id and artifact_run_ids[1] == left_run_id
    decision = _mapping(payload.get("decision_compare"))
    trade = _mapping(payload.get("trade_lifecycle_compare"))
    material = _mapping(payload.get("material"))
    material_diff = _mapping(payload.get("material_diff"))
    runtime = _mapping(payload.get("runtime_ordering"))
    wallet_trace = _mapping(payload.get("wallet_trace"))
    wallet_market_time = _mapping(payload.get("wallet_market_time_ordering"))
    generated_at = _mtime_iso(path)

    left_material = _mapping(material.get(left_run_id))
    right_material = _mapping(material.get(right_run_id))
    missing_ids, extra_ids = _decision_id_lists(decision, reversed_order=reversed_order)
    verdict_changes = _verdict_changes(decision, reversed_order=reversed_order)
    first_divergence = _first_divergence(payload, reversed_order=reversed_order)
    return GoldenEvidenceDTO(
        available=True,
        status="available",
        artifact_path=_display_path(path),
        generated_at=generated_at,
        verdict=str(payload.get("verdict") or "unknown"),
        fail_reasons=[str(reason) for reason in payload.get("fail_reasons") or []],
        semantic_fingerprint_match=_field_match(left_material, right_material, material_diff, "report_semantic_fingerprint"),
        operational_fingerprint_match=_field_match(left_material, right_material, material_diff, "report_operational_fingerprint"),
        data_snapshot_hash_match=_field_match(left_material, right_material, material_diff, "data_snapshot_hash"),
        material_config_hash_match=_field_match(left_material, right_material, material_diff, "material_config_hash"),
        strategy_hash_match=_field_match(left_material, right_material, material_diff, "strategy_hash"),
        decision_count_left=_side_value(decision, "left_count", "right_count", reversed_order),
        decision_count_right=_side_value(decision, "right_count", "left_count", reversed_order),
        missing_decision_count=_side_count(decision, ("missing_decision_count", "missing_ids_count"), ("extra_decision_count", "extra_ids_count"), reversed_order),
        extra_decision_count=_side_count(decision, ("extra_decision_count", "extra_ids_count"), ("missing_decision_count", "missing_ids_count"), reversed_order),
        missing_decision_ids=missing_ids,
        extra_decision_ids=extra_ids,
        decision_diff_full_lists_available=_decision_full_lists_available(decision),
        verdict_change_count=_int_or_none(decision.get("verdict_change_count")),
        verdict_changes=verdict_changes,
        verdict_changes_full_available=_sequence_key_present(decision, "verdict_changes"),
        trade_lifecycle_equal=_bool_or_none(trade.get("equal")),
        trade_count_left=_side_value(trade, "left_count", "right_count", reversed_order),
        trade_count_right=_side_value(trade, "right_count", "left_count", reversed_order),
        wallet_trace_missing_left=_int_or_none(_mapping(wallet_trace.get(left_run_id)).get("missing_wallet_trace_count")),
        wallet_trace_missing_right=_int_or_none(_mapping(wallet_trace.get(right_run_id)).get("missing_wallet_trace_count")),
        wallet_market_time_overtake_left=_overtake_count(_mapping(wallet_market_time.get(left_run_id))),
        wallet_market_time_overtake_right=_overtake_count(_mapping(wallet_market_time.get(right_run_id))),
        runtime_ordering_left=_mapping(runtime.get(left_run_id)),
        runtime_ordering_right=_mapping(runtime.get(right_run_id)),
        first_divergence=first_divergence,
        raw={"artifact_run_ids": artifact_run_ids},
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _mtime_iso(path: Path) -> Optional[str]:
    try:
        timestamp = path.stat().st_mtime
    except OSError:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _field_match(
    left_material: Mapping[str, Any],
    right_material: Mapping[str, Any],
    material_diff: Mapping[str, Any],
    field: str,
) -> Optional[bool]:
    left = left_material.get(field)
    right = right_material.get(field)
    if left not in (None, "") or right not in (None, ""):
        return left == right
    if field in material_diff:
        return False
    return None


def _side_value(payload: Mapping[str, Any], normal_key: str, reversed_key: str, reversed_order: bool) -> Optional[int]:
    key = reversed_key if reversed_order else normal_key
    return _int_or_none(payload.get(key))


def _side_count(payload: Mapping[str, Any], normal_keys: Sequence[str], reversed_keys: Sequence[str], reversed_order: bool) -> Optional[int]:
    keys = reversed_keys if reversed_order else normal_keys
    for key in keys:
        if key in payload:
            return _int_or_none(payload.get(key))
    return None


def _int_or_none(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    return None


def _list_of_strings(value: Any) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value if str(item).strip()]
    if value not in (None, ""):
        return [str(value)]
    return []


def _decision_id_lists(decision: Mapping[str, Any], *, reversed_order: bool) -> tuple[list[str], list[str]]:
    missing = _list_of_strings(_first_present(decision, ("missing_decision_ids", "missing_ids")))
    extra = _list_of_strings(_first_present(decision, ("extra_decision_ids", "extra_ids")))
    if not missing and decision.get("first_missing_id"):
        missing = [str(decision.get("first_missing_id"))]
    if not extra and decision.get("first_extra_id"):
        extra = [str(decision.get("first_extra_id"))]
    return (extra, missing) if reversed_order else (missing, extra)


def _first_present(payload: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in payload:
            return payload.get(key)
    return None


def _sequence_key_present(payload: Mapping[str, Any], key: str) -> bool:
    value = payload.get(key)
    return key in payload and isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _decision_full_lists_available(decision: Mapping[str, Any]) -> bool:
    return (
        _sequence_key_present(decision, "missing_decision_ids")
        and _sequence_key_present(decision, "extra_decision_ids")
    ) or (
        _sequence_key_present(decision, "missing_ids")
        and _sequence_key_present(decision, "extra_ids")
    )


def _verdict_changes(decision: Mapping[str, Any], *, reversed_order: bool) -> list[dict[str, Any]]:
    changes = decision.get("verdict_changes")
    if isinstance(changes, Sequence) and not isinstance(changes, (str, bytes, bytearray)):
        normalized = [_maybe_swap_change(_mapping(change), reversed_order=reversed_order) for change in changes if isinstance(change, Mapping)]
    else:
        normalized = []
    first = _mapping(decision.get("first_verdict_change"))
    if first and not normalized:
        normalized = [_maybe_swap_change(first, reversed_order=reversed_order)]
    return normalized


def _maybe_swap_change(change: Mapping[str, Any], *, reversed_order: bool) -> dict[str, Any]:
    row = dict(change)
    if reversed_order and "left" in row and "right" in row:
        row["left"], row["right"] = row.get("right"), row.get("left")
    if reversed_order:
        for left_key, right_key in (
            ("left_verdict", "right_verdict"),
            ("left_reason", "right_reason"),
            ("left_action", "right_action"),
            ("left_accepted", "right_accepted"),
            ("left_wallet_snapshot", "right_wallet_snapshot"),
        ):
            if left_key in row or right_key in row:
                row[left_key], row[right_key] = row.get(right_key), row.get(left_key)
    return row


def _overtake_count(payload: Mapping[str, Any]) -> Optional[int]:
    if "overtake_count" in payload:
        return _int_or_none(payload.get("overtake_count"))
    if "first_overtake" in payload:
        return 1 if payload.get("first_overtake") else 0
    return None


def _first_divergence(payload: Mapping[str, Any], *, reversed_order: bool) -> FirstDivergenceDTO:
    if _semantic_clean(payload):
        return FirstDivergenceDTO(
            present=False,
            divergence_type="none",
            explanation="No semantic divergence detected by golden evidence.",
            source="golden",
        )

    raw = _mapping(payload.get("first_divergence"))
    if not raw:
        return FirstDivergenceDTO(
            present=False,
            divergence_type="not_computed",
            explanation="Golden evidence did not include a first divergence.",
            source="golden",
        )
    left_value = raw.get("right") if reversed_order else raw.get("left")
    right_value = raw.get("left") if reversed_order else raw.get("right")
    section = str(raw.get("section") or "unknown")
    field = raw.get("field")
    left_row = _mapping(left_value)
    right_row = _mapping(right_value)
    row = left_row or right_row
    field_path = f"{section}.{field}" if field else section
    return FirstDivergenceDTO(
        present=True,
        divergence_type=_divergence_type(section, field),
        symbol=row.get("symbol"),
        timeframe=row.get("timeframe"),
        bar_time=row.get("bar_time") or row.get("entry_time"),
        decision_id=row.get("decision_id"),
        trade_id=row.get("trade_id"),
        field_path=field_path,
        left_value=left_value,
        right_value=right_value,
        explanation=f"Golden comparison first divergence in {field_path}.",
        source="golden",
    )


def _semantic_clean(payload: Mapping[str, Any]) -> bool:
    decision = _mapping(payload.get("decision_compare"))
    trade = _mapping(payload.get("trade_lifecycle_compare"))
    material_diff = _mapping(payload.get("material_diff"))
    semantic_material_fields = {"material_config_hash", "data_snapshot_hash", "strategy_hash", "report_semantic_fingerprint"}
    return (
        str(payload.get("verdict") or "").upper() == "PASS"
        or (
            not any(field in material_diff for field in semantic_material_fields)
            and not _count_any(decision, ("missing_decision_count", "missing_ids_count"))
            and not _count_any(decision, ("extra_decision_count", "extra_ids_count"))
            and not _int_or_none(decision.get("verdict_change_count"))
            and trade.get("equal") is not False
        )
    )


def _count_any(payload: Mapping[str, Any], keys: Sequence[str]) -> Optional[int]:
    for key in keys:
        count = _int_or_none(payload.get(key))
        if count is not None:
            return count
    return None


def _divergence_type(section: str, field: Any) -> str:
    if section == "decisions":
        return "decision_divergence"
    if section == "trade_lifecycle":
        return "trade_lifecycle_divergence"
    if section == "material":
        return f"material_{field or 'divergence'}"
    if section == "summary_metrics":
        return f"metric_{field or 'divergence'}"
    return f"{section}_divergence"


__all__ = ["read_golden_comparison_evidence"]
