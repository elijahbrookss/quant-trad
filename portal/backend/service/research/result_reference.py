"""Resolve Scientific Attempt evidence only from canonical QT result records."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from market_data.frozen import semantic_hash

from portal.backend.service.provenance import evidence_source_revision
from portal.backend.service.reports import contract as reports_contract
from portal.backend.service.storage.repos import runs as runs_repo

from . import repository as research_repository
from . import service as research_service


_CHECK_ATTEMPT_ADAPTER = "scientific_attempt.event_fact_check.v1"
_BACKTEST_ATTEMPT_ADAPTER = "scientific_attempt.backtest_replay.v1"


def _required(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"canonical_result_reference_invalid: {field} is required")
    return normalized


def _integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"canonical_result_reference_invalid: {field} must be an integer")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"canonical_result_reference_invalid: {field} must be an integer"
        ) from exc
    if normalized < 0:
        raise ValueError(
            f"canonical_result_reference_invalid: {field} must be nonnegative"
        )
    return normalized


def _number(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"canonical_result_reference_invalid: {field} must be numeric")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"canonical_result_reference_invalid: {field} must be numeric"
        ) from exc
    return normalized


def _utc(value: Any, *, field: str) -> datetime:
    raw = _required(value, field=field)
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(
            f"canonical_result_reference_invalid: {field} must be ISO-8601"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _numeric_metrics(*sources: Mapping[str, Any]) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for source in sources:
        for key, value in sorted(source.items()):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            metrics[str(key)] = float(value)
    return metrics


def _finalize_reference(
    evidence: Mapping[str, Any],
    *,
    expected_dataset_binding: Mapping[str, Any] | None,
    expected_trial_inputs: Mapping[str, Any] | None,
    authority_binding: Mapping[str, Any] | None,
) -> dict[str, Any]:
    result = dict(evidence)
    canonical = dict(result.get("canonical_result_reference") or {})
    canonical.pop("reference_hash", None)
    dataset_id = _required(canonical.get("dataset_id"), field="Dataset identity")
    dataset_hash = _required(canonical.get("dataset_hash"), field="Dataset hash")

    if expected_dataset_binding is not None:
        expected_id = _required(
            expected_dataset_binding.get("dataset_id"), field="expected Dataset identity"
        )
        expected_hash = _required(
            expected_dataset_binding.get("dataset_hash"), field="expected Dataset hash"
        )
        if (dataset_id, dataset_hash) != (expected_id, expected_hash):
            raise ValueError(
                "canonical_result_reference_binding_mismatch: result Dataset differs "
                "from the authority-assigned Dataset"
            )

    trial_inputs = dict(expected_trial_inputs or {})
    aliases = {
        "check_id": "item_id",
        "item_id": "item_id",
        "check_family": "check_family",
        "definition_id": "check_family",
        "definition_hash": "definition_hash",
        "request_hash": "request_hash",
        "plan_hash": "plan_hash",
        "result_hash": "result_hash",
        "evidence_hash": "evidence_hash",
        "run_id": "run_id",
        "replay_run_id": "replay_run_id",
        "strategy_id": "strategy_id",
        "strategy_hash": "strategy_hash",
    }
    disagreements: list[str] = []
    for trial_key, reference_key in aliases.items():
        expected = trial_inputs.get(trial_key)
        if expected in (None, ""):
            continue
        if str(canonical.get(reference_key) or "") != str(expected):
            disagreements.append(trial_key)
    if disagreements:
        raise ValueError(
            "canonical_result_reference_binding_mismatch: result differs from "
            "registered trial inputs fields=" + ",".join(sorted(disagreements))
        )

    if authority_binding:
        canonical["authority_binding"] = dict(authority_binding)
    canonical["reference_hash"] = semantic_hash(canonical)
    result["canonical_result_reference"] = canonical
    result["typed_evidence_hash"] = semantic_hash(
        {
            key: value
            for key, value in result.items()
            if key not in {"canonical_result_reference", "typed_evidence_hash"}
        }
        | {"canonical_result_reference": canonical}
    )
    return result


def _check_reference(reference: Mapping[str, Any]) -> dict[str, Any]:
    item_id = _required(
        reference.get("item_id") or reference.get("check_id"), field="item_id"
    )
    if reference.get("evidence_projection") not in (None, ""):
        raise ValueError(
            "canonical_result_reference_invalid: caller-selected evidence projections "
            "are forbidden"
        )
    item = research_repository.get_item(item_id)
    if str(item.get("kind") or "") != "research_check":
        raise ValueError(
            "canonical_result_reference_invalid: referenced item is not a Check"
        )
    payload = item.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError(
            "canonical_result_reference_invalid: Check has no evidence payload"
        )
    definition, request, plan, evidence, result = (
        research_service._validate_research_check_evidence_payload(payload)
    )
    classified = research_service._project_evidence_classification(item)
    if (
        classified.get("evidence_classification") != "frozen_replayable"
        or classified.get("replayable") is not True
        or classified.get("observation_eligible") is not True
        or str(item.get("status") or "") != "tested"
        or str(result.result.get("status") or "") != "completed"
    ):
        raise ValueError(
            "canonical_result_reference_invalid: Check is not completed canonical "
            "frozen evidence"
        )
    if _required(reference.get("result_hash"), field="result_hash") != result.result_hash:
        raise ValueError("canonical_result_reference_hash_mismatch: Check result")
    if _required(reference.get("evidence_hash"), field="evidence_hash") != evidence.evidence_hash:
        raise ValueError("canonical_result_reference_hash_mismatch: Check evidence")

    replay = research_service.replay_research_check(item_id)
    if replay.get("status") != "matched" or replay.get("matches") is not True:
        raise ValueError(
            "canonical_result_reference_invalid: Check has no matched deterministic replay"
        )
    binding = dict(evidence.input_binding)
    dataset_id = _required(binding.get("dataset_id"), field="Check Dataset identity")
    dataset_hash = _required(binding.get("dataset_hash"), field="Check Dataset hash")
    sample_count = _integer(result.result.get("sample_count"), field="result.sample_count")
    calendar_days = _integer(
        result.result.get("distinct_utc_days") or 0,
        field="result.distinct_utc_days",
    )
    statistics = dict(result.result.get("statistics") or {})
    model = dict(statistics.get("model") or {})
    metrics = _numeric_metrics(
        {
            "sample_count": sample_count,
            "analysis_sample_count": result.result.get("analysis_sample_count"),
            "candidate_count": result.result.get("candidate_count"),
            "distinct_utc_days": calendar_days,
        },
        {
            "delta_log_loss": model.get("delta_log_loss"),
            "delta_brier": model.get("delta_brier"),
            "delta_roc_auc": model.get("delta_roc_auc"),
            "valid_fold_count": model.get("valid_fold_count"),
            "oos_count": model.get("oos_count"),
        },
    )
    normalized_reference = {
        "schema_version": "canonical_result_reference.v2",
        "adapter": _CHECK_ATTEMPT_ADAPTER,
        "kind": "check",
        "item_id": item_id,
        "check_family": definition.definition_id,
        "definition_hash": definition.definition_hash,
        "request_hash": request.request_hash,
        "plan_hash": plan.plan_hash,
        "result_hash": result.result_hash,
        "evidence_hash": evidence.evidence_hash,
        "dataset_id": dataset_id,
        "dataset_hash": dataset_hash,
        "binding_hash": binding.get("binding_hash"),
        "code_revision": evidence.code_revision,
        "replay_result_hash": replay.get("replayed_result_hash"),
        "replay_evidence_hash": replay.get("replayed_evidence_hash"),
    }
    return {
        "schema_version": "canonical_scientific_result_evidence.v2",
        "artifact_hash": result.result_hash,
        "reproducible": True,
        "sample_count": sample_count,
        "trade_count": 0,
        "calendar_days": calendar_days,
        "exposure": 0.0,
        "metric_results": metrics,
        "check_analysis_status": result.result.get("analysis_status"),
        "canonical_result_reference": normalized_reference,
    }


def _backtest_report(
    *, run_id: str, expected_fingerprint: str
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    report = dict(reports_contract.get_run_research_dataset(run_id))
    metadata = dict(report.get("metadata") or {})
    readiness = dict(report.get("readiness") or {})
    fingerprint = _required(
        metadata.get("report_semantic_fingerprint"),
        field="report.metadata.report_semantic_fingerprint",
    )
    if expected_fingerprint != fingerprint:
        raise ValueError("canonical_result_reference_hash_mismatch: Backtest report")
    if str(metadata.get("run_type") or "").strip().lower() != "backtest":
        raise ValueError(
            "canonical_result_reference_invalid: referenced run is not a Backtest"
        )
    if str(metadata.get("status") or "").strip().lower() != "completed":
        raise ValueError(
            "canonical_result_reference_invalid: Backtest is not completed"
        )
    if readiness.get("safe_to_compare") is not True:
        raise ValueError(
            "canonical_result_reference_invalid: Backtest report is not comparison-ready"
        )
    for field in ("strategy_id", "strategy_hash", "dataset_id", "dataset_hash"):
        _required(metadata.get(field), field=f"Backtest {field}")
    stored = runs_repo.get_bot_run(run_id)
    if not isinstance(stored, Mapping):
        raise ValueError(
            "canonical_result_reference_invalid: persisted Backtest run is unavailable"
        )
    stored = dict(stored)
    if (
        str(stored.get("run_type") or "").strip().lower() != "backtest"
        or str(stored.get("status") or "").strip().lower() != "completed"
    ):
        raise ValueError(
            "canonical_result_reference_invalid: persisted run is not a completed Backtest"
        )
    _required(stored.get("runtime_source_revision"), field="runtime_source_revision")
    _required(stored.get("runtime_contract_version"), field="runtime_contract_version")
    return report, metadata, stored


def _backtest_reference(reference: Mapping[str, Any]) -> dict[str, Any]:
    if reference.get("evidence_projection") not in (None, ""):
        raise ValueError(
            "canonical_result_reference_invalid: caller-selected evidence projections "
            "are forbidden"
        )
    run_id = _required(reference.get("run_id"), field="run_id")
    replay_run_id = _required(reference.get("replay_run_id"), field="replay_run_id")
    if replay_run_id == run_id:
        raise ValueError(
            "canonical_result_reference_invalid: Backtest replay must be a distinct run"
        )
    fingerprint = _required(
        reference.get("report_semantic_fingerprint"),
        field="report_semantic_fingerprint",
    )
    replay_fingerprint = _required(
        reference.get("replay_report_semantic_fingerprint"),
        field="replay_report_semantic_fingerprint",
    )
    report, metadata, stored = _backtest_report(
        run_id=run_id, expected_fingerprint=fingerprint
    )
    _replay_report, replay_metadata, replay_stored = _backtest_report(
        run_id=replay_run_id, expected_fingerprint=replay_fingerprint
    )
    if fingerprint != replay_fingerprint:
        raise ValueError(
            "canonical_result_reference_invalid: Backtest replay semantic fingerprint differs"
        )
    material_fields = (
        "strategy_id",
        "strategy_hash",
        "dataset_id",
        "dataset_hash",
        "material_config_hash",
        "data_snapshot_hash",
    )
    disagreements = [
        field
        for field in material_fields
        if metadata.get(field) != replay_metadata.get(field)
    ]
    for field in ("runtime_source_revision", "runtime_contract_version"):
        if stored.get(field) != replay_stored.get(field):
            disagreements.append(field)
    if disagreements:
        raise ValueError(
            "canonical_result_reference_invalid: Backtest replay material identity "
            "differs fields=" + ",".join(sorted(disagreements))
        )

    report_builder_revision = evidence_source_revision()
    summary = dict(report.get("summary") or {})
    portfolio = dict(report.get("portfolio_metrics") or {})
    trade_count = _integer(
        summary.get("closed_trades")
        if summary.get("closed_trades") is not None
        else summary.get("total_trades"),
        field="Backtest closed trade count",
    )
    exposure_value = portfolio.get("exposure_pct")
    if exposure_value is None:
        exposure_value = summary.get("exposure_pct")
    exposure = _number(exposure_value, field="Backtest exposure_pct")
    window = dict(metadata.get("simulated_window") or {})
    start = _utc(window.get("start"), field="Backtest simulated window start")
    end = _utc(window.get("end"), field="Backtest simulated window end")
    if end <= start:
        raise ValueError(
            "canonical_result_reference_invalid: Backtest simulated window is invalid"
        )
    calendar_days = max(1, (end.date() - start.date()).days)
    metrics = _numeric_metrics(summary, portfolio)
    normalized_reference = {
        "schema_version": "canonical_result_reference.v2",
        "adapter": _BACKTEST_ATTEMPT_ADAPTER,
        "kind": "backtest",
        "run_id": run_id,
        "replay_run_id": replay_run_id,
        "report_semantic_fingerprint": fingerprint,
        "replay_report_semantic_fingerprint": replay_fingerprint,
        "strategy_id": metadata["strategy_id"],
        "strategy_hash": metadata["strategy_hash"],
        "dataset_id": metadata["dataset_id"],
        "dataset_hash": metadata["dataset_hash"],
        "runtime_source_revision": stored["runtime_source_revision"],
        "runtime_contract_version": stored["runtime_contract_version"],
        "report_builder_source_revision": report_builder_revision,
        "report_schema_version": report.get("schema_version"),
    }
    return {
        "schema_version": "canonical_scientific_result_evidence.v2",
        "artifact_hash": fingerprint,
        "reproducible": True,
        "sample_count": trade_count,
        "trade_count": trade_count,
        "calendar_days": calendar_days,
        "exposure": exposure,
        "execution_quality_class": str(
            dict(report.get("readiness") or {}).get("execution_quality_class") or "X0"
        ).upper(),
        "metric_results": metrics,
        "canonical_result_reference": normalized_reference,
    }


def resolve_canonical_result_reference(
    raw: Any,
    *,
    expected_dataset_binding: Mapping[str, Any] | None = None,
    expected_trial_inputs: Mapping[str, Any] | None = None,
    authority_binding: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve and bind one typed canonical result without caller projections."""

    if not isinstance(raw, Mapping):
        raise ValueError(
            "canonical_result_reference_required: completed Scientific Attempts "
            "must attach a canonical Check or replayed Backtest result"
        )
    reference = dict(raw)
    kind = str(reference.get("kind") or "").strip().lower()
    if kind == "check":
        evidence = _check_reference(reference)
    elif kind == "backtest":
        evidence = _backtest_reference(reference)
    else:
        raise ValueError(
            "canonical_result_reference_invalid: kind must be check or backtest"
        )
    return _finalize_reference(
        evidence,
        expected_dataset_binding=expected_dataset_binding,
        expected_trial_inputs=expected_trial_inputs,
        authority_binding=authority_binding,
    )


__all__ = ["resolve_canonical_result_reference"]
