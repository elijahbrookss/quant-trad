"""Canonical preview and frozen-evidence execution for registered Checks."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from engines.indicator_engine.contracts import IndicatorGapRejectedError
from market_data.frozen import semantic_hash
from market_data.frozen import normalize_frozen_market_data_read_binding
from research_science.check import (
    CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
    CHECK_MODE_EVIDENCE,
    CHECK_MODE_PREVIEW,
    CHECK_RESULT_SCHEMA_VERSION,
    CheckDefinition,
    CheckEvidenceBinding,
    CheckRequest,
    CheckResult,
    ResolvedCheckPlan,
    ScalarAssertionSpec,
    evaluate_scalar_assertions,
)

from portal.backend.service.indicators.indicator_service.runtime_validation import (
    collect_runtime_output_evidence_for_instance,
)
from portal.backend.service.market import candle_service
from portal.backend.service.market.frozen_dataset_service import (
    resolve_frozen_dataset_read_binding,
)
from portal.backend.service.market.runtime_market_data import RuntimeMarketDataResolver
from portal.backend.service.provenance import evidence_source_revision, source_revision
from portal.backend.service.reports import contract as reports_contract
from portal.backend.service.storage.repos.market_data import market_data_repo

from . import checks
from .event_fact_evaluator import EVENT_FACT_ANALYSIS
from .registry import CHECK_REGISTRY


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _indicator_requirements_by_consumer(
    plan: ResolvedCheckPlan,
) -> dict[str, list[dict[str, Any]]]:
    declarations: dict[str, list[dict[str, Any]]] = {}
    for requirement in plan.market_data_requirements:
        consumer_id = str(requirement.get("consumer_id") or "")
        if not consumer_id or consumer_id == "check":
            continue
        raw_input = None
        for binding in requirement.get("bindings") or []:
            if str(binding.get("consumer_id") or "") == consumer_id:
                candidate = binding.get("input")
                if isinstance(candidate, Mapping):
                    raw_input = dict(candidate)
                    break
        if raw_input is None:
            raw_input = {
                key: requirement.get(key)
                for key in (
                    "key",
                    "fact_type",
                    "contract_version",
                    "timeframe_seconds",
                    "instrument_role",
                    "instrument_ref",
                    "dimensions",
                    "alignment",
                    "max_staleness_seconds",
                    "required",
                    "allow_gaps",
                    "known_at_required",
                    "required_fields",
                    "lookback_bars",
                    "lookback_seconds",
                )
                if key in requirement
            }
            raw_input["key"] = str(requirement.get("alias") or "").split(":")[-1]
        declarations.setdefault(consumer_id, []).append(raw_input)
    return declarations


def _assertions(request: CheckRequest) -> list[ScalarAssertionSpec]:
    return [
        ScalarAssertionSpec(
            metric_path=str(raw.get("metric_path") or ""),
            operator=str(raw.get("operator") or ""),
            threshold=raw.get("threshold"),
        )
        for raw in request.parameters.get("assertions") or []
    ]


def _quality(plan: ResolvedCheckPlan, *, evidence: Mapping[str, Any] | None = None) -> dict[str, Any]:
    gaps = list((evidence or {}).get("recorded_gaps") or plan.quality_evidence)
    return {
        "status": "degraded" if gaps else "clean",
        "gap_policy": plan.gap_policy,
        "recorded_gap_count": len(gaps),
        "recorded_gaps": gaps,
        "missing_coverage": list(plan.missing_coverage),
    }


def _filter_indicator_evidence(
    evidence: Mapping[str, Any], plan: ResolvedCheckPlan
) -> dict[str, Any]:
    start = str(plan.evaluation_range["start"])
    end = str(plan.evaluation_range["end_exclusive"])
    return {
        **dict(evidence),
        "outputs": [
            dict(row)
            for row in evidence.get("outputs") or []
            if start <= str(row.get("time") or "") < end
        ],
        "evaluation_range": dict(plan.evaluation_range),
    }


def _load_market_inputs(
    definition: CheckDefinition,
    request: CheckRequest,
    plan: ResolvedCheckPlan,
    *,
    resolver: RuntimeMarketDataResolver,
) -> dict[str, Any]:
    scope = dict(request.scope)
    start = str(plan.materialization_range["start"])
    end = str(plan.materialization_range["end_exclusive"])
    timeframe = str(scope.get("timeframe") or scope.get("interval") or "")
    primary_requirement = next(
        row
        for row in plan.market_data_requirements
        if str(row.get("alias") or "") == "primary_bars"
    )
    instrument_id = str(primary_requirement["instrument_id"])
    candles = candle_service.fetch_ohlcv_by_instrument(
        instrument_id, start, end, timeframe
    )
    data_quality = _quality(
        plan,
        evidence=(resolver.dataset_binding if resolver.dataset_binding is not None else None),
    )
    inputs: dict[str, Any] = {
        "candles": candles,
        "detector": dict(request.parameters.get("detector") or {}),
        "outcomes": dict(request.parameters.get("outcomes") or {}),
        "statistics": dict(request.parameters.get("statistics") or {}),
        "data_quality": data_quality,
    }
    if plan.indicator_graph:
        indicator_id = str(scope.get("indicator_id") or "")
        bound_gaps = list(
            (resolver.dataset_binding or {}).get("recorded_gaps")
            or plan.quality_evidence
        )
        candle_aliases = {
            str(row.get("alias") or "")
            for row in plan.market_data_requirements
            if str(row.get("fact_type") or "") == "candle.ohlcv"
        }
        candle_gaps = [
            dict(row)
            for row in bound_gaps
            if str(row.get("alias") or "") in candle_aliases
        ]
        try:
            evidence = collect_runtime_output_evidence_for_instance(
                indicator_id,
                start,
                end,
                timeframe,
                symbol=scope.get("symbol"),
                datasource=scope.get("datasource"),
                exchange=scope.get("exchange"),
                instrument_id=instrument_id,
                indicator_param_overrides=scope.get("indicator_param_overrides"),
                candle_frame=candles,
                market_data_resolver=resolver,
                market_data_requirements_by_consumer=_indicator_requirements_by_consumer(
                    plan
                ),
                gap_policy=plan.gap_policy,
                gap_rewarm_bars=int(plan.warmup.get("bars") or 0),
                recorded_gap_evidence=candle_gaps,
                expected_indicator_graph=plan.indicator_graph,
                indicator_plan_start=str(plan.evaluation_range["start"]),
                indicator_plan_end=str(plan.materialization_range["end_exclusive"]),
            )
        except IndicatorGapRejectedError as exc:
            inputs["indicator_gap_rejection"] = {
                "indicator_id": exc.indicator_id,
                "gap": dict(exc.gap),
                "policy": "reject",
                "action": "rejected",
            }
            evidence = {
                "schema_version": "indicator_output_evidence.v1",
                "indicator_graph": [dict(row) for row in plan.indicator_graph],
                "indicator_graph_hash": semantic_hash(
                    {"indicators": list(plan.indicator_graph)}
                ),
                "candles": [],
                "outputs": [],
                "gap_transitions": [],
            }
        inputs["indicator_evidence"] = _filter_indicator_evidence(evidence, plan)
    if bool(plan.execution.get("fact_history_required", False)):
        requirements = {
            str(row["alias"]): dict(row)
            for row in plan.market_data_requirements
            if str(row.get("consumer_id") or "") == "check"
            and str(row.get("alias") or "") != "primary_bars"
        }
        histories: dict[str, Any] = {}
        for alias, requirement in requirements.items():
            histories[alias] = resolver.causal_history(
                consumer_id="check",
                requirement=requirement,
                primary_instrument_id=instrument_id,
                start=_utc(requirement["required_start"]),
                end=_utc(requirement["required_end"]),
                evaluation_time=_utc(plan.materialization_range["end_exclusive"]),
            )
        inputs["fact_requirements_by_alias"] = requirements
        inputs["fact_records_by_alias"] = histories
    return inputs


def _evaluate(
    definition: CheckDefinition,
    request: CheckRequest,
    plan: ResolvedCheckPlan,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
    result = dict(evaluator.evaluate(plan=plan, inputs=inputs))
    assertion_result = evaluate_scalar_assertions(result, _assertions(request))
    return {
        **result,
        **assertion_result,
        "promotion_authority": False,
        "execution_authority": False,
    }


def _numeric_record_material(record: Any) -> dict[str, Any]:
    source = getattr(record, "source", None)
    fact = getattr(record, "fact", None)
    if fact is None:
        raise RuntimeError("check_evidence_input_invalid: numeric fact record is malformed")
    return {
        "series_id": int(getattr(record, "series_id")),
        "revision": int(getattr(record, "revision")),
        "market_commit_seq": int(getattr(record, "market_commit_seq")),
        "ingestion_run_id": str(getattr(record, "ingestion_run_id")),
        "source_identity_key": str(getattr(record, "source_identity_key")),
        "source": {
            "provider": str(getattr(source, "provider", "")),
            "venue": str(getattr(source, "venue", "")),
            "source_kind": str(getattr(source, "source_kind", "")),
            "adapter_version": str(getattr(source, "adapter_version", "")),
        },
        "provenance": dict(getattr(record, "provenance", {}) or {}),
        "fact": dict(fact.to_dict()),
    }


def _execution_input_hashes(
    plan: ResolvedCheckPlan,
    inputs: Mapping[str, Any],
) -> dict[str, str]:
    indicator = dict(inputs.get("indicator_evidence") or {})
    indicator_graph = list(indicator.get("indicator_graph") or plan.indicator_graph)
    indicator_graph_hash = str(indicator.get("indicator_graph_hash") or "").strip()
    if not indicator_graph_hash:
        indicator_graph_hash = semantic_hash({"indicators": indicator_graph})
    indicator_output_hash = semantic_hash(
        {
            "schema_version": "check_indicator_output_material.v1",
            "runtime_path": indicator.get("runtime_path"),
            "indicator_graph_hash": indicator_graph_hash,
            "window": dict(indicator.get("window") or {}),
            "output_types": dict(indicator.get("output_types") or {}),
            "ready_counts": dict(indicator.get("ready_counts") or {}),
            "not_ready_counts": dict(indicator.get("not_ready_counts") or {}),
            "outputs": list(indicator.get("outputs") or []),
        }
    )
    histories = {
        str(alias): [
            _numeric_record_material(record) for record in records
        ]
        for alias, records in sorted(
            dict(inputs.get("fact_records_by_alias") or {}).items()
        )
    }
    fact_input_hash = semantic_hash(
        {
            "schema_version": "check_fact_input_material.v1",
            "requirements": dict(inputs.get("fact_requirements_by_alias") or {}),
            "histories": histories,
        }
    )
    gap_transition_hash = semantic_hash(
        {
            "schema_version": "check_gap_transition_material.v1",
            "policy": plan.gap_policy,
            "transitions": list(indicator.get("gap_transitions") or []),
            "discontinuities": list(
                indicator.get("continuity_discontinuities") or []
            ),
            "rejection": inputs.get("indicator_gap_rejection"),
        }
    )
    return {
        "indicator_graph_hash": indicator_graph_hash,
        "indicator_output_hash": indicator_output_hash,
        "fact_input_hash": fact_input_hash,
        "gap_transition_hash": gap_transition_hash,
    }


def _immutable_run_binding(
    run_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    metadata = dict(run_evidence.get("metadata") or {})
    readiness = dict(run_evidence.get("readiness") or {})
    semantic_fingerprint = str(
        metadata.get("report_semantic_fingerprint") or ""
    ).strip()
    if not semantic_fingerprint:
        semantic_fingerprint = semantic_hash(
            {
                key: value
                for key, value in dict(run_evidence).items()
                if key not in {"narrative_summary", "performance"}
            }
        )
    return {
        "schema_version": "immutable_run_research_binding.v1",
        "run_id": str(metadata.get("run_id") or "").strip(),
        "report_schema_version": str(run_evidence.get("schema_version") or ""),
        "report_semantic_fingerprint": semantic_fingerprint,
        "dataset_id": metadata.get("dataset_id"),
        "dataset_hash": metadata.get("dataset_hash"),
        "strategy_id": metadata.get("strategy_id"),
        "strategy_hash": metadata.get("strategy_hash"),
        "readiness": {
            "dataset_status": readiness.get("dataset_status"),
            "results_status": readiness.get("results_status"),
            "safe_to_compare": bool(readiness.get("safe_to_compare", False)),
        },
        "provider_access": False,
    }


def _verified_run_evidence(
    request: CheckRequest,
    supplied: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    expected = dict(request.immutable_run_evidence or {})
    run_id = str(expected.get("run_id") or request.scope.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("check_run_evidence_invalid: run_id is required")
    run_evidence = dict(
        supplied or reports_contract.get_run_research_dataset(run_id)
    )
    actual = _immutable_run_binding(run_evidence)
    if actual != expected:
        raise ValueError(
            "check_run_evidence_invalid: immutable run binding changed before execution"
        )
    return run_evidence, actual


def execute_check_preview(
    definition: CheckDefinition,
    request: CheckRequest,
    plan: ResolvedCheckPlan,
    *,
    run_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute an ephemeral Check against one captured mutable-store watermark."""

    if request.mode != CHECK_MODE_PREVIEW:
        raise ValueError("check_preview_mode_required")
    revision = source_revision()
    watermark = int(
        plan.materialization_range.get("as_of_commit_seq")
        or market_data_repo.current_commit_seq()
    )
    resolver = RuntimeMarketDataResolver(
        store=market_data_repo,
        as_of_commit_seq=watermark,
        instrument_bindings=dict(request.scope.get("market_data_bindings") or {}),
    )
    with candle_service.market_data_preview_read_scope(
        as_of_commit_seq=watermark,
        source_revision=revision,
    ):
        if definition.definition_id in {
            checks.RUN_SIGNAL_SUMMARY,
            checks.RUN_DECISION_TRADE_COMPARISON,
        }:
            resolved_run_evidence, run_binding = _verified_run_evidence(
                request, run_evidence
            )
            inputs = {
                "run_evidence": resolved_run_evidence,
                "detector": dict(request.parameters.get("detector") or {}),
                "outcomes": dict(request.parameters.get("outcomes") or {}),
                "data_quality": {
                    "status": "best_effort_current",
                    "run_binding": run_binding,
                },
            }
        else:
            inputs = _load_market_inputs(
                definition, request, plan, resolver=resolver
            )
        result = _evaluate(definition, request, plan, inputs)
    semantic = {
        "definition_hash": definition.definition_hash,
        "request_hash": request.request_hash,
        "plan_hash": plan.plan_hash,
        "result": result,
        "watermark": watermark,
        "source_revision": revision,
    }
    return {
        "schema_version": "research_check_preview.v2",
        "mode": CHECK_MODE_PREVIEW,
        "status": result.get("status"),
        "definition": definition.to_dict(),
        "request": request.to_dict(),
        "plan": plan.to_dict(),
        "provenance": {
            "kind": "mutable_store_watermark",
            "as_of_commit_seq": watermark,
            "source_revision": revision,
            "mutable_store": True,
            "ephemeral": True,
            "replayable": False,
            "observation_eligible": False,
            "provider_call_performed": False,
        },
        "result": result,
        "preview_hash": semantic_hash(semantic),
    }


def execute_check_evidence(
    definition: CheckDefinition,
    request: CheckRequest,
    plan: ResolvedCheckPlan,
    *,
    run_evidence: Mapping[str, Any] | None = None,
    expected_input_binding: Mapping[str, Any] | None = None,
) -> tuple[ResolvedCheckPlan, CheckEvidenceBinding, CheckResult]:
    """Execute provider-free against a server-resolved frozen Dataset binding."""

    if request.mode != CHECK_MODE_EVIDENCE:
        raise ValueError("check_evidence_mode_required")
    is_run_check = definition.definition_id in {
        checks.RUN_SIGNAL_SUMMARY,
        checks.RUN_DECISION_TRADE_COMPARISON,
    }
    if is_run_check:
        resolved_run_evidence, binding = _verified_run_evidence(
            request, run_evidence
        )
        if expected_input_binding is not None and dict(expected_input_binding) != binding:
            raise ValueError(
                "check_replay_input_binding_mismatch: immutable run binding changed"
            )
    else:
        if not request.dataset_id:
            raise ValueError("check_evidence_dataset_required")
        expected_binding = (
            normalize_frozen_market_data_read_binding(expected_input_binding)
            if expected_input_binding is not None
            else None
        )
        if expected_binding is not None and str(expected_binding["dataset_id"]) != str(
            request.dataset_id
        ):
            raise ValueError(
                "check_replay_input_binding_mismatch: request Dataset differs"
            )
        subject_snapshots = {
            str(row["instrument_id"]): dict(row.get("snapshot") or {})
            for row in (expected_binding or {}).get("subjects") or []
        }
        binding = resolve_frozen_dataset_read_binding(
            dataset_id=request.dataset_id,
            requirements=plan.market_data_requirements,
            store=market_data_repo,
            instrument_loader=(
                lambda instrument_id: subject_snapshots[str(instrument_id)]
                if str(instrument_id) in subject_snapshots
                else (_ for _ in ()).throw(
                    ValueError(
                        "check_replay_input_binding_mismatch: persisted subject is missing"
                    )
                )
            )
            if expected_binding is not None
            else None,
        )
        if expected_binding is not None and binding != expected_binding:
            raise ValueError(
                "check_replay_input_binding_mismatch: frozen input binding changed"
            )
    bound_plan_payload = plan.to_dict()
    bound_plan_payload.pop("plan_hash", None)
    bound_plan_payload["materialization_range"] = dict(
        plan.materialization_range
    )
    if not is_run_check:
        bound_plan_payload["materialization_range"]["as_of_commit_seq"] = int(
            binding["max_commit_seq"]
        )
    bound_plan_payload["missing_coverage"] = []
    bound_plan_payload["quality_evidence"] = list(
        binding.get("recorded_gaps") or []
    )
    bound_plan = ResolvedCheckPlan.from_dict(bound_plan_payload)
    revision = evidence_source_revision()
    if is_run_check:
        inputs = {
            "run_evidence": resolved_run_evidence,
            "detector": dict(request.parameters.get("detector") or {}),
            "outcomes": dict(request.parameters.get("outcomes") or {}),
            "data_quality": dict(
                resolved_run_evidence.get("readiness") or {}
            ),
        }
        result_payload = _evaluate(definition, request, bound_plan, inputs)
    else:
        resolver = RuntimeMarketDataResolver(
            store=market_data_repo,
            dataset_binding=binding,
            instrument_bindings=dict(request.scope.get("market_data_bindings") or {}),
        )
        with candle_service.market_data_read_scope(dataset_binding=binding):
            inputs = _load_market_inputs(
                definition, request, bound_plan, resolver=resolver
            )
            result_payload = _evaluate(definition, request, bound_plan, inputs)
    input_hashes = _execution_input_hashes(bound_plan, inputs)
    run_quality = (
        dict(resolved_run_evidence.get("readiness") or {})
        if is_run_check
        else dict(binding.get("quality") or {})
    )
    run_gaps = (
        dict(resolved_run_evidence.get("candle_gaps") or {})
        if is_run_check
        else {"recorded_gaps": binding.get("recorded_gaps") or []}
    )
    evidence = CheckEvidenceBinding(
        schema_version=CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=bound_plan.plan_hash,
        code_revision=revision,
        evidence_kind=(
            "immutable_run_evidence" if is_run_check else "frozen_market_data"
        ),
        input_binding=binding,
        indicator_graph_hash=input_hashes["indicator_graph_hash"],
        indicator_output_hash=input_hashes["indicator_output_hash"],
        fact_input_hash=input_hashes["fact_input_hash"],
        gap_transition_hash=input_hashes["gap_transition_hash"],
        quality_hash=semantic_hash(run_quality),
        gaps_hash=semantic_hash(run_gaps),
    )
    result = CheckResult(
        schema_version=CHECK_RESULT_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=bound_plan.plan_hash,
        evidence_hash=evidence.evidence_hash,
        evaluator_id=definition.evaluator_id,
        evaluator_version=definition.evaluator_version,
        result=result_payload,
    )
    return bound_plan, evidence, result


__all__ = ["execute_check_evidence", "execute_check_preview"]
