from __future__ import annotations

from dataclasses import replace

import pytest

from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION
from market_data.frozen import (
    build_frozen_market_data_read_binding,
    frozen_subject_snapshot_hash,
)
from research_science.check import (
    CHECK_DEFINITION_SCHEMA_VERSION,
    CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
    CHECK_MODE_EVIDENCE,
    CHECK_PLAN_SCHEMA_VERSION,
    CHECK_REQUEST_SCHEMA_VERSION,
    CHECK_RESULT_SCHEMA_VERSION,
    GAP_POLICY_REJECT,
    CheckDefinition,
    CheckEvidenceBinding,
    CheckRegistry,
    CheckRequest,
    CheckResult,
    ResolvedCheckPlan,
    ScalarAssertionSpec,
    evaluate_scalar_assertions,
    verify_check_replay,
)


class _Evaluator:
    evaluator_id = "event_fact"
    version = "1"

    def evaluate(self, *, plan, inputs):
        return {"plan_hash": plan.plan_hash, "input_count": len(inputs)}


def _definition(**changes) -> CheckDefinition:
    values = {
        "schema_version": CHECK_DEFINITION_SCHEMA_VERSION,
        "definition_id": "event_fact_analysis",
        "definition_version": "1",
        "evaluator_id": "event_fact",
        "evaluator_version": "1",
        "request_schema_version": CHECK_REQUEST_SCHEMA_VERSION,
        "result_schema_version": CHECK_RESULT_SCHEMA_VERSION,
        "material_rules": {
            "facts": [{"alias": "reference_price", "alignment": "latest_known"}],
            "outcomes": {"horizons": [2, 6, 12]},
            "assertions": [],
        },
    }
    values.update(changes)
    return CheckDefinition(**values)


def _request(definition: CheckDefinition, **changes) -> CheckRequest:
    values = {
        "schema_version": CHECK_REQUEST_SCHEMA_VERSION,
        "mode": CHECK_MODE_EVIDENCE,
        "definition_id": definition.definition_id,
        "definition_version": definition.definition_version,
        "definition_hash": definition.definition_hash,
        "scope": {
            "instrument_id": "instrument-1",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
        },
        "parameters": {"gap_policy": GAP_POLICY_REJECT},
        "dataset_id": "mds_" + "a" * 32,
    }
    values.update(changes)
    return CheckRequest(**values)


def _plan(request: CheckRequest, **changes) -> ResolvedCheckPlan:
    values = {
        "schema_version": CHECK_PLAN_SCHEMA_VERSION,
        "request_hash": request.request_hash,
        "market_data_requirements": (
            {
                "alias": "primary",
                "fact_type": CANDLE_FACT_TYPE,
                "contract_version": CANDLE_FACT_VERSION,
                "instrument_id": "instrument-1",
                "timeframe_seconds": 3600,
                "source_policy": {"mode": "exact"},
                "alignment": "exact_interval",
            },
        ),
        "indicator_graph": (),
        "evaluation_range": {
            "start": "2026-01-01T00:00:00Z",
            "end_exclusive": "2026-01-02T00:00:00Z",
        },
        "materialization_range": {
            "start": "2025-12-31T00:00:00Z",
            "end_exclusive": "2026-01-02T06:00:00Z",
        },
        "warmup": {"bars": 24},
        "outcome_tail": {"bars": 6},
        "gap_policy": GAP_POLICY_REJECT,
    }
    values.update(changes)
    return ResolvedCheckPlan(**values)


def _frozen_binding() -> dict:
    dataset_hash = "a" * 64
    return build_frozen_market_data_read_binding(
        dataset_id="mds_" + "a" * 32,
        dataset_hash=dataset_hash,
        max_commit_seq=10,
        subjects=(
            {
                "instrument_id": "instrument-1",
                "snapshot_hash": frozen_subject_snapshot_hash(
                    {"id": "instrument-1", "symbol": "TEST"}
                ),
                "snapshot": {"id": "instrument-1", "symbol": "TEST"},
            },
        ),
        series=(
            {
                "alias": "primary",
                "series_id": 7,
                "instrument_id": "instrument-1",
                "fact_type": CANDLE_FACT_TYPE,
                "contract_version": CANDLE_FACT_VERSION,
                "timeframe_seconds": 3600,
                "range_start": "2025-12-31T00:00:00Z",
                "range_end": "2026-01-02T06:00:00Z",
                "row_count": 54,
                "max_commit_seq": 10,
                "material_hash": "material",
                "provenance_hash": "provenance",
                "quality_hash": "quality",
                "source_binding": {
                    "series_id": 7,
                    "resolved_source_identity_keys": ["source-a"],
                },
            },
        ),
        quality={"status": "recorded"},
    )


def _evidence(definition: CheckDefinition, request: CheckRequest, plan: ResolvedCheckPlan):
    return CheckEvidenceBinding(
        schema_version=CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=plan.plan_hash,
        code_revision="abc123",
        evidence_kind="frozen_market_data",
        input_binding=_frozen_binding(),
        indicator_graph_hash="indicator-graph",
        indicator_output_hash="indicator-output",
        fact_input_hash="fact-input",
        gap_transition_hash="gap-transitions",
        quality_hash="quality",
        gaps_hash="gaps",
    )


def test_check_contract_hashes_are_stable_and_material_changes_are_distinct() -> None:
    definition = _definition()
    assert _definition().definition_hash == definition.definition_hash
    changed = _definition(
        material_rules={
            **definition.material_rules,
            "assertions": [{"metric_path": "sample_count", "operator": "gte", "threshold": 30}],
        }
    )
    assert changed.definition_hash != definition.definition_hash
    with pytest.raises(ValueError, match="check_definition_hash_mismatch"):
        replace(definition, material_rules={"changed": True})


def test_evidence_request_requires_exactly_one_immutable_input() -> None:
    definition = _definition()
    with pytest.raises(ValueError, match="check_evidence_input_required"):
        _request(definition, dataset_id=None)
    with pytest.raises(ValueError, match="check_evidence_input_required"):
        _request(
            definition,
            immutable_run_evidence={"run_id": "run-1", "evidence_hash": "hash"},
        )


def test_registry_resolves_exact_versions_and_rejects_duplicates() -> None:
    registry = CheckRegistry()
    evaluator = _Evaluator()
    definition = _definition()
    registry.register_evaluator(evaluator)
    registry.register_definition(definition)

    assert registry.resolve(definition.definition_id, "1") == (definition, evaluator)
    with pytest.raises(ValueError, match="check_evaluator_duplicate"):
        registry.register_evaluator(evaluator)
    with pytest.raises(ValueError, match="check_definition_duplicate"):
        registry.register_definition(definition)
    with pytest.raises(ValueError, match="check_definition_unavailable"):
        registry.resolve_definition(definition.definition_id, "2")


def test_evidence_and_result_hashes_bind_all_semantic_inputs() -> None:
    definition = _definition()
    request = _request(definition)
    plan = _plan(request)
    evidence = _evidence(definition, request, plan)
    result = CheckResult(
        schema_version=CHECK_RESULT_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=plan.plan_hash,
        evidence_hash=evidence.evidence_hash,
        evaluator_id=definition.evaluator_id,
        evaluator_version=definition.evaluator_version,
        result={"sample_count": 42, "perf": {"duration_ms": 99}},
    )
    same = CheckResult(
        schema_version=CHECK_RESULT_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=plan.plan_hash,
        evidence_hash=evidence.evidence_hash,
        evaluator_id=definition.evaluator_id,
        evaluator_version=definition.evaluator_version,
        result={"sample_count": 42, "perf": {"duration_ms": 1}},
    )
    changed = replace(result, result={"sample_count": 41}, result_hash="")

    assert result.result_hash == same.result_hash
    assert changed.result_hash != result.result_hash
    assert verify_check_replay(result, same)["matches"] is True
    assert verify_check_replay(result, changed)["matches"] is False

    changed_quality = replace(
        evidence,
        quality_hash="changed-quality",
        input_hash="",
        evidence_hash="",
    )
    changed_input = replace(
        evidence,
        fact_input_hash="changed-fact-input",
        input_hash="",
        evidence_hash="",
    )
    changed_configuration = _plan(
        request,
        execution={"evaluator_id": "event_fact", "configuration_version": "2"},
    )
    assert changed_quality.evidence_hash != evidence.evidence_hash
    assert changed_input.evidence_hash != evidence.evidence_hash
    assert changed_configuration.plan_hash != plan.plan_hash


def test_assertions_have_no_verdict_when_absent_and_never_silently_pass_missing() -> None:
    assert evaluate_scalar_assertions({"sample_count": 30}, [])["verdict"] is None
    evaluated = evaluate_scalar_assertions(
        {"sample_count": 30},
        [
            ScalarAssertionSpec("sample_count", "gte", 30),
            ScalarAssertionSpec("metrics.missing", "gt", 0),
        ],
    )
    assert [row["status"] for row in evaluated["assertions"]] == [
        "passed",
        "indeterminate",
    ]
    assert evaluated["verdict"] == "indeterminate"
