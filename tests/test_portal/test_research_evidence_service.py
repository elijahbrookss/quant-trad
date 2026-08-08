from __future__ import annotations

from typing import Any

import pytest

from market_data.frozen import (
    build_frozen_market_data_read_binding,
    frozen_subject_snapshot_hash,
    semantic_hash,
)
from research_science.check import (
    CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
    CHECK_MODE_EVIDENCE,
    CHECK_PLAN_SCHEMA_VERSION,
    CHECK_REQUEST_SCHEMA_VERSION,
    CHECK_RESULT_SCHEMA_VERSION,
    CheckEvidenceBinding,
    CheckRequest,
    CheckResult,
    ResolvedCheckPlan,
)

from portal.backend.service.research import service
from portal.backend.service.research.registry import (
    materialize_check_definition,
    pin_check_definition_to_plan,
)


def _contracts():
    dataset_hash = "a" * 64
    dataset_id = "mds_" + dataset_hash[:32]
    operation = {
            "mode": CHECK_MODE_EVIDENCE,
            "title": "Frozen evidence",
            "check_family": "raw_forward_outcome",
            "dataset_id": dataset_id,
            "scope": {
            "instrument_id": "instrument-1",
            "timeframe": "1h",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
            },
            "detector": {
                "type": "raw_condition",
                "field": "close",
                "operator": "gt",
                "value": 1,
            },
            "outcomes": {"forward_bars": [1]},
            "gap_policy": "reject",
        }
    definition = materialize_check_definition(
        operation, mode=CHECK_MODE_EVIDENCE
    )
    request = CheckRequest(
        schema_version=CHECK_REQUEST_SCHEMA_VERSION,
        mode=CHECK_MODE_EVIDENCE,
        definition_id=definition.definition_id,
        definition_version=definition.definition_version,
        definition_hash=definition.definition_hash,
        scope=operation["scope"],
        parameters={
            key: definition.material_rules[key]
            for key in (
                "detector",
                "outcomes",
                "statistics",
                "assertions",
                "inputs",
                "gap_policy",
                "gap_rewarm_bars",
            )
        },
        dataset_id=dataset_id,
    )
    plan = ResolvedCheckPlan(
        schema_version=CHECK_PLAN_SCHEMA_VERSION,
        request_hash=request.request_hash,
        market_data_requirements=(
            {
                "alias": "primary_bars",
                "consumer_id": "check",
                "instrument_id": "instrument-1",
                "fact_type": "candle.ohlcv",
                "contract_version": "candle.ohlcv.v1",
                "timeframe_seconds": 3600,
                "dimensions": {},
                "alignment": "exact_interval",
                "max_staleness_seconds": None,
                "known_at_required": True,
                "required_fields": ["open", "high", "low", "close", "known_at"],
                "series_required": True,
                "frame_missing_policy": "indicator_owned",
                "source_policy": {"mode": "exact"},
                "required_start": "2025-12-31T10:00:00.000000Z",
                "required_end": "2026-01-02T01:00:00.000000Z",
            },
        ),
        indicator_graph=(),
        evaluation_range={
            "start": "2026-01-01T00:00:00.000000Z",
            "end_exclusive": "2026-01-02T00:00:00.000000Z",
        },
        materialization_range={
            "start": "2025-12-31T10:00:00.000000Z",
            "end_exclusive": "2026-01-02T01:00:00.000000Z",
            "as_of_commit_seq": 7,
        },
        warmup={"bars": 14, "seconds": 50400, "timeframe_seconds": 3600},
        outcome_tail={
            "horizons": [1],
            "required_horizons": [1],
            "bars": 1,
            "seconds": 3600,
            "horizon_kind": "bars",
            "entry_lag_bars": 0,
            "invalidation_max_bars": 0,
        },
        gap_policy="reject",
        execution={
            "input_kind": "market_data",
            "indicator_ids": [],
            "warmup_floor_bars": 14,
            "feature_lookback_bars": 0,
            "feature_windows_seconds_by_alias": {},
            "outcome_horizons": [1],
            "required_outcome_horizons": [1],
            "horizon_kind": "bars",
            "entry_lag_bars": 0,
            "invalidation_max_bars": 0,
        },
    )
    definition, request, plan = pin_check_definition_to_plan(
        definition, request, plan
    )
    requirement = dict(plan.market_data_requirements[0])
    binding = build_frozen_market_data_read_binding(
        dataset_id=dataset_id,
        dataset_hash=dataset_hash,
        max_commit_seq=7,
        series=[
            {
                "alias": "primary_bars",
                "series_id": 1,
                "identity_key": "series-1",
                "instrument_id": "instrument-1",
                "fact_type": "candle.ohlcv",
                "contract_version": "candle.ohlcv.v1",
                "timeframe_seconds": 3600,
                "range_start": "2025-12-31T10:00:00.000000Z",
                "range_end": "2026-01-02T01:00:00.000000Z",
                "row_count": 39,
                "max_commit_seq": 7,
                "material_hash": "material",
                "provenance_hash": "provenance",
                "quality_hash": "quality",
                "source_summary": {
                    "counts": {"source-a": 39},
                    "sources": {"source-a": {"provider": "provider-a"}},
                },
                "requirement": requirement,
                "source_binding": {
                    "schema_version": "market_data_source_binding.v1",
                    "mode": "exact",
                    "series_id": 1,
                    "resolved_source_identity_keys": ["source-a"],
                    "sources": {"source-a": {"provider": "provider-a"}},
                    "selection_rule": "latest_known_then_commit_seq_then_source_identity.v1",
                },
            }
        ],
        subjects=[
            {
                "instrument_id": "instrument-1",
                "snapshot_hash": frozen_subject_snapshot_hash(
                    {"id": "instrument-1", "symbol": "TEST"}
                ),
                "snapshot": {"id": "instrument-1", "symbol": "TEST"},
            }
        ],
        quality={"status": "clean"},
    )
    evidence = CheckEvidenceBinding(
        schema_version=CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=plan.plan_hash,
        code_revision="revision-1",
        evidence_kind="frozen_market_data",
        input_binding=binding,
        indicator_graph_hash=semantic_hash({"indicators": []}),
        indicator_output_hash=semantic_hash({"indicator_outputs": []}),
        fact_input_hash=semantic_hash({"fact_inputs": {}}),
        gap_transition_hash=semantic_hash({"gap_transitions": []}),
        quality_hash=semantic_hash(binding["quality"]),
        gaps_hash=semantic_hash({"recorded_gaps": []}),
    )
    result = CheckResult(
        schema_version=CHECK_RESULT_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=plan.plan_hash,
        evidence_hash=evidence.evidence_hash,
        evaluator_id=definition.evaluator_id,
        evaluator_version=definition.evaluator_version,
        result={
            "schema_version": "research_check_result.v1",
            "status": "completed",
            "sample_count": 3,
            "assertions": [],
            "verdict": None,
            "promotion_authority": False,
            "execution_authority": False,
        },
    )
    return definition, request, plan, evidence, result


def _event_operation(*, dataset_id: str | None) -> dict[str, Any]:
    return {
        "mode": CHECK_MODE_EVIDENCE,
        "title": "Generic event-and-fact evidence",
        "check_family": "event_fact_analysis",
        **({"dataset_id": dataset_id} if dataset_id else {}),
        "scope": {
            "indicator_id": "indicator-1",
            "instrument_id": "instrument-1",
            "timeframe": "1h",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
        },
        "detector": {
            "type": "indicator_event",
            "output_name": "atr_expansion",
            "event_keys": [
                {"key": "atr_expansion_long", "direction": "long"}
            ],
        },
        "outcomes": {"horizons": [1]},
        "inputs": [
            {
                "alias": "reference_price",
                "fact_type": "market.reference_price",
                "contract_version": "market.reference_price.v1",
                "dimensions": {"quote_currency": "USD"},
                "source_policy": {
                    "mode": "exact",
                    "source_identity_key": "source-a",
                },
            }
        ],
        "gap_policy": "reject",
    }


def test_unqualified_run_rejects_and_preview_cannot_persist_or_create_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        service,
        "plan_research_check",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        service,
        "pin_check_definition_to_plan",
        lambda definition, request, plan: (definition, request, plan),
    )
    monkeypatch.setattr(
        service,
        "execute_check_preview",
        lambda *_args, **_kwargs: {
            "schema_version": "research_check_preview.v2",
            "mode": "preview",
            "status": "completed",
            "provenance": {
                "ephemeral": True,
                "replayable": False,
                "observation_eligible": False,
            },
        },
    )
    monkeypatch.setattr(
        service.repository,
        "create_item",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("preview must not persist")
        ),
    )

    payload = {
        "title": "Preview",
        "scope": {
            "instrument_id": "instrument-1",
            "timeframe": "1h",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
        },
        "detector": {
            "type": "raw_condition",
            "field": "close",
            "operator": "gt",
            "value": 1,
        },
    }
    with pytest.raises(ValueError, match="check_evidence_mode_required"):
        service.run_research_check(payload)

    result = service.evaluate_research_check(payload)

    assert result["schema_version"] == "research_check_preview.v2"
    assert result["provenance"]["observation_eligible"] is False


def test_evidence_execution_rejects_missing_dataset() -> None:
    with pytest.raises(ValueError, match="check_evidence_input_required"):
        service.run_research_check(_event_operation(dataset_id=None))


def test_generic_research_item_creation_cannot_forge_a_check() -> None:
    with pytest.raises(ValueError, match="creation_reserved"):
        service.create_research_item(
            {
                "kind": "research_check",
                "title": "Caller-authored pseudo evidence",
                "payload": {"schema_version": "research_check_payload.v2"},
            }
        )


def test_preview_only_legacy_family_cannot_persist_new_v2_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition, request, plan, evidence, result = _contracts()
    monkeypatch.setattr(
        service.repository,
        "create_item",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("preview-only family must not persist")
        ),
    )

    with pytest.raises(ValueError, match="family_preview_only"):
        service.persist_research_check_evidence(
            {"title": "Frozen evidence"},
            definition=definition,
            request=request,
            plan=plan,
            evidence=evidence.to_dict(),
            result=result.to_dict(),
        )


def test_legacy_check_is_readable_but_not_replayable_or_observation_eligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = {
        "id": "legacy-check",
        "kind": "research_check",
        "payload": {"schema_version": "research_check_payload.v1", "result": {}},
    }
    monkeypatch.setattr(service.repository, "get_item", lambda *_args, **_kwargs: legacy)

    projected = service.get_research_item("legacy-check")
    replay = service.replay_research_check("legacy-check")

    assert projected["evidence_classification"] == "legacy_unpinned"
    assert projected["replayable"] is False
    assert replay["status"] == "not_replayable"
    with pytest.raises(ValueError, match="not durable replayable evidence"):
        service.create_observation_from_check_evidence("legacy-check", {})


def test_older_frozen_contract_is_preserved_but_not_claimed_replayable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    historical = {
        "id": "historical-frozen-check",
        "kind": "research_check",
        "payload": {
            "schema_version": "research_check_payload.v2",
            "plan": {"schema_version": "research.check_plan.v1"},
            "evidence": {
                "schema_version": "research.check_evidence_binding.v1"
            },
        },
    }
    monkeypatch.setattr(
        service.repository, "get_item", lambda *_args, **_kwargs: historical
    )

    projected = service.get_research_item("historical-frozen-check")

    assert projected["evidence_classification"] == "legacy_frozen_unverifiable"
    assert projected["replayable"] is False
    assert projected["observation_eligible"] is False


def test_identical_evidence_replay_preserves_result_and_evidence_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition, request, plan, evidence, result = _contracts()
    item = {
        "id": "check-1",
        "kind": "research_check",
        "payload": {
            "schema_version": "research_check_payload.v2",
            "evidence_classification": "frozen_replayable",
            "definition": definition.to_dict(),
            "request": request.to_dict(),
            "plan": plan.to_dict(),
            "evidence": evidence.to_dict(),
            "result": result.to_dict(),
            "replayable": True,
            "observation_eligible": True,
        },
    }
    monkeypatch.setattr(service.repository, "get_item", lambda *_args, **_kwargs: item)
    monkeypatch.setattr(service, "_evidence_source_revision", lambda: "revision-1")
    monkeypatch.setattr(
        service,
        "execute_check_evidence",
        lambda *_args, **_kwargs: (plan, evidence, result),
    )

    replay = service.replay_research_check("check-1")

    assert replay["status"] == "matched"
    assert replay["matches"] is True
    assert replay["original_result_hash"] == replay["replayed_result_hash"]


def test_evidence_replay_rejects_dirty_source_tree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition, request, plan, evidence, result = _contracts()
    item = {
        "id": "check-1",
        "kind": "research_check",
        "payload": {
            "schema_version": "research_check_payload.v2",
            "evidence_classification": "frozen_replayable",
            "definition": definition.to_dict(),
            "request": request.to_dict(),
            "plan": plan.to_dict(),
            "evidence": evidence.to_dict(),
            "result": result.to_dict(),
            "replayable": True,
            "observation_eligible": True,
        },
    }
    monkeypatch.setattr(service.repository, "get_item", lambda *_args, **_kwargs: item)
    monkeypatch.setattr(
        service,
        "_evidence_source_revision",
        lambda: (_ for _ in ()).throw(RuntimeError("source_tree_dirty")),
    )

    replay = service.replay_research_check("check-1")

    assert replay["status"] == "source_revision_unavailable"
    assert replay["matches"] is False
    assert replay["provider_call_performed"] is False
    assert replay["reasons"] == ["source_tree_dirty"]


@pytest.mark.parametrize("tamper", ["quality", "gaps", "graph"])
def test_evidence_validator_rejects_rehashed_cross_layer_disagreement(
    tamper: str,
) -> None:
    definition, request, plan, evidence, result = _contracts()
    binding = dict(evidence.input_binding)
    binding.pop("binding_hash", None)
    if tamper == "quality":
        binding["quality"] = {"status": "tampered"}
    if tamper == "gaps":
        binding["recorded_gaps"] = [
            {
                "alias": "primary_bars",
                "series_id": 1,
                "start": "2026-01-01T01:00:00Z",
                "end": "2026-01-01T02:00:00Z",
            }
        ]
    tampered_evidence = CheckEvidenceBinding(
        schema_version=CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=plan.plan_hash,
        code_revision=evidence.code_revision,
        evidence_kind="frozen_market_data",
        input_binding=binding,
        indicator_graph_hash=(
            semantic_hash({"indicators": [{"tampered": True}]})
            if tamper == "graph"
            else evidence.indicator_graph_hash
        ),
        indicator_output_hash=evidence.indicator_output_hash,
        fact_input_hash=evidence.fact_input_hash,
        gap_transition_hash=evidence.gap_transition_hash,
        quality_hash=evidence.quality_hash,
        gaps_hash=evidence.gaps_hash,
    )
    tampered_result = CheckResult(
        schema_version=CHECK_RESULT_SCHEMA_VERSION,
        definition_hash=definition.definition_hash,
        request_hash=request.request_hash,
        plan_hash=plan.plan_hash,
        evidence_hash=tampered_evidence.evidence_hash,
        evaluator_id=definition.evaluator_id,
        evaluator_version=definition.evaluator_version,
        result=result.result,
    )

    with pytest.raises(ValueError, match="check_evidence_payload_invalid"):
        service._validate_research_check_evidence_payload(
            {
                "schema_version": "research_check_payload.v2",
                "definition": definition.to_dict(),
                "request": request.to_dict(),
                "plan": plan.to_dict(),
                "evidence": tampered_evidence.to_dict(),
                "result": tampered_result.to_dict(),
            }
        )


def test_evidence_validator_rejects_request_rules_rehashed_away_from_definition() -> None:
    definition, request, plan, evidence, result = _contracts()
    changed_parameters = {**dict(request.parameters), "gap_policy": "continue_degraded"}
    changed_request = CheckRequest(
        schema_version=CHECK_REQUEST_SCHEMA_VERSION,
        mode=request.mode,
        definition_id=request.definition_id,
        definition_version=request.definition_version,
        definition_hash=request.definition_hash,
        scope=request.scope,
        parameters=changed_parameters,
        dataset_id=request.dataset_id,
    )
    changed_plan = ResolvedCheckPlan(
        **{
            **plan.to_dict(),
            "request_hash": changed_request.request_hash,
            "gap_policy": "continue_degraded",
            "plan_hash": "",
        }
    )
    changed_evidence = CheckEvidenceBinding(
        **{
            **evidence.to_dict(),
            "request_hash": changed_request.request_hash,
            "plan_hash": changed_plan.plan_hash,
            "evidence_hash": "",
        }
    )
    changed_result = CheckResult(
        **{
            **result.to_dict(),
            "request_hash": changed_request.request_hash,
            "plan_hash": changed_plan.plan_hash,
            "evidence_hash": changed_evidence.evidence_hash,
            "result_hash": "",
        }
    )

    with pytest.raises(ValueError, match="material definition"):
        service._validate_research_check_evidence_payload(
            {
                "schema_version": "research_check_payload.v2",
                "definition": definition.to_dict(),
                "request": changed_request.to_dict(),
                "plan": changed_plan.to_dict(),
                "evidence": changed_evidence.to_dict(),
                "result": changed_result.to_dict(),
            }
        )


def test_evidence_validator_rejects_evaluator_input_kind_substitution() -> None:
    definition, request, plan, evidence, result = _contracts()
    substituted_evidence = CheckEvidenceBinding(
        schema_version=evidence.schema_version,
        definition_hash=evidence.definition_hash,
        request_hash=evidence.request_hash,
        plan_hash=evidence.plan_hash,
        code_revision=evidence.code_revision,
        evidence_kind="immutable_run_evidence",
        input_binding={"run_id": "run-1"},
        indicator_graph_hash=evidence.indicator_graph_hash,
        indicator_output_hash=evidence.indicator_output_hash,
        fact_input_hash=evidence.fact_input_hash,
        gap_transition_hash=evidence.gap_transition_hash,
        quality_hash=semantic_hash({}),
        gaps_hash=semantic_hash({"recorded_gaps": []}),
    )
    substituted_result = CheckResult(
        schema_version=result.schema_version,
        definition_hash=result.definition_hash,
        request_hash=result.request_hash,
        plan_hash=result.plan_hash,
        evidence_hash=substituted_evidence.evidence_hash,
        evaluator_id=result.evaluator_id,
        evaluator_version=result.evaluator_version,
        result=result.result,
    )

    with pytest.raises(ValueError, match="input kind disagrees"):
        service._validate_research_check_evidence_payload(
            {
                "schema_version": "research_check_payload.v2",
                "definition": definition.to_dict(),
                "request": request.to_dict(),
                "plan": plan.to_dict(),
                "evidence": substituted_evidence.to_dict(),
                "result": substituted_result.to_dict(),
            }
        )


def test_evidence_validator_rejects_rehashed_plan_scope_disagreement() -> None:
    definition, request, plan, evidence, result = _contracts()
    changed_plan = ResolvedCheckPlan(
        **{
            **plan.to_dict(),
            "evaluation_range": {
                **dict(plan.evaluation_range),
                "start": "2026-01-01T06:00:00.000000Z",
            },
            "plan_hash": "",
        }
    )
    changed_evidence = CheckEvidenceBinding(
        **{
            **evidence.to_dict(),
            "plan_hash": changed_plan.plan_hash,
            "evidence_hash": "",
        }
    )
    changed_result = CheckResult(
        **{
            **result.to_dict(),
            "plan_hash": changed_plan.plan_hash,
            "evidence_hash": changed_evidence.evidence_hash,
            "result_hash": "",
        }
    )

    with pytest.raises(ValueError, match="request/plan semantic disagreement"):
        service._validate_research_check_evidence_payload(
            {
                "schema_version": "research_check_payload.v2",
                "definition": definition.to_dict(),
                "request": request.to_dict(),
                "plan": changed_plan.to_dict(),
                "evidence": changed_evidence.to_dict(),
                "result": changed_result.to_dict(),
            }
        )


def test_evidence_validator_rejects_rehashed_binding_requirement_disagreement() -> None:
    definition, request, plan, evidence, result = _contracts()
    binding = dict(evidence.input_binding)
    binding.pop("binding_hash", None)
    series = [dict(row) for row in binding["series"]]
    series[0].pop("source_binding_hash", None)
    series[0]["requirement"] = {
        **dict(series[0]["requirement"]),
        "required_start": "2026-01-01T00:00:00.000000Z",
    }
    binding["series"] = series
    changed_evidence = CheckEvidenceBinding(
        **{
            **evidence.to_dict(),
            "input_binding": binding,
            "evidence_hash": "",
            "input_hash": "",
        }
    )
    changed_result = CheckResult(
        **{
            **result.to_dict(),
            "evidence_hash": changed_evidence.evidence_hash,
            "result_hash": "",
        }
    )

    with pytest.raises(ValueError, match="binding requirement disagrees"):
        service._validate_research_check_evidence_payload(
            {
                "schema_version": "research_check_payload.v2",
                "definition": definition.to_dict(),
                "request": request.to_dict(),
                "plan": plan.to_dict(),
                "evidence": changed_evidence.to_dict(),
                "result": changed_result.to_dict(),
            }
        )
