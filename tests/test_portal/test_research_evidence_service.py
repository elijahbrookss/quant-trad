from __future__ import annotations

from typing import Any

import pytest

from market_data.frozen import build_frozen_market_data_read_binding, semantic_hash
from research_science.check import (
    CHECK_DEFINITION_SCHEMA_VERSION,
    CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
    CHECK_MODE_EVIDENCE,
    CHECK_PLAN_SCHEMA_VERSION,
    CHECK_REQUEST_SCHEMA_VERSION,
    CHECK_RESULT_SCHEMA_VERSION,
    CheckDefinition,
    CheckEvidenceBinding,
    CheckRequest,
    CheckResult,
    ResolvedCheckPlan,
)

from portal.backend.service.research import service
from portal.backend.service.research.registry import normalize_check_request


def _contracts():
    dataset_hash = "a" * 64
    dataset_id = "mds_" + dataset_hash[:32]
    definition, request = normalize_check_request(
        {
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
        },
        mode=CHECK_MODE_EVIDENCE,
    )
    plan = ResolvedCheckPlan(
        schema_version=CHECK_PLAN_SCHEMA_VERSION,
        request_hash=request.request_hash,
        market_data_requirements=(),
        indicator_graph=(),
        evaluation_range={
            "start": "2026-01-01T00:00:00Z",
            "end_exclusive": "2026-01-02T00:00:00Z",
        },
        materialization_range={
            "start": "2026-01-01T00:00:00Z",
            "end_exclusive": "2026-01-02T00:00:00Z",
            "as_of_commit_seq": 7,
        },
        warmup={"bars": 0, "seconds": 0, "timeframe_seconds": 3600},
        outcome_tail={"bars": 0, "seconds": 0, "horizon_kind": "bars"},
        gap_policy="reject",
    )
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
                "range_start": "2026-01-01T00:00:00Z",
                "range_end": "2026-01-02T00:00:00Z",
                "row_count": 24,
                "max_commit_seq": 7,
                "material_hash": "material",
                "provenance_hash": "provenance",
                "quality_hash": "quality",
                "source_summary": {
                    "counts": {"source-a": 24},
                    "sources": {"source-a": {"provider": "provider-a"}},
                },
            }
        ],
        subjects=[
            {
                "instrument_id": "instrument-1",
                "snapshot_hash": "subject",
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
        indicator_graph_hash=semantic_hash({"indicator_graph": []}),
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
        service.run_research_check(
            {
                "mode": "evidence",
                "title": "Evidence",
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
                "gap_policy": "reject",
            }
        )


def test_v2_evidence_persists_without_automatic_observation_and_can_support_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition, request, plan, evidence, result = _contracts()
    created: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []

    def create_item(**kwargs):
        item = {"id": kwargs.get("item_id") or f"item-{len(created) + 1}", **kwargs}
        created.append(item)
        return item

    monkeypatch.setattr(service.repository, "create_item", create_item)
    monkeypatch.setattr(
        service.repository,
        "create_link",
        lambda **kwargs: links.append({"id": f"link-{len(links) + 1}", **kwargs})
        or links[-1],
    )

    persisted = service.persist_research_check_evidence(
        {"title": "Frozen evidence"},
        definition=definition,
        request=request,
        plan=plan,
        evidence=evidence.to_dict(),
        result=result.to_dict(),
    )

    assert [row["kind"] for row in created] == ["research_check"]
    assert persisted["check"]["payload"]["schema_version"] == "research_check_payload.v2"
    assert persisted["observation_eligible"] is True
    assert links[0]["target_type"] == "market_dataset"

    monkeypatch.setattr(
        service.repository, "get_item", lambda _item_id, **_kwargs: persisted["check"]
    )
    observed = service.create_observation_from_check_evidence(
        persisted["check"]["id"], {"title": "Observed relationship"}
    )
    assert observed["observation"]["kind"] == "observation"
    assert observed["observation"]["payload"]["result_hash"] == result.result_hash


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
    monkeypatch.setattr(service, "_source_revision", lambda: "revision-1")
    monkeypatch.setattr(
        service,
        "execute_check_evidence",
        lambda *_args, **_kwargs: (plan, evidence, result),
    )

    replay = service.replay_research_check("check-1")

    assert replay["status"] == "matched"
    assert replay["matches"] is True
    assert replay["original_result_hash"] == replay["replayed_result_hash"]
