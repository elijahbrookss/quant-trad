from __future__ import annotations

from types import SimpleNamespace

import pytest

from market_data.frozen import semantic_hash

from portal.backend.service.research import authority, result_reference


def _completed_check(monkeypatch: pytest.MonkeyPatch) -> None:
    item = {
        "id": "check-1",
        "kind": "research_check",
        "status": "tested",
        "payload": {"schema_version": "research_check_payload.v2"},
    }
    definition = SimpleNamespace(
        definition_id="event_fact_analysis",
        definition_hash="definition-hash",
    )
    request = SimpleNamespace(request_hash="request-hash")
    plan = SimpleNamespace(plan_hash="plan-hash")
    evidence = SimpleNamespace(
        evidence_hash="evidence-hash",
        code_revision="revision-1",
        input_binding={
            "dataset_id": "mds_" + "a" * 32,
            "dataset_hash": "a" * 64,
            "binding_hash": "binding-hash",
        },
    )
    result = SimpleNamespace(
        result_hash="result-hash",
        result={
            "status": "completed",
            "analysis_status": "completed",
            "sample_count": 12,
            "analysis_sample_count": 10,
            "candidate_count": 13,
            "distinct_utc_days": 6,
            "statistics": {
                "model": {
                    "delta_log_loss": 0.04,
                    "delta_brier": 0.01,
                    "delta_roc_auc": 0.02,
                    "valid_fold_count": 3,
                    "oos_count": 8,
                }
            },
        },
    )
    monkeypatch.setattr(result_reference.research_repository, "get_item", lambda _id: item)
    monkeypatch.setattr(
        result_reference.research_service,
        "_validate_research_check_evidence_payload",
        lambda _payload: (definition, request, plan, evidence, result),
    )
    monkeypatch.setattr(
        result_reference.research_service,
        "_project_evidence_classification",
        lambda _item: {
            **item,
            "evidence_classification": "frozen_replayable",
            "replayable": True,
            "observation_eligible": True,
        },
    )
    monkeypatch.setattr(
        result_reference.research_service,
        "replay_research_check",
        lambda _id: {
            "status": "matched",
            "matches": True,
            "replayed_result_hash": "result-hash",
            "replayed_evidence_hash": "evidence-hash",
        },
    )


def _backtest_report(*, run_id: str, run_type: str = "backtest") -> dict:
    return {
        "schema_version": "run_research_dataset.v1",
        "metadata": {
            "run_id": run_id,
            "run_type": run_type,
            "status": "completed",
            "report_semantic_fingerprint": "semantic-hash",
            "strategy_id": "strategy-1",
            "strategy_hash": "strategy-hash",
            "dataset_id": "mds_" + "a" * 32,
            "dataset_hash": "a" * 64,
            "material_config_hash": "material-hash",
            "data_snapshot_hash": "snapshot-hash",
            "simulated_window": {
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-02-01T00:00:00Z",
            },
        },
        "readiness": {
            "safe_to_compare": True,
            "execution_quality_class": "X4",
        },
        "summary": {"closed_trades": 9, "net_pnl": 12.5},
        "portfolio_metrics": {"exposure_pct": 0.25},
    }


def _stored_run(*, run_id: str, run_type: str = "backtest") -> dict:
    return {
        "run_id": run_id,
        "run_type": run_type,
        "status": "completed",
        "runtime_source_revision": "revision-1",
        "runtime_contract_version": "runtime-contract.v1",
    }


def test_check_reference_rejects_caller_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        result_reference.research_repository,
        "get_item",
        lambda _id: (_ for _ in ()).throw(AssertionError("must fail before lookup")),
    )
    with pytest.raises(ValueError, match="projections are forbidden"):
        result_reference.resolve_canonical_result_reference(
            {
                "kind": "check",
                "item_id": "check-1",
                "result_hash": "result-hash",
                "evidence_hash": "evidence-hash",
                "evidence_projection": {"sample_count": "item.title"},
            }
        )


def test_blocked_check_cannot_become_scientific_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _completed_check(monkeypatch)
    monkeypatch.setattr(
        result_reference.research_service,
        "_project_evidence_classification",
        lambda item: {
            **item,
            "evidence_classification": "frozen_replayable",
            "replayable": True,
            "observation_eligible": False,
        },
    )
    with pytest.raises(ValueError, match="not completed canonical"):
        result_reference.resolve_canonical_result_reference(
            {
                "kind": "check",
                "item_id": "check-1",
                "result_hash": "result-hash",
                "evidence_hash": "evidence-hash",
            }
        )


def test_check_reference_uses_fixed_hashed_projection_and_attempt_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _completed_check(monkeypatch)
    dataset_binding = {
        "dataset_id": "mds_" + "a" * 32,
        "dataset_hash": "a" * 64,
    }
    evidence = result_reference.resolve_canonical_result_reference(
        {
            "kind": "check",
            "item_id": "check-1",
            "result_hash": "result-hash",
            "evidence_hash": "evidence-hash",
        },
        expected_dataset_binding=dataset_binding,
        expected_trial_inputs={"check_id": "check-1"},
        authority_binding={"kind": "scientific_attempt", "attempt_id": "attempt-1"},
    )

    assert evidence["sample_count"] == 12
    assert evidence["exposure"] == 0.0
    assert evidence["metric_results"]["delta_brier"] == 0.01
    canonical = evidence["canonical_result_reference"]
    assert canonical["dataset_id"] == dataset_binding["dataset_id"]
    assert canonical["authority_binding"]["attempt_id"] == "attempt-1"
    material = dict(canonical)
    reference_hash = material.pop("reference_hash")
    assert reference_hash == semantic_hash(material)
    assert evidence["typed_evidence_hash"]


def test_check_reference_rejects_attempt_dataset_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _completed_check(monkeypatch)
    with pytest.raises(ValueError, match="authority-assigned Dataset"):
        result_reference.resolve_canonical_result_reference(
            {
                "kind": "check",
                "item_id": "check-1",
                "result_hash": "result-hash",
                "evidence_hash": "evidence-hash",
            },
            expected_dataset_binding={
                "dataset_id": "mds_" + "b" * 32,
                "dataset_hash": "b" * 64,
            },
        )


def test_paper_run_cannot_be_represented_as_backtest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        result_reference.reports_contract,
        "get_run_research_dataset",
        lambda run_id: _backtest_report(run_id=run_id, run_type="paper"),
    )
    with pytest.raises(ValueError, match="not a Backtest"):
        result_reference.resolve_canonical_result_reference(
            {
                "kind": "backtest",
                "run_id": "run-1",
                "replay_run_id": "run-2",
                "report_semantic_fingerprint": "semantic-hash",
                "replay_report_semantic_fingerprint": "semantic-hash",
            }
        )


def test_backtest_reference_requires_distinct_matching_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        result_reference.reports_contract,
        "get_run_research_dataset",
        lambda run_id: _backtest_report(run_id=run_id),
    )
    monkeypatch.setattr(
        result_reference.runs_repo,
        "get_bot_run",
        lambda run_id: _stored_run(run_id=run_id),
    )
    monkeypatch.setattr(
        result_reference,
        "evidence_source_revision",
        lambda: "builder-revision",
    )
    evidence = result_reference.resolve_canonical_result_reference(
        {
            "kind": "backtest",
            "run_id": "run-1",
            "replay_run_id": "run-2",
            "report_semantic_fingerprint": "semantic-hash",
            "replay_report_semantic_fingerprint": "semantic-hash",
        },
        expected_dataset_binding={
            "dataset_id": "mds_" + "a" * 32,
            "dataset_hash": "a" * 64,
        },
        expected_trial_inputs={"strategy_id": "strategy-1"},
    )

    assert evidence["reproducible"] is True
    assert evidence["sample_count"] == 9
    assert evidence["trade_count"] == 9
    assert evidence["calendar_days"] == 31
    assert evidence["canonical_result_reference"]["replay_run_id"] == "run-2"


def test_completed_attempt_rejects_unattached_caller_evidence() -> None:
    with pytest.raises(ValueError, match="caller_supplied_forbidden"):
        authority.complete_attempt(
            {
                "actor_id": "runner-1",
                "actor_role": "experiment_runner",
                "request_id": "request-1",
                "attempt_id": "attempt-1",
                "status": "completed",
                "result_evidence": {
                    "artifact_hash": "caller-claim",
                    "reproducible": True,
                    "sample_count": 100,
                    "exposure": 1.0,
                },
            }
        )
