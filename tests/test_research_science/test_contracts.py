from __future__ import annotations

from copy import deepcopy

import pytest

from research_science import (
    CANDIDATE_SCHEMA_VERSION,
    PROTOCOL_SCHEMA_VERSION,
    SCIENTIFIC_EVIDENCE_SCHEMA_VERSION,
    BlindnessClass,
    CandidateSnapshot,
    DatasetAssignment,
    LeakagePolicy,
    ScientificEvidence,
    ScientificProtocol,
    SearchBudget,
    WalkForwardPlan,
    adjusted_p_values,
    classify_scientific_quality,
    deterministic_block_bootstrap_ci,
)


def _protocol() -> ScientificProtocol:
    leakage = LeakagePolicy(
        max_feature_lookback_bars=20,
        label_horizon_bars=5,
        max_holding_period_bars=12,
        order_expiry_bars=3,
    )
    return ScientificProtocol(
        schema_version=PROTOCOL_SCHEMA_VERSION,
        protocol_id="protocol-1",
        family_name="btc-momentum",
        economic_claim_intent="selection",
        datasets=(
            DatasetAssignment(
                dataset_id="dataset-train",
                dataset_hash="a" * 64,
                role="train",
                window_start="2020-01-01T00:00:00Z",
                window_end="2022-01-01T00:00:00Z",
            ),
            DatasetAssignment(
                dataset_id="dataset-validation",
                dataset_hash="b" * 64,
                role="validation",
                window_start="2022-01-01T00:00:00Z",
                window_end="2023-01-01T00:00:00Z",
            ),
            DatasetAssignment(
                dataset_id="dataset-holdout-secret",
                dataset_hash="c" * 64,
                role="holdout",
                window_start="2023-01-01T00:00:00Z",
                window_end="2024-01-01T00:00:00Z",
                blind_alias="final-window-a",
            ),
        ),
        blindness=BlindnessClass.PLATFORM_CONTROLLED_HISTORICAL,
        budget=SearchBudget(
            max_attempts=25,
            max_runtime_seconds=3600,
            max_compute_units=100,
            max_validation_feedback_uses=5,
        ),
        leakage=leakage,
        walk_forward=WalkForwardPlan.build(
            train_bars=100,
            validation_bars=25,
            step_bars=25,
            fold_count=3,
            leakage=leakage,
        ),
        instrument_ids=("BTC-USD",),
        allowed_mutation_dimensions=(
            "initial_graph",
            "expressions",
            "parameter_values",
        ),
        benchmark_ids=("buy-and-hold",),
        primary_metric="net_sharpe",
        secondary_metrics=("net_return",),
        safety_metrics=("max_drawdown",),
        alpha=0.05,
        minimum_sample_count=30,
        minimum_trade_count=20,
        minimum_calendar_days=180,
        minimum_exposure=1000,
        minimum_execution_quality_class="X2",
        execution_stress_ids=("cost-plus-25bps", "latency-plus-one-bar"),
        multiple_testing_method="holm",
        robustness_requirements=(
            "cost_stress",
            "latency_stress",
            "parameter_neighborhood",
            "subperiod",
        ),
        statistical_method_versions={
            "bootstrap": "moving-block-bootstrap.v1",
            "multiple_testing": "holm.v1",
            "walk_forward": "chronological-walk-forward.v1",
        },
        policy_versions={
            "dataset": "frozen-dataset-policy.v1",
            "execution": "execution-quality-policy.v1",
            "science": "scientific-quality-policy.v1",
            "strategy": "typed-strategy-policy.v1",
        },
        created_by="human:research-owner",
        authorized_by="human:research-owner",
        authorization_request_id="authorize-protocol-1",
    )


def test_protocol_is_hash_verified_and_redacts_workflow_sealed_holdout() -> None:
    protocol = _protocol()
    assert ScientificProtocol.from_dict(protocol.to_private_dict()) == protocol
    public = protocol.to_public_dict()
    holdout = next(row for row in public["datasets"] if row["role"] == "holdout")
    assert holdout["blind_alias"] == "final-window-a"
    assert holdout["dataset_id"] is None
    assert holdout["dataset_hash"] is None
    assert holdout["window_start"] is None
    assert public["blindness_claim"].startswith("controlled_workflow_non_exposure_only")

    tampered = deepcopy(protocol.to_private_dict())
    tampered["budget"]["max_attempts"] = 999
    with pytest.raises(ValueError, match="scientific_protocol_hash_mismatch"):
        ScientificProtocol.from_dict(tampered)


def test_leakage_gap_is_derived_and_walk_forward_folds_are_deterministic() -> None:
    protocol = _protocol()
    assert protocol.leakage.contamination_horizon_bars == 20
    assert [fold.validation_start_bar for fold in protocol.walk_forward.folds] == [120, 145, 170]
    assert [fold.embargo_end_bar for fold in protocol.walk_forward.folds] == [165, 190, 215]
    with pytest.raises(ValueError, match="derived contamination horizon"):
        LeakagePolicy(
            max_feature_lookback_bars=20,
            label_horizon_bars=5,
            max_holding_period_bars=12,
            order_expiry_bars=3,
            purge_bars=5,
        )


def test_candidate_freeze_hashes_every_material_strategy_input() -> None:
    candidate = CandidateSnapshot(
        schema_version=CANDIDATE_SCHEMA_VERSION,
        candidate_id="candidate-1",
        family_id="family-1",
        protocol_hash=_protocol().protocol_hash,
        source_attempt_id="attempt-7",
        strategy_artifact_hash="d" * 64,
        parameter_artifact_hash="e" * 64,
        execution_model_hash="2" * 64,
        metric_contract_hash="3" * 64,
        research_dataset_hashes=("a" * 64, "b" * 64),
        evidence_hashes=("f" * 64, "1" * 64),
        frozen_by="agent:researcher",
    )
    assert CandidateSnapshot.from_dict(candidate.to_dict()) == candidate
    tampered = deepcopy(candidate.to_dict())
    tampered["parameter_artifact_hash"] = "0" * 64
    with pytest.raises(ValueError, match="research_candidate_hash_mismatch"):
        CandidateSnapshot.from_dict(tampered)


def _evidence(**overrides: object) -> ScientificEvidence:
    payload = {
        "schema_version": SCIENTIFIC_EVIDENCE_SCHEMA_VERSION,
        "reproducible": True,
        "protocol_bound": True,
        "attempts_registered": 20,
        "attempts_accounted": 20,
        "budget_compliant": True,
        "benchmark_present": True,
        "walk_forward_complete": True,
        "leakage_controls_applied": True,
        "candidate_frozen_before_holdout": True,
        "holdout_used_once": True,
        "blindness": BlindnessClass.PLATFORM_CONTROLLED_HISTORICAL,
        "sample_count": 100,
        "trade_count": 80,
        "calendar_days": 365,
        "exposure": 5000,
        "minimum_sample_count": 30,
        "minimum_trade_count": 20,
        "minimum_calendar_days": 180,
        "minimum_exposure": 1000,
        "execution_quality_sufficient": True,
        "safety_metrics_passed": True,
        "raw_p_value": 0.001,
        "adjusted_p_value": 0.02,
        "alpha": 0.05,
        "confidence_interval_low": 0.1,
        "robustness_passed": (
            "cost_stress",
            "latency_stress",
            "parameter_neighborhood",
            "subperiod",
        ),
        "robustness_required": (
            "cost_stress",
            "latency_stress",
            "parameter_neighborhood",
            "subperiod",
        ),
        "cost_stress_passed": True,
        "latency_stress_passed": True,
        "failed_trials_retained": True,
        "family_closed_before_holdout": True,
    }
    payload.update(overrides)
    return ScientificEvidence(**payload)


def test_scientific_quality_advances_only_when_each_authority_layer_exists() -> None:
    assert classify_scientific_quality(_evidence()).scientific_quality_class.value == "S4"
    assert classify_scientific_quality(
        _evidence(confidence_interval_low=-0.01)
    ).scientific_quality_class.value == "S3"
    assert classify_scientific_quality(
        _evidence(holdout_used_once=False)
    ).scientific_quality_class.value == "S2"
    assert classify_scientific_quality(
        _evidence(leakage_controls_applied=False)
    ).scientific_quality_class.value == "S1"
    assert classify_scientific_quality(
        _evidence(attempts_registered=19, attempts_accounted=18)
    ).scientific_quality_class.value == "S0"


def test_multiple_testing_and_block_bootstrap_are_deterministic() -> None:
    assert adjusted_p_values((0.01, 0.03, 0.2), method="bonferroni") == pytest.approx(
        (0.03, 0.09, 0.6)
    )
    assert adjusted_p_values((0.01, 0.03, 0.2), method="holm") == pytest.approx(
        (0.03, 0.06, 0.2)
    )
    first = deterministic_block_bootstrap_ci(
        (0.1, 0.2, -0.1, 0.4, 0.3, 0.5),
        block_size=2,
        resamples=500,
        confidence=0.95,
        seed_material="protocol-hash|candidate-hash",
    )
    second = deterministic_block_bootstrap_ci(
        (0.1, 0.2, -0.1, 0.4, 0.3, 0.5),
        block_size=2,
        resamples=500,
        confidence=0.95,
        seed_material="protocol-hash|candidate-hash",
    )
    assert first == second
